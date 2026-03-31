import importlib
import inspect
import json
import logging
import multiprocessing
import os
import pkgutil
import signal
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, ReadTimeoutError

from ..stores.aws import (
    StatesStoreApiClient,
    StatesStoreSyncService,
    attach_dfakto_states_extensions,
)
from ..stores.store_registry import StoreRegistry
from ..utils.config_loader import (
    get_states_store_sync_config,
    get_stepfunctions_config,
    get_worker_settings,
    load_logging_config,
)
from ..utils.logging_config import configure_process_logging
from .base_worker import BaseWorker

logger = logging.getLogger(__name__)

# Step Functions error codes where the activity task is gone or the token is invalid
_STEP_FUNCTIONS_TASK_ABORT_CODES = frozenset({"TaskTimedOut", "TaskDoesNotExist"})

CreateStepFunctionsClient = Optional[Callable[..., Any]]


def _client_error_code(exc: BaseException) -> Optional[str]:
    if isinstance(exc, ClientError):
        return (exc.response.get("Error") or {}).get("Code")
    return None


def is_stepfunctions_network_recoverable(exc: BaseException) -> bool:
    """Return True when recreating the boto client may help (read timeout, transient HTTP)."""
    if isinstance(exc, ReadTimeoutError):
        return True
    cur: Optional[BaseException] = exc
    seen = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ReadTimeoutError):
            return True
        cur = cur.__cause__ or cur.__context__
    msg = str(exc)
    low = msg.lower()
    # Legacy / botocore edge cases where the tree is not always ReadTimeoutError
    if "timeout on endpoint url" in low or "read timed out" in low:
        return True
    if "504 gateway time-out" in low or "http504" in low:
        return True
    return False


def is_stepfunctions_task_abort_error(exc: BaseException) -> bool:
    """Return True when the activity task should be treated as canceled / invalid."""
    code = _client_error_code(exc)
    if code and code in _STEP_FUNCTIONS_TASK_ABORT_CODES:
        return True
    if isinstance(exc, ClientError):
        text = str(exc)
        if any(
            token in text
            for token in (
                "TaskToken is invalid",
                "TaskDoesNotExist",
                "TaskTimedOut",
            )
        ):
            return True
    return False


class WorkerManager:
    def __init__(self, config_path: str = None, workers_module: str = None):
        self.config_path = config_path
        self.workers_module = workers_module or "workers"
        self.workers = []
        self._process_records = []  # List of (process, worker_class, activity_arn)
        self._log_queue = None
        self.log_listener = None
        self.running = False
        self._stop_event = multiprocessing.Event()
        self._main_thread = None
        self._states_store_sync_service = None
        self._shared_store_manager = None
        self._shared_store_definitions = None
        self._shared_store_metadata = None
        self._shared_store_lock = None

    def _publish_shared_store_state(self, definitions, metadata):
        """Publish merged store definitions to shared state (single writer)."""
        if (
            self._shared_store_definitions is None
            or self._shared_store_metadata is None
            or self._shared_store_lock is None
        ):
            return
        with self._shared_store_lock:
            self._shared_store_definitions[:] = [dict(item) for item in definitions]
            self._shared_store_metadata.clear()
            for key, value in metadata.items():
                self._shared_store_metadata[key] = dict(value)

    def _discover_workers(self) -> List[type]:
        """Discover workers from the specified module"""
        worker_classes = []
        workers_module = self.workers_module
        if workers_module is None:
            return worker_classes

        try:
            if isinstance(workers_module, str):
                module = importlib.import_module(workers_module)
            else:
                module = workers_module

            if hasattr(module, "__path__"):
                package_path = module.__path__
            else:
                package_path = [os.path.dirname(module.__file__)]

            for _, module_name, _ in pkgutil.iter_modules(package_path):
                try:
                    full_module_name = f"{workers_module}.{module_name}"
                    module = importlib.import_module(full_module_name)

                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if (
                            issubclass(obj, BaseWorker)
                            and obj is not BaseWorker
                            and obj.__module__ == full_module_name
                        ):
                            worker_classes.append(obj)

                except ImportError as e:
                    logger.warning("Could not import %s: %s", full_module_name, e)
                    continue

        except ImportError as e:
            logger.error("Could not import workers module '%s': %s", workers_module, e)
            return []

        return worker_classes

    def __register_activities(self, worker_names: List[str]) -> Dict[str, str]:
        sfn = WorkerManager.create_stepfunctions_client()
        settings = get_worker_settings()
        prefix = settings["env_prefix"]

        activity_arns = {}

        for worker_name in worker_names:
            activity_name = f"{prefix}_{worker_name}" if prefix else worker_name
            response = sfn.create_activity(name=activity_name)
            activity_arns[worker_name] = response["activityArn"]

        return activity_arns

    def _spawn_worker_process(self, worker_class, activity_arn):
        """Spawn a single worker process. Returns (process, worker_class, activity_arn)."""
        settings = get_worker_settings()
        process = multiprocessing.Process(
            target=worker_loop,
            args=(
                self._log_queue,
                worker_class.__name__,
                worker_class.__module__,
                activity_arn,
                settings["heartbeat_delay"],
                self._shared_store_definitions,
                self._shared_store_metadata,
                self._shared_store_lock,
                None,
            ),
            daemon=True,
        )
        process.start()
        return process, worker_class, activity_arn

    @staticmethod
    def create_stepfunctions_client(endpoint_url_override: str = None):
        config = get_stepfunctions_config()
        return boto3.client(
            "stepfunctions",
            endpoint_url=endpoint_url_override or config["endpoint_url"],
            region_name=config["region_name"],
            aws_access_key_id=config["aws_access_key_id"],
            aws_secret_access_key=config["aws_secret_access_key"],
            config=boto3.session.Config(
                connect_timeout=5,
                read_timeout=60,
                retries={"max_attempts": 3},
            ),
        )

    def start(self):
        """Start the worker manager"""
        if self.running:
            raise RuntimeError("Worker manager is already running")

        self.running = True
        self._stop_event.clear()
        self._main_thread = threading.current_thread()

        mgr_logger, log_queue, log_listener = load_logging_config()
        self._log_queue = log_queue
        self.log_listener = log_listener

        mgr_logger.info("Loading stores...")
        StoreRegistry.load()
        initial_definitions, initial_metadata = StoreRegistry.export_snapshot()
        self._shared_store_manager = multiprocessing.Manager()
        self._shared_store_definitions = self._shared_store_manager.list()
        self._shared_store_metadata = self._shared_store_manager.dict()
        self._shared_store_lock = self._shared_store_manager.Lock()
        self._publish_shared_store_state(initial_definitions, initial_metadata)
        StoreRegistry.configure_shared_state(
            self._shared_store_definitions,
            self._shared_store_metadata,
            self._shared_store_lock,
        )

        sync_config = get_states_store_sync_config()
        if sync_config["enabled"]:
            mgr_logger.info("Starting States store sync service...")
            sync_sfn_client = WorkerManager.create_stepfunctions_client(
                endpoint_url_override=sync_config["base_url"] or None
            )
            attach_dfakto_states_extensions(sync_sfn_client)
            client = StatesStoreApiClient(
                stepfunctions_client=sync_sfn_client,
                poll_timeout_seconds=sync_config["poll_timeout_seconds"],
                request_timeout_seconds=sync_config["request_timeout_seconds"],
            )
            self._states_store_sync_service = StatesStoreSyncService(
                client=client,
                environment_name=sync_config["environment_name"],
                heartbeat_seconds=sync_config["status_heartbeat_seconds"],
                on_registry_updated=self._publish_shared_store_state,
            )
            self._states_store_sync_service.start()

        mgr_logger.info("Discovering worker classes...")
        worker_classes = self._discover_workers()
        mgr_logger.info("Found worker classes: %s", [w.name for w in worker_classes])

        mgr_logger.info("Registering activities...")
        activity_arns = self.__register_activities([cls.name for cls in worker_classes])

        mgr_logger.info("Launching worker threads...")
        settings = get_worker_settings()

        for worker_class in worker_classes:
            worker_name = worker_class.name
            for _i in range(settings["max_concurrency"]):
                record = self._spawn_worker_process(
                    worker_class, activity_arns[worker_name]
                )
                self._process_records.append(record)

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        mgr_logger.info("Workers running. Press Ctrl+C to stop.")

        try:
            while self.running and not self._stop_event.is_set():
                dead_records = [
                    (p, wc, arn)
                    for p, wc, arn in self._process_records
                    if not p.is_alive()
                ]
                if dead_records:
                    mgr_logger.warning(
                        "Found %d dead processes, restarting...", len(dead_records)
                    )
                    for process, worker_class, activity_arn in dead_records:
                        process.join(timeout=1)
                        self._process_records.remove(
                            (process, worker_class, activity_arn)
                        )
                        new_record = self._spawn_worker_process(
                            worker_class, activity_arn
                        )
                        self._process_records.append(new_record)

                time.sleep(10)
        except KeyboardInterrupt:
            mgr_logger.info("Received keyboard interrupt")
        finally:
            self.stop()

    def stop(self, timeout: int = 30):
        """
        Gracefully stop the worker manager and all worker processes.

        Args:
            timeout (int): Maximum time to wait for processes to terminate gracefully
        """
        if not self.running:
            return
        stop_logger = logging.getLogger(__name__)
        stop_logger.info("Stopping worker manager...")
        self.running = False
        self._stop_event.set()

        processes = [r[0] for r in self._process_records]
        if not processes:
            stop_logger.info("No worker processes to stop")
            self._process_records.clear()
            self._cleanup_resources()
            return

        stop_logger.info(
            "Sending termination signal to %d worker processes...", len(processes)
        )
        time.sleep(2)

        alive_processes = [p for p in processes if p.is_alive()]
        if alive_processes:
            stop_logger.info("Sending SIGTERM to %d processes...", len(alive_processes))
            for process in alive_processes:
                try:
                    if process.is_alive():
                        process.terminate()
                except Exception as e:
                    stop_logger.warning(
                        "Error terminating process %s: %s", process.pid, e
                    )

        stop_logger.info(
            "Waiting up to %d seconds for processes to terminate gracefully...",
            timeout,
        )
        start_time = time.time()

        for process in processes:
            remaining_time = max(0, timeout - (time.time() - start_time))
            if remaining_time > 0 and process.is_alive():
                try:
                    process.join(timeout=remaining_time)
                    if process.is_alive():
                        stop_logger.warning(
                            "Process %s did not terminate within timeout", process.pid
                        )
                    else:
                        stop_logger.debug(
                            "Process %s terminated gracefully", process.pid
                        )
                except Exception as e:
                    stop_logger.warning("Error joining process %s: %s", process.pid, e)

        still_alive = [p for p in processes if p.is_alive()]
        if still_alive:
            stop_logger.warning(
                "Force killing %d processes that didn't terminate gracefully...",
                len(still_alive),
            )
            for process in still_alive:
                try:
                    if process.is_alive():
                        stop_logger.warning("Force killing process %s", process.pid)
                        process.kill()
                        process.join(timeout=5)
                        if process.is_alive():
                            stop_logger.error("Failed to kill process %s", process.pid)
                        else:
                            stop_logger.info(
                                "Process %s force killed successfully", process.pid
                            )
                except Exception as e:
                    stop_logger.error(
                        "Error force killing process %s: %s", process.pid, e
                    )

        self._process_records.clear()
        self._cleanup_resources()
        stop_logger.info("Worker manager stopped successfully")

    def _cleanup_resources(self):
        """Clean up logging and store resources"""
        cleanup_logger = logging.getLogger(__name__)

        if self._states_store_sync_service:
            try:
                cleanup_logger.info("Stopping States store sync service...")
                self._states_store_sync_service.stop()
                self._states_store_sync_service = None
            except Exception as e:
                cleanup_logger.error("Error stopping States store sync service: %s", e)

        if self.log_listener:
            try:
                cleanup_logger.info("Stopping log listener...")
                self.log_listener.stop()
                self.log_listener = None
            except Exception as e:
                cleanup_logger.error("Error stopping log listener: %s", e)

        if self._shared_store_manager:
            try:
                self._shared_store_manager.shutdown()
            except Exception as e:
                cleanup_logger.error("Error stopping shared store manager: %s", e)
            finally:
                self._shared_store_manager = None
                self._shared_store_definitions = None
                self._shared_store_metadata = None
                self._shared_store_lock = None

        cleanup_fn = getattr(StoreRegistry, "cleanup", None)
        if callable(cleanup_fn):
            try:
                cleanup_fn()
            except Exception as e:
                cleanup_logger.error("Error cleaning up store registry: %s", e)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        handler_logger = logging.getLogger(__name__)
        handler_logger.info(
            "Received signal %s, initiating graceful shutdown...", signum
        )
        self._stop_event.set()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup"""
        self.stop()


class WorkerActivityLoop:
    """Poll Step Functions for activity tasks and run a worker in a child process."""

    def __init__(
        self,
        log_queue,
        worker_class_name: str,
        worker_module: str,
        activity_arn: str,
        heartbeat_delay: float,
        shared_store_definitions=None,
        shared_store_metadata=None,
        shared_store_lock=None,
        create_stepfunctions_client: CreateStepFunctionsClient = None,
    ):
        self._log_queue = log_queue
        self._worker_class_name = worker_class_name
        self._worker_module = worker_module
        self._activity_arn = activity_arn
        self._heartbeat_delay = heartbeat_delay
        self._shared_store_definitions = shared_store_definitions
        self._shared_store_metadata = shared_store_metadata
        self._shared_store_lock = shared_store_lock
        self._create_client = (
            create_stepfunctions_client or WorkerManager.create_stepfunctions_client
        )

    def run(self) -> None:
        module = importlib.import_module(self._worker_module)
        worker_class = getattr(module, self._worker_class_name)
        if (
            self._shared_store_definitions is not None
            and self._shared_store_metadata is not None
            and self._shared_store_lock is not None
        ):
            StoreRegistry.configure_shared_state(
                self._shared_store_definitions,
                self._shared_store_metadata,
                self._shared_store_lock,
            )
        worker_instance = worker_class()
        proc_logger = configure_process_logging(self._log_queue, worker_instance.name)
        name = worker_instance.name

        sfn = self._create_client()
        last_client_refresh = time.time()
        client_refresh_interval = 3600

        while True:
            task_token = None
            try:
                current_time = time.time()
                if current_time - last_client_refresh > client_refresh_interval:
                    proc_logger.info(
                        "Performing scheduled client refresh for worker %s", name
                    )
                    sfn = self._create_client()
                    last_client_refresh = current_time

                task = sfn.get_activity_task(
                    activityArn=self._activity_arn, workerName=f"worker-{name}"
                )
                task_token = task.get("taskToken")
                if not task_token:
                    time.sleep(1)
                    continue

                worker_instance.set_task_token(task_token)

                stop_heartbeat = threading.Event()

                def heartbeat_thread():
                    nonlocal sfn
                    heartbeat_sfn = sfn
                    while not stop_heartbeat.is_set():
                        try:
                            if task_token:
                                heartbeat_sfn.send_task_heartbeat(taskToken=task_token)
                        except Exception as hb_exc:
                            if is_stepfunctions_network_recoverable(hb_exc):
                                heartbeat_sfn = self._create_client()
                                sfn = heartbeat_sfn
                            elif is_stepfunctions_task_abort_error(hb_exc):
                                proc_logger.info(
                                    "Task was aborted or timed out for worker %s: %s",
                                    name,
                                    hb_exc,
                                )
                                worker_instance.cancel_current_task()
                                break
                            else:
                                proc_logger.error(
                                    "Heartbeat error for %s: %s", name, hb_exc
                                )
                        time.sleep(self._heartbeat_delay)

                heartbeat = threading.Thread(target=heartbeat_thread, daemon=True)
                heartbeat.start()

                input_data = json.loads(task["input"])
                proc_logger.info("Executing worker %s...", name)
                result = worker_instance.handle(input_data)

                stop_heartbeat.set()
                heartbeat.join(timeout=1.0)

                if result["status"] == "error":
                    proc_logger.error(
                        "Error while executing worker %s: %s",
                        name,
                        result["error_message"],
                    )
                    try:
                        sfn.send_task_failure(
                            taskToken=task_token,
                            error="WorkerError",
                            cause=result["error_message"],
                        )
                    except Exception as send_exc:
                        proc_logger.error("Failed to send task failure: %s", send_exc)
                elif result["status"] == "canceled":
                    proc_logger.info(
                        "Worker %s was canceled during execution: %s",
                        name,
                        result.get("message", ""),
                    )
                else:
                    try:
                        sfn.send_task_success(
                            taskToken=task_token, output=json.dumps(result)
                        )
                    except Exception as send_exc:
                        proc_logger.error("Failed to send task success: %s", send_exc)

            except Exception as e:
                if is_stepfunctions_network_recoverable(e):
                    sfn = self._create_client()
                    last_client_refresh = time.time()
                else:
                    proc_logger.error("[ERROR] %s: %s", name, e)
                    if task_token:
                        try:
                            sfn.send_task_failure(
                                taskToken=task_token,
                                error="WorkerError",
                                cause=str(e),
                            )
                        except Exception as send_exc:
                            proc_logger.error(
                                "Failed to send task failure: %s", send_exc
                            )
                time.sleep(self._heartbeat_delay)


def worker_loop(
    log_queue,
    worker_class_name,
    worker_module,
    activity_arn,
    heartbeat_delay,
    shared_store_definitions=None,
    shared_store_metadata=None,
    shared_store_lock=None,
    create_stepfunctions_client: CreateStepFunctionsClient = None,
):
    """Multiprocessing entrypoint: run the activity loop for one worker."""
    WorkerActivityLoop(
        log_queue,
        worker_class_name,
        worker_module,
        activity_arn,
        heartbeat_delay,
        shared_store_definitions,
        shared_store_metadata,
        shared_store_lock,
        create_stepfunctions_client,
    ).run()
