import hashlib
import json
import logging
import threading
import time
from typing import Dict, List, Tuple

from ..store_registry import StoreRegistry
from ...utils.config_loader import load_store_config
from .api_client import StatesStoreApiClient
from .status_tracker import SOURCE_LOCAL, SOURCE_STATES, StoreStatusTracker

logger = logging.getLogger(__name__)


def _is_transient_longpoll_error(exc: Exception) -> bool:
    msg = str(exc)
    if "Read timeout on endpoint URL" in msg:
        return True
    if "HTTP504" in msg or "504 Gateway Time-out" in msg:
        return True
    return False


class StatesStoreSyncService:
    def __init__(
        self,
        client: StatesStoreApiClient,
        environment_name: str,
        heartbeat_seconds: int = 60,
        on_registry_updated=None,
    ):
        self.client = client
        self.environment_name = environment_name
        self.heartbeat_seconds = heartbeat_seconds
        self.on_registry_updated = on_registry_updated
        self.stop_event = threading.Event()
        self.status_tracker = StoreStatusTracker()
        self._threads: List[threading.Thread] = []
        self._states_stores_version_token = None
        self._force_check_continuation_token = None
        self._local_store_health_due: Dict[Tuple[str, str], float] = {}
        self._states_store_health_due: Dict[Tuple[str, str], float] = {}
        self._last_registry_fingerprint = None

    @staticmethod
    def _normalize_states_config(config_value, store_name: str):
        if isinstance(config_value, dict):
            return config_value
        if isinstance(config_value, str):
            try:
                parsed = json.loads(config_value)
            except json.JSONDecodeError:
                logger.warning(
                    "States store '%s' config is not valid JSON string; using empty config.",
                    store_name,
                )
                return {}
            if isinstance(parsed, dict):
                return parsed
            logger.warning(
                "States store '%s' config JSON is not an object; using empty config.",
                store_name,
            )
            return {}
        if config_value is None:
            return {}
        logger.warning(
            "States store '%s' config has unsupported type '%s'; using empty config.",
            store_name,
            type(config_value).__name__,
        )
        return {}

    @staticmethod
    def _registry_fingerprint(definitions: List[dict], metadata: Dict[str, dict]) -> str:
        encoded = json.dumps(
            {"definitions": definitions, "metadata": metadata},
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _local_store_version_token(local_stores: List[dict]) -> str:
        encoded = json.dumps(local_stores, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _local_stores_payload(configs: List[dict]) -> List[dict]:
        payload = []
        for store_def in configs:
            payload.append(
                {
                    "name": store_def["Name"],
                    "type": store_def["Type"],
                    "enableHealthCheck": store_def.get("EnableHealthCheck", False),
                    "healthCheckDelaySeconds": store_def.get("HealthCheckDelaySeconds"),
                }
            )
        return payload

    @staticmethod
    def _registry_name(name: str, source: str, has_collision: bool) -> str:
        if source == SOURCE_STATES and has_collision:
            return f"states::{name}"
        return name

    def _merge_store_definitions(self, local_defs: List[dict], states_defs: List[dict]) -> List[dict]:
        merged = []
        local_names = {s["Name"] for s in local_defs}
        registry_metadata = {}

        for local in local_defs:
            merged.append(dict(local))
            registry_metadata[local["Name"]] = {
                "source": SOURCE_LOCAL,
                "display_name": local["Name"],
                "health_check_enabled": local.get("EnableHealthCheck", False),
                "health_check_delay_seconds": local.get("HealthCheckDelaySeconds"),
            }
            self.status_tracker.mark(SOURCE_LOCAL, local["Name"], "Success")

        for states_store in states_defs:
            store_name = states_store["name"]
            has_collision = store_name in local_names
            registry_name = self._registry_name(store_name, SOURCE_STATES, has_collision)
            normalized_config = self._normalize_states_config(
                states_store.get("config", {}), store_name
            )
            if has_collision:
                logger.warning(
                    "Store name collision for '%s'. States store is registered as '%s'.",
                    store_name,
                    registry_name,
                )

            merged.append(
                {
                    "Name": registry_name,
                    "Type": states_store["type"],
                    "Config": normalized_config,
                }
            )
            registry_metadata[registry_name] = {
                "source": SOURCE_STATES,
                "display_name": store_name,
                "health_check_enabled": states_store.get("enableHealthCheck", False),
                "health_check_delay_seconds": states_store.get("healthCheckDelaySeconds"),
            }
            self.status_tracker.mark(SOURCE_STATES, store_name, "Success")

        StoreRegistry.replace_from_definitions_best_effort(merged, registry_metadata)
        fingerprint = self._registry_fingerprint(merged, registry_metadata)
        if self.on_registry_updated and fingerprint != self._last_registry_fingerprint:
            self.on_registry_updated(merged, registry_metadata)
            self._last_registry_fingerprint = fingerprint
        return merged

    def _run_health_checks(self):
        now = time.time()
        metadata = StoreRegistry.snapshot_metadata()
        for registry_name, item in metadata.items():
            source = item.get("source", SOURCE_LOCAL)
            display_name = item.get("display_name", registry_name)
            enabled = item.get("health_check_enabled", False)
            interval = item.get("health_check_delay_seconds") or self.heartbeat_seconds
            if not enabled:
                continue

            key = (source, display_name)
            schedule = (
                self._states_store_health_due
                if source == SOURCE_STATES
                else self._local_store_health_due
            )
            if now < schedule.get(key, 0):
                continue
            schedule[key] = now + max(int(interval), 1)

            try:
                store = StoreRegistry.get(registry_name)
                if hasattr(store, "connect"):
                    store.connect()
                self.status_tracker.mark(source, display_name, "Success")
            except Exception as exc:
                self.status_tracker.mark(source, display_name, "Error", str(exc))

    def _sync_loop(self):
        while not self.stop_event.is_set():
            try:
                logger.debug("States store sync started")
                local_defs = load_store_config()
                local_payload = self._local_stores_payload(local_defs)
                request_payload = {
                    "workerServiceEnvironmentName": self.environment_name,
                    "statesStoresVersionToken": self._states_stores_version_token,
                    "localStoresVersionToken": self._local_store_version_token(local_payload),
                    "localStores": local_payload,
                }
                response = self.client.sync_stores(request_payload)
                token = response.get("statesStoresVersionToken")
                states_stores = response.get("statesStores")
                if token:
                    self._states_stores_version_token = token
                if isinstance(states_stores, list):
                    self._merge_store_definitions(local_defs, states_stores)
            except Exception as exc:
                if _is_transient_longpoll_error(exc):
                    logger.debug("States store sync long-poll timed out, retrying...")
                else:
                    logger.error("States store sync loop error: %s", exc)
                self.stop_event.wait(2)

    def _force_check_loop(self):
        while not self.stop_event.is_set():
            try:
                payload = {
                    "continuationToken": self._force_check_continuation_token,
                    "environmentName": self.environment_name,
                }
                response = self.client.get_force_check_requests(payload)
                if not response:
                    continue

                self._force_check_continuation_token = response.get("continuationToken")
                for req in response.get("statesStoreUpdateRequests", []):
                    self.status_tracker.mark(SOURCE_STATES, req["storeName"], "Success")
                for req in response.get("workerStoreUpdateRequests", []):
                    self.status_tracker.mark(SOURCE_LOCAL, req["storeName"], "Success")

                self.client.post_store_status(
                    self.status_tracker.snapshot_payload(self.environment_name)
                )
            except Exception as exc:
                if _is_transient_longpoll_error(exc):
                    logger.warning(
                        "States force-check long-poll timed out, retrying..."
                    )
                else:
                    logger.error("States force-check loop error: %s", exc)
                self.stop_event.wait(2)

    def _status_loop(self):
        while not self.stop_event.is_set():
            try:
                self._run_health_checks()
                payload = self.status_tracker.snapshot_payload(self.environment_name)
                self.client.post_store_status(payload)
            except Exception as exc:
                logger.error("States status loop error: %s", exc)
            self.stop_event.wait(self.heartbeat_seconds)

    def start(self):
        self._threads = [
            threading.Thread(target=self._sync_loop, name="states-store-sync", daemon=True),
            threading.Thread(
                target=self._force_check_loop, name="states-store-force-check", daemon=True
            ),
            threading.Thread(target=self._status_loop, name="states-store-status", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def stop(self):
        self.stop_event.set()
        for thread in self._threads:
            thread.join(timeout=2)
