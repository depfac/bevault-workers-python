import os
import json
from dotenv import load_dotenv
from .logging_config import configure_multiprocessing_logging, parse_log_level

load_dotenv()


def load_store_config():
    """Load store definitions from STORE_CONFIG, else config.json, else []."""
    raw = os.getenv("STORE_CONFIG")
    if raw:
        return json.loads(raw)
    path = "config.json"
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def get_stepfunctions_config():
    """Return Step Functions client credentials and endpoint settings from env vars."""
    return {
        "aws_access_key_id": os.getenv("stepFunctions__authenticationKey"),
        "aws_secret_access_key": os.getenv("stepFunctions__authenticationSecret"),
        "region_name": os.getenv("stepFunctions__awsRegion", "us-east-1"),
        "endpoint_url": os.getenv("stepFunctions__serviceUrl"),
    }


def get_worker_settings():
    """Return runtime worker settings parsed from env vars."""
    return {
        "heartbeat_delay": int(os.getenv("stepFunctions__DefaultHeartbeatDelay", "5")),
        "max_concurrency": int(os.getenv("stepFunctions__DefaultMaxConcurrency", "5")),
        "env_prefix": os.getenv("stepFunctions__EnvironmentName", ""),
        "role_arn": os.getenv("stepFunctions__roleArn"),
    }


def get_states_store_sync_config():
    """Return configuration used by the optional States store sync service."""
    service_url = os.getenv("stepFunctions__serviceUrl", "")
    return {
        "enabled": os.getenv("stepFunctions__enableStatesStoreSync", "false").lower()
        in ("1", "true", "yes", "on"),
        "environment_name": os.getenv("stepFunctions__EnvironmentName", ""),
        "base_url": os.getenv("stepFunctions__statesStoreBaseUrl", service_url).rstrip(
            "/"
        ),
        "poll_timeout_seconds": int(
            os.getenv("stepFunctions__statesPollTimeoutSeconds", "70")
        ),
        "status_heartbeat_seconds": int(
            os.getenv("stepFunctions__statesStatusHeartbeatSeconds", "60")
        ),
        "request_timeout_seconds": int(
            os.getenv("stepFunctions__statesRequestTimeoutSeconds", "15")
        ),
    }


def load_logging_config(config_path="logging_config.json"):
    """Load logging configuration from file and bootstrap multiprocessing logging."""
    if os.getenv("logging_config_path"):
        config_path = os.getenv("logging_config_path")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)

        # Extract logging configuration
        log_config = config.get("logging", {})
        minimum_level = log_config.get("minimumLevel", {})
        default_level = parse_log_level(minimum_level.get("default", "Information"))
        level_overrides = minimum_level.get("override", {})

        # Get file path from config
        file_config = next(
            (w for w in log_config.get("writeTo", []) if w.get("name") == "File"), {}
        )
        file_args = file_config.get("args", {})
        log_path = file_args.get("path", "/var/logs/bevault_workers/bevault_workers.log")
        max_file_size = file_args.get("fileSizeLimitBytes", 10000000)
        max_files = file_args.get("retainedFileCountLimit", 10)

        # Split path into directory and filename
        log_dir = os.path.dirname(log_path)
        log_file = os.path.basename(log_path)

        # Configure logging
        logger, log_queue, log_listener = configure_multiprocessing_logging(
            log_dir,
            log_file,
            max_file_size,
            max_files,
            default_level,
            level_overrides,
        )
        return logger, log_queue, log_listener
    else:
        # Use default configuration
        logger, log_queue, log_listener = configure_multiprocessing_logging(
            minimum_level=parse_log_level("Information")
        )
        return logger, log_queue, log_listener
