# src/logging_config.py
import multiprocessing
import os
import logging
import json
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler


def parse_log_level(level_name, fallback=logging.INFO):
    """
    Parse Serilog-like level names into Python logging levels.
    """
    if isinstance(level_name, int):
        return level_name
    if not isinstance(level_name, str):
        return fallback

    normalized = level_name.strip().lower()
    level_map = {
        "trace": logging.DEBUG,
        "debug": logging.DEBUG,
        "information": logging.INFO,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "warn": logging.WARNING,
        "error": logging.ERROR,
        "fatal": logging.CRITICAL,
        "critical": logging.CRITICAL,
    }
    return level_map.get(normalized, fallback)


def apply_logger_overrides(level_overrides):
    """
    Apply per-logger levels from config (e.g. urllib3 -> Warning).
    """
    if not isinstance(level_overrides, dict):
        return
    for logger_name, level_name in level_overrides.items():
        logging.getLogger(logger_name).setLevel(parse_log_level(level_name))


class StandardConsoleFormatter(logging.Formatter):
    """
    Custom formatter for console output similar to Serilog format
    """

    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt, datefmt)

    def formatException(self, exc_info):
        """Format exception information as text."""
        result = super().formatException(exc_info)
        return result

    def format(self, record):
        """Format log record as text."""
        # Store the original format
        original_fmt = self._style._fmt

        # If there's an exception, include it
        if record.exc_info:
            record.exc_text = self.formatException(record.exc_info)
        else:
            record.exc_text = ""

        # Format the record
        result = super().format(record)

        # Restore the original format
        self._style._fmt = original_fmt

        return result


class JsonFormatter(logging.Formatter):
    """
    Custom JSON formatter
    """

    def __init__(self, format_str=None, rename_fields=None):
        super().__init__()
        self.format_str = format_str
        self.rename_fields = rename_fields or {}

    def format(self, record):
        log_record = {}

        # Add basic fields
        log_record["timestamp"] = self.formatTime(record)
        log_record["level"] = record.levelname
        log_record["message"] = record.getMessage()
        log_record["SourceContext"] = record.name
        # Add process and thread info for multiprocessing debugging
        log_record["process"] = record.process
        log_record["processName"] = record.processName

        # Add exception info if present
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)

        # Apply field renaming
        for orig, new in self.rename_fields.items():
            if hasattr(record, orig):
                log_record[new] = getattr(record, orig)

        return json.dumps(log_record)


def create_handlers(
    log_dir="/var/logs/bevault_workers",
    log_file="bevault_workers.log",
    max_file_size=10000000,
    max_files=10,
    minimum_level=logging.INFO,
):
    """
    Create and return console and file handlers with proper formatting
    """
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    # Configure console handler with standard output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(minimum_level)
    console_format = "[%(asctime)s %(processName)s %(levelname)s] %(message)s <s:%(name)s>%(exc_text)s"
    console_formatter = StandardConsoleFormatter(
        console_format,
        datefmt="%Y-%m-%d %H:%M:%S.%f"[:-3],  # Truncate microseconds to milliseconds
    )
    console_handler.setFormatter(console_formatter)

    # Configure file handler with JSON output and rotation
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_file_size,  # 10MB
        backupCount=max_files,
        delay=True,  # Don't create file until first log
    )
    file_handler.setLevel(minimum_level)

    # Create JSON formatter
    json_formatter = JsonFormatter(
        format_str="%(timestamp)s %(level)s %(message)s %(name)s %(exc_info)s",
        rename_fields={
            "levelname": "level",
            "name": "SourceContext",
            "asctime": "timestamp",
        },
    )
    file_handler.setFormatter(json_formatter)

    return console_handler, file_handler


def configure_multiprocessing_logging(
    log_dir="/var/logs/bevault_workers",
    log_file="bevault_workers.log",
    max_file_size=10000000,
    max_files=10,
    minimum_level=logging.INFO,
    level_overrides=None,
):
    """
    Configure logging for multiprocessing setup
    Returns the logger, log queue, and queue listener
    """
    # Create handlers
    console_handler, file_handler = create_handlers(
        log_dir, log_file, max_file_size, max_files, minimum_level
    )

    # Configure root logger for parent process
    logger = logging.getLogger()
    logger.setLevel(minimum_level)

    # Remove existing handlers if any
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Add handlers directly to the parent process logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    apply_logger_overrides(level_overrides)

    # Create a queue for child processes
    log_queue = multiprocessing.Queue(-1)

    # Create a queue listener for child process logs
    queue_listener = QueueListener(
        log_queue, console_handler, file_handler, respect_handler_level=True
    )

    # Start the queue listener in a background thread
    queue_listener.start()

    return logger, log_queue, queue_listener


def configure_process_logging(log_queue, name=None):
    """
    Configure logging for a child process
    """
    # Create a logger for this process
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Add a queue handler that sends to the main process
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    # Log a startup message
    process_name = multiprocessing.current_process().name
    if name:
        logger.info(f"Process {process_name} ({name}) started")
    else:
        logger.info(f"Process {process_name} started")

    return logger
