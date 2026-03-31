import sys
import logging
from pathlib import Path

_root = Path(__file__).resolve().parent
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root / "src"))

from bevault_workers.workers.worker_manager import WorkerManager

logger = logging.getLogger(__name__)


def main():
    """Run the worker application entrypoint."""
    try:
        # Initialize the worker manager with configuration
        manager = WorkerManager(
            config_path="config.json",
            workers_module="dev_workers",
        )

        # Start the worker manager (this will block until stopped)
        manager.start()

    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.exception("Application error: %s", e)
        raise


if __name__ == "__main__":
    main()
