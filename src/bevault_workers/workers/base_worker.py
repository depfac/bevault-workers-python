from abc import ABC, abstractmethod
import logging
import threading


class BaseWorker(ABC):
    name: str  # Activity name for Step Function

    @abstractmethod
    def handle(self, input_data):
        pass

    def __init__(self):
        self.logger = None
        self.current_task_token = None
        self._canceled = False

    def get_logger(self):
        # Get the process-specific logger
        return logging.getLogger(f"worker.{self.name}")

    def set_task_token(self, task_token):
        """Set the current task token for tracking purposes"""
        self.current_task_token = task_token
        self._canceled = False

    def cancel_current_task(self):
        """Signal that the current task should be canceled"""
        self._canceled = True

    def is_canceled(self):
        """Check if the current task has been canceled"""
        return self._canceled
