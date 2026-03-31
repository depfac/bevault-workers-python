"""Unit tests for worker_manager error helpers and activity loop behavior."""

import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError

from bevault_workers.workers.base_worker import BaseWorker
from bevault_workers.workers.worker_manager import (
    WorkerActivityLoop,
    is_stepfunctions_network_recoverable,
    is_stepfunctions_task_abort_error,
)


def test_network_recoverable_read_timeout():
    assert is_stepfunctions_network_recoverable(
        ReadTimeoutError(endpoint_url="http://x/")
    )


def test_network_recoverable_legacy_message():
    err = Exception("Read timeout on endpoint URL: http://localhost/")
    assert is_stepfunctions_network_recoverable(err)


def test_network_recoverable_http504_message():
    assert is_stepfunctions_network_recoverable(Exception("HTTP504"))


def test_network_not_recoverable_generic():
    assert not is_stepfunctions_network_recoverable(ValueError("plain"))


def test_task_abort_client_error_codes():
    err = ClientError(
        {"Error": {"Code": "TaskTimedOut", "Message": "x"}}, "SendTaskHeartbeat"
    )
    assert is_stepfunctions_task_abort_error(err)

    err2 = ClientError(
        {"Error": {"Code": "TaskDoesNotExist", "Message": "x"}}, "SendTaskHeartbeat"
    )
    assert is_stepfunctions_task_abort_error(err2)


def test_task_abort_message_fallback():
    err = ClientError(
        {"Error": {"Code": "Other", "Message": "TaskToken is invalid"}}, "op"
    )
    assert is_stepfunctions_task_abort_error(err)


class _LoopTestWorker(BaseWorker):
    name = "_loop_test_worker"

    def handle(self, input_data):
        return {"status": "success", "result": input_data}


def _install_dummy_worker_module():
    mod_name = "bevault_workers_test_worker_loop_dummy"
    mod = types.ModuleType(mod_name)
    mod.LoopWorker = _LoopTestWorker
    sys.modules[mod_name] = mod
    return mod_name


def test_activity_loop_poll_error_without_token_skips_send_task_failure():
    mod_name = _install_dummy_worker_module()
    send_failure_calls = []

    class FakeSfn:
        def get_activity_task(self, **kwargs):
            raise RuntimeError("poll failed before token")

        def send_task_failure(self, **kwargs):
            send_failure_calls.append(kwargs)

        def send_task_success(self, **kwargs):
            pytest.fail("send_task_success should not be called")

        def send_task_heartbeat(self, **kwargs):
            pytest.fail("heartbeat should not be called")

    def factory():
        return FakeSfn()

    stub_logger = MagicMock()
    with (
        patch(
            "bevault_workers.workers.worker_manager.configure_process_logging",
            return_value=stub_logger,
        ),
        patch(
            "bevault_workers.workers.worker_manager.time.sleep",
            side_effect=KeyboardInterrupt,
        ),
    ):
        loop = WorkerActivityLoop(
            log_queue=MagicMock(),
            worker_class_name="LoopWorker",
            worker_module=mod_name,
            activity_arn="arn:aws:states:eu-west-1:123:activity:test",
            heartbeat_delay=0.01,
            create_stepfunctions_client=factory,
        )
        with pytest.raises(KeyboardInterrupt):
            loop.run()

    assert send_failure_calls == []
    del sys.modules[mod_name]
