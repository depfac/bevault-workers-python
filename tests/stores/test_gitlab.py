"""Unit tests for GitLab FileStore with mocked requests."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from bevault_workers.stores.gitlab import Store

TOKEN = "gitlab://myStore/main/docs/README.md"


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_open_read(mock_session_cls, gitlab_config):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_response = MagicMock()
    mock_response.content = b"hello"
    mock_response.raise_for_status = MagicMock()
    mock_session.get.return_value = mock_response

    store = Store(gitlab_config)
    result = store.openRead(TOKEN)

    assert result == b"hello"
    mock_session.get.assert_called_once()
    call_kw = mock_session.get.call_args
    assert "docs%2FREADME.md" in call_kw[0][0]
    assert call_kw[1]["params"] == {"ref": "main"}


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_get_file_name(mock_session_cls, gitlab_config):
    mock_session_cls.return_value = MagicMock()
    store = Store(gitlab_config)
    assert store.getFileName(TOKEN) == "README.md"


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_list_files_empty(mock_session_cls, gitlab_config):
    mock_session_cls.return_value = MagicMock()
    store = Store(gitlab_config)
    assert store.listFiles() == []


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_exists_true(mock_session_cls, gitlab_config):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_session.get.return_value = mock_response

    store = Store(gitlab_config)
    assert store.exists(TOKEN) is True
    mock_response.close.assert_called_once()


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_exists_false(mock_session_cls, gitlab_config):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_session.get.return_value = mock_response

    store = Store(gitlab_config)
    assert store.exists(TOKEN) is False
    mock_response.close.assert_called_once()


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_exists_raises_on_server_error(mock_session_cls, gitlab_config):
    mock_session = MagicMock()
    mock_session_cls.return_value = mock_session
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.HTTPError("fail")
    mock_session.get.return_value = mock_response

    store = Store(gitlab_config)
    with pytest.raises(requests.HTTPError):
        store.exists(TOKEN)
    mock_response.close.assert_called_once()


@pytest.mark.parametrize(
    "method,args",
    [
        ("createFileToken", ("x.txt",)),
        ("openWrite", (TOKEN, b"x")),
        ("delete", (TOKEN,)),
    ],
)
@patch("bevault_workers.stores.gitlab.requests.Session")
def test_readonly_methods_raise(mock_session_cls, gitlab_config, method, args):
    mock_session_cls.return_value = MagicMock()
    store = Store(gitlab_config)
    with pytest.raises(NotImplementedError) as exc:
        getattr(store, method)(*args)
    assert "read-only" in str(exc.value).lower()
    assert method in str(exc.value)


@patch("bevault_workers.stores.gitlab.requests.Session")
def test_connect_noop(mock_session_cls, gitlab_config):
    mock_session_cls.return_value = MagicMock()
    store = Store(gitlab_config)
    store.connect()
