"""Unit tests for SFTP FileStore with mocked paramiko."""

import types
from unittest.mock import MagicMock, patch

import pytest

from bevault_workers.stores.sftp import Store


def make_attr(filename, is_dir=False):
    """Create SFTPAttributes-like object for listdir_attr mock."""
    st_mode = 0o040000 if is_dir else 0o100000
    return types.SimpleNamespace(filename=filename, st_mode=st_mode)


def wire_store_sftp_session(store, mock_sftp, mock_ssh_instance=None):
    """Attach SFTP client and mark SSH transport active so _ensure_connection does not reconnect."""
    ssh = mock_ssh_instance if mock_ssh_instance is not None else MagicMock()
    transport = MagicMock()
    transport.is_active.return_value = True
    ssh.get_transport.return_value = transport
    store.ssh_client = ssh
    store.sftp_client = mock_sftp


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_init_default_port(mock_ssh_class, sftp_config):
    """Port defaults to 22 when omitted."""
    sftp_config.pop("Port", None)
    store = Store(sftp_config)
    assert store.port == 22


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_init_prefix_normalized(mock_ssh_class, sftp_config):
    """Prefix gets trailing slash if missing."""
    sftp_config["Prefix"] = "/uploads"
    store = Store(sftp_config)
    assert store.prefix == "/uploads/"


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_create_file_token_with_prefix(mock_ssh_class, sftp_config):
    """Token format: sftp://host/file.txt (prefix not included in token)."""
    store = Store(sftp_config)
    token = store.createFileToken("file.txt")
    assert token == "sftp://sftp.example.com/file.txt"


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_create_file_token_without_prefix(mock_ssh_class, sftp_config):
    """Token format: sftp://host/file.txt when prefix is /."""
    sftp_config["Prefix"] = "/"
    store = Store(sftp_config)
    token = store.createFileToken("file.txt")
    assert token == "sftp://sftp.example.com/file.txt"


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_get_file_name(mock_ssh_class, sftp_config):
    """Extract basename from token path."""
    store = Store(sftp_config)
    assert store.getFileName("sftp://host/uploads/sub/file.txt") == "file.txt"


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_list_files(mock_ssh_class, sftp_config):
    """Recursive walk, suffix filter, returns full tokens."""
    mock_sftp = MagicMock()
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    mock_sftp.listdir_attr.side_effect = [
        [make_attr("file.txt", is_dir=False), make_attr("subdir", is_dir=True)],
        [make_attr("nested.csv", is_dir=False)],
    ]

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp, mock_ssh_instance)

    result = store.listFiles(prefix="", suffix=".txt")
    # Tokens should not include prefix (prefix is in config, not token)
    assert "sftp://sftp.example.com/file.txt" in result
    assert len([t for t in result if t.endswith(".txt")]) == 1


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_open_read(mock_ssh_class, sftp_config):
    """Calls sftp.file(path, 'r').read(), returns bytes. Prefix is added internally."""
    mock_sftp = MagicMock()
    mock_file = MagicMock()
    mock_file.read.return_value = b"file content"
    mock_file.__enter__ = lambda self: self
    mock_file.__exit__ = lambda *a: None
    mock_sftp.file.return_value = mock_file

    mock_ssh_instance = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp, mock_ssh_instance)

    # Token without prefix - prefix should be added internally
    result = store.openRead("sftp://host/file.txt")
    assert result == b"file content"
    mock_sftp.file.assert_called_once_with("/uploads/file.txt", "r")


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_open_write(mock_ssh_class, sftp_config):
    """Calls _mkdir_p then putfo(BytesIO(content), path). Prefix is added internally."""
    mock_sftp = MagicMock()
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp, mock_ssh_instance)

    content = b"hello world"
    # Token without prefix - prefix should be added internally
    store.openWrite("sftp://host/file.txt", content)

    mock_sftp.putfo.assert_called_once()
    call_args = mock_sftp.putfo.call_args
    assert call_args[0][1] == "/uploads/file.txt"
    assert call_args[0][0].read() == content


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_open_write_creates_parent_dirs(mock_ssh_class, sftp_config):
    """_mkdir_p calls mkdir for missing parent dirs."""
    mock_sftp = MagicMock()
    mock_sftp.stat.side_effect = [OSError(), OSError()]

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp)
    store._mkdir_p("/uploads/sub/file.txt")

    assert mock_sftp.mkdir.call_count >= 1


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_delete(mock_ssh_class, sftp_config):
    """Calls sftp.remove(path). Prefix is added internally."""
    mock_sftp = MagicMock()
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp, mock_ssh_instance)
    # Token without prefix - prefix should be added internally
    store.delete("sftp://host/file.txt")

    mock_sftp.remove.assert_called_once_with("/uploads/file.txt")


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_exists_true(mock_ssh_class, sftp_config):
    """stat succeeds -> returns True. Prefix is added internally."""
    mock_sftp = MagicMock()
    mock_sftp.stat.return_value = MagicMock()

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp)

    # Token without prefix - prefix should be added internally
    assert store.exists("sftp://host/file.txt") is True
    mock_sftp.stat.assert_called_once_with("/uploads/file.txt")


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_exists_false(mock_ssh_class, sftp_config):
    """stat raises IOError -> returns False. Prefix is added internally."""
    mock_sftp = MagicMock()
    mock_sftp.stat.side_effect = IOError()

    store = Store(sftp_config)
    wire_store_sftp_session(store, mock_sftp)

    # Token without prefix - prefix should be added internally
    assert store.exists("sftp://host/file.txt") is False
    mock_sftp.stat.assert_called_once_with("/uploads/file.txt")


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_ensure_connection_lazy(mock_ssh_class, sftp_config):
    """First operation calls connect() if not connected."""
    mock_sftp = MagicMock()
    mock_sftp.stat.return_value = MagicMock()
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    store = Store(sftp_config)
    assert store.sftp_client is None

    store.exists("sftp://host/file.txt")

    mock_ssh_instance.connect.assert_called_once()
    assert store.sftp_client is mock_sftp


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_connect_password_auth(mock_ssh_class, sftp_config):
    """connect passes password in kwargs when configured."""
    mock_ssh_instance = MagicMock()
    mock_sftp = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    store = Store(sftp_config)
    store.connect()

    mock_ssh_instance.connect.assert_called_once()
    call_kwargs = mock_ssh_instance.connect.call_args[1]
    assert call_kwargs["password"] == "testpass"
    assert "hostname" in call_kwargs
    assert call_kwargs["hostname"] == "sftp.example.com"


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_connect_key_auth(mock_ssh_class, sftp_config_key_auth):
    """connect passes key_filename when KeyFilename in config."""
    mock_ssh_instance = MagicMock()
    mock_sftp = MagicMock()
    mock_ssh_instance.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh_instance

    store = Store(sftp_config_key_auth)
    store.connect()

    mock_ssh_instance.connect.assert_called_once()
    call_kwargs = mock_ssh_instance.connect.call_args[1]
    assert call_kwargs["key_filename"] == "/path/to/key"
    assert "password" not in call_kwargs
