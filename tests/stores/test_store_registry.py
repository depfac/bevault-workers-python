"""Tests for StoreRegistry.get_store_from_filetoken."""

from unittest.mock import patch, MagicMock

import pytest

from bevault_workers.stores import StoreRegistry, UnknownStoreError, FileStore, DbStore


class DummyFileStore(FileStore):
    """Minimal concrete FileStore for type checks."""

    def __init__(self, config):
        self.config = config

    def connect(self):
        pass

    def createFileToken(self, filename: str) -> str:
        return f"dummy://dummyStore/{filename}"

    def listFiles(self, prefix: str = "", suffix: str = "") -> list:
        return []

    def getFileName(self, fileToken: str) -> str:
        return "file.txt"

    def openRead(self, fileToken: str):
        return b""

    def openWrite(self, fileToken: str, content: bytes):
        pass

    def delete(self, fileToken: str):
        pass

    def exists(self, fileToken: str) -> bool:
        return False


class DummyDbStore(DbStore):
    """Minimal concrete DbStore for type checks."""

    def __init__(self, config):
        self.config = config

    def connect(self):
        pass

    def execute(self, query: str, params=None):
        return None


@pytest.fixture(autouse=True)
def clear_registry():
    """Ensure a clean StoreRegistry between tests."""
    StoreRegistry.clear()
    yield
    StoreRegistry.clear()


@patch("bevault_workers.stores.store_registry.load_store_config")
@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_get_store_from_filetoken_valid_sftp(mock_resolve, mock_load_config):
    """Valid SFTP token resolves to the sftp FileStore."""
    sftp_conf = {
        "Host": "sftp.example.com",
        "Port": 22,
        "Username": "user",
        "Password": "pass",
        "Prefix": "/uploads/",
    }
    mock_load_config.return_value = [
        {"Name": "sftpStore", "Type": "sftp", "Config": sftp_conf},
    ]

    # Use real SFTP Store class to ensure module name is "sftp"
    from bevault_workers.stores.sftp import Store as SftpStore

    mock_resolve.return_value = SftpStore

    StoreRegistry.load()

    token = "sftp://sftpStore/path/to/file.txt"
    store = StoreRegistry.get_store_from_filetoken(token)

    assert isinstance(store, SftpStore)


@patch("bevault_workers.stores.s3.boto3.client")
@patch("bevault_workers.stores.store_registry.load_store_config")
def test_get_store_from_filetoken_valid_s3(mock_load_config, mock_boto_client):
    """Valid S3 token resolves to the s3 FileStore."""
    mock_boto_client.return_value = MagicMock()
    s3_conf = {
        "BucketName": "test-bucket",
        "AccessKey": "ak",
        "SecretKey": "sk",
        "ServiceUrl": "http://localhost:9000",
        "Prefix": "prefix/",
    }
    mock_load_config.return_value = [
        {"Name": "s3Store", "Type": "s3", "Config": s3_conf},
    ]

    StoreRegistry.load()

    token = "s3://s3Store/path/to/file.txt"
    store = StoreRegistry.get_store_from_filetoken(token)

    from bevault_workers.stores.s3 import Store as S3Store

    assert isinstance(store, S3Store)


@patch("bevault_workers.stores.gitlab.requests.Session")
@patch("bevault_workers.stores.store_registry.load_store_config")
def test_get_store_from_filetoken_valid_gitlab(mock_load_config, mock_session_cls):
    """Valid GitLab token resolves to the gitlab FileStore."""
    mock_session_cls.return_value = MagicMock()
    gitlab_conf = {
        "BaseUri": "https://gitlab.com",
        "AccessToken": "token",
        "ProjectId": "210",
    }
    mock_load_config.return_value = [
        {"Name": "gitlabStore", "Type": "gitlab", "Config": gitlab_conf},
    ]

    StoreRegistry.load()

    token = "gitlab://gitlabStore/main/path/to/file.txt"
    store = StoreRegistry.get_store_from_filetoken(token)

    from bevault_workers.stores.gitlab import Store as GitlabStore

    assert isinstance(store, GitlabStore)


def test_get_store_from_filetoken_invalid_format():
    """Malformed tokens raise a ValueError."""
    with pytest.raises(ValueError) as exc:
        StoreRegistry.get_store_from_filetoken("not-a-token")

    assert "Invalid filetoken format" in str(exc.value)


@patch("bevault_workers.stores.store_registry.load_store_config")
@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_get_store_from_filetoken_unknown_protocol(mock_resolve, mock_load_config):
    """Protocol not matching any FileStore protocol raises a ValueError."""
    # Register a single SFTP FileStore
    sftp_conf = {
        "Host": "sftp.example.com",
        "Port": 22,
        "Username": "user",
        "Password": "pass",
        "Prefix": "/uploads/",
    }
    mock_load_config.return_value = [
        {"Name": "sftpStore", "Type": "sftp", "Config": sftp_conf},
    ]

    from bevault_workers.stores.sftp import Store as SftpStore

    mock_resolve.return_value = SftpStore

    StoreRegistry.load()

    # Use an unknown protocol with an existing FileStore name
    token = "unknown://sftpStore/path/to/file.txt"
    with pytest.raises(ValueError) as exc:
        StoreRegistry.get_store_from_filetoken(token)

    assert "does not correspond to a known FileStore" in str(exc.value)


@patch("bevault_workers.stores.store_registry.load_store_config")
def test_get_store_from_filetoken_store_not_found(mock_load_config):
    """Missing store name raises a ValueError."""
    # No stores configured
    mock_load_config.return_value = []
    StoreRegistry.load()

    token = "sftp://missingStore/path/to/file.txt"
    with pytest.raises(ValueError) as exc:
        StoreRegistry.get_store_from_filetoken(token)

    assert "Store 'missingStore' not found in registry" in str(exc.value)


@patch("bevault_workers.stores.store_registry.load_store_config")
@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_get_store_from_filetoken_store_not_filestore(mock_resolve, mock_load_config):
    """Store exists but is not a FileStore -> ValueError."""
    db_conf = {"host": "localhost"}
    mock_load_config.return_value = [
        {"Name": "dbStore", "Type": "postgresql", "Config": db_conf},
    ]

    mock_resolve.return_value = DummyDbStore

    StoreRegistry.load()

    token = "postgresql://dbStore/path/to/file.txt"
    with pytest.raises(ValueError) as exc:
        StoreRegistry.get_store_from_filetoken(token)

    assert "is not a FileStore" in str(exc.value)


@patch("bevault_workers.stores.store_registry.load_store_config")
@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_get_store_from_filetoken_protocol_mismatch_for_store(
    mock_resolve, mock_load_config
):
    """Protocol differs from the resolved FileStore's protocol -> ValueError."""
    sftp_conf = {
        "Host": "sftp.example.com",
        "Port": 22,
        "Username": "user",
        "Password": "pass",
        "Prefix": "/uploads/",
    }
    mock_load_config.return_value = [
        {"Name": "sftpStore", "Type": "sftp", "Config": sftp_conf},
    ]

    from bevault_workers.stores.sftp import Store as SftpStore

    mock_resolve.return_value = SftpStore

    StoreRegistry.load()

    # Known FileStore protocol is "sftp", but token uses "s3"
    token = "s3://sftpStore/path/to/file.txt"
    with pytest.raises(ValueError) as exc:
        StoreRegistry.get_store_from_filetoken(token)

    assert "does not correspond to a known FileStore" in str(exc.value) or (
        "does not match FileStore" in str(exc.value)
    )


@patch("bevault_workers.stores.store_registry.load_store_config")
def test_get_unknown_store_error_message_is_explicit(mock_load_config):
    """Missing store names surface a full message for orchestrator cause / logs."""
    mock_load_config.return_value = []
    StoreRegistry.load()

    with pytest.raises(UnknownStoreError) as exc_info:
        StoreRegistry.get("missingStore")

    msg = str(exc_info.value)
    assert "missingStore" in msg
    assert "not found in the store registry" in msg
    assert isinstance(exc_info.value, KeyError)
