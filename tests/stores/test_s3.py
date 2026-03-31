"""Unit tests for S3 FileStore with mocked boto3."""

from unittest.mock import MagicMock, patch

import pytest

from bevault_workers.stores.s3 import Store


@patch("bevault_workers.stores.s3.boto3.client")
def test_create_file_token(mock_boto_client, s3_config):
    """Token format: s3://bucket/filename (prefix not included in token)."""
    mock_boto_client.return_value = MagicMock()
    store = Store(s3_config)
    token = store.createFileToken("file.txt")
    assert token == "s3://test-bucket/file.txt"


@patch("bevault_workers.stores.s3.boto3.client")
def test_get_file_name(mock_boto_client, s3_config):
    """Returns basename from token path."""
    mock_boto_client.return_value = MagicMock()
    store = Store(s3_config)
    result = store.getFileName("s3://bucket/sub/file.txt")
    assert result == "file.txt"


@patch("bevault_workers.stores.s3.boto3.client")
def test_list_files(mock_boto_client, s3_config):
    """Paginator used, suffix filter applied."""
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "prefix/file.txt"}, {"Key": "prefix/file.csv"}]},
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_boto_client.return_value = mock_s3

    store = Store(s3_config)
    result = store.listFiles(prefix="", suffix=".txt")

    # Tokens should not include prefix (prefix is in config, not token)
    assert "file.txt" in result
    assert "file.csv" not in result
    mock_s3.get_paginator.assert_called_once_with("list_objects_v2")


@patch("bevault_workers.stores.s3.boto3.client")
def test_open_read(mock_boto_client, s3_config):
    """get_object called, returns Body.read(). Prefix is added internally."""
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b"file content"
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_boto_client.return_value = mock_s3

    store = Store(s3_config)
    # Token without prefix - prefix should be added internally
    result = store.openRead("s3://test-bucket/file.txt")

    assert result == b"file content"
    mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="prefix/file.txt")


@patch("bevault_workers.stores.s3.boto3.client")
def test_open_write(mock_boto_client, s3_config):
    """put_object called with correct Bucket, Key, Body. Prefix is added internally."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    store = Store(s3_config)
    content = b"hello world"
    # Token without prefix - prefix should be added internally
    store.openWrite("s3://test-bucket/file.txt", content)

    mock_s3.put_object.assert_called_once_with(
        Bucket="test-bucket", Key="prefix/file.txt", Body=content
    )


@patch("bevault_workers.stores.s3.boto3.client")
def test_delete(mock_boto_client, s3_config):
    """delete_object called. Prefix is added internally."""
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    store = Store(s3_config)
    # Token without prefix - prefix should be added internally
    store.delete("s3://test-bucket/file.txt")

    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="prefix/file.txt"
    )


@patch("bevault_workers.stores.s3.boto3.client")
def test_exists_true(mock_boto_client, s3_config):
    """head_object succeeds -> True. Prefix is added internally."""
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {}
    mock_boto_client.return_value = mock_s3

    store = Store(s3_config)
    # Token without prefix - prefix should be added internally
    assert store.exists("s3://test-bucket/file.txt") is True
    mock_s3.head_object.assert_called_once_with(Bucket="test-bucket", Key="prefix/file.txt")


@patch("bevault_workers.stores.s3.boto3.client")
def test_exists_false(mock_boto_client, s3_config):
    """ClientError on head_object -> False. Prefix is added internally."""
    client_error = type("ClientError", (Exception,), {})
    mock_s3 = MagicMock()
    mock_s3.exceptions.ClientError = client_error
    mock_s3.head_object.side_effect = client_error("Not found")
    mock_boto_client.return_value = mock_s3

    store = Store(s3_config)
    # Token without prefix - prefix should be added internally
    result = store.exists("s3://test-bucket/file.txt")

    assert result is False
    mock_s3.head_object.assert_called_once_with(Bucket="test-bucket", Key="prefix/file.txt")


@patch("bevault_workers.stores.s3.boto3.client")
def test_connect_noop(mock_boto_client, s3_config):
    """connect() does not raise."""
    mock_boto_client.return_value = MagicMock()
    store = Store(s3_config)
    store.connect()
