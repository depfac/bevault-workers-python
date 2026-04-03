"""Shared pytest fixtures for store tests."""

import pytest


@pytest.fixture
def sftp_config():
    """Minimal valid SFTP store config."""
    return {
        "Host": "sftp.example.com",
        "Port": 22,
        "Username": "testuser",
        "Password": "testpass",
        "Prefix": "/uploads/",
    }


@pytest.fixture
def sftp_config_key_auth():
    """SFTP config with key-based auth."""
    return {
        "Host": "sftp.example.com",
        "Username": "testuser",
        "KeyFilename": "/path/to/key",
        "Prefix": "/",
    }


@pytest.fixture
def s3_config():
    """Minimal valid S3 store config."""
    return {
        "BucketName": "test-bucket",
        "AccessKey": "ak",
        "SecretKey": "sk",
        "ServiceUrl": "http://localhost:9000",
        "Prefix": "prefix/",
    }


@pytest.fixture
def postgresql_config():
    """Minimal valid PostgreSQL store config."""
    return {
        "host": "127.0.0.1",
        "port": "5432",
        "user": "testuser",
        "password": "testpass",
        "dbname": "testdb",
    }


@pytest.fixture
def gitlab_config():
    """Minimal valid GitLab store config."""
    return {
        "BaseUri": "https://gitlab.com",
        "AccessToken": "test-token",
        "ProjectId": "123",
    }


@pytest.fixture
def sqlserver_config():
    """Minimal valid SQL Server store config."""
    return {
        "host": "127.0.0.1",
        "port": "1433",
        "user": "testuser",
        "password": "testpass",
        "dbname": "testdb",
    }
