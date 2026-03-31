from unittest.mock import MagicMock, patch

from bevault_workers.stores.sftp import Store


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_sftp_keeps_legacy_configuration_keys(mock_ssh_client_cls):
    ssh_client = MagicMock()
    ssh_client.open_sftp.return_value = MagicMock()
    mock_ssh_client_cls.return_value = ssh_client

    store = Store(
        {
            "Host": "legacy-host",
            "Port": 22,
            "Username": "legacy-user",
            "Password": "legacy-pass",
            "Prefix": "/legacy/",
        }
    )

    store.connect()

    ssh_client.connect.assert_called_once_with(
        hostname="legacy-host",
        port=22,
        username="legacy-user",
        password="legacy-pass",
    )


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_sftp_supports_states_host_name_and_base_path(mock_ssh_client_cls):
    ssh_client = MagicMock()
    ssh_client.open_sftp.return_value = MagicMock()
    mock_ssh_client_cls.return_value = ssh_client

    store = Store(
        {
            "hostName": "states-host",
            "port": 23,
            "username": "states-user",
            "password": "states-pass",
            "basePath": "/inbound",
        }
    )

    assert store.prefix == "/inbound/"
    store.connect()

    ssh_client.connect.assert_called_once_with(
        hostname="states-host",
        port=23,
        username="states-user",
        password="states-pass",
    )


@patch("bevault_workers.stores.sftp.paramiko.SSHClient")
def test_sftp_supports_key_path_and_key_password_variants(mock_ssh_client_cls):
    ssh_client = MagicMock()
    ssh_client.open_sftp.return_value = MagicMock()
    mock_ssh_client_cls.return_value = ssh_client

    store = Store(
        {
            "HostName": "states-host",
            "Username": "states-user",
            "PrivateKeyPath": "/keys/id_rsa",
            "KeyPassword": "p@ssphrase",
            "BasePath": "/",
        }
    )

    store.connect()

    ssh_client.connect.assert_called_once_with(
        hostname="states-host",
        port=22,
        username="states-user",
        key_filename="/keys/id_rsa",
        passphrase="p@ssphrase",
    )
