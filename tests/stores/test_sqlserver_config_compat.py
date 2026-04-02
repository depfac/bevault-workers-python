from unittest.mock import patch

from bevault_workers.stores.sqlserver import Store


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_sqlserver_uses_connection_string_when_present(mock_connect):
    cs = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=cs-host;DATABASE=cs-db;UID=cs-user;PWD=cs-pass"
    )
    store = Store(
        {
            "host": "localhost",
            "port": 1433,
            "database": "northwind",
            "username": "user1",
            "password": "secret",
            "connectionString": cs,
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(cs)


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_sqlserver_uses_states_parameter_names_when_connection_string_empty(mock_connect):
    store = Store(
        {
            "host": "localhost",
            "port": 1433,
            "database": "northwind",
            "username": "worker",
            "password": "secret",
            "connectionString": "",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=northwind;"
        "UID=worker;"
        "PWD=secret"
    )


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_sqlserver_keeps_legacy_parameter_names(mock_connect):
    store = Store(
        {
            "host": "legacy-host",
            "port": "1433",
            "dbname": "legacy-db",
            "user": "legacy-user",
            "password": "legacy-pass",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=legacy-host,1433;"
        "DATABASE=legacy-db;"
        "UID=legacy-user;"
        "PWD=legacy-pass"
    )


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_sqlserver_accepts_server_alias(mock_connect):
    store = Store(
        {
            "server": "sql.example.com",
            "user": "u",
            "password": "p",
            "dbname": "db1",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=sql.example.com,1433;"
        "DATABASE=db1;"
        "UID=u;"
        "PWD=p"
    )


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_sqlserver_optional_encrypt_and_trust(mock_connect):
    store = Store(
        {
            "host": "h",
            "user": "u",
            "password": "p",
            "dbname": "d",
            "encrypt": "no",
            "trustServerCertificate": True,
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=h,1433;"
        "DATABASE=d;"
        "UID=u;"
        "PWD=p;"
        "Encrypt=no;"
        "TrustServerCertificate=yes"
    )
