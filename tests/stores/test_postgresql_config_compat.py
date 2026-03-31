from unittest.mock import patch

from bevault_workers.stores.postgresql import Store


@patch("bevault_workers.stores.postgresql.psycopg.connect")
def test_postgresql_uses_connection_string_when_present(mock_connect):
    store = Store(
        {
            "host": "localhost",
            "port": 5432,
            "database": "northwind",
            "username": "user1",
            "password": "secret",
            "connectionString": "Host=cs-host;Port=5432;Database=cs-db;Username=cs-user;Password=cs-pass",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        "Host=cs-host;Port=5432;Database=cs-db;Username=cs-user;Password=cs-pass"
    )


@patch("bevault_workers.stores.postgresql.psycopg.connect")
def test_postgresql_uses_states_parameter_names_when_connection_string_empty(mock_connect):
    store = Store(
        {
            "host": "localhost",
            "port": 5432,
            "database": "northwind",
            "username": "worker",
            "password": "secret",
            "connectionString": "",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        host="localhost",
        port=5432,
        dbname="northwind",
        user="worker",
        password="secret",
    )


@patch("bevault_workers.stores.postgresql.psycopg.connect")
def test_postgresql_keeps_legacy_parameter_names(mock_connect):
    store = Store(
        {
            "host": "legacy-host",
            "port": "5432",
            "dbname": "legacy-db",
            "user": "legacy-user",
            "password": "legacy-pass",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        host="legacy-host",
        port="5432",
        dbname="legacy-db",
        user="legacy-user",
        password="legacy-pass",
    )
