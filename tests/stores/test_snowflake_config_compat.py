from unittest.mock import patch

from bevault_workers.stores.snowflake import Store


_EXPECTED_DISCRETE = {
    "user": "worker",
    "password": "secret",
    "host": "localhost",
    "database": "northwind",
    "port": 443,
    "warehouse": "WH",
}


@patch("snowflake.connector.connect")
def test_snowflake_uses_connection_string_when_present(mock_connect):
    store = Store(
        {
            "host": "ignored",
            "database": "ignored",
            "username": "ignored",
            "password": "ignored",
            "connectionString": "snowflake://cs-user:cs-pass@cs-account/cs-db?warehouse=cs-wh",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        user="cs-user",
        password="cs-pass",
        account="cs-account",
        database="cs-db",
        warehouse="cs-wh",
    )


@patch("snowflake.connector.connect")
def test_snowflake_uses_states_parameter_names_when_connection_string_empty(
    mock_connect,
):
    store = Store(
        {
            "host": "localhost",
            "database": "northwind",
            "username": "worker",
            "password": "secret",
            "warehouse": "WH",
            "connectionString": "",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(**_EXPECTED_DISCRETE)


@patch("snowflake.connector.connect")
def test_snowflake_keeps_legacy_parameter_names(mock_connect):
    store = Store(
        {
            "host": "legacy-host",
            "port": "443",
            "dbname": "legacy-db",
            "user": "legacy-user",
            "password": "legacy-pass",
            "warehouse": "legacy-wh",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        user="legacy-user",
        password="legacy-pass",
        host="legacy-host",
        database="legacy-db",
        port=443,
        warehouse="legacy-wh",
    )


@patch("snowflake.connector.connect")
def test_snowflake_accepts_account_identifier_alias(mock_connect):
    store = Store(
        {
            "accountIdentifier": "acc-from-states",
            "username": "u",
            "password": "p",
            "database": "db1",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        user="u",
        password="p",
        database="db1",
        account="acc-from-states",
    )


@patch("snowflake.connector.connect")
def test_snowflake_accepts_conninfo_alias(mock_connect):
    store = Store(
        {
            "host": "h.example.com",
            "database": "db1",
            "username": "u",
            "password": "p",
            "conninfo": "snowflake://a:b@c/d",
        }
    )

    store.connect()

    mock_connect.assert_called_once_with(
        user="a",
        password="b",
        account="c",
        database="d",
    )
