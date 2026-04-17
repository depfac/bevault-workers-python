"""Unit tests for Snowflake DbStore with mocked snowflake.connector."""

from unittest.mock import MagicMock, patch

import pytest

from bevault_workers.stores.snowflake import Store

_EXPECTED_DISCRETE_KWARGS = {
    "user": "testuser",
    "password": "testpass",
    "host": "org-account.region.privatelink.snowflakecomputing.com",
    "database": "testdb",
    "port": 443,
    "warehouse": "TEST_WH",
}


def _make_mock_connection(cursor_results=None, cursor_description=None, rowcount=0):
    """Create a mock Snowflake connection with cursor that yields the given results."""
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock()
    mock_cursor.fetchall.return_value = cursor_results if cursor_results is not None else []
    mock_cursor.description = cursor_description
    mock_cursor.rowcount = rowcount
    mock_cursor.close = MagicMock()

    mock_connection = MagicMock()
    mock_connection.is_closed = MagicMock(return_value=False)
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.commit = MagicMock()
    mock_connection.rollback = MagicMock()

    return mock_connection, mock_cursor


@patch("snowflake.connector.connect")
def test_connect(mock_connect, snowflake_config):
    """connect() passes normalized kwargs to snowflake.connector.connect."""
    mock_connection = MagicMock()
    mock_connection.is_closed = MagicMock(return_value=False)
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    mock_connect.assert_called_once_with(**_EXPECTED_DISCRETE_KWARGS)
    assert store.connection is mock_connection


@patch("snowflake.connector.connect")
def test_connect_default_port_when_omitted(mock_connect):
    """Omitted port defaults to 443."""
    mock_connection = MagicMock()
    mock_connection.is_closed = MagicMock(return_value=False)
    mock_connect.return_value = mock_connection

    store = Store(
        {
            "host": "h.example.com",
            "username": "u",
            "password": "p",
            "database": "db1",
        }
    )
    store.connect()

    mock_connect.assert_called_once_with(
        user="u",
        password="p",
        host="h.example.com",
        database="db1",
        port=443,
    )


@patch("snowflake.connector.connect")
def test_connect_account_only(mock_connect):
    """Account identifier without host does not pass host or port."""
    mock_connection = MagicMock()
    mock_connection.is_closed = MagicMock(return_value=False)
    mock_connect.return_value = mock_connection

    store = Store(
        {
            "account": "xy12345.us-east-1.aws",
            "username": "u",
            "password": "p",
            "database": "db1",
            "warehouse": "WH",
        }
    )
    store.connect()

    mock_connect.assert_called_once_with(
        user="u",
        password="p",
        database="db1",
        account="xy12345.us-east-1.aws",
        warehouse="WH",
    )


@patch("snowflake.connector.connect")
def test_connect_host_and_account(mock_connect):
    """Host and account may both be set (e.g. private link)."""
    mock_connection = MagicMock()
    mock_connection.is_closed = MagicMock(return_value=False)
    mock_connect.return_value = mock_connection

    store = Store(
        {
            "host": "privatelink.example.snowflakecomputing.com",
            "port": "443",
            "account": "org-account",
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
        host="privatelink.example.snowflakecomputing.com",
        port=443,
        account="org-account",
    )


@patch("snowflake.connector.connect")
def test_execute_select_query(mock_connect, snowflake_config):
    """SELECT queries return results from fetchall()."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[(1, "a"), (2, "b")],
        cursor_description=[("id",), ("name",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    result = store.execute("SELECT * FROM t")

    assert result == [(1, "a"), (2, "b")]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM t")
    mock_cursor.fetchall.assert_called_once()
    mock_connection.commit.assert_not_called()


@patch("snowflake.connector.connect")
def test_execute_select_query_empty_result(mock_connect, snowflake_config):
    """SELECT queries with no rows return None."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    result = store.execute("SELECT * FROM t WHERE 1=0")

    assert result is None
    mock_cursor.fetchall.assert_called_once()


@patch("snowflake.connector.connect")
def test_execute_insert_query(mock_connect, snowflake_config):
    """DML returns rowcount and commits."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_description=None,
        rowcount=3,
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    result = store.execute("INSERT INTO t (a) VALUES (1)")

    assert result == 3
    mock_cursor.execute.assert_called_once_with("INSERT INTO t (a) VALUES (1)")
    mock_cursor.fetchall.assert_not_called()
    mock_connection.commit.assert_called_once()


@patch("snowflake.connector.connect")
def test_execute_with_params(mock_connect, snowflake_config):
    """Parameterized queries use %s binds."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    result = store.execute("SELECT * FROM t WHERE id = %s", (42,))

    assert result == [(1,)]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM t WHERE id = %s", (42,))


@patch("snowflake.connector.connect")
def test_execute_multiple_calls(mock_connect, snowflake_config):
    """Multiple execute() calls reuse the same connection."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    store.execute("SELECT 1")
    store.execute("SELECT 2")

    mock_connect.assert_called_once()
    assert mock_cursor.execute.call_count == 2


@patch("snowflake.connector.connect")
def test_ensure_connection_when_none(mock_connect, snowflake_config):
    """_ensure_connection() calls connect() when connection is None."""
    mock_connection, _ = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    assert store.connection is None

    store._ensure_connection()

    mock_connect.assert_called_once_with(**_EXPECTED_DISCRETE_KWARGS)
    assert store.connection is mock_connection


@patch("snowflake.connector.connect")
def test_ensure_connection_when_closed(mock_connect, snowflake_config):
    """_ensure_connection() reconnects when is_closed() is True."""
    conn1, _ = _make_mock_connection(cursor_results=[(1,)], cursor_description=[("id",)])
    conn2, _ = _make_mock_connection(cursor_results=[(2,)], cursor_description=[("id",)])
    mock_connect.side_effect = [conn1, conn2]

    store = Store(snowflake_config)
    store.connect()
    conn1.is_closed.return_value = True

    store._ensure_connection()

    assert mock_connect.call_count == 2
    assert store.connection is conn2


@patch("snowflake.connector.connect")
def test_ensure_connection_when_open(mock_connect, snowflake_config):
    """_ensure_connection() does not reconnect when connection is open."""
    mock_connection, _ = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    store._ensure_connection()

    mock_connect.assert_called_once()


@patch("snowflake.connector.connect")
def test_execute_auto_reconnect(mock_connect, snowflake_config):
    """execute() reconnects when connection reports closed."""
    conn1, cur1 = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    conn2, cur2 = _make_mock_connection(
        cursor_results=[(2,)],
        cursor_description=[("id",)],
    )
    mock_connect.side_effect = [conn1, conn2]

    store = Store(snowflake_config)
    store.connect()

    result1 = store.execute("SELECT 1")
    conn1.is_closed.return_value = True

    result2 = store.execute("SELECT 2")

    assert result1 == [(1,)]
    assert result2 == [(2,)]
    assert mock_connect.call_count == 2
    cur1.execute.assert_called_once_with("SELECT 1")
    cur2.execute.assert_called_once_with("SELECT 2")


@patch("snowflake.connector.connect")
def test_execute_rollback_on_error(mock_connect, snowflake_config):
    """execute() rolls back when cursor.execute raises."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_description=None,
        rowcount=0,
    )
    mock_cursor.execute.side_effect = RuntimeError("boom")
    mock_connect.return_value = mock_connection

    store = Store(snowflake_config)
    store.connect()

    with pytest.raises(RuntimeError, match="boom"):
        store.execute("INSERT INTO t (a) VALUES (1)")

    mock_connection.rollback.assert_called_once()


def test_invalid_connection_string_raises():
    """Non-snowflake connection strings raise ValueError."""
    with pytest.raises(ValueError, match="snowflake://"):
        Store(
            {
                "connectionString": "odbc:driver=Snowflake;",
                "host": "h",
                "username": "u",
                "password": "p",
                "database": "d",
            }
        )


def test_discrete_config_missing_host_or_account_raises():
    with pytest.raises(ValueError, match="host or account"):
        Store(
            {
                "username": "u",
                "password": "p",
                "database": "d",
            }
        )
