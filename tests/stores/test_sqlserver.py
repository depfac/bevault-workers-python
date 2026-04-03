"""Unit tests for SQL Server DbStore with mocked pyodbc."""

from unittest.mock import MagicMock, patch

import pytest

from bevault_workers.stores.sqlserver import Store

_EXPECTED_ODBC_LOCAL = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=127.0.0.1,1433;"
    "DATABASE=testdb;"
    "UID=testuser;"
    "PWD=testpass"
)


def _make_mock_connection(cursor_results=None, cursor_description=None, rowcount=0):
    """Create a mock pyodbc connection with cursor that yields the given results."""
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock()
    mock_cursor.fetchall.return_value = cursor_results if cursor_results is not None else []
    mock_cursor.description = cursor_description
    mock_cursor.rowcount = rowcount
    mock_cursor.close = MagicMock()

    mock_connection = MagicMock()
    mock_connection.closed = False
    mock_connection.cursor.return_value = mock_cursor
    mock_connection.commit = MagicMock()
    mock_connection.rollback = MagicMock()

    return mock_connection, mock_cursor


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_connect(mock_connect, sqlserver_config):
    """connect() establishes connection with built ODBC string."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    mock_connect.assert_called_once_with(_EXPECTED_ODBC_LOCAL)
    assert store.connection is mock_connection


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_select_query(mock_connect, sqlserver_config):
    """SELECT queries return results from fetchall()."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[(1, "a"), (2, "b")],
        cursor_description=[("id",), ("name",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    result = store.execute("SELECT * FROM t")

    assert result == [(1, "a"), (2, "b")]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM t")
    mock_cursor.fetchall.assert_called_once()
    mock_connection.commit.assert_not_called()


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_select_query_empty_result(mock_connect, sqlserver_config):
    """SELECT queries with no results return None."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    result = store.execute("SELECT * FROM t WHERE 1=0")

    assert result is None
    mock_cursor.fetchall.assert_called_once()


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_insert_query(mock_connect, sqlserver_config):
    """INSERT/UPDATE/DELETE queries return rowcount and commit."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_description=None,
        rowcount=3,
    )
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    result = store.execute("INSERT INTO t (a) VALUES (1)")

    assert result == 3
    mock_cursor.execute.assert_called_once_with("INSERT INTO t (a) VALUES (1)")
    mock_cursor.fetchall.assert_not_called()
    mock_connection.commit.assert_called_once()


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_with_params(mock_connect, sqlserver_config):
    """Parameterized queries pass params to cursor.execute()."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    result = store.execute("SELECT * FROM t WHERE id = ?", (42,))

    assert result == [(1,)]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM t WHERE id = ?", (42,))


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_multiple_calls(mock_connect, sqlserver_config):
    """Multiple execute() calls reuse the same connection."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    store.execute("SELECT 1")
    store.execute("SELECT 2")

    mock_connect.assert_called_once()
    assert mock_cursor.execute.call_count == 2


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_ensure_connection_when_none(mock_connect, sqlserver_config):
    """_ensure_connection() calls connect() when connection is None."""
    mock_connection, _ = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    assert store.connection is None

    store._ensure_connection()

    mock_connect.assert_called_once_with(_EXPECTED_ODBC_LOCAL)
    assert store.connection is mock_connection


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_ensure_connection_when_closed(mock_connect, sqlserver_config):
    """_ensure_connection() reconnects when connection is closed."""
    conn1, _ = _make_mock_connection(cursor_results=[(1,)], cursor_description=[("id",)])
    conn2, _ = _make_mock_connection(cursor_results=[(2,)], cursor_description=[("id",)])
    mock_connect.side_effect = [conn1, conn2]

    store = Store(sqlserver_config)
    store.connect()
    conn1.closed = True

    store._ensure_connection()

    assert mock_connect.call_count == 2
    assert store.connection is conn2


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_ensure_connection_when_open(mock_connect, sqlserver_config):
    """_ensure_connection() does not reconnect when connection is open."""
    mock_connection, _ = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    mock_connection.closed = False
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    store._ensure_connection()

    mock_connect.assert_called_once()


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_auto_reconnect(mock_connect, sqlserver_config):
    """execute() auto-reconnects when connection is closed between calls."""
    conn1, cur1 = _make_mock_connection(
        cursor_results=[(1,)],
        cursor_description=[("id",)],
    )
    conn2, cur2 = _make_mock_connection(
        cursor_results=[(2,)],
        cursor_description=[("id",)],
    )
    mock_connect.side_effect = [conn1, conn2]

    store = Store(sqlserver_config)
    store.connect()

    result1 = store.execute("SELECT 1")
    conn1.closed = True

    result2 = store.execute("SELECT 2")

    assert result1 == [(1,)]
    assert result2 == [(2,)]
    assert mock_connect.call_count == 2
    cur1.execute.assert_called_once_with("SELECT 1")
    cur2.execute.assert_called_once_with("SELECT 2")


@patch("bevault_workers.stores.sqlserver.pyodbc.connect")
def test_execute_rollback_on_error(mock_connect, sqlserver_config):
    """execute() rolls back when cursor.execute raises."""
    mock_connection, mock_cursor = _make_mock_connection(
        cursor_description=None,
        rowcount=0,
    )
    mock_cursor.execute.side_effect = RuntimeError("boom")
    mock_connect.return_value = mock_connection

    store = Store(sqlserver_config)
    store.connect()

    with pytest.raises(RuntimeError, match="boom"):
        store.execute("INSERT INTO t (a) VALUES (1)")

    mock_connection.rollback.assert_called_once()
