import pyodbc

from .base_store import DbStore
from .store_utils import get_first

_DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
_DEFAULT_PORT = 1433


def _yes_no_odbc(value):
    """Map config value to 'yes' / 'no' for ODBC attributes, or None if unset."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "yes" if value else "no"
    s = str(value).strip().lower()
    if s in ("yes", "true", "1", "on"):
        return "yes"
    if s in ("no", "false", "0", "off"):
        return "no"
    return None


def _build_odbc_connection_string(config):
    """Build an ODBC connection string from discrete config fields."""
    host = get_first(config, "host", "server")
    if host is None or (isinstance(host, str) and not host.strip()):
        raise ValueError("sqlserver store requires 'host' or 'server' when no connection string is set")

    port = get_first(config, "port")
    if port is None:
        port = _DEFAULT_PORT
    driver = get_first(config, "driver", "odbcDriver") or _DEFAULT_DRIVER
    user = get_first(config, "user", "username", "userid")
    password = get_first(config, "password")
    database = get_first(config, "dbname", "database")

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={host},{port}",
    ]
    if database is not None and str(database).strip():
        parts.append(f"DATABASE={database}")
    if user is not None:
        parts.append(f"UID={user}")
    if password is not None:
        parts.append(f"PWD={password}")

    encrypt = _yes_no_odbc(get_first(config, "encrypt", "Encrypt"))
    if encrypt is not None:
        parts.append(f"Encrypt={encrypt}")

    trust = _yes_no_odbc(
        get_first(config, "trustServerCertificate", "trustservercertificate")
    )
    if trust is not None:
        parts.append(f"TrustServerCertificate={trust}")

    return ";".join(parts)


class Store(DbStore):
    """
    Microsoft SQL Server store using ODBC (pyodbc).

    Use ``Type: "sqlserver"`` in store definitions.

    Configuration:
    {
        "Name": "storeName",
        "Type": "sqlserver",
        "Config": {
            "host": "127.0.0.1",
            "port": "1433",
            "user": "",
            "password": "",
            "dbname": ""
        }
    }

    If ``connectionString`` or ``ConnectionString`` is a non-empty string, it is
    passed directly to ``pyodbc.connect`` and other fields are ignored.

    Optional keys: ``driver`` / ``odbcDriver`` (default ODBC Driver 18 for SQL
    Server), ``encrypt``, ``trustServerCertificate`` (bool or yes/no strings).

    Parameterized SQL uses ``?`` placeholders (pyodbc), not ``%s`` (PostgreSQL).
    """

    @classmethod
    def _normalize_config(cls, config):
        connection_string = get_first(config, "connectionString", "conninfo")
        if isinstance(connection_string, str) and connection_string.strip():
            return connection_string.strip(), None
        return None, _build_odbc_connection_string(config)

    def __init__(self, config):
        self._direct_connection_string, self._built_connection_string = (
            self._normalize_config(config)
        )
        self.connection = None

    def _connection_string(self):
        if self._direct_connection_string:
            return self._direct_connection_string
        return self._built_connection_string

    def connect(self):
        """Establish a connection to the SQL Server database."""
        self.connection = pyodbc.connect(self._connection_string())

    def _ensure_connection(self):
        """Ensure the connection is open and valid. Reconnect if necessary."""
        if self.connection is None:
            self.connect()
            return
        if getattr(self.connection, "closed", False):
            self.connect()

    def execute(self, query, params=None):
        """
        Execute a query on the database.

        Args:
            query: SQL query string
            params: Optional parameters (use ``?`` placeholders for pyodbc)

        Returns:
            Query results for SELECT queries, or rowcount for other queries
        """
        self._ensure_connection()

        cur = self.connection.cursor()
        try:
            if params is None:
                cur.execute(query)
            else:
                cur.execute(query, params)
            if cur.description:
                results = cur.fetchall()
                return results if results else None
            self.connection.commit()
            return cur.rowcount
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cur.close()
