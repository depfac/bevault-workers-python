"""Snowflake SQL store using snowflake-connector-python."""

from urllib.parse import parse_qs, unquote, urlparse

from .base_store import DbStore
from .store_utils import get_first

import snowflake.connector  # pyright: ignore[reportMissingImports]

_DEFAULT_PORT = 443

class Store(DbStore):
    """
    Snowflake store using ``snowflake.connector``.

    Use ``Type: "snowflake"`` in store definitions.

    Discrete configuration requires ``username``, ``password``, ``database``, and
    at least one of ``host`` or ``account`` (non-empty).

    Example with a custom host (e.g. private link); ``port`` defaults to 443:

    {
        "Name": "storeName",
        "Type": "snowflake",
        "Config": {
            "host": "org-account.region.privatelink.snowflakecomputing.com",
            "port": "443",
            "account": "org-account",
            "database": "MY_DB",
            "warehouse": "MY_WH",
            "username": "user",
            "password": "secret"
        }
    }

    Example with Snowflake account identifier only (no ``host`` / ``port``):

    {
        "Name": "storeName",
        "Type": "snowflake",
        "Config": {
            "account": "xy12345.us-east-1.aws",
            "database": "MY_DB",
            "warehouse": "MY_WH",
            "username": "user",
            "password": "secret"
        }
    }

    ``account`` may also be supplied as ``accountIdentifier``. ``port`` defaults
    to 443 when ``host`` is set and ``port`` is omitted. ``warehouse`` is optional.

    If ``connectionString`` or ``conninfo`` is a non-empty string, it must be a
    ``snowflake://`` URL, for example::

        snowflake://user:pass@account_id/MY_DB?warehouse=MY_WH

    For private-link style endpoints you may use::

        snowflake://user:pass@/MY_DB?host=hostname&warehouse=MY_WH

    Query parameters ``warehouse``, ``schema``, ``role``, ``host``, ``port``,
    and ``account`` are passed through when present.

    Parameterized SQL uses ``%s`` placeholders (Snowflake connector), like PostgreSQL.
    """
    def _parse_snowflake_url(url: str) -> dict:
        """Parse a ``snowflake://`` URL into kwargs for ``snowflake.connector.connect``."""
        parsed = urlparse(url)
        if parsed.scheme.casefold() != "snowflake":
            raise ValueError(
                "snowflake store connectionString must use the snowflake:// scheme "
                "(e.g. snowflake://user:pass@account_identifier/DATABASE?warehouse=WH)."
            )

        kwargs = {}
        if parsed.username:
            kwargs["user"] = unquote(parsed.username)
        if parsed.password is not None:
            kwargs["password"] = unquote(parsed.password)

        account = parsed.hostname
        if account:
            kwargs["account"] = account

        path = parsed.path.strip("/")
        if path:
            kwargs["database"] = path.split("/")[0]

        qs = parse_qs(parsed.query, keep_blank_values=True)

        def first(key: str):
            vals = qs.get(key)
            return vals[0] if vals else None

        for key in ("warehouse", "schema", "role"):
            val = first(key)
            if val is not None and str(val).strip() != "":
                kwargs[key] = val

        host = first("host")
        if host is not None and str(host).strip() != "":
            kwargs["host"] = str(host).strip()

        acc_q = first("account")
        if acc_q is not None and str(acc_q).strip() != "":
            kwargs["account"] = str(acc_q).strip()

        port = first("port")
        if port is not None and str(port).strip() != "":
            kwargs["port"] = int(port)

        return kwargs


    def _build_discrete_kwargs(config: dict) -> dict:
        """Build connect kwargs from discrete host/port/account/database fields."""
        host = get_first(config, "host")
        account = get_first(config, "account", "accountIdentifier")
        port = get_first(config, "port")
        user = get_first(config, "username", "user", "userid")
        password = get_first(config, "password")
        database = get_first(config, "database", "dbname")
        warehouse = get_first(config, "warehouse")

        has_host = host is not None and str(host).strip() != ""
        has_account = account is not None and str(account).strip() != ""

        missing = []
        if not has_host and not has_account:
            missing.append("host or account")
        if user is None or (isinstance(user, str) and not str(user).strip()):
            missing.append("username")
        if password is None:
            missing.append("password")
        if database is None or (isinstance(database, str) and not str(database).strip()):
            missing.append("database")
        if missing:
            raise ValueError(
                "snowflake store requires "
                + ", ".join(missing)
                + " when no connectionString is set"
            )

        kwargs = {
            "user": str(user).strip(),
            "password": password,
            "database": str(database).strip(),
        }
        if has_host:
            kwargs["host"] = str(host).strip()
            kwargs["port"] = int(port) if port is not None else _DEFAULT_PORT
        if has_account:
            kwargs["account"] = str(account).strip()
        if warehouse is not None and str(warehouse).strip() != "":
            kwargs["warehouse"] = str(warehouse).strip()
        return kwargs

    def _connection_is_closed(connection) -> bool:
        if connection is None:
            return True
        is_closed = getattr(connection, "is_closed", None)
        if callable(is_closed):
            return is_closed()
        return bool(getattr(connection, "closed", False))

    @classmethod
    def _normalize_config(cls, config):
        connection_string = get_first(config, "connectionString", "conninfo")
        if isinstance(connection_string, str) and connection_string.strip():
            raw = connection_string.strip()
            if raw.casefold().startswith("snowflake://"):
                return _parse_snowflake_url(raw)
            raise ValueError(
                "snowflake store connectionString must be a snowflake:// URL."
            )
        return _build_discrete_kwargs(config)

    def __init__(self, config):
        self._connect_kwargs = self._normalize_config(config)
        self.connection = None

    def connect(self):
        """Establish a connection to Snowflake."""

        self.connection = snowflake.connector.connect(**self._connect_kwargs)

    def _ensure_connection(self):
        if self.connection is None or _connection_is_closed(self.connection):
            self.connect()

    def execute(self, query, params=None):
        """
        Execute a query on Snowflake.

        Args:
            query: SQL string (use ``%s`` for bind parameters).
            params: Optional sequence of parameter values.

        Returns:
            Result rows for SELECT-like queries, or rowcount for others.
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
