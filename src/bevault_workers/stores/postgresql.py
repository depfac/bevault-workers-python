import psycopg
from .base_store import DbStore
from .store_utils import get_first


class Store(DbStore):
    """
    Configuration:
    {
        "Name": "storeName",
        "Type": "postgresql",
        "Config": {
            "host": "127.0.0.1",
            "port": "5432",
            "user": "",
            "password": "",
            "dbname": ""
    }
    """

    @classmethod
    def _normalize_config(cls, config):
        connection_string = get_first(config, "connectionString", "conninfo")
        if isinstance(connection_string, str) and connection_string.strip():
            return {}, connection_string.strip()

        host = get_first(config, "host")
        port = get_first(config, "port")
        user = get_first(config, "user", "username", "userid")
        password = get_first(config, "password")
        dbname = get_first(config, "dbname", "database")

        normalized = {}
        if host is not None:
            normalized["host"] = host
        if port is not None:
            normalized["port"] = port
        if user is not None:
            normalized["user"] = user
        if password is not None:
            normalized["password"] = password
        if dbname is not None:
            normalized["dbname"] = dbname
        return normalized, None

    def __init__(self, config):
        self.config, self.connection_string = self._normalize_config(config)
        self.connection = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        if self.connection_string:
            self.connection = psycopg.connect(self.connection_string)
            return
        self.connection = psycopg.connect(**self.config)

    def _ensure_connection(self):
        """Ensure the connection is open and valid. Reconnect if necessary."""
        if self.connection is None or self.connection.closed:
            self.connect()

    def execute(self, query, params=None):
        """
        Execute a query on the database.
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query results for SELECT queries, or rowcount for other queries
        """
        # Ensure connection is open before executing
        self._ensure_connection()
        
        # Use transaction context manager for automatic commit/rollback
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(query, params)
                # Check if the query might return results (has a description)
                if cur.description:
                    results = cur.fetchall()
                    # Return results only if there are any
                    return results if results else None

                # For non-SELECT queries, return affected row count
                return cur.rowcount
