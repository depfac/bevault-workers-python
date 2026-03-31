"""Abstract store interfaces for database, HTTP API, and file-backed backends."""

from abc import ABC, abstractmethod
from urllib.parse import urlparse


class Store(ABC):
    """Base type for all pluggable data stores."""

    @abstractmethod
    def connect(self):
        """Open or validate a connection to the store (no-op where not applicable)."""
        pass


class DbStore(Store):
    """Store that runs SQL against a database."""

    @abstractmethod
    def execute(self, query: str, params=None):
        """Run *query* with optional *params*; return rows for SELECT, else rowcount."""
        pass


class ApiStore(Store):
    """Store that performs HTTP requests against a REST or similar API."""

    @abstractmethod
    def post(self, relative_url: str, payload=None, headers=None):
        """POST *payload* to *relative_url* with optional *headers*."""
        pass


class FileStore(Store):
    """File-oriented store using opaque file tokens (e.g. ``s3://bucket/key``)."""

    @abstractmethod
    def createFileToken(self, filename: str) -> str:
        """Build a file token for *filename* (path only; store prefix is applied internally)."""
        pass

    @abstractmethod
    def listFiles(self, prefix: str = "", suffix: str = "") -> list:
        """Return paths or tokens under *prefix*, optionally filtered by *suffix*."""
        pass

    @abstractmethod
    def getFileName(self, fileToken: str) -> str:
        """Return the basename segment of *fileToken*."""
        pass

    @abstractmethod
    def openRead(self, fileToken: str):
        """Read object bytes for *fileToken*."""
        pass

    @abstractmethod
    def openWrite(self, fileToken: str, content: bytes):
        """Write *content* to *fileToken*."""
        pass

    @abstractmethod
    def delete(self, fileToken: str):
        """Remove the object named by *fileToken*."""
        pass

    @abstractmethod
    def exists(self, fileToken: str) -> bool:
        """Return True if *fileToken* exists in the store."""
        pass

    # Internal helper methods for prefix handling
    def _extract_filepath_from_token(self, filetoken: str) -> str:
        """Extract the filepath from a filetoken URL.

        Args:
            filetoken: Token in format '<protocol>://<identifier>/<filepath>'

        Returns:
            Filepath without leading "/"
        """
        parsed = urlparse(filetoken)
        return parsed.path.lstrip("/")

    def _add_prefix_to_path(self, filepath: str) -> str:
        """Add the configured prefix to a filepath.

        Args:
            filepath: Filepath without prefix

        Returns:
            Full path with prefix added
        """
        if not hasattr(self, "prefix") or not self.prefix:
            return filepath

        # Handle SFTP-style prefix (normalized to end with "/", might be "/" for root)
        if self.prefix == "/":
            return f"/{filepath}" if not filepath.startswith("/") else filepath

        # For SFTP: prefix ends with "/", strip it before concatenating
        # For S3: prefix doesn't end with "/", use as-is
        prefix_normalized = self.prefix.rstrip("/")
        if prefix_normalized:
            return f"{prefix_normalized}/{filepath}"

        return filepath

    def _remove_prefix_from_path(self, filepath: str) -> str:
        """Remove the configured prefix from a filepath if present.

        Args:
            filepath: Full filepath that may include prefix

        Returns:
            Relative path without prefix
        """
        if not hasattr(self, "prefix") or not self.prefix:
            return filepath

        # Handle SFTP-style prefix (normalized to end with "/", might be "/" for root)
        if self.prefix == "/":
            return filepath.lstrip("/")

        # For SFTP: prefix ends with "/", compare with rstrip("/")
        # For S3: prefix doesn't end with "/", compare as-is
        prefix_normalized = self.prefix.rstrip("/")
        if prefix_normalized and filepath.startswith(prefix_normalized):
            relative = filepath[len(prefix_normalized) :].lstrip("/")
            return relative

        return filepath
