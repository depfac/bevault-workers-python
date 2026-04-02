import posixpath
from urllib.parse import quote, urlparse, urljoin

import requests

from .base_store import FileStore
from .store_utils import get_first


class Store(FileStore):
    """Read-only FileStore backed by the GitLab repository files API."""

    def __init__(self, config):
        self.base_uri = (get_first(config, "baseUri") or "").rstrip("/")
        self.access_token = get_first(config, "accessToken")
        self.project_id = get_first(config, "projectId")
        self._session = requests.Session()
        self._session.headers["PRIVATE-TOKEN"] = self.access_token

    def connect(self):
        pass

    def _readonly_error(self, operation: str) -> NotImplementedError:
        return NotImplementedError(
            f"GitLab FileStore is read-only; {operation} is not supported."
        )

    def createFileToken(self, filename: str) -> str:
        raise self._readonly_error("createFileToken")

    def listFiles(self, prefix: str = "", suffix: str = "") -> list:
        return []

    def _parse_branch_and_repo_path(self, file_token: str) -> tuple[str, str]:
        """Return (branch, repo_relative_path) from token ``gitlab://store/branch/path/to/file``."""
        relative = self._extract_filepath_from_token(file_token)
        if not relative:
            raise ValueError(
                "Invalid GitLab file token: missing branch and file path after store name."
            )
        parts = relative.split("/")
        branch = parts[0]
        if not branch:
            raise ValueError("Invalid GitLab file token: empty branch name.")
        repo_path = "/".join(parts[1:]) if len(parts) > 1 else ""
        if not repo_path:
            raise ValueError(
                "Invalid GitLab file token: missing repository file path after branch."
            )
        return branch, repo_path

    def _raw_url(self, repo_path: str) -> str:
        encoded = quote(repo_path, safe="")
        return urljoin(
            f"{self.base_uri}/",
            f"api/v4/projects/{self.project_id}/repository/files/{encoded}/raw",
        )

    def getFileName(self, fileToken: str) -> str:
        _, repo_path = self._parse_branch_and_repo_path(fileToken)
        return posixpath.basename(repo_path) if repo_path else ""

    def openRead(self, fileToken: str):
        branch, repo_path = self._parse_branch_and_repo_path(fileToken)
        url = self._raw_url(repo_path)
        response = self._session.get(url, params={"ref": branch})
        response.raise_for_status()
        return response.content

    def openWrite(self, fileToken: str, content: bytes):
        raise self._readonly_error("openWrite")

    def delete(self, fileToken: str):
        raise self._readonly_error("delete")

    def exists(self, fileToken: str) -> bool:
        branch, repo_path = self._parse_branch_and_repo_path(fileToken)
        url = self._raw_url(repo_path)
        response = self._session.get(url, params={"ref": branch}, stream=True)
        try:
            if response.status_code == 404:
                return False
            response.raise_for_status()
            return True
        finally:
            response.close()
