import stat
import posixpath
from io import BytesIO
import paramiko
from .base_store import FileStore
from .store_utils import get_first


class Store(FileStore):
    def __init__(self, config):
        self.host = get_first(config, "host", "hostName")
        self.port = get_first(config, "port") or 22
        self.username = get_first(config, "username")
        self.password = get_first(config, "password")
        self.key_filename = get_first(
            config, "keyFilename", "keyPath", "privateKeyPath"
        )
        self.key_password = get_first(config, "keyPassword", "passphrase")
        self.prefix = get_first(config, "prefix", "basePath") or "/"
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"
        self.ssh_client = None
        self.sftp_client = None

    def _close_connection(self):
        if self.sftp_client is not None:
            try:
                self.sftp_client.close()
            except Exception:
                pass
            self.sftp_client = None
        if self.ssh_client is not None:
            try:
                self.ssh_client.close()
            except Exception:
                pass
            self.ssh_client = None

    def _connection_alive(self):
        if self.ssh_client is None:
            return False
        transport = self.ssh_client.get_transport()
        if transport is None:
            return False
        return transport.is_active()

    def connect(self):
        self._close_connection()
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {
            "hostname": self.host,
            "port": self.port,
            "username": self.username,
        }
        if self.key_filename:
            connect_kwargs["key_filename"] = self.key_filename
        if self.key_password:
            connect_kwargs["passphrase"] = self.key_password
        if self.password:
            connect_kwargs["password"] = self.password
        self.ssh_client.connect(**connect_kwargs)
        self.sftp_client = self.ssh_client.open_sftp()

    def _ensure_connection(self):
        if not self._connection_alive():
            self._close_connection()
            self.connect()

    def _get_remote_path(self, fileToken):
        """Extract filepath from filetoken and add the configured prefix."""
        filepath = self._extract_filepath_from_token(fileToken)
        return self._add_prefix_to_path(filepath)

    def _mkdir_p(self, path):
        """Create parent directories for the given path if they don't exist."""
        dir_path = posixpath.dirname(path)
        if not dir_path or dir_path == ".":
            return
        parts = [p for p in dir_path.split("/") if p]
        current = ""
        for part in parts:
            current = f"{current}/{part}" if current else f"/{part}"
            try:
                self.sftp_client.stat(current)
            except (IOError, OSError):
                self.sftp_client.mkdir(current)

    def createFileToken(self, filename):
        """Create a filetoken without including the configured prefix.
        
        The prefix is stored in the store configuration and will be added
        internally when the filetoken is used.
        """
        path = filename.lstrip("/")
        return f"sftp://{self.host}/{path}"

    def listFiles(self, prefix="", suffix=""):
        self._ensure_connection()
        base = (self.prefix + prefix).rstrip("/") or "."
        result = []

        def walk(dir_path):
            try:
                for entry in self.sftp_client.listdir_attr(dir_path):
                    full_path = (
                        f"{dir_path}/{entry.filename}"
                        if dir_path != "."
                        else entry.filename
                    )
                    if dir_path != "." and not full_path.startswith("/"):
                        full_path = f"/{full_path}"
                    if entry.st_mode is not None and stat.S_ISDIR(entry.st_mode):
                        walk(full_path)
                    elif entry.filename.endswith(suffix):
                        # Remove prefix from path for token (prefix is in config, not token)
                        relative_path = self._remove_prefix_from_path(full_path)
                        path_for_token = (
                            f"/{relative_path}"
                            if not relative_path.startswith("/")
                            else relative_path
                        )
                        token = f"sftp://{self.host}{path_for_token}"
                        result.append(token)
            except (IOError, OSError):
                pass

        walk(base)
        return result

    def getFileName(self, fileToken):
        filepath = self._extract_filepath_from_token(fileToken)
        return posixpath.basename(filepath)

    def openRead(self, fileToken):
        self._ensure_connection()
        path = self._get_remote_path(fileToken)
        with self.sftp_client.file(path, "r") as f:
            return f.read()

    def openWrite(self, fileToken, content: bytes):
        self._ensure_connection()
        path = self._get_remote_path(fileToken)
        self._mkdir_p(path)
        self.sftp_client.putfo(BytesIO(content), path)

    def delete(self, fileToken):
        self._ensure_connection()
        path = self._get_remote_path(fileToken)
        self.sftp_client.remove(path)

    def exists(self, fileToken):
        self._ensure_connection()
        path = self._get_remote_path(fileToken)
        try:
            self.sftp_client.stat(path)
            return True
        except (IOError, OSError):
            return False
