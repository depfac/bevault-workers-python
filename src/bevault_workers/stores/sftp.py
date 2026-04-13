import errno
import stat
import posixpath
from io import BytesIO
import paramiko
from .base_store import FileStore
from .store_utils import get_first


def _connection_lost_exc(exc: BaseException) -> bool:
    """True if *exc* indicates the SSH/SFTP session is no longer usable."""
    if isinstance(exc, EOFError):
        return True
    if not isinstance(exc, OSError):
        return False
    msg = str(exc).lower()
    if "socket is closed" in msg:
        return True
    if exc.errno is not None and exc.errno in (
        errno.EPIPE,
        errno.ECONNRESET,
        errno.ENOTCONN,
        errno.ECONNABORTED,
    ):
        return True
    return False


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

    def _run_with_reconnect(self, fn):
        self._ensure_connection()
        try:
            return fn()
        except (OSError, EOFError) as e:
            if not _connection_lost_exc(e):
                raise
            self._close_connection()
            self.connect()
            return fn()

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
            except (IOError, OSError) as e:
                if _connection_lost_exc(e):
                    raise
                self.sftp_client.mkdir(current)

    def createFileToken(self, filename):
        """Create a filetoken without including the configured prefix.

        The prefix is stored in the store configuration and will be added
        internally when the filetoken is used.
        """
        path = filename.lstrip("/")
        return f"sftp://{self.host}/{path}"

    def listFiles(self, prefix="", suffix=""):
        def _do():
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
                            relative_path = self._remove_prefix_from_path(full_path)
                            path_for_token = (
                                f"/{relative_path}"
                                if not relative_path.startswith("/")
                                else relative_path
                            )
                            token = f"sftp://{self.host}{path_for_token}"
                            result.append(token)
                except (IOError, OSError) as e:
                    if _connection_lost_exc(e):
                        raise

            walk(base)
            return result

        return self._run_with_reconnect(_do)

    def getFileName(self, fileToken):
        filepath = self._extract_filepath_from_token(fileToken)
        return posixpath.basename(filepath)

    def openRead(self, fileToken):
        path = self._get_remote_path(fileToken)

        def _do():
            with self.sftp_client.file(path, "r") as f:
                return f.read()

        return self._run_with_reconnect(_do)

    def openWrite(self, fileToken, content: bytes):
        path = self._get_remote_path(fileToken)

        def _do():
            self._mkdir_p(path)
            self.sftp_client.putfo(BytesIO(content), path)

        return self._run_with_reconnect(_do)

    def delete(self, fileToken):
        path = self._get_remote_path(fileToken)

        def _do():
            self.sftp_client.remove(path)

        return self._run_with_reconnect(_do)

    def exists(self, fileToken):
        path = self._get_remote_path(fileToken)

        def _do():
            try:
                self.sftp_client.stat(path)
                return True
            except (IOError, OSError) as e:
                if _connection_lost_exc(e):
                    raise
                return False

        return self._run_with_reconnect(_do)
