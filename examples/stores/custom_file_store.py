"""Example custom FileStore template (explicit Type: ``stores.custom_file_store``).

Implement the methods below before using this store in production. See
:class:`bevault_workers.stores.base_store.FileStore` for the full contract.
Bare Types like ``s3`` resolve to ``stores.s3``; use a neutral module name here
and set ``Type`` to ``stores.custom_file_store`` in config.
"""

from bevault_workers import FileStore


class Store(FileStore):
    def connect(self):
        raise NotImplementedError

    def createFileToken(self, filename: str) -> str:
        raise NotImplementedError

    def listFiles(self, prefix: str = "", suffix: str = "") -> list:
        raise NotImplementedError

    def getFileName(self, fileToken: str) -> str:
        raise NotImplementedError

    def openRead(self, fileToken: str):
        raise NotImplementedError

    def openWrite(self, fileToken: str, content: bytes):
        raise NotImplementedError

    def delete(self, fileToken: str):
        raise NotImplementedError

    def exists(self, fileToken: str) -> bool:
        raise NotImplementedError
