"""Example custom DbStore template (explicit Type: ``stores.custom_db_store``).

Implement ``connect`` and ``execute`` before using this store in production.
Bare Types like ``postgresql`` resolve to ``stores.postgresql``; use a neutral
module name here and set ``Type`` to ``stores.custom_db_store`` in config.
"""

from bevault_workers import DbStore


class Store(DbStore):
    def __init__(self, config):
        self.config = config

    def connect(self):
        raise NotImplementedError

    def execute(self, query, params=None):
        raise NotImplementedError
