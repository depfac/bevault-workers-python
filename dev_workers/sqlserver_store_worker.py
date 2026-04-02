import json
from bevault_workers.workers.worker_manager import BaseWorker
from bevault_workers.stores.store_registry import StoreRegistry


class SqlServerProbeWorker(BaseWorker):
    name = "test_sqlserver"

    def handle(self, input_data):
        """
        Run a simple query against the configured SQL Server store (current database).
        """
        try:
            sql_store = StoreRegistry.get(input_data["storeName"])
            sql_store.connect()
            result = sql_store.execute("SELECT DB_NAME() AS current_database")

            return {"status": "success", "result": json.dumps(result,default=str)}  # type: ignore
        except Exception as e:
            return {"status": "error", "error_message": str(e)}  # type: ignore
