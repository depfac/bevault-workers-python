import json

from bevault_workers.workers.worker_manager import BaseWorker
from bevault_workers.stores.store_registry import StoreRegistry


class SnowflakeProbeWorker(BaseWorker):
    name = "test_snowflake"

    def handle(self, input_data):
        """
        Run a simple query against the configured Snowflake store.

        input_data:
            storeName (str): store name from config
            query (str, optional): SQL to run; default ``SELECT CURRENT_DATABASE()``
        """
        try:
            store = StoreRegistry.get(input_data["storeName"])
            store.connect()
            query = input_data.get("query") or "SELECT CURRENT_DATABASE()"
            result = store.execute(query)

            return {"status": "success", "result": json.dumps(result, default=str)}  # type: ignore
        except Exception as e:
            return {"status": "error", "error_message": str(e)}  # type: ignore
