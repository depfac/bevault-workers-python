from bevault_workers.workers.worker_manager import BaseWorker
from bevault_workers.stores.store_registry import StoreRegistry


class DataProcessorWorker(BaseWorker):
    name = "test_postgres"

    def handle(self, input_data):
        """
        Run a simple query against the configured PostgreSQL store (current database).
        """
        try:
            postgre_store = StoreRegistry.get(input_data["storeName"])
            postgre_store.connect()
            result = postgre_store.execute(query="SELECT current_database()")

            return {"status": "success", "result": result}  # type: ignore
        except Exception as e:
            return {"status": "error", "error_message": str(e)}  # type: ignore
