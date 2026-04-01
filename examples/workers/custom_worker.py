from bevault_workers import BaseWorker
from bevault_workers import StoreRegistry

class CustomWorker(BaseWorker):
    name = "my_custom_worker"
    def handle(self, input_data):
        """
        Example: load a DbStore by registry name.

        ``input_data["outputStore"]`` must match a store ``Name`` in config.json
        (e.g. ``example-db-store`` when using ``examples/config.json.sample``).
        Implement ``examples/stores/custom_db_store.py`` before expecting real
        SQL execution.
        """
        logger = self.get_logger()
        logger.info(f"Starting executing custom worker") 
        try:
            #This is how you can get a store from the registry
            postgreStore = StoreRegistry.get(input_data["outputStore"])
            result = postgreStore.execute(query = f"SELECT VERSION()")
            
            return {
                "status": "success",
                "result": result
            }
        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e)
            }

