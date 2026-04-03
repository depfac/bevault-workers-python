from bevault_workers import BaseWorker
from bevault_workers import StoreRegistry

class CustomWorker(BaseWorker):
    name = "my_custom_worker"

    def handle(self, input_data):
        """
        Example: This worker demonstrates how to use a PostgreSQL store to get the database version.

        The value of ``input_data["outputStore"]`` must correspond to a store ``Name`` defined in your ``config.json`` or in States.
        The worker retrieves the referenced store from the registry and executes a simple SQL query to fetch the database version.
        """
        logger = self.get_logger()
        logger.info("Starting executing custom worker")
        try:
            store = StoreRegistry.get(input_data["outputStore"])
            result = store.execute(query="SELECT VERSION()")

            return {
                "status": "success",
                "result": result,
            }
        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e),
            }
