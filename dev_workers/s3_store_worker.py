from bevault_workers.workers.worker_manager import BaseWorker
from bevault_workers.stores.store_registry import StoreRegistry


class S3ListFilesWorker(BaseWorker):
    name = "test_s3"

    def handle(self, input_data):
        """
        List object keys in the configured S3 store bucket.

        input_data:
            storeName (str): store name from config
            prefix (str, optional): passed to listFiles
            suffix (str, optional): filter keys ending with this suffix
        """
        try:
            store = StoreRegistry.get(input_data["storeName"])
            store.connect()
            prefix = input_data.get("prefix", "")
            suffix = input_data.get("suffix", "")
            files = store.listFiles(prefix=prefix, suffix=suffix)
            return {"status": "success", "result": files}  # type: ignore
        except Exception as e:
            return {"status": "error", "error_message": str(e)}  # type: ignore
