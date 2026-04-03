import base64

from bevault_workers.workers.worker_manager import BaseWorker
from bevault_workers.stores.store_registry import StoreRegistry


class ReadFileTokenWorker(BaseWorker):
    name = "read_filetoken"

    def handle(self, input_data):
        """
        Read a file using a file token and return its content (base64) and file name.

        input_data:
            fileToken (str): e.g. gitlab://<store>/<branch>/path/to/file
        """
        try:
            file_token = input_data["fileToken"]
            store = StoreRegistry.get_store_from_filetoken(file_token)
            store.connect()
            data = store.openRead(file_token)
            file_name = store.getFileName(file_token)
            return {
                "status": "success",
                "result": {
                    "contentBase64": data.decode("utf-8"),
                    "fileName": file_name,
                },
            }  # type: ignore
        except Exception as e:
            return {"status": "error", "error_message": str(e)}  # type: ignore
