# README.md
# beVault Workers Library

A Python library for creating beVault's States workers with pluggable data stores. This library provides everything for building scalable, distributed workers that can process Step Functions activities with support for multiple data backends.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Workers](#workers)
- [Stores](#stores)

## Features

- 🚀 **Easy Worker Creation**: Simple base class for implementing custom workers
- 🔌 **Pluggable Data Stores**: Support for PostgreSQL, S3, SFTP, and custom stores. Plus, the possibility to create your own data stores
- ⚙️ **Configurable**: JSON-based configuration for stores
- 🔄 **Auto-Discovery**: Automatic worker discovery and registration
- 📊 **Built-in Logging**: Comprehensive logging with configurable outputs
- 🛡️ **Robust Error Handling**: Graceful shutdown and error recovery
- 📈 **Scalable**: Multi-process architecture with configurable concurrency

## Installation

### From PyPI

Install the latest published package from PyPI:

```bash
pip install bevault_workers
```

### For development

Clone this project and install the library in editable mode with development extras:

```powershell
git clone https://github.com/depfac/bevault-workers-python.git
cd python-worker-framework
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

Run the local entrypoint to test workers from `dev_workers` and validate store behavior:

```powershell
python main.py
```

After that, you can import the library in your project.
**Warning** The library is named `bevault_workers` (PyPI package `bevault_workers`)
```python
import bevault_workers
```

## Project Structure

Here's a recommended structure for a basic project using the library:
```text
python-worker-framework/
├── main.py                        # Dev entrypoint (loads dev_workers)
├── config.json                    # Local store configuration
├── logging_config.json            # Logging configuration (optional)
├── dev_workers/                   # Workers used for local testing
├── src/
│   └── bevault_workers/           # Published library package
│       ├── workers/
│       ├── stores/
│       └── utils/
├── tests/                         # Test suite
├── pyproject.toml                 # Package metadata and dependencies
└── README.md
```
### main.py example
```python
from bevault_workers import WorkerManager

def main():
    """Main entry point for the worker application"""
    try:
        # Initialize the worker manager with configuration
        manager = WorkerManager(
            config_path="config.json",
            workers_module="workers"  # Discover workers from the 'workers' module
        )

        # Start the worker manager (this will block until stopped)
        manager.start()

    except KeyboardInterrupt:
        print("Application interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
        raise

if __name__ == "__main__":
    main()
```
### Add your custom workers
CFR [here](#workers) to see how to create a custom worker

### .env file
Create a `.env` file in the root directory with the following variables:

```
stepFunctions__authenticationKey=YOUR_AUTH_KEY
stepFunctions__authenticationSecret=YOUR_AUTH_SECRET
stepFunctions__awsRegion=us-east-1
stepFunctions__DefaultHeartbeatDelay=5
stepFunctions__DefaultMaxConcurrency=3
stepFunctions__EnvironmentName=python
stepFunctions__roleArn=YOUR_ROLE_ARN
stepFunctions__serviceUrl=df2-states
stepFunctions__enableStatesStoreSync=false
stepFunctions__statesStoreBaseUrl=
stepFunctions__statesPollTimeoutSeconds=70
stepFunctions__statesStatusHeartbeatSeconds=60
stepFunctions__statesRequestTimeoutSeconds=15
```
#### Environment Variables Details

| Variable | Description |
|----------|-------------|
| `stepFunctions__authenticationKey` | Authentication key for AWS Step Functions |
| `stepFunctions__authenticationSecret` | Authentication secret for AWS Step Functions |
| `stepFunctions__awsRegion` | AWS region (default: us-east-1) |
| `stepFunctions__DefaultHeartbeatDelay` | Heartbeat delay in seconds (default: 5) |
| `stepFunctions__DefaultMaxConcurrency` | Maximum concurrency (default: 3) |
| `stepFunctions__EnvironmentName` | Environment name (default: python) |
| `stepFunctions__roleArn` | AWS IAM role ARN |
| `stepFunctions__serviceUrl` | Service URL for Step Functions (default: df2-states) |
| `stepFunctions__enableStatesStoreSync` | Enable dFakto States store synchronization (default: false) |
| `stepFunctions__statesStoreBaseUrl` | Optional override for the store sync API base URL |
| `stepFunctions__statesPollTimeoutSeconds` | Long-poll timeout in seconds (default: 70) |
| `stepFunctions__statesStatusHeartbeatSeconds` | Store status heartbeat period in seconds (default: 60) |
| `stepFunctions__statesRequestTimeoutSeconds` | Non-long-poll request timeout in seconds (default: 15) |

### States store synchronization
When `stepFunctions__enableStatesStoreSync=true`, the worker manager starts a background synchronization service that:

- Uses AWS-signed Step Functions extension calls (`X-Amz-Target`) for dFakto store APIs.
- Sends local stores to `DfaktoStatesSyncStores`.
- Long-polls for states-defined stores and refreshes `StoreRegistry` in memory.
- Long-polls force-check requests with `DfaktoStatesGetStoreForceCheckRequests`.
- Posts periodic and on-demand store status updates to `DfaktoStatesPostStoreStatus`.

If a local and a states-defined store share the same name, the states store is registered with the internal prefix `states::` (for example `states::myStore`) so both instances remain addressable.

### config.json file
You can configure stores in two ways:

- **Locally in `config.json`** (recommended for development and local runs)
- **In beVault States** by enabling store synchronization (`stepFunctions__enableStatesStoreSync=true`)

For full store configuration details and supported store types, refer to the official beVault documentation: [Stores reference](https://support.bevault.io/en/bevault-documentation/current-version/reference-guide/states-workers-reference-guide/stores).

If you use local configuration, create a `config.json` file in the root directory. Example:

```json
[
    {
        "Name": "postgresqlStore",
        "Type": "postgresql",
        "Config": {
            "host": "",
            "port": "5432",
            "user": "",
            "password": "",
            "dbname": ""
        }
    },
    {
        "Name": "sftpStore",
        "Type": "sftp",
        "Config": {
            "Host": "sftp.example.com",
            "Port": 22,
            "Username": "myuser",
            "Password": "secret",
            "Prefix": "/uploads/"
        }
    }
]
```

For key-based authentication with SFTP, use `KeyFilename` instead of `Password`:

```json
"KeyFilename": "/path/to/private_key"
```

### States/.NET compatibility for existing stores

The stores keep backward compatibility with the legacy Python config keys and also accept States/.NET-style keys for:

- `postgresql`
- `s3`
- `sftp`

For database stores, when both parameter fields and a connection string are present:

- if `connectionString` (or `ConnectionString`) is non-empty, it is used;
- if it is empty or null, parameter fields are used.

Example States-style PostgreSQL config:

```json
{
  "host": "localhost",
  "port": 5432,
  "database": "northwind__mig__pgsql_migration",
  "username": "metavault",
  "password": "xxx",
  "connectionString": ""
}
```
### logging_config.json file

Create a `logging_config.json` file to configure the logging behavior:

```json
{
    "logging": {
      "minimumLevel": {
        "default": "Information",
        "override": {
          "urllib3": "Warning",
          "boto3": "Warning",
          "botocore": "Warning",
          "paramiko": "Warning",
          "requests": "Warning"
        }
      },
      "writeTo": [
        {
          "name": "Console",
          "args": {
            "outputTemplate": "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level}] {Message:lj} <s:{SourceContext}>{NewLine}{Exception}"
          }
        },
        {
          "name": "File",
          "args": {
            "path": "/var/logs/bevault_workers/bevault_workers.log",
            "rollOnFileSizeLimit": true,
            "fileSizeLimitBytes": 10000000,
            "retainedFileCountLimit": 10
          }
        }
      ]
    }
}
```

## Workers
Your custom workers will be automatically loaded from the package you specified while creating a new WorkerManager in your main.py.
If not specified, the package "workers" is scanned by default.

All your custom workers should extend the BaseWorker class from this bevault_workers library. Here is a simple example:
```python
from bevault_workers import BaseWorker

class DataProcessorWorker(BaseWorker):
  name = "my_custom_worker"

    def handle(self, input_data):
        """Process the input data"""
        try:
            # Your business logic here
            processed_data = self.process_data(input_data)

            return {
                "status": "success",
                "result": processed_data
            }
        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e)
            }

    def process_data(self, data):
        # Your custom processing logic
        return {"processed": True, "data": data}
```

## Stores
Stores are data endpoints that you can use to extract from or send data to with your workers. The make a store available in your workers, you need to configure them in your [config.json](#configjson-file) file.

Here is an example of worker that uses a postgresql store:
```python
from bevault_workers import BaseWorker

class DataProcessorWorker(BaseWorker):
    name = "my_custom_worker"
    def handle(self, input_data):
      """
      extract version number of PostgreSQL

      Args:
          dbStore: The PostgreSQL store
      Returns:
          Version number of postgresql
      """
        try:
            postgreStore = StoreRegistry.get(input_data["dbStore"])
            postgreStore.connect()
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
```

### Create your own Stores
You can create your own Store by creating a file with a class Store that extends the DbStore of FileStore class. Here is an example of custom implementation of the postgresql store:
**custom_postgresql.py**
```python
import psycopg
from bevault_workers import DbStore

class Store(DbStore):
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        self.connection = psycopg.connect(**self.config)

    def execute(self, query, params=None):
        with self.connection as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                # Check if the query might return results (has a description)
                if cur.description:
                    results = cur.fetchall()
                    # Return results only if there are any
                    return results if results else None

                # For non-SELECT queries, return affected row count
                return cur.rowcount
```
Here is an example of a worker that uses an SFTP store to read and write files:

```python
from bevault_workers import BaseWorker
from bevault_workers.stores.store_registry import StoreRegistry

class FileHandlerWorker(BaseWorker):
    name = "file_handler"
    def handle(self, input_data):
        sftp_store = StoreRegistry.get(input_data["fileStore"])
        sftp_store.connect()
        token = sftp_store.createFileToken("example.txt")
        sftp_store.openWrite(token, b"Hello from worker")
        content = sftp_store.openRead(token)
        return {"status": "success", "content": content.decode()}
```

Note that you can override the implementation of a **built-in** store by placing a module in your project whose **basename matches** the bare **Type** (for example, `Type: "postgresql"` resolves to `stores.postgresql`, then `bevault_workers.stores.postgresql`). The store **Name** in JSON is only the instance identifier passed to `StoreRegistry.get(...)` in workers, not the implementation selector.

To keep neutral filenames (for example `stores/custom_db_store.py`), set **Type** to a fully qualified path such as `stores.custom_db_store` or `stores.custom_db_store:Store` instead of a bare name like `postgresql`.

