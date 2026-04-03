# beVault Workers Library

This library is used to create **custom workers** for beVault’s orchestrator, **States**. 
See the [Architecture & Installation](https://support.bevault.io/en/bevault-documentation/current-version/architecture-installation) overview for how States, workers, and stores fit together.

The library aims to simplify worker creation and runtime handling while letting you reuse **beVault’s store configuration in States** ([Stores reference](https://support.bevault.io/en/bevault-documentation/current-version/reference-guide/states-workers-reference-guide/stores)). Use it when the [built-in workers](https://support.bevault.io/en/bevault-documentation/current-version/reference-guide/states-workers-reference-guide/workers) are not enough for your data project—for example: extracting a specific file format, running heavier logic in parallel, or integrating external tools.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [How to use this library](#how-to-use-this-library)
  - [Minimal setup (examples)](#minimal-setup-examples)
  - [Create custom workers](#create-custom-workers)
  - [Create your own store](#create-your-own-store)
  - [Override existing stores](#override-existing-stores)
  - [Configuration reference](#configuration-reference)
- [Contribute to this project](#contribute-to-this-project)

## Features

- **Easy worker creation**: `BaseWorker` subclasses, `WorkerManager`, and automatic discovery of workers from a Python package (`workers_module`).
- **Pluggable data stores**: Supports the same store types as beVault ([Stores reference](https://support.bevault.io/en/bevault-documentation/current-version/reference-guide/states-workers-reference-guide/stores)). You can add your own store implementations or override built-in ones. As of beVault 3.10, you can use the data store definitions configured in States from your workers (see [States store synchronization](#states-store-synchronization)); a local definition in `config.json` overrides States for the same store name.
- **Configurable**: JSON-based configuration for stores, optional logging configuration file.
- **Auto-discovery**: Workers are registered automatically from the configured package.
- **Built-in logging**: Configurable logging outputs (see [logging_config.json](#logging_configjson-file)).
- **Robust error handling**: Graceful shutdown and error recovery paths in the worker manager.
- **Scalable**: Multi-process execution with configurable concurrency so you can run several worker instances.

## Installation

### From PyPI

```bash
pip install bevault_workers
```

Import name and PyPI package: `bevault_workers`.

```python
import bevault_workers
```

### For development

Clone this repository and install in editable mode with development extras:

```powershell
git clone https://github.com/depfac/bevault-workers-python.git
cd bevault-workers-python
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

From the repository root you can run `python main.py` to exercise workers under `dev_workers/` and validate store behavior. For a consumer-style layout, use the [`examples/`](examples/) folder (see below).

## How to use this library

### Minimal setup (examples)

The fastest way to start is to copy the [`examples/`](examples/) folder into a new project and adjust filenames as needed:

| Source | Purpose |
|--------|---------|
| [`examples/main.py`](examples/main.py) | Entry point: `WorkerManager` with `config_path` and `workers_module` |
| [`examples/.env.sample`](examples/.env.sample) | Copy to `.env` and set Step Functions / States credentials and URLs |
| [`examples/logging_config.json.sample`](examples/logging_config.json.sample) | Copy to `logging_config.json` (optional); or set `logging_config_path` in `.env` |
| [`examples/config.json.sample`](examples/config.json.sample) | Copy to `config.json` (Optional); Used to configure localy the stores available for your workers |
| [`examples/workers/custom_worker.py`](examples/workers/custom_worker.py) | At least one worker module extending `BaseWorker` |
| [`examples/requirements.txt`](examples/requirements.txt) | Pin `bevault-workers` for your app |

You do **not** need a custom store module for this minimal path: store types such as `postgresql`, `s3`, and `sftp` are provided by this library. For a walkthrough that replaces a built-in store with your own module, see [`examples/stores/`](examples/stores/).

### Create custom workers

Workers are loaded from the package passed to `WorkerManager` (`workers_module`, default `"workers"`). Each worker must subclass `BaseWorker` and set a unique `name`.

```python
from bevault_workers import BaseWorker

class DataProcessorWorker(BaseWorker):
    name = "my_custom_worker"

    def handle(self, input_data):
        try:
            processed_data = self.process_data(input_data)
            return {"status": "success", "result": processed_data}
        except Exception as e:
            return {"status": "error", "error_message": str(e)}

    def process_data(self, data):
        return {"processed": True, "data": data}
```

Example using a database store by name (the name must match a store `Name` in `config.json`):

```python
from bevault_workers import BaseWorker, StoreRegistry

class DataProcessorWorker(BaseWorker):
    name = "my_custom_worker"

    def handle(self, input_data):
        try:
            db = StoreRegistry.get(input_data["dbStore"])
            result = db.execute("SELECT VERSION()")
            return {"status": "success", "result": result}
        except Exception as e:
            return {"status": "error", "error_message": str(e)}
```

### Create your own store

Stores are the connectors your workers use to read or write data. Configure instances in [`config.json`](#configjson-file); resolve them in code with `StoreRegistry.get("<Name>")`.

**Database stores (`DbStore`)** expose SQL. You must implement `connect()` and `execute(query, params=None)`. SELECT queries should return rows; other statements typically return affected row count.

**File stores (`FileStore`)** expose file-like operations using opaque file tokens (for example `s3://bucket/key`). You must implement `connect()`, `createFileToken`, `listFiles`, `getFileName`, `openRead`, `openWrite`, `delete`, and `exists`.

Illustrative `DbStore` implementation:

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
                if cur.description:
                    results = cur.fetchall()
                    return results if results else None
                return cur.rowcount
```

Example worker using an SFTP-backed `FileStore`:

```python
from bevault_workers import BaseWorker, StoreRegistry

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

### Override existing stores

You can replace a **built-in** implementation by adding a module in **your** project whose **basename** matches the bare **Type** (for example, `Type: "postgresql"` resolves to `bevault_workers.stores.postgresql` first, then your `stores.postgresql`). The store **Name** in JSON is only the instance identifier for `StoreRegistry.get(...)`, not the implementation selector.

To use a neutral filename (for example `stores/custom_db_store.py`), set **Type** to a fully qualified path such as `stores.custom_db_store` or `stores.custom_db_store:Store` instead of a bare name like `postgresql`.

### Configuration reference

#### Project structure

Recommended layout for an application that uses this library:

```text
your-project/
├── main.py
├── config.json
├── logging_config.json          # optional
├── .env
├── workers/                     # or another package; set workers_module
│   └── ...
└── requirements.txt
```

#### .env file

Create a `.env` file (see [`examples/.env.sample`](examples/.env.sample)) with variables such as:

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
logging_config_path=logging_config.json
```

| Variable | Description |
|----------|-------------|
| `stepFunctions__authenticationKey` | Authentication key for States (see [documentation](https://support.bevault.io/en/bevault-documentation/current-version/reference-guide/states-user-reference-guide/settings/secret-management)) |
| `stepFunctions__authenticationSecret` | Authentication secret for States |
| `stepFunctions__awsRegion` | Set 'us-east-1' if you use States. If you use AWS Step Functions, set it to the region of your instance. |
| `stepFunctions__DefaultHeartbeatDelay` | Heartbeat delay in seconds (default: 5) |
| `stepFunctions__DefaultMaxConcurrency` | Maximum concurrency (default: 3) |
| `stepFunctions__EnvironmentName` | Environment name (default: python) |
| `stepFunctions__serviceUrl` | Service URL for States (default: states) |
| `stepFunctions__enableStatesStoreSync` | Enable States store synchronization (default: false). Only compatible with beVault's version 3.10 and above |
| `stepFunctions__statesStoreBaseUrl` | Optional override for the store sync API base URL |
| `stepFunctions__statesPollTimeoutSeconds` | Long-poll timeout in seconds (default: 70) |
| `stepFunctions__statesStatusHeartbeatSeconds` | Store status heartbeat period in seconds (default: 60) |
| `stepFunctions__statesRequestTimeoutSeconds` | Non-long-poll request timeout in seconds (default: 15) |
| `logging_config_path` | Optional path to logging JSON (default: `logging_config.json`) |

#### States store synchronization

This feature is available only with **beVault 3.10 and above**. When you set `stepFunctions__enableStatesStoreSync=true`, the worker process can **reuse the data store definitions configured in States**, so you do not have to duplicate them entirely in your project. If the same store is **also** defined in your local `config.json`, **the local definition takes precedence** and is used instead of the one coming from States.

#### config.json file

You can configure stores in two ways:

- **Locally in `config.json`** (typical for development, overrides, or runs without States sync)
- **From beVault States** when store synchronization is enabled (`stepFunctions__enableStatesStoreSync=true`, beVault 3.10+); local entries override States for the same store name

For field-level details and supported store types, see the [Stores reference](https://support.bevault.io/en/bevault-documentation/current-version/reference-guide/states-workers-reference-guide/stores).

Example with PostgreSQL and SFTP:

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
        "Name": "sqlServerStore",
        "Type": "sqlserver",
        "Config": {
            "host": "",
            "port": "1433",
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

#### logging_config.json file

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

## Contribute to this project

Contributions are welcome. **New or improved store implementations** are especially valuable because they extend interoperability with beVault and the States ecosystem. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests, and pull request guidelines.
