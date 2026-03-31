import json
from unittest.mock import MagicMock, patch

from bevault_workers.stores.aws import (
    TARGET_FORCE_CHECK,
    TARGET_POST_STATUS,
    TARGET_SYNC_STORES,
    SOURCE_LOCAL,
    SOURCE_STATES,
    StatesStoreApiClient,
    StatesStoreSyncService,
    attach_dfakto_states_extensions,
)
from bevault_workers.stores.store_registry import StoreRegistry


class DummyHttpResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self.text = text
        self.content = (
            json.dumps(payload).encode("utf-8") if payload is not None else b""
        )


class DummySignerCredentials:
    def get_frozen_credentials(self):
        class Creds:
            access_key = "ak"
            secret_key = "sk"
            token = None

        return Creds()


def _build_fake_client(send_response):
    fake_client = MagicMock()
    fake_client.meta.endpoint_url = "http://states:5500"
    fake_client.meta.region_name = "us-east-1"
    fake_client._request_signer._credentials = DummySignerCredentials()
    fake_client._endpoint.http_session.send.return_value = send_response
    return fake_client


def test_attach_extension_uses_expected_targets():
    fake_client = _build_fake_client(
        DummyHttpResponse(payload={"statesStoresVersionToken": "vt1", "statesStores": []})
    )
    attach_dfakto_states_extensions(fake_client)

    fake_client.dfakto_states_sync_stores({"k": "v"}, timeout=1)
    request1 = fake_client._endpoint.http_session.send.call_args_list[0][0][0]
    assert request1.headers["X-Amz-Target"] == TARGET_SYNC_STORES

    fake_client.dfakto_states_get_store_force_check_requests({"k": "v"}, timeout=1)
    request2 = fake_client._endpoint.http_session.send.call_args_list[1][0][0]
    assert request2.headers["X-Amz-Target"] == TARGET_FORCE_CHECK

    fake_client.dfakto_states_post_store_status({"k": "v"}, timeout=1)
    request3 = fake_client._endpoint.http_session.send.call_args_list[2][0][0]
    assert request3.headers["X-Amz-Target"] == TARGET_POST_STATUS


def test_extension_returns_none_on_201():
    fake_client = _build_fake_client(DummyHttpResponse(status_code=201))
    attach_dfakto_states_extensions(fake_client)
    result = fake_client.dfakto_states_get_store_force_check_requests(
        {"environmentName": "python"}, timeout=1
    )
    assert result is None


def test_api_client_sync_stores():
    client_impl = MagicMock()
    client_impl.dfakto_states_sync_stores.return_value = {
        "statesStoresVersionToken": "vt1",
        "statesStores": [],
    }
    client = StatesStoreApiClient(stepfunctions_client=client_impl)

    result = client.sync_stores({"workerServiceEnvironmentName": "python"})
    client_impl.dfakto_states_sync_stores.assert_called_once()
    assert result["statesStoresVersionToken"] == "vt1"


def test_api_client_force_check_no_content():
    client_impl = MagicMock()
    client_impl.dfakto_states_get_store_force_check_requests.return_value = None
    client = StatesStoreApiClient(stepfunctions_client=client_impl)

    result = client.get_force_check_requests({"environmentName": "python"})
    assert result is None


@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_service_merges_local_and_states_stores(mock_resolve):
    class DummyStore:
        def __init__(self, config):
            self.config = config

        def connect(self):
            return None

    mock_resolve.return_value = DummyStore
    StoreRegistry.clear()

    client = MagicMock()
    service = StatesStoreSyncService(client=client, environment_name="python")
    local_defs = [{"Name": "localStore", "Type": "dummy", "Config": {"k": "v"}}]
    states_defs = [
        {
            "name": "statesStore",
            "type": "dummy",
            "enableHealthCheck": True,
            "healthCheckDelaySeconds": 10,
            "config": {"a": "b"},
        }
    ]

    merged = service._merge_store_definitions(local_defs, states_defs)
    assert len(merged) == 2
    assert StoreRegistry.get("localStore").config == {"k": "v"}
    assert StoreRegistry.get("statesStore").config == {"a": "b"}

    metadata = StoreRegistry.snapshot_metadata()
    assert metadata["localStore"]["source"] == SOURCE_LOCAL
    assert metadata["statesStore"]["source"] == SOURCE_STATES


@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_service_namespaces_states_collision(mock_resolve):
    class DummyStore:
        def __init__(self, config):
            self.config = config

    mock_resolve.return_value = DummyStore
    StoreRegistry.clear()

    client = MagicMock()
    service = StatesStoreSyncService(client=client, environment_name="python")
    local_defs = [{"Name": "same", "Type": "dummy", "Config": {"local": True}}]
    states_defs = [{"name": "same", "type": "dummy", "config": {"states": True}}]

    service._merge_store_definitions(local_defs, states_defs)

    assert StoreRegistry.get("same").config == {"local": True}
    assert StoreRegistry.get("states::same").config == {"states": True}


@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_service_skips_invalid_store_but_keeps_valid(mock_resolve):
    class DummyStore:
        def __init__(self, config):
            self.config = config

    def resolve(type_name):
        if type_name == "broken":
            raise ImportError("broken store type")
        return DummyStore

    mock_resolve.side_effect = resolve
    StoreRegistry.clear()

    client = MagicMock()
    service = StatesStoreSyncService(client=client, environment_name="python")
    local_defs = [{"Name": "goodLocal", "Type": "dummy", "Config": {"ok": 1}}]
    states_defs = [
        {"name": "goodStates", "type": "dummy", "config": {"ok": 2}},
        {"name": "badStates", "type": "broken", "config": {"bad": True}},
    ]

    service._merge_store_definitions(local_defs, states_defs)

    assert StoreRegistry.get("goodLocal").config == {"ok": 1}
    assert StoreRegistry.get("goodStates").config == {"ok": 2}
    assert "badStates" not in StoreRegistry.all()


@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_service_parses_states_json_string_config(mock_resolve):
    class DummyStore:
        def __init__(self, config):
            self.config = config

    mock_resolve.return_value = DummyStore
    StoreRegistry.clear()

    client = MagicMock()
    service = StatesStoreSyncService(client=client, environment_name="python")
    local_defs = []
    states_defs = [
        {
            "name": "postgres-wizard",
            "type": "dummy",
            "config": '{"host":"127.0.0.1","port":"5432","dbname":"postgres"}',
        }
    ]

    service._merge_store_definitions(local_defs, states_defs)

    assert StoreRegistry.get("postgres-wizard").config == {
        "host": "127.0.0.1",
        "port": "5432",
        "dbname": "postgres",
    }


@patch("bevault_workers.stores.store_registry.StoreRegistry._resolve_store_class")
def test_service_keeps_states_dict_config_shape(mock_resolve):
    class DummyStore:
        def __init__(self, config):
            self.config = config

    mock_resolve.return_value = DummyStore
    StoreRegistry.clear()

    client = MagicMock()
    service = StatesStoreSyncService(client=client, environment_name="python")
    local_defs = []
    states_defs = [
        {
            "name": "postgres-wizard",
            "type": "dummy",
            "config": {
                "host": "localhost",
                "port": 5432,
                "database": "northwind",
                "username": "metavault",
                "password": "xxx",
                "connectionString": "",
            },
        }
    ]

    service._merge_store_definitions(local_defs, states_defs)

    assert StoreRegistry.get("postgres-wizard").config == {
        "host": "localhost",
        "port": 5432,
        "database": "northwind",
        "username": "metavault",
        "password": "xxx",
        "connectionString": "",
    }
