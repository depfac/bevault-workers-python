import json
from types import MethodType

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError

TARGET_SYNC_STORES = "AWSStepFunctions.DfaktoStatesSyncStores"
TARGET_FORCE_CHECK = "AWSStepFunctions.DfaktoStatesGetStoreForceCheckRequests"
TARGET_POST_STATUS = "AWSStepFunctions.DfaktoStatesPostStoreStatus"
API_VERSION = "2016-11-23"


def _get_frozen_credentials(stepfunctions_client):
    credentials = stepfunctions_client._request_signer._credentials
    if hasattr(credentials, "get_frozen_credentials"):
        return credentials.get_frozen_credentials()
    return credentials


def _invoke_dfakto_extension(stepfunctions_client, amz_target: str, payload: dict):
    endpoint = stepfunctions_client.meta.endpoint_url.rstrip("/") + "/"
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": amz_target,
        "X-Amz-Api-Version": API_VERSION,
    }
    request = AWSRequest(method="POST", url=endpoint, data=body, headers=headers)
    credentials = _get_frozen_credentials(stepfunctions_client)
    signer = SigV4Auth(credentials, "states", stepfunctions_client.meta.region_name)
    signer.add_auth(request)
    prepared_request = request.prepare()
    response = stepfunctions_client._endpoint.http_session.send(prepared_request)

    status_code = response.status_code
    if status_code == 201:
        return None
    if status_code >= 400:
        body_text = response.text or ""
        raise ClientError(
            {
                "Error": {
                    "Code": f"HTTP{status_code}",
                    "Message": body_text,
                },
                "ResponseMetadata": {"HTTPStatusCode": status_code},
            },
            amz_target.split(".")[-1],
        )

    if not response.content:
        return {}
    return json.loads(response.content.decode("utf-8"))


def attach_dfakto_states_extensions(stepfunctions_client):
    def dfakto_states_sync_stores(self, request_payload: dict, timeout: int):
        return _invoke_dfakto_extension(self, TARGET_SYNC_STORES, request_payload)

    def dfakto_states_get_store_force_check_requests(
        self, request_payload: dict, timeout: int
    ):
        return _invoke_dfakto_extension(self, TARGET_FORCE_CHECK, request_payload)

    def dfakto_states_post_store_status(self, request_payload: dict, timeout: int):
        return _invoke_dfakto_extension(self, TARGET_POST_STATUS, request_payload)

    stepfunctions_client.dfakto_states_sync_stores = MethodType(
        dfakto_states_sync_stores, stepfunctions_client
    )
    stepfunctions_client.dfakto_states_get_store_force_check_requests = MethodType(
        dfakto_states_get_store_force_check_requests, stepfunctions_client
    )
    stepfunctions_client.dfakto_states_post_store_status = MethodType(
        dfakto_states_post_store_status, stepfunctions_client
    )
    return stepfunctions_client
