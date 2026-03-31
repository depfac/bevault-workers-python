from typing import Optional


class StatesStoreApiClient:
    def __init__(
        self,
        stepfunctions_client,
        poll_timeout_seconds: int = 70,
        request_timeout_seconds: int = 15,
    ):
        self.client = stepfunctions_client
        self.poll_timeout_seconds = poll_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds

    def sync_stores(self, payload: dict) -> dict:
        return self.client.dfakto_states_sync_stores(
            payload, timeout=self.poll_timeout_seconds
        )

    def get_force_check_requests(self, payload: dict) -> Optional[dict]:
        return self.client.dfakto_states_get_store_force_check_requests(
            payload, timeout=self.poll_timeout_seconds
        )

    def post_store_status(self, payload: dict) -> None:
        self.client.dfakto_states_post_store_status(
            payload, timeout=self.request_timeout_seconds
        )
