import threading
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

SOURCE_LOCAL = "local"
SOURCE_STATES = "states"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class StoreStatusTracker:
    def __init__(self):
        self._lock = threading.RLock()
        self._data: Dict[Tuple[str, str], dict] = {}

    def mark(self, source: str, name: str, status: str, message: Optional[str] = None):
        now = utc_now_iso()
        key = (source, name)
        with self._lock:
            current = self._data.get(key, {})
            last_success = current.get("lastSuccessDate")
            if status in ("Success", "Disabled"):
                last_success = now
            self._data[key] = {
                "name": name,
                "status": status,
                "lastUpdateDate": now,
                "lastSuccessDate": last_success,
                "message": message,
            }

    def snapshot_payload(self, environment_name: str) -> dict:
        with self._lock:
            states = []
            local = []
            for (source, _), value in self._data.items():
                if source == SOURCE_STATES:
                    states.append(dict(value))
                else:
                    local.append(dict(value))
        return {
            "environmentName": environment_name,
            "statesStoreStatus": states,
            "localStoreStatus": local,
        }
