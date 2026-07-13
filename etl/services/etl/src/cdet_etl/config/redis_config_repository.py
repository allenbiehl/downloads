import json
import threading
from cdet_etl.config.base_config_repository import BaseConfigRepository

class RedisConfigRepository(BaseConfigRepository):
    """Thread-safe Redis integration monitoring repository."""
    def __init__(self, *, redis_client, prefix: str = "dataflow_config"):
        self._client = redis_client
        self._prefix = prefix.strip(":")
        self._version_cache: dict[str, str] = {}
        self._lock = threading.Lock()

    def _build_keys(self, dataflow_id: str) -> tuple[str, str]:
        return f"{self._prefix}:{dataflow_id}", f"{self._prefix}:version:{dataflow_id}"

    def is_modified(self, dataflow_id: str) -> bool:
        _, version_key = self._build_keys(dataflow_id)
        current_version_bytes = self._client.get(version_key)
        if not current_version_bytes:
            return False
            
        current_version = current_version_bytes.decode("utf-8")
        
        with self._lock:
            last_version = self._version_cache.get(dataflow_id, "")
            return current_version != last_version

    def fetch_blueprint(self, dataflow_id: str) -> dict:
        data_key, version_key = self._build_keys(dataflow_id)
        raw_payload = self._client.get(data_key)
        if not raw_payload:
            raise KeyError(f"No key mapping found for target: '{data_key}'")

        current_version_bytes = self._client.get(version_key)
        if current_version_bytes:
            with self._lock:
                self._version_cache[dataflow_id] = current_version_bytes.decode("utf-8")

        return json.loads(raw_payload.decode("utf-8"))
