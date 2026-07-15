import os
import threading
from cdet_etl.config.base_config_repository import BaseConfigRepository
from cdet_etl.config.file_config_loader import FileConfigLoader

class MultiFileConfigRepository(BaseConfigRepository):
    """Thread-safe Multi-File directory monitoring repository."""
    def __init__(self, *, directory_path: str = None):
        if not directory_path:
            directory_path = os.getenv("DATAFLOW_CONFIG_DIR")

        self._directory = directory_path.rstrip("/")
        self._timestamp_cache: dict[str, float] = {}
        self._lock = threading.Lock()

    def _build_path(self, dataflow_id: str) -> str:
        return f"{self._directory}/{dataflow_id}-config.yaml"

    def is_modified(self, dataflow_id: str) -> bool:
        file_path = self._build_path(dataflow_id)
        if not os.path.exists(file_path):
            return False

        with self._lock:
            current_mtime = os.path.getmtime(file_path)
            last_mtime = self._timestamp_cache.get(dataflow_id, 0.0)
            return current_mtime > last_mtime

    def fetch_blueprint(self, dataflow_id: str) -> dict:
        file_path = self._build_path(dataflow_id)
        config_data = FileConfigLoader.load_from_file(file_path)
        
        with self._lock:
            self._timestamp_cache[dataflow_id] = os.path.getmtime(file_path)
            
        return config_data
