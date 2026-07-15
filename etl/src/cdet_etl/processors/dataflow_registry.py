import threading
from cdet_etl.config.base_config_repository import BaseConfigRepository
from cdet_etl.config.env_property_resolver import EnvPropertyResolver
from cdet_etl.config.multifile_config_repository import MultiFileConfigRepository

from cdet_etl.processors.dataflow_factory import DataFlowFactory
from cdet_etl.processors.dataflow_processor import DataFlowProcessor

class DataFlowRegistry:
    """
    Thread-Safe Dynamic Pipeline Infrastructure Cache.
    Thread-locked to handle concurrent data processing without data races.
    """
    def __init__(self, *, repository: BaseConfigRepository = None):
        self._repository = repository or MultiFileConfigRepository()
        self._processor_cache = {}
        self._lock = threading.RLock()

    def get_processor(self, dataflow_id: str):
        """
        Thread-safe retrieval of an operational processor.
        Synchronizes cache evictions and cache-miss constructions cleanly.
        """
        with self._lock:
            try:
                if self._repository.is_modified(dataflow_id):
                    if dataflow_id in self._processor_cache:
                        print(f"[THREAD LOCK CACHE INVALIDATE] Invalidation triggered by thread: {threading.get_ident()} for '{dataflow_id}'")
                        del self._processor_cache[dataflow_id]

                if dataflow_id in self._processor_cache:
                    return self._processor_cache[dataflow_id]
                
                return self._load_processor(dataflow_id)

            except Exception as err:
                print(f"[REGISTRY SECURITY RECOVERY] Invalidation failed. Preserving safe engine cache state. Detail: {err}")
                if dataflow_id in self._processor_cache:
                    return self._processor_cache[dataflow_id]
                raise err

    def _load_processor(self, dataflow_id: str) -> DataFlowProcessor:
        """
        Instiantite new processor and cache in memory for repeated use
        """
        print(f"[THREAD LOCK CACHE MISS] Building processor via thread: {threading.get_ident()} for '{dataflow_id}'")
        raw_blueprint = self._repository.fetch_blueprint(dataflow_id)
        resolved_blueprint = EnvPropertyResolver.resolve_properties(raw_blueprint)
        compiled_processor = DataFlowFactory.create_processor(resolved_blueprint)
        self._processor_cache[dataflow_id] = compiled_processor
        return compiled_processor
