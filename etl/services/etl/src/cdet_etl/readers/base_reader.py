from abc import ABC, abstractmethod

from cdet_etl.readers.base_data_source import BaseDataSource

class BaseReader(ABC):

    def __init__(self, properties: dict | None = None):
        self._properties = properties or {}

    """
    Agnostic public entry shell.
    Enforces a strict, hook-free public contract across all platform implementations.
    """
    @abstractmethod
    def read_stream(self, source: BaseDataSource):
        """Yields an active generator stream of standard Pandas DataFrames from the source."""
