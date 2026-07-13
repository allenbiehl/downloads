# base_writer_framework.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseWriteContext(ABC):
    """
    Agnostic Orchestrator for streaming write lifecycles.
    Manages active buffers, job run signatures, and isolated operational states.
    """
    @abstractmethod
    def init_transaction(self, *, source_uri: str | None = None):
        """Preps the transactional streaming workspace before ingest loops begin."""

    @abstractmethod
    def write_chunk(self, df: pd.DataFrame):
        """Incrementally routes incoming data frames down into format stream engines."""

    @abstractmethod
    def commit_transaction(self):
        """Finalizes, closes buffers, and commits all active objects transactionally."""

    @abstractmethod
    def abort_transaction(self):
        """Rolls back open transactions and purges active workspace buffers cleanly."""
