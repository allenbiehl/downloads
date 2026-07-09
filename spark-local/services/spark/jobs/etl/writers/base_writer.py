import time
from abc import ABC, abstractmethod
import pandas as pd

class BaseWriter(ABC):
    """
    Core Agnostic Template Class for Transactional Storage Sinks.
    Manages unified retries and the template execution loop lifecycle.
    """
    def _execute_with_retry(self, action_name: str, func, *args, **kwargs):
        """Unified Protected Retry Circuit Breaker with 3x backoff."""
        for attempt in range(1, 4):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"[Attempt {attempt}/3] Failed {action_name}: {str(e)}")
                if attempt == 3:
                    raise e
                time.sleep(2 ** attempt)

    @abstractmethod
    def _init_transaction(self):
        """Preps the transaction context before streaming chunks."""

    @abstractmethod
    def _write_chunk(self, df: pd.DataFrame):
        """Flushes a single streaming DataFrame chunk to storage immediately."""

    @abstractmethod
    def _commit_transaction(self):
        """Atomic Commit: Materializes all written data concurrently."""

    @abstractmethod
    def _abort_transaction(self):
        """Atomic Rollback: Purges all uncommitted data footprints."""

    def write_stream(self, chunk_stream):
        """Inversion of Control Template Method enforcing flat memory limits."""
        try:
            self._init_transaction()
            for df in chunk_stream:
                if df.empty:
                    continue
                self._write_chunk(df)
            self._commit_transaction()
        except Exception as pipeline_fault:
            print(f"\n[CRITICAL SINK FAULT] Transaction failed: {pipeline_fault}. Rolling back...")
            try:
                self._abort_transaction()
            except Exception as rollback_failure:
                print(f"[FATAL UNWOUND ENGINE] Stranded data risk! Abort failed: {rollback_failure}")
            raise pipeline_fault
