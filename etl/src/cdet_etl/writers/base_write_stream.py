# base_writer_framework.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseWriteStream(ABC):
    """
    Pure Agnostic Interface for format writers.
    Operates as the concrete contract type for format-specific block serialization.
    """
    @abstractmethod
    def format_chunk(self, partition_path: str, group_df: pd.DataFrame) -> bytes:
        """Serializes a DataFrame chunk into its specific binary format."""

    @abstractmethod
    def close_and_finalize(self, partition_path: str) -> bytes:
        """Closes any active file footers/buffers and extracts residual trailing bytes."""
