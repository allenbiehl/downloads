# pylint: disable=missing-module-docstring
# pylint: disable=too-few-public-methods
from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    """Interface for pluggable business logic transformations."""

    @abstractmethod
    def transform(self, chunk_stream: pd.DataFrame) -> pd.DataFrame:
        """
        Public method for transforming input stream to output stream
        """
