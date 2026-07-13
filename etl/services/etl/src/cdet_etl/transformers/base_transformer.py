# pylint: disable=missing-module-docstring
# pylint: disable=too-few-public-methods
from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    """Interface for pluggable business logic transformations."""
    _properties: dict
    
    def __init__(self, properties: dict | None = None):
        self._properties = properties or {}

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Public method for transforming input stream to output stream
        """
