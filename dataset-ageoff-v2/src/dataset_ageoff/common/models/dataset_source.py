
from dataclasses import dataclass

@dataclass(frozen=True)
class DatasetSource:
    """Explicit configuration model defining a single dataset storage rule."""
    dataset_name: str
    source_name: str
    uri: str
    retention_days: int