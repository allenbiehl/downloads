from dataclasses import dataclass

from dataset_ageoff.common.models.dataset_source import DatasetSource

@dataclass(frozen=True)
class ExtractionTask:
    """Explicit data model representing a single daily or monthly folder to purge."""
    source_config: DatasetSource
    prefix: str
    output_s3_path: str 
    transaction_id: str
