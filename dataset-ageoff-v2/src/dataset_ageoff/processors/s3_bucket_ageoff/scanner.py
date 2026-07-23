from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Tuple

from dataset_ageoff.processors.s3_bucket_ageoff.ledger import LocalProvenanceLedger
from dataset_ageoff.processors.s3_bucket_ageoff.models import ExtractionTask

@dataclass(frozen=True)
class S3DatasetScannerConfig:
    """Configuration blueprint for the storage namespaces listing operations."""
    max_years_back: int = field(default=25)

class S3DatasetScanner:
    """
    Interrogates the NetApp filesystem and populates the local disk ledger.
    """
    
    _config: S3DatasetScannerConfig
    _ledger: LocalProvenanceLedger
    _s3_client: Any    
    _paginator: Any

    def __init__(self, s3_client: Any, ledger: LocalProvenanceLedger, 
                 config: S3DatasetScannerConfig | None = None) -> None:
        self._config = config or S3DatasetScannerConfig()
        self._ledger = ledger
        self._s3_client = s3_client        
        self._paginator = self._s3_client.get_paginator('list_objects_v2')

    def locate_earliest_data_year(self, bucket_name: str) -> int | None:
        """Main Thread: Quickly finds the earliest sequential year prefix containing data."""
        current_year: int = datetime.now().year
        start_search_year: int = current_year - self._config.max_years_back
        
        for year in range(start_search_year, current_year + 1):
            try:
                response: Dict[str, Any] = self._s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=f"{year}/", MaxKeys=1
                )
                if 'Contents' in response:
                    return year
            except Exception as e:
                print(f" -> Error probing year {year} for {bucket_name}: {e}")
                return None
        return None

    def ingest_keys_to_ledger(self, bucket_name: str, task: ExtractionTask) -> int:
        """Worker Method: Reads objects from NetApp and stages them straight into local storage."""
        discovered_count: int = 0
        for page in self._paginator.paginate(Bucket=bucket_name, Prefix=task.prefix):
            if 'Contents' not in page:
                continue
            
            items_found: List[Tuple[str, int]] = [
                (obj['Key'], int(obj.get('Size', 0))) 
                for obj in page['Contents']
            ]
            discovered_count += len(items_found)
            self._ledger.stage_keys_with_sizes(bucket_name, task.prefix, items_found)
                    
        return discovered_count
