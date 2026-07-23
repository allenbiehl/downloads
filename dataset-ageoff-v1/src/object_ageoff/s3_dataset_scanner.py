from datetime import datetime
import os
from typing import Any, Dict, List
import boto3
from botocore.config import Config
from object_ageoff.ledger import LocalProvenanceLedger
from object_ageoff.models import ExtractionTask


class S3DatasetScanner:
    """Responsibility: Interrogates the NetApp filesystem and populates the local disk ledger."""
    
    _s3_client: Any
    _ledger: LocalProvenanceLedger
    _paginator: Any
    _dry_run: bool

    def __init__(self, s3_client: Any, ledger: LocalProvenanceLedger, dry_run: bool = True) -> None:
        self._s3_client = s3_client
        self._ledger = ledger
        self._paginator = self._s3_client.get_paginator('list_objects_v2')
        self._dry_run = dry_run

    def locate_earliest_data_year(self, bucket_name: str, max_years_back: int = 25) -> int | None:
        """Main Thread: Quickly finds the earliest sequential year prefix containing data."""
        current_year: int = datetime.now().year
        start_search_year: int = current_year - max_years_back
        
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
            
            keys_found: List[str] = [obj['Key'] for obj in page['Contents']]
            discovered_count += len(keys_found)
            
            self._ledger.stage_keys(bucket_name, task.prefix, keys_found)
                    
        return discovered_count

    def emulate_or_execute_delete(self, bucket: str, keys: List[str]) -> Dict[str, List[Any]]:
        """
        Executes real deletions or emulates them by printing the expected outcome 
        to stdout while returning mock success tokens to maintain ledger balance.
        """
        if self._dry_run:
            mock_deleted: List[Dict[str, str]] = []
            for k in keys:
                print(f"      [Dry Run Intent] Would delete file: s3://{bucket}/{k}")
                mock_deleted.append({'Key': k})
            return {"Deleted": mock_deleted, "Errors": []}

        # Live Production Path
        formatted_keys = [{'Key': k} for k in keys]
        return self._s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': formatted_keys, 'Quiet': False}
        )
