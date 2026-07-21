import concurrent.futures
from datetime import datetime, timedelta
import random
import queue
from typing import Any, List
import boto3
from botocore.config import Config


class S3StructuralTestDataGenerator:
    """Responsibility: Generates structurally distinct dataset timelines to test S3 scanners."""
    
    def __init__(self, endpoint_url: str | None = None, 
                 aws_access_key_id: str | None = None, 
                 aws_secret_access_key: str | None = None,
                 max_workers: int = 64) -> None:
        
        s3_config: Config = Config(
            max_pool_connections=max_workers + 10,
            s3={'addressing_style': 'path'}
        )
        
        if endpoint_url:
            self._s3_client: Any = boto3.client(
                's3', endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                config=s3_config
            )
        else:
            self._s3_client: Any = boto3.client('s3', config=s3_config)
            
        self._max_workers: int = max_workers

    def _upload_mock_file(self, bucket_name: str, target_date: datetime, file_index: int) -> str:
        """Constructs a deterministic key string and performs a mock payload upload."""
        date_prefix: str = target_date.strftime("%Y/%m/%d")
        key_name: str = f"{date_prefix}/mock-payload-{file_index}.json"
        mock_payload: bytes = b'{"status": "test_data"}'
        
        try:
            self._s3_client.put_object(Bucket=bucket_name, Key=key_name, Body=mock_payload)
            return key_name
        except Exception as e:
            print(f" [Upload Error] Failed to write key {key_name}: {e}")
            raise e

    def populate_bucket_with_timeline(self, bucket_name: str, target_dates: List[datetime]) -> None:
        """Uploads a specific pre-calculated timeline array into the target bucket."""
        print(f"\n" + "=" * 70)
        print(f"POPULATING BUCKET STRUCTURALLY: {bucket_name}")
        print(f"Uploading {len(target_dates)} files tailored to test layout edge-cases...")
        print("=" * 70)
        
        try:
            self._s3_client.create_bucket(Bucket=bucket_name)
        except Exception:
            pass

        uploaded_count: int = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(self._upload_mock_file, bucket_name, date_val, index): index
                for index, date_val in enumerate(target_dates)
            }
            
            for completed_future in concurrent.futures.as_completed(futures):
                try:
                    completed_future.result()
                    uploaded_count += 1
                    if uploaded_count % 500 == 0:
                        print(f"    [Progress] Populated {uploaded_count}/{len(target_dates)} objects...")
                except Exception:
                    pass
