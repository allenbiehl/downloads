import os
import concurrent.futures
import json
import time
from typing import List, Any, Dict

from object_ageoff.ledger import LocalProvenanceLedger
from object_ageoff.models import ExtractionTask
from object_ageoff.s3_dataset_scanner import S3DatasetScanner
from object_ageoff.timeline import LifecycleTimelineCalculator

class KubernetesLifecycleOrchestrator:
    """Responsibility: Governs cluster job execution, parallel threading pools, and final logging."""
    
    _scanner: S3DatasetScanner
    _s3_client: Any
    _ledger: LocalProvenanceLedger
    _timeline: LifecycleTimelineCalculator
    _audit_bucket: str
    _max_workers: int
    _dry_run: bool

    def __init__(self, scanner: S3DatasetScanner, orchestrator_s3_client: Any, 
                 ledger: LocalProvenanceLedger, timeline: LifecycleTimelineCalculator, 
                 audit_bucket: str, max_workers: int = 64, dry_run: bool = True) -> None:
        self._scanner = scanner
        self._s3_client = orchestrator_s3_client
        self._ledger = ledger
        self._timeline = timeline
        self._audit_bucket = audit_bucket
        self._max_workers = max_workers
        self._dry_run = dry_run

    def process_all_datasets(self, bucket_list: List[str]) -> None:
        """Loops target datasets sequentially to ensure complete processing isolation."""
        print("=" * 75)
        print(f" RUN MODE: {'[DRY RUN EMULATION]' if self._dry_run else '[LIVE KUBERNETES PRODUCTION]'}")
        print(f" Cutoff Bound: {self._timeline.cutoff_date.strftime('%Y/%m/%d')}")
        print("=" * 75)

        for bucket in bucket_list:
            print(f"\nActivating Storage Lifecycle Strategy For: {bucket}")
            self._orchestrate_single_bucket(bucket)

    def _orchestrate_single_bucket(self, bucket_name: str) -> None:
        start_year: int | None = self._scanner.locate_earliest_data_year(bucket_name)
        if not start_year:
            print(f" -> Skipping '{bucket_name}': No historical records found.")
            return
            
        hybrid_tasks: List[ExtractionTask] = self._timeline.generate_hybrid_prefixes(start_year)
        
        if not hybrid_tasks:
            print(f" -> No expired records found for '{bucket_name}' under current timeline constraints.")
            return

        print(f" -> Found data boundary starting at {start_year}. Spinning up thread pool emulation...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(self._execute_prefix_lifecycle, bucket_name, task): task
                for task in hybrid_tasks
            }
            for future in concurrent.futures.as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"   [CRITICAL SYSTEM ERROR] Chunk {task.prefix} failed: {exc}")

    def _execute_prefix_lifecycle(self, bucket_name: str, task: ExtractionTask) -> None:
        """Thread Worker Layer: Manages streaming partitions locally and emulates transactions."""
        # 1. Ingestion via scanner (gathers real keys from NetApp to see what actually matches)
        self._scanner.ingest_keys_to_ledger(bucket_name, task)

        # 2. Deletion processing (runs through the ledger logs row-by-row)
        while True:
            batch_keys: List[str] = self._ledger.get_queued_batch(bucket_name, task.prefix, limit=1000)
            if not batch_keys:
                break
            self._purge_and_log_batch(bucket_name, task.prefix, batch_keys)

        # 3. Export ONLY confirmed successes
        successes: List[Dict[str, Any]] = self._ledger.export_and_clean_success_log(bucket_name, task.prefix)
        
        if successes:
            clean_prefix: str = task.prefix.replace('/', '-')
            s3_key: str = f"audit_trails/{bucket_name}/{clean_prefix}_{int(time.time())}.jsonl"
            jsonl_payload: str = "\n".join([json.dumps(r) for r in successes])
            
            if self._dry_run:
                print(f"   [Dry Run Summary] {task.prefix} -> Would commit {len(successes)} lines to audit log: s3://{self._audit_bucket}/{s3_key}")
            else:
                self._s3_client.put_object(
                    Bucket=self._audit_bucket, Key=s3_key,
                    Body=jsonl_payload.encode('utf-8'), ContentType='application/x-jsonlines'
                )
                print(f"   [Thread Complete] {task.prefix} -> Committed {len(successes)} verified deletions to audit log.")
        else:
            print(f"   [Thread Complete] {task.prefix} -> No files found matching expiration criteria.")

    def _purge_and_log_batch(self, bucket: str, prefix: str, keys: List[str]) -> None:
        successes: List[str] = []
        failures: List[Dict[str, str]] = []
        
        try:
            # Delegate tracking mechanics straight to the scanner layer to filter for Dry Runs
            response = self._scanner.emulate_or_execute_delete(bucket, keys)
            
            for success in response.get('Deleted', []):
                successes.append(success['Key'])
            for error in response.get('Errors', []):
                failures.append({'key': error['Key'], 'error': error['Message']})
        except Exception as global_err:
            for k in keys:
                failures.append({'key': k, 'error': str(global_err)})

        self._ledger.update_batch_status(bucket, successes, failures)
