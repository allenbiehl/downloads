from dataclasses import dataclass, field
import os
import concurrent.futures
import json
from datetime import datetime
from typing import List, Any, Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dataset_ageoff.common.models.dataset_source import DatasetSource
from dataset_ageoff.processors.nfs_directory_ageoff.ledger import LocalNfsProvenanceLedger
from dataset_ageoff.processors.nfs_directory_ageoff.models import ExtractionTask
from dataset_ageoff.processors.nfs_directory_ageoff.purger import NfsDatasetPurger
from dataset_ageoff.processors.nfs_directory_ageoff.scanner import NfsDatasetScanner
from dataset_ageoff.processors.nfs_directory_ageoff.timeline import LifecycleTimelineCalculator, LifecycleTimelineCalculatorConfig

@dataclass(frozen=True)
class ContinuousNfsLifecycleOrchestratorConfig:
    """Configuration blueprint governing runtime thread pool structures and storage targets."""
    audit_bucket: str = field(default_factory=lambda: os.getenv("COMPLIANCE_AUDIT_BUCKET", "corporate-lifecycle-audit-reporting"))
    max_workers: int = field(default=16)
    dry_run: bool = field(default=True)
    records_per_parquet_part: int = field(default=50000)

class ContinuousNfsLifecycleOrchestrator:
    """
    Responsibility: Governs non-destructive multi-bucket NFS audit processing, 
    preventing log duplicates.
    """
    
    _config: ContinuousNfsLifecycleOrchestratorConfig
    _scanner: NfsDatasetScanner
    _purger: NfsDatasetPurger
    _s3_client: Any
    _ledger: LocalNfsProvenanceLedger

    def __init__(
        self, 
        scanner: NfsDatasetScanner, 
        purger: NfsDatasetPurger, 
        s3_client: Any, 
        ledger: LocalNfsProvenanceLedger, 
        config: ContinuousNfsLifecycleOrchestratorConfig | None = None
    ) -> None:
        self._config = config or ContinuousNfsLifecycleOrchestratorConfig()
        self._scanner = scanner
        self._purger = purger
        self._s3_client = s3_client
        self._ledger = ledger

    def process_all_datasets(self, dataset_configs: List[DatasetSource]) -> None:
        """
        Accepts a structured list of DatasetSource instances.
        """
        print("=" * 85)
        print(f" RUN MODE: {'[AUDIT ONLY EMULATION]' if self._config.dry_run else '[LIVE CONTINUOUS NFS AUDIT]'}")
        print("=" * 85)

        for config in dataset_configs:
            print(f"\nActivating NFS Lifecycle Strategy For: {config.uri} [{config.dataset_name} / {config.source_name}]")
            self._orchestrate_single_mount(config)

    def _orchestrate_single_mount(self, source_config: DatasetSource) -> None:
        mount_path: str = source_config.uri
        
        start_year: int | None = self._scanner.locate_earliest_data_year(mount_path)
        if not start_year:
            print(f" -> Skipping '{source_config.uri}': No historical records found.")
            return
            
        timeline_config = LifecycleTimelineCalculatorConfig(age_off_days=source_config.retention_days)
        timeline_engine = LifecycleTimelineCalculator(config=timeline_config)
        active_tasks: List[ExtractionTask] = timeline_engine.generate_tasks(start_year, source_config)

        if not active_tasks:
            print(f" -> No expired records found for '{source_config.uri}' under timeline constraints.")
            return

        print(f" -> Found data boundary starting at {start_year}. Cutoff: {timeline_engine.cutoff_date.strftime('%Y/%m/%d')}")
        print(f" -> Spawning worker threads to execute {len(active_tasks)} tasks concurrently...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._config.max_workers) as executor:
            futures = {
                executor.submit(self._execute_prefix_lifecycle, mount_path, task): task
                for task in active_tasks
            }
            for future in concurrent.futures.as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"   [CRITICAL SYSTEM ERROR] Task {task.prefix} failed: {exc}")

    def _execute_prefix_lifecycle(self, mount_path: str, task: ExtractionTask) -> None:
        """
        Thread Worker Layer: Clears files via local disk unlinking and delivers log 
        components to S3.
        """
        self._scanner.ingest_keys_to_ledger(mount_path, task)

        while True:
            batch_items: List[tuple[str, int]] = self._ledger.get_queued_batch(mount_path, task.prefix, limit=1000)
            if not batch_items:
                break
                
            just_keys: List[str] = [item[0] for item in batch_items]
            self._purge_and_log_batch(mount_path, task.prefix, just_keys)

        successes: List[Dict[str, Any]] = self._ledger.export_and_preserve_success_log(mount_path, task.prefix)
        
        if successes:
            total_records: int = len(successes)
            total_bytes_purged: int = 0
            part_counter: int = 0
            
            for i in range(0, total_records, self._config.records_per_parquet_part):
                part_counter += 1
                sub_batch = successes[i : i + self._config.records_per_parquet_part]
                
                df = pd.DataFrame(sub_batch)
                total_bytes_purged += int(df['file_size'].sum())
                df_to_save = df.drop(columns=['file_size'])
                
                schema = pa.schema([('key', pa.string()), ('ts', pa.string())])
                table = pa.Table.from_pandas(df_to_save, schema=schema)
                
                sink = pa.BufferOutputStream()
                pq.write_table(table, sink, compression='SNAPPY')
                parquet_bytes: bytes = sink.getvalue().to_pybytes()
                
                part_s3_key: str = f"{task.output_s3_path}/part_{task.transaction_id}_{part_counter}.parquet"
                
                if not self._config.dry_run:
                    self._s3_client.put_object(
                        Bucket=self._config.audit_bucket, Key=part_s3_key,
                        Body=parquet_bytes, ContentType='application/octet-stream'
                    )

            summary_payload = {
                "transaction_id": task.transaction_id,
                "dataset": task.source_config.dataset_name,
                "source": task.source_config.uri,
                "total_parts": part_counter,
                "total_bytes": str(total_bytes_purged),
                "total_records": total_records,
                "processed_at": datetime.now().isoformat()
            }
            
            summary_s3_key: str = f"{task.output_s3_path}/summary_{task.transaction_id}.json"
            
            if not self._config.dry_run:
                self._s3_client.put_object(
                    Bucket=self._config.audit_bucket, Key=summary_s3_key,
                    Body=json.dumps(summary_payload), ContentType='application/json'
                )
                print(f"   [Thread Complete] {task.prefix} [TX: {task.transaction_id}] -> Created {part_counter} parts ({total_records} files).")
            else:
                print(f"   [Dry Run Summary] {task.prefix} [TX: {task.transaction_id}] -> Would write {part_counter} Parquet parts and summary to: s3://{self._config.audit_bucket}/{task.output_s3_path}/")
        else:
            print(f"   [Thread Complete] {task.prefix} -> No objects were found matching expiration criteria.")

    def _purge_and_log_batch(self, mount_path: str, prefix: str, keys: List[str]) -> None:
        successes: List[str] = []
        failures: List[Dict[str, str]] = []

        try:
            response = self._purger.execute_or_emulate_delete(mount_path, keys)

            for success in response.get('Deleted', []):
                successes.append(success['Key'])
            for error in response.get('Errors', []):
                failures.append({'key': error['Key'], 'error': error['Message']})
        except Exception as global_err:
            error_class_name: str = global_err.__class__.__name__
            detailed_error_msg: str = f"{error_class_name}: {str(global_err)}"
            
            print(f"   [PURGE EXCEPTION] Batch processing failed for prefix {prefix}: {detailed_error_msg}")
            
            for k in keys:
                failures.append({
                    'key': k, 
                    'error': detailed_error_msg
                })

        self._ledger.update_batch_status(mount_path, successes, failures)
