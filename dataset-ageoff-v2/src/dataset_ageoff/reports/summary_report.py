import json
import logging
import os
from typing import List, Dict, Any

from dataset_ageoff.common.config.file_config_loader import FileConfigLoader
from dataset_ageoff.processors.nfs_directory_common.models import AuditConfig
import pyarrow.fs as pafs

from dataset_ageoff.common.models.dataset_source import DatasetSource

logger = logging.getLogger(__name__)

class SummaryReport:
    """Discovers and aggregates day/month transaction matrices using unified source configuration blueprints."""

    # Class-level member variable type declarations safely encapsulated
    _config_path: str
    _config: AuditConfig
    _fs: pafs.S3FileSystem

    def __init__(self, config_path: str) -> None:
        self._config_path = config_path
        self._config = AuditConfig(**FileConfigLoader.load(config_path))        
        
        # Instantiate PyArrow's optimized C++ file system matching your writer settings
        self._fs = pafs.S3FileSystem(
            access_key=self._config.s3_access_key,
            secret_key=self._config.s3_secret_key,
            endpoint_override=self._config.s3_endpoint_override,
            scheme=self._config.s3_scheme,
            region=self._config.s3_region
        )

    def create(
        self, 
        year: str, 
        months: List[str], 
        source_configs: List[DatasetSource]
    ) -> List[dict]:
        """Crawls paths and flattens them into a unified report matrix split by protocol URIs."""
        report_rows: List[dict] = []

        for source_config in source_configs:
            for month in months:
                # Clean and format paths to prevent double slashes natively
                clean_bucket: str = self._config.s3_bucket.strip("/")
                clean_prefix: str = self._config.s3_prefix.strip("/")
                
                # Reconstruct cleanly with exactly one single delimiter token per block
                month_prefix_path: str = f"{clean_bucket}/{clean_prefix}/{year}/{month}/"
                
                logger.info("Scanning for transaction maps inside: '%s'", month_prefix_path)
                aggregation_ledger: Dict[str, Dict[str, int]] = {}
                
                try:
                    selector = pafs.FileSelector(base_dir=month_prefix_path, recursive=True)
                    file_infos = self._fs.get_file_info(selector)
                    
                    for file_info in file_infos:
                        path_str: str = file_info.path
                        file_name: str = os.getenv("SUMMARY_LEAF", os.path.basename(path_str))
                        
                        # FIX: Enforce strict path matching down to the source subdirectory tier
                        # E.g., verifies path contains '/project_A/bucket_1/' or '/project_A/bucket_2/'
                        target_sub_directory: str = f"/{source_config.dataset_name}/{source_config.source_name}/"
                        
                        if target_sub_directory in path_str and file_name.startswith("summary_") and file_name.endswith(".json"):
                            try:
                                with self._fs.open_input_stream(path_str) as s3_stream:
                                    raw_bytes: bytes = s3_stream.read()
                                    metrics: dict = json.loads(raw_bytes.decode('utf-8'))
                                
                                target_uri: str = metrics.get("source", "unknown://location")
                                
                                if target_uri not in aggregation_ledger:
                                    aggregation_ledger[target_uri] = {"bytes": 0, "records": 0}
                                    
                                aggregation_ledger[target_uri]["bytes"] += int(metrics.get("total_bytes", 0))
                                aggregation_ledger[target_uri]["records"] += int(metrics.get("total_records", 0))
                                
                            except Exception as file_err:
                                logger.error("Skipping corrupt summary file at '%s': %s", path_str, file_err)
                                continue

                    # Construct report lines directly using clean URI fields
                    for uri_string, totals in aggregation_ledger.items():
                        report_rows.append({
                            "year": year,
                            "month": month,
                            "dataset": source_config.dataset_name,
                            "source": uri_string,        
                            "size": str(totals["bytes"]),     
                            "count": totals["records"]        
                        })

                except Exception as err:
                    if "Path does not exist" in str(err) or "NoSuchKey" in str(err):
                        continue
                    raise err

        return report_rows
