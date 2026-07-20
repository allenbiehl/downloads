import json
import logging
from dataset_ageoff.audit.models import AuditConfig, DatasetDirectory
from dataset_ageoff.config.file_config_loader import FileConfigLoader
import pyarrow.fs as pafs
from typing import List

logger = logging.getLogger(__name__)

class DatasetReportReconstitutor:
    """Discovers and aggregates universal summary.json files across MinIO dataset paths for months or quarters."""

    def __init__(self, config_path: str):
        self._config_path = config_path
        self._config = AuditConfig(
            **FileConfigLoader.load(config_path)
        )        
        
        # Instantiate PyArrow's optimized C++ file system matching your writer settings
        self._fs = pafs.S3FileSystem(
            access_key=self._config.s3_access_key,
            secret_key=self._config.s3_secret_key,
            endpoint_override=self._config.s3_endpoint_override,
            scheme=self._config.s3_scheme,
            # force_dest_bucket_style=not self._config.s3_force_virtual_addressing,
            region=self._config.s3_region
        )

    def generate_report(
        self, 
        year: str, 
        months: List[str], 
        datasets: List[DatasetDirectory]
    ) -> List[dict]:
        """Crawls MinIO paths across multiple months/projects and flattens them into a unified report matrix."""
        report_rows = []

        # Iterate over projects and months sequentially to preserve natural grouping order
        for dataset in datasets:
            for month in months:
                # Dynamically inject structural segments from the loop variables
                # E.g., bucket-name/ageoff/period/2026/01/project_A/summary.json
                summary_path = (
                    f"{self._config.s3_bucket}/{self._config.s3_prefix}/"
                    f"{year}/{month}/{dataset.name}/summary.json"
                )
                
                logger.info("Retrieving dimensional footprint from '%s'", summary_path)
                
                try:
                    # Open an optimized input stream channel directly from MinIO
                    with self._fs.open_input_stream(summary_path) as s3_stream:
                        raw_bytes = s3_stream.read()
                        metrics = json.loads(raw_bytes.decode('utf-8'))
                    
                    # Decouple the metadata properties back into your structured matrix layout
                    report_rows.append({
                        "year": year,
                        "month": month,
                        "project": dataset.name,
                        "size": metrics["total_bytes"],    # Combined size value string
                        "count": metrics["total_records"]    # Total files/records matching run
                    })
                    
                except Exception as err:
                    # Gracefully bypass missing entries if a project has no records for a specific month
                    if "Path does not exist" in str(err) or "NoSuchKey" in str(err):
                        logger.warning("No summary found for project '%s' at target partition %s/%s.", 
                                       dataset.name, year, month)
                        continue
                    logger.error("Failed to parse summary layout for '%s' in %s/%s: %s", 
                                 dataset.name, year, month, err)
                    raise err

        return report_rows

