from dataclasses import dataclass, field
from datetime import datetime
import os
from typing import List, Tuple

from dataset_ageoff.processors.nfs_directory_ageoff.ledger import LocalNfsProvenanceLedger
from dataset_ageoff.processors.nfs_directory_ageoff.models import ExtractionTask

@dataclass(frozen=True)
class NfsDatasetScannerConfig:
    """Configuration blueprint for the storage namespaces listing operations."""
    max_years_back: int = field(default=25)

class NfsDatasetScanner:
    """
    Interrogates a targeted NFS directory mount root
    and indexes files matching the extraction task time window based on mtime.
    """
    
    _config: NfsDatasetScannerConfig
    _ledger: LocalNfsProvenanceLedger


    def __init__(
        self, 
        ledger: LocalNfsProvenanceLedger, 
        config: NfsDatasetScannerConfig | None = None
    ) -> None:
        self._config = config or NfsDatasetScannerConfig()
        self._ledger = ledger


    def locate_earliest_data_year(self, mount_path: str) -> int | None:
        """
        Main Thread: Dynamically discovers the absolute oldest file modification year
        present at the root of the targeted dataset mount path directory.
        """
        clean_mount_path = mount_path.replace("nfs://", "")
        if not os.path.exists(clean_mount_path) or not os.path.isdir(clean_mount_path):
            return None

        earliest_year: int = datetime.now().year

        try:
            # High-performance POSIX file descriptor sweep across the target root directory
            with os.scandir(clean_mount_path) as entries:
                for entry in entries:
                    try:
                        # Extract the physical modification time of the file/folder entry
                        stat_info = entry.stat(follow_symlinks=False)
                        mtime_year = datetime.fromtimestamp(stat_info.st_mtime).year
                        
                        if mtime_year < earliest_year:
                            earliest_year = mtime_year
                    except (OSError, FileNotFoundError):
                        continue
        except OSError as err:
            print(f" -> Failed to scan root metadata for {mount_path}: {err}")
            return None

        return earliest_year

    def ingest_keys_to_ledger(self, mount_path: str, task: ExtractionTask) -> int:
        """Worker Thread: Recursively crawls the targeted NFS source directory mount

        and stages files whose real physical mtime falls into the task prefix window.
        """
        discovered_count: int = 0
        
        # Ensure the protocol indicator token has been cleanly stripped
        clean_mount_path = mount_path.replace("nfs://", "")
        if not os.path.exists(clean_mount_path):
            return discovered_count

        items_buffer: List[Tuple[str, int]] = []
        prefix_len: int = len(task.prefix)
        
        # Recursively crawl only this explicit dataset source mount path space
        for root, _, files in os.walk(clean_mount_path):
            for file_name in files:
                full_path = os.path.join(root, file_name)
                
                try:
                    # 1. Read the low-level POSIX file statistics from disk metadata blocks
                    file_stat = os.stat(full_path)
                    file_size: int = file_stat.st_size
                    
                    # 2. Extract and parse the file modification time signature
                    mtime_datetime = datetime.fromtimestamp(file_stat.st_mtime)
                    file_mtime_prefix: str = mtime_datetime.strftime("%Y/%m/" if prefix_len == 8 else "%Y/%m/%d/")
                    
                    # 3. AGE FILTER: If the real file modification date matches the active task task segment, stage it
                    if file_mtime_prefix == task.prefix:
                        # Construct a relative key path tracking identifier format
                        relative_key: str = os.path.relpath(full_path, clean_mount_path)
                        items_buffer.append((relative_key, file_size))
                        discovered_count += 1
                    
                    # Batch insertions to prevent database transaction latency bottlenecks
                    if len(items_buffer) >= 2000:
                        self._ledger.stage_keys_with_sizes(mount_path, task.prefix, items_buffer)
                        items_buffer.clear()
                        
                except (OSError, FileNotFoundError):
                    # Gracefully bypass dead links or transient network disruptions
                    continue
                    
        if items_buffer:
            self._ledger.stage_keys_with_sizes(mount_path, task.prefix, items_buffer)
            
        return discovered_count
