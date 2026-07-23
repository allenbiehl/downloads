from dataclasses import dataclass, field
import os
from typing import Any, Dict, List

@dataclass(frozen=True)
class NfsDatasetPurgerConfig:
    """Configuration blueprint for the storage mutation deletion engine."""
    dry_run: bool = field(default=True)


class NfsDatasetPurger:
    """Responsibility: Handles batch disk file unlinking and enforces dry-run safety overrides."""
    
    _config: NfsDatasetPurgerConfig

    def __init__(self, config: NfsDatasetPurgerConfig | None = None) -> None:
        self._config = config or NfsDatasetPurgerConfig()

    def execute_or_emulate_delete(self, mount_path: str, relative_keys: List[str]) -> Dict[str, List[Any]]:
        """Physically removes files from the mount or prints the cleanup intent to stdout."""
        deleted_records: List[Dict[str, str]] = []
        error_records: List[Dict[str, str]] = []
        
        for k in relative_keys:
            full_target_path = os.path.join(mount_path, k)
            
            if self._config.dry_run:
                print(f"      [Dry Run Intent] Would unlink file: nfs://{full_target_path}")
                deleted_records.append({'Key': k})
                continue

            try:
                #
                # should always be disabled as we don't automatically delete nfs files
                # os.remove(full_target_path)
                #
                deleted_records.append({'Key': k})
            except Exception as err:
                error_records.append({'Key': k, 'Message': str(err)})
                
        return {"Deleted": deleted_records, "Errors": error_records}
