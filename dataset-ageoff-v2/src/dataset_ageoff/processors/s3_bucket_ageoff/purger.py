from dataclasses import dataclass, field
from typing import Any, Dict, List

@dataclass(frozen=True)
class S3DatasetPurgerConfig:
    """Configuration blueprint for the storage mutation deletion engine."""
    dry_run: bool = field(default=True)

class S3DatasetPurger:
    """Responsibility: Handles batch mutations and enforces dry-run safety overrides."""
    
    _config: S3DatasetPurgerConfig
    _s3_client: Any

    def __init__(self, s3_client: Any, config: S3DatasetPurgerConfig | None = None) -> None:
        self._config = config or S3DatasetPurgerConfig()
        self._s3_client = s3_client

    def execute_or_emulate_delete(self, bucket: str, keys: List[str]) -> Dict[str, List[Any]]:
        """Executes real deletions or emulates them by printing intents to stdout."""
        if self._config.dry_run:
            mock_deleted: List[Dict[str, str]] = []
            for k in keys:
                print(f"      [Dry Run Intent] Would delete file: s3://{bucket}/{k}")
                mock_deleted.append({'Key': k})
            return {"Deleted": mock_deleted, "Errors": []}

        formatted_keys = [{'Key': k} for k in keys]
        return self._s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': formatted_keys, 'Quiet': False}
        )
