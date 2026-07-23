from dataclasses import dataclass, field
from datetime import datetime
import os
import sqlite3
from typing import List, Dict, Any, Tuple

@dataclass(frozen=True)
class LocalProvenanceLedgerConfig:
    """Configuration blueprint for the local relational SQLite block storage engine."""
    db_mount_path: str = field(
        default_factory=lambda: os.getenv("LEDGER_DB_PATH", "/mnt/storagegrid-ledger/provenance.db")
    )

class LocalProvenanceLedger:
    """
    Manages local transactional SQLite state on mounted storage.
    """
    
    _config: LocalProvenanceLedgerConfig

    def __init__(self, config: LocalProvenanceLedgerConfig | None = None) -> None:
        self._config = config or LocalProvenanceLedgerConfig()
        dir_name: str = os.path.dirname(self._config.db_mount_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._config.db_mount_path, timeout=30000, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        """Creates the local transactional tracking schemas. Static production definition."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS flow_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_bucket TEXT,
                    s3_key TEXT,
                    task_prefix TEXT,
                    file_size_bytes INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'QUEUED',
                    error_msg TEXT,
                    processed_at TEXT,
                    UNIQUE(target_bucket, s3_key)
                )
            """)
            conn.commit()

    def stage_keys_with_sizes(self, bucket: str, prefix: str, items: List[Tuple[str, int]]) -> None:
        """
        Appends discovered keys and their real storage sizes to the local database ledger.
        """
        with self._get_connection() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO flow_files (target_bucket, s3_key, task_prefix, file_size_bytes) VALUES (?, ?, ?, ?)",
                [(bucket, key, prefix, size) for key, size in items]
            )
            conn.commit()

    def get_queued_batch(self, bucket: str, prefix: str, limit: int = 1000) -> List[Tuple[str, int]]:
        """Fetches the next execution batch array directly out of block storage."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT s3_key, file_size_bytes FROM flow_files 
                WHERE target_bucket = ? AND task_prefix = ? AND status = 'QUEUED' 
                LIMIT ?
            """, (bucket, prefix, limit))
            return [row for row in cursor.fetchall()]

    def update_batch_status(self, bucket: str, successes: List[str], failures: List[Dict[str, str]]) -> None:
        """Updates the local database state atomically via explicit ACID parameters."""
        timestamp: str = datetime.now().isoformat()
        with self._get_connection() as conn:
            if successes:
                conn.executemany("""
                    UPDATE flow_files SET status = 'SUCCESS', processed_at = ? 
                    WHERE target_bucket = ? AND s3_key = ?
                """, [(timestamp, bucket, key) for key in successes])
            if failures:
                conn.executemany("""
                    UPDATE flow_files SET status = 'FAILED', error_msg = ?, processed_at = ? 
                    WHERE target_bucket = ? AND s3_key = ?
                """, [(f['error'], timestamp, bucket, f['key']) for f in failures])
            conn.commit()

    def export_and_clean_success_log(self, bucket: str, prefix: str) -> List[Dict[str, Any]]:
        """
        Retrieves ONLY successful deletions to compile the primary audit, then wipes 
        the data partition.
        """
        records: List[Dict[str, Any]] = []

        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT s3_key, file_size_bytes, processed_at FROM flow_files 
                WHERE target_bucket = ? AND task_prefix = ? AND status = 'SUCCESS'
            """, (bucket, prefix))

            for row in cursor.fetchall():
                records.append({
                    "key": row[0],
                    "file_size": row[1],
                    "ts": row[2]
                })

            conn.execute("""
                DELETE FROM flow_files 
                WHERE target_bucket = ? AND task_prefix = ?
            """, (bucket, prefix))
            conn.commit()

        return records
