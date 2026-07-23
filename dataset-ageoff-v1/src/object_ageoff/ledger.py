from datetime import datetime
import os
import sqlite3
from typing import List, Dict, Any

class LocalProvenanceLedger:
    """Responsibility: Manages local transactional SQLite state on mounted block storage."""
    
    # Class-level member variable type declarations
    _db_path: str

    def __init__(self, db_mount_path: str = "/data/provenance.db") -> None:
        self._db_path = db_mount_path
        
        dir_name: str = os.path.dirname(self._db_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
            
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Creates a thread-safe connection optimized for concurrent workloads."""
        conn = sqlite3.connect(self._db_path, timeout=30000, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        """Creates the local transactional tracking schemas."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS flow_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_bucket TEXT,
                    s3_key TEXT,
                    task_prefix TEXT,
                    status TEXT DEFAULT 'QUEUED',
                    error_msg TEXT,
                    processed_at TEXT,
                    UNIQUE(target_bucket, s3_key)
                )
            """)
            conn.commit()

    def stage_keys(self, bucket: str, prefix: str, keys: List[str]) -> None:
        """Appends discovered keys to the local disk engine ledger."""
        with self._get_connection() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO flow_files (target_bucket, s3_key, task_prefix) VALUES (?, ?, ?)",
                [(bucket, key, prefix) for key in keys]
            )
            conn.commit()

    def get_queued_batch(self, bucket: str, prefix: str, limit: int = 1000) -> List[str]:
        """Fetches the next execution batch array directly out of block storage."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT s3_key FROM flow_files 
                WHERE target_bucket = ? AND task_prefix = ? AND status = 'QUEUED' 
                LIMIT ?
            """, (bucket, prefix, limit))
            # FIX: Explicitly unpack row[0] to return a clean list of string keys instead of list of tuples
            return [row[0] for row in cursor.fetchall()]

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
        """Retrieves ONLY successful deletions to compile the primary audit, then wipes the data partition."""
        records: List[Dict[str, Any]] = []
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT s3_key, processed_at FROM flow_files 
                WHERE target_bucket = ? AND task_prefix = ? AND status = 'SUCCESS'
            """, (bucket, prefix))
            
            # FIX: Explicitly extract indices row[0] and row[1] out of the tuple
            for row in cursor.fetchall():
                records.append({
                    "key": row[0], "ts": row[1]
                })
                
            # Permanently wipe this chunk table space from local disk block memory.
            conn.execute("""
                DELETE FROM flow_files 
                WHERE target_bucket = ? AND task_prefix = ?
            """, (bucket, prefix))
            conn.commit()
            
        return records
