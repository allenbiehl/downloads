import os
import uuid
import pandas as pd
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from cdet_etl.writers.base_writer import BaseWriter

class OpenSearchWriter(BaseWriter):
    """Hardened OpenSearch historical append-only writer with flat memory chunk streaming."""
    def __init__(self):
        super().__init__()
        self._host = os.getenv("OPENSEARCH_HOST", "localhost")
        self._port = int(os.getenv("OPENSEARCH_PORT", 9200))
        self._target_index = os.getenv("OPENSEARCH_INDEX", "historical_telemetry")
        self._client = None
        self._pipeline_run_id = None
        self._indexed_count = 0

    def _init_transaction(self):
        # 1. Generate a completely unique transaction run token for this specific execution
        self._pipeline_run_id = f"run_{uuid.uuid4().hex[:12]}"
        self._indexed_count = 0
        
        # 2. Directly initialize the open connection pool securely
        self._client = OpenSearch(
            hosts=[{"host": self._host, "port": self._port}],
            use_ssl=False if self._endpoint_url and self._endpoint_url.startswith("http://") else True,
            verify_certs=self._ssl_verify,
            ssl_show_warn=False
        )
        print(f"[OPENSEARCH INIT] Opened transaction boundary. Assigning Run ID: {self._pipeline_run_id}")

    def _write_chunk(self, df: pd.DataFrame):
        # Drop temporary partition columns since OpenSearch indexes horizontally
        if "date_partition" in df.columns:
            df = df.drop(columns=["date_partition"])
            
        bulk_actions = []
        for record in df.to_dict(orient="records"):
            # Inject write-ahead status isolation flags directly into the document
            record["pipeline_run_id"] = self._pipeline_run_id
            record["status"] = "uncommitted"  # <-- Hidden from standard consumer queries
            
            bulk_actions.append({
                "_index": self._target_index,
                "_source": record
            })
            
        if not bulk_actions:
            return

        # FLUSH IMMEDIATELY: Write this chunk over the network to OpenSearch now!
        # Reuses the base class _execute_with_retry method to handle connection hiccups
        success, errors = self._execute_with_retry(
            f"Bulk Uploading Chunk Segment ({len(bulk_actions)} rows)",
            bulk, self._client, bulk_actions, stats_only=True
        )
        self._indexed_count += success
        
        if errors > 0:
            raise RuntimeError(f"OpenSearch internal chunk parsing errors encountered: {errors}")

    def _commit_transaction(self):
        print(f"\n--- COMMITTING ALL {self._indexed_count:,} DOCUMENTS IN OPEN-SEARCH ---")
        
        commit_query = {
            "script": {
                "source": "ctx._source.status = 'committed'",
                "lang": "painless"
            },
            "query": {
                "term": {
                    "pipeline_run_id": self._pipeline_run_id
                }
            }
        }
        
        # Flip the status from 'uncommitted' to 'committed' globally across the target ID (3x Retries)
        self._execute_with_retry(
            "Finalizing Status Update Commit",
            self._client.update_by_query,
            index=self._target_index,
            body=commit_query,
            wait_for_completion=True
        )
        print("Success! OpenSearch historical transaction finalized and visible.")

    def _abort_transaction(self):
        if not self._client or not self._pipeline_run_id:
            return

        abort_query = {
            "query": {
                "term": {
                    "pipeline_run_id": self._pipeline_run_id
                }
            }
        }
        
        # EMERGENCY ABORT BOUNDARY: Completely purge the uncommitted streaming footprint (3x Retries)
        self._execute_with_retry(
            "Rolling back uncommitted records via Delete-By-Query",
            self._client.delete_by_query,
            index=self._target_index,
            body=abort_query,
            wait_for_completion=True
        )
        print("Rollback successful. OpenSearch historical indices remain clean.")
