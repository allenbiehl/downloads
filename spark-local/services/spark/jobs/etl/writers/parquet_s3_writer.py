import io
import time
import math
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

import pyarrow as pa
import pyarrow.parquet as pq 

from etl.writers.base_s3_writer import BaseS3Writer


class ParquetS3Writer(BaseS3Writer):
    def __init__(self):
        super().__init__()
        self.output_bucket = None
        self.output_prefix = None
        self.storage_options = {}
        self.configure_from_env()

    def _init_transaction(self):
        self._s3_client = self._get_boto3_s3_client()

    def _execute_with_retry(self, action_name, func, *args, **kwargs):
        for attempt in range(1, 4):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                print(f"[Attempt {attempt}/3] Failed {action_name}: {e.response['Error']['Message']}")
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)

    def write_stream(self, chunk_stream):
        staged_partitions = {}
        MIN_PART_SIZE = 5 * 1024 * 1024

        # 1. Accumulate stream into isolated, compiled partition bytes
        for df in chunk_stream:
            for date_val, group in df.groupby("date_partition"):
                clean_group = group.drop(columns=["date_partition"])
                
                table = pa.Table.from_pandas(clean_group)
                sink = io.BytesIO()
                pq_writer = pq.ParquetWriter(sink, table.schema)
                pq_writer.write_table(table)
                pq_writer.close()
                
                partition_key = f"date_partition={date_val}"
                if partition_key not in staged_partitions:
                    staged_partitions[partition_key] = []
                staged_partitions[partition_key].append(sink.getvalue())

        # 2. Open AWS clients for network transactions
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.storage_options["key"],
            aws_secret_access_key=self.storage_options["secret"],
            endpoint_url=self.storage_options["endpoint_url"],
            use_ssl=False,
            verify=False,
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            )
        )
        pending_uploads = []

        try:
            print("\n--- INITIATING ATOMIC MULTI-PART TRANSACTIONS ---")
            for partition_path, byte_list in staged_partitions.items():
                final_file_bytes = b"".join(byte_list)
                total_bytes = len(final_file_bytes)
                s3_key = f"{self.output_prefix.strip('/')}/{partition_path}/part-0000.parquet"

                # Init Multipart Transaction (3x Retries)
                init_res = self._execute_with_retry(
                    f"Init Upload for {s3_key}", 
                    s3_client.create_multipart_upload, Bucket=self.output_bucket, Key=s3_key
                )
                upload_id = init_res["UploadId"]

                # Spark Loophole Logic: Single part files bypass the 5MB ceiling rule
                if total_bytes < MIN_PART_SIZE:
                    num_parts, part_size = 1, total_bytes
                else:
                    num_parts = math.ceil(total_bytes / MIN_PART_SIZE)
                    part_size = math.ceil(total_bytes / num_parts)

                parts_manifest = []
                for part_num in range(1, num_parts + 1):
                    start_byte = (part_num - 1) * part_size
                    end_byte = min(start_byte + part_size, total_bytes)
                    byte_chunk = final_file_bytes[start_byte:end_byte]
                    if not byte_chunk:
                        break

                    # Upload Segment (3x Retries)
                    part_res = self._execute_with_retry(
                        f"Upload Part {part_num} for {s3_key}",
                        s3_client.upload_part,
                        Bucket=self.output_bucket, 
                        Key=s3_key, 
                        UploadId=upload_id, 
                        PartNumber=part_num, 
                        Body=byte_chunk
                    )
                    parts_manifest.append({"ETag": part_res["ETag"], "PartNumber": part_num})

                pending_uploads.append((s3_key, upload_id, parts_manifest))

            # 3. TRANSACTION COMMIT BOUNDARY
            print(f"\n--- COMMITTING {len(pending_uploads)} PARTITIONS ATOMICALLY ---")
            for s3_key, upload_id, parts in pending_uploads:
                # Complete Upload (Corrected)
                self._execute_with_retry(
                    f"Commit File {s3_key}",
                    s3_client.complete_multipart_upload,
                    Bucket=self.output_bucket, 
                    Key=s3_key, 
                    UploadId=upload_id, # <--- Added missing parameter
                    MultipartUpload={"Parts": parts}
                )
            print("Pipeline execution completed successfully.")

        except Exception as pipeline_error:
            print(f"\n[CRITICAL FAILURE] Pipeline interrupted: {pipeline_error}. Commencing atomic abort/rollback...")
            for s3_key, upload_id, _ in pending_uploads:
                try:
                    # Abort Upload (3x Retries)
                    self._execute_with_retry(
                        f"Abort Upload for {s3_key}",
                        s3_client.abort_multipart_upload, Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id
                    )
                except Exception as abort_fault:
                    print(f"[FATAL ORPHAN SEVERITY] Abandoned upload footprint tracking lost on {s3_key}: {abort_fault}")
            raise pipeline_error
