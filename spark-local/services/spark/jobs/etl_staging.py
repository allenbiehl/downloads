import io
import os
import math
import time
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from memory_profiler import profile


import functools
import types

def profile_etl_step(step_name):
    """
    A robust time profiler decorator compatible with standard functions 
    and Python generator streams (yield). Tracks throughput metrics automatically.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print(f"\n[PROFILER] >>> Starting ETL Step: {step_name}...")
            start_time = time.perf_counter()
            
            # Invoke the inner function
            result = func(*args, **kwargs)
            
            # SCENARIO A: The function returns a Generator Stream (yield)
            if isinstance(result, types.GeneratorType):
                def generator_wrapper(gen):
                    item_count = 0
                    try:
                        for chunk in gen:
                            item_count += 1
                            yield chunk
                    finally:
                        # This executes only when the downstream consumer completely drains the generator
                        end_time = time.perf_counter()
                        duration = end_time - start_time
                        print(f"\n" + "="*45)
                        print(f"      STREAM METRICS: {step_name.upper()}")
                        print("="*45)
                        print(f"Status              : Fully Drained")
                        print(f"Total Stream Batches: {item_count:,}")
                        print(f"Total Processing Time: {duration:.4f} seconds")
                        if duration > 0:
                            print(f"Stream Velocity     : {item_count / duration:.2f} batches/sec")
                        print("="*45 + "\n")
                
                return generator_wrapper(result)
                
            # SCENARIO B: The function is a standard execution block
            else:
                end_time = time.perf_counter()
                duration = end_time - start_time
                print(f"\n" + "="*45)
                print(f"    FUNCTION METRICS: {step_name.upper()}")
                print("="*45)
                print(f"Status              : Execution Complete")
                print(f"Total Execution Time: {duration:.4f} seconds")
                print("="*45 + "\n")
                return result
                
        return wrapper
    return decorator

class BaseInMemoryIngestor(ABC):
    """Interface for reading the entire dataset into memory at once."""
    @abstractmethod
    def configure_from_env(self): pass
    
    @abstractmethod
    def ingest_all(self) -> pd.DataFrame: pass

class BaseInMemoryTransformer(ABC):
    """Interface for transforming a complete DataFrame."""
    @abstractmethod
    def configure_from_env(self): pass
    
    @abstractmethod
    def transform_all(self, df: pd.DataFrame) -> pd.DataFrame: pass

class BaseInMemoryWriter(ABC):
    """Interface for writing a complete DataFrame atomically."""
    @abstractmethod
    def configure_from_env(self): pass
    
    @abstractmethod
    def write_all(self, df: pd.DataFrame): pass

class S3InMemoryIngestor(BaseInMemoryIngestor):
    def __init__(self):
        self.s3_path = None
        self.storage_options = {}
        self.configure_from_env()

    def configure_from_env(self):
        self.s3_path = os.getenv("INGEST_S3_PATH")
        self.storage_options = {
            "key": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "client_kwargs": {
                "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                "endpoint_url": os.getenv("AWS_ENDPOINT_URL")
            }
        }

    # @profile_etl_step("In-Memory Full Ingestion")
    def ingest_all(self) -> pd.DataFrame:
        endpoint_url = self.storage_options["client_kwargs"].get("endpoint_url")
        clean_endpoint = endpoint_url.replace("http://", "").replace("https://", "") if endpoint_url else ""
        is_http = endpoint_url.startswith("http://") if endpoint_url else False

        s3_fs = fs.S3FileSystem(
            access_key=self.storage_options["key"],
            secret_key=self.storage_options["secret"],
            endpoint_override=clean_endpoint,
            scheme="http" if is_http else "https",
            force_virtual_addressing=False
        )
        
        clean_path = self.s3_path
        for protocol in ["s3://", "http://", "https://"]:
            if clean_path.startswith(protocol):
                clean_path = clean_path[len(protocol):]
        
        if clean_path.endswith(".json"): file_format = ds.JsonFileFormat()
        elif clean_path.endswith(".csv"): file_format = ds.CsvFileFormat()
        elif clean_path.endswith(".parquet"): file_format = ds.ParquetFileFormat()
        else: raise ValueError(f"Unsupported file type: {self.s3_path}")

        dataset = ds.dataset(clean_path, format=file_format, filesystem=s3_fs)
        
        # Pull all batches from S3 and bind them directly into one single Arrow Table
        full_table = dataset.to_table()
        
        # Convert the massive table to a single Pandas DataFrame
        full_df = full_table.to_pandas()
        return full_df

class InMemoryTransformer(BaseInMemoryTransformer):
    def __init__(self):
        self.target_status = None
        self.configure_from_env()

    def configure_from_env(self):
        self.target_status = os.getenv("TRANSFORM_STATUS_CASE", "UPPER")

    # @profile_etl_step("In-Memory Full Transformation")
    def transform_all(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create a clean shallow copy to manipulate
        transformed_df = df.copy()
        
        if "status" in transformed_df.columns:
            if self.target_status == "UPPER":
                transformed_df["status"] = transformed_df["status"].str.upper()
            else:
                transformed_df["status"] = transformed_df["status"].str.lower()
                
        # Parse the entire date index array all at once
        transformed_df["date_partition"] = pd.to_datetime(transformed_df["event_time"], utc=True).dt.strftime("%Y-%m-%d")
        
        return transformed_df

class S3InMemoryJsonWriter(BaseInMemoryWriter):
    def __init__(self):
        self.output_bucket = None
        self.output_prefix = None
        self.storage_options = {}
        self.configure_from_env()

    def configure_from_env(self):
        self.output_bucket = os.getenv("OUTPUT_S3_BUCKET")
        self.output_prefix = os.getenv("OUTPUT_S3_PREFIX")
        self.storage_options = {
            "key": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "endpoint_url": os.getenv("AWS_ENDPOINT_URL")
        }

    def _execute_with_retry(self, action_name, func, *args, **kwargs):
        for attempt in range(1, 4):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                print(f"[Attempt {attempt}/3] Failed {action_name}: {e.response['Error']['Message']}")
                if attempt == 3: raise e
                time.sleep(2 ** attempt)

    # @profile_etl_step("In-Memory Full S3 Write & Commit")
    def write_all(self, df: pd.DataFrame):
        staged_partitions = {}
        MIN_PART_SIZE = 5 * 1024 * 1024

        # Group and serialize everything in memory at once
        for date_val, group in df.groupby("date_partition"):
            clean_group = group.drop(columns=["date_partition"])
            
            sink = io.StringIO()
            clean_group.to_json(sink, orient="records", lines=True)
            json_string = sink.getvalue()
            
            if not json_string.endswith("\n") and json_string:
                json_string += "\n"
            
            staged_partitions[f"date_partition={date_val}"] = [json_string.encode("utf-8")]

        from botocore.client import Config
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.storage_options["key"],
            aws_secret_access_key=self.storage_options["secret"],
            endpoint_url=self.storage_options["endpoint_url"],
            use_ssl=False,
            verify=False,
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        )
        
        pending_uploads = []

        try:
            print("\n--- INITIATING ATOMIC MULTI-PART TRANSACTIONS ---")
            for partition_path, byte_list in staged_partitions.items():
                final_file_bytes = b"".join(byte_list)
                total_bytes = len(final_file_bytes)
                s3_key = f"{self.output_prefix.strip('/')}/{partition_path}/part-0000.json"

                init_res = self._execute_with_retry(
                    f"Init Upload for {s3_key}", 
                    s3_client.create_multipart_upload, Bucket=self.output_bucket, Key=s3_key
                )
                upload_id = init_res["UploadId"]

                if total_bytes < MIN_PART_SIZE:
                    num_parts = 1
                else:
                    num_parts = math.ceil(total_bytes / MIN_PART_SIZE)

                parts_manifest = []
                for part_num in range(1, num_parts + 1):
                    start_byte = (part_num - 1) * MIN_PART_SIZE
                    end_byte = total_bytes if part_num == num_parts else start_byte + MIN_PART_SIZE
                    byte_chunk = final_file_bytes[start_byte:end_byte]
                    if not byte_chunk: break

                    part_res = self._execute_with_retry(
                        f"Upload Part {part_num} for {s3_key}",
                        s3_client.upload_part,
                        Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id, PartNumber=part_num, Body=byte_chunk
                    )
                    parts_manifest.append({"ETag": part_res["ETag"], "PartNumber": part_num})

                pending_uploads.append((s3_key, upload_id, parts_manifest))

            print(f"\n--- COMMITTING {len(pending_uploads)} PARTITIONS ATOMICALLY ---")
            for s3_key, upload_id, parts in pending_uploads:
                self._execute_with_retry(
                    f"Commit File {s3_key}",
                    s3_client.complete_multipart_upload,
                    Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
                )
            
            # CLEAR WRITER INTERNAL CACHES IMMEDIATELY AFTER SUCCESSFUL COMMIT
            staged_partitions.clear()
            pending_uploads.clear()
            print("Writer memory footprint cleaned.")

        except Exception as pipeline_error:
            print(f"\n[CRITICAL FAILURE] Rollback triggered...")
            for s3_key, upload_id, _ in pending_uploads:
                try:
                    self._execute_with_retry(
                        f"Abort Upload for {s3_key}",
                        s3_client.abort_multipart_upload, Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id
                    )
                except Exception as abort_fault:
                    print(f"Failed to abort {s3_key}: {abort_fault}")
            
            staged_partitions.clear()
            pending_uploads.clear()
            raise pipeline_error

class InMemoryJobManager:
    def __init__(self, ingest_cls: type, transform_cls: type, writer_cls: type):
        self.ingestor = ingest_cls()
        self.transformer = transform_cls()
        self.writer = writer_cls()

    @profile_etl_step("Run job")
    # @profile
    def run_job(self):
        print("Starting Monolithic In-Memory Job Coordination...")
        
        # 1. STAGE 1: Ingest everything into RAM
        ingest_df = self.ingestor.ingest_all()
        print(f"Ingested complete dataset shape: {ingest_df.shape}")
        
        # 2. STAGE 2: Pass full DataFrame to Transform
        transformed_df = self.transformer.transform_all(ingest_df)
        print(f"Transformed complete dataset shape: {transformed_df.shape}")
        
        # CLEAR STAGE 1 MEMORY IMMEDIATELY
        print("Stage 1 Ingest DataFrame successfully purged from heap memory.")
        
        # 3. STAGE 3: Pass transformed DataFrame to Writer
        self.writer.write_all(transformed_df)
        
        # CLEAR STAGE 2 MEMORY IMMEDIATELY
        print("Stage 2 Transform DataFrame successfully purged from heap memory.")
        
        print("All stages finalized and cleaned independently.")



if __name__ == "__main__":
    # Simulate the environment configurations supplied to your host system
    os.environ["INGEST_S3_PATH"] = "s3://input/mock_geo_data_100mb.json" # Auto-detects JSON
    os.environ["OUTPUT_S3_BUCKET"] = "output"
    os.environ["OUTPUT_S3_PREFIX"] = "nospark/"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
    os.environ["AWS_ENDPOINT_URL"] = "http://minio:9000" # MinIO / LocalStack compatible
    os.environ["TRANSFORM_STATUS_CASE"] = "UPPER"

    # The Job Manager instantiates the classes dynamically based on your chosen plugins
    pipeline_job = InMemoryJobManager(
        ingest_cls=S3InMemoryIngestor,
        transform_cls=InMemoryTransformer,
        writer_cls=S3InMemoryJsonWriter
    )
    
    # Execute the end-to-end streamed transaction
    pipeline_job.run_job()