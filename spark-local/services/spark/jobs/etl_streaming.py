import io
import json
import math
import os
import time
import uuid
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import fs
import pyarrow.parquet as pq 
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from botocore.client import Config
from memory_profiler import profile
from confluent_kafka import Producer, KafkaException

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

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


class BaseIngestor(ABC):
    """Interface for reading raw data chunks from a source storage layer."""
    @abstractmethod
    def configure_from_env(self): pass
    
    @abstractmethod
    def stream_chunks(self) -> pd.DataFrame: pass

class BaseTransformer(ABC):
    """Interface for pluggable business logic transformations."""
    @abstractmethod
    def configure_from_env(self): pass
    
    @abstractmethod
    def transform(self, chunk_stream) -> pd.DataFrame: pass

class BaseWriter(ABC):
    """Interface for processing and writing data atomically to a destination."""
    @abstractmethod
    def configure_from_env(self): pass
    
    @abstractmethod
    def write_stream(self, chunk_stream): pass

class S3StreamIngestor(BaseIngestor):
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
                "endpoint_url": os.getenv("AWS_ENDPOINT_URL") # Supports MinIO
            }
        }

    def stream_chunks(self):
        endpoint_url = self.storage_options["client_kwargs"].get("endpoint_url") # e.g. "http://localhost:9000"
        
        # Strip protocols out for PyArrow's endpoint_override parameter string
        clean_endpoint = endpoint_url.replace("http://", "").replace("https://", "") if endpoint_url else ""
        
        # Detect if the target configuration implies a non-secure local HTTP connection
        is_secure = not endpoint_url.startswith("http://") if endpoint_url else True

        s3_fs = fs.S3FileSystem(
            access_key=self.storage_options["key"],
            secret_key=self.storage_options["secret"],
            endpoint_override=clean_endpoint,
            scheme="http" if not is_secure else "https", # <--- Forces PyArrow to drop SSL
            force_virtual_addressing=False               # <--- Forces Path-style addressing for MinIO
        )
        clean_path = self.s3_path.replace("s3://", "")
        
        # Extension checking logic natively managed by class routing
        if clean_path.endswith(".json"): file_format = ds.JsonFileFormat()
        elif clean_path.endswith(".csv"): file_format = ds.CsvFileFormat()
        elif clean_path.endswith(".parquet"): file_format = ds.ParquetFileFormat()
        else: raise ValueError(f"Unsupported file type for path: {self.s3_path}")

        dataset = ds.dataset(clean_path, format=file_format, filesystem=s3_fs)
        
        for record_batch in dataset.to_batches():
            # 1. Wrap the single batch into a PyArrow Table object
            table_chunk = pa.Table.from_batches([record_batch])
            
            # 2. Convert the Table to a standard Pandas DataFrame
            chunk_df = table_chunk.to_pandas()
            
            # 3. Yield the DataFrame chunk to your pluggable transformer
            yield chunk_df

class AnalyticsTransformer(BaseTransformer):
    def __init__(self):
        self.target_status = None
        self.configure_from_env()

    def configure_from_env(self):
        # Example variable to prove plugins can read their own specific env variables
        self.target_status = os.getenv("TRANSFORM_STATUS_CASE", "UPPER")

    def transform(self, chunk_stream):
        for df in chunk_stream:
            # --- CUSTOM TRANSFORMATION PLUGIN LOGIC ---
            if "status" in df.columns:
                if self.target_status == "UPPER":
                    df["status"] = df["status"].str.upper()
                else:
                    df["status"] = df["status"].str.lower()
                    
            # Explicitly force robust, timezone-aware ISO parsing to a standardized column name
            df["date_partition"] = pd.to_datetime(df["event_time"], utc=True).dt.strftime("%Y-%m-%d")
            # ------------------------------------------
            yield df

class S3AtomicParquetWriter(BaseWriter):
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

    @profile
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
                    if not byte_chunk: break

                    # Upload Segment (3x Retries)
                    part_res = self._execute_with_retry(
                        f"Upload Part {part_num} for {s3_key}",
                        s3_client.upload_part,
                        Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id, PartNumber=part_num, Body=byte_chunk
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

class S3AtomicJsonWriter(BaseWriter):
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

    # @profile_etl_step("S3 Multipart Write & Atomic Commit (JSON)")
    def write_stream(self, chunk_stream):
        staged_partitions = {}
        MIN_PART_SIZE = 5 * 1024 * 1024

        # 1. Accumulate incoming chunks into in-memory JSON Lines strings
        for df in chunk_stream:
            for date_val, group in df.groupby("date_partition"):
                clean_group = group.drop(columns=["date_partition"])
                
                # FIX: Serialize straight to JSON Lines byte strings using a memory buffer
                sink = io.StringIO()
                clean_group.to_json(sink, orient="records", lines=True)
                json_string = sink.getvalue()
                
                # Ensure every added file segment finishes cleanly with a newline
                if not json_string.endswith("\n") and json_string:
                    json_string += "\n"
                
                partition_key = f"date_partition={date_val}"
                if partition_key not in staged_partitions:
                    staged_partitions[partition_key] = []
                staged_partitions[partition_key].append(json_string.encode("utf-8"))

        # 2. Directly initialize the boto3 S3 client
        from botocore.client import Config
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
            print("\n--- INITIATING ATOMIC MULTI-PART TRANSACTIONS (JSON) ---")
            for partition_path, byte_list in staged_partitions.items():
                final_file_bytes = b"".join(byte_list)
                total_bytes = len(final_file_bytes)
                
                # FIX: Save file with .json extension instead of .parquet
                s3_key = f"{self.output_prefix.strip('/')}/{partition_path}/part-0000.json"

                # Init Multipart Transaction (3x Retries)
                init_res = self._execute_with_retry(
                    f"Init Upload for {s3_key}", 
                    s3_client.create_multipart_upload, Bucket=self.output_bucket, Key=s3_key
                )
                upload_id = init_res["UploadId"]

                # --- FIXED CHUNK SLICING LOGIC ---
                if total_bytes < MIN_PART_SIZE:
                    # Single-part loophole (can be any size)
                    num_parts = 1
                else:
                    # Every preceding part will be exactly 5MB
                    # The final part will carry the leftover remainder bytes
                    num_parts = math.ceil(total_bytes / MIN_PART_SIZE)

                parts_manifest = []
                for part_num in range(1, num_parts + 1):
                    # Calculate sequential windows based on static 5MB offsets
                    start_byte = (part_num - 1) * MIN_PART_SIZE
                    
                    if part_num == num_parts:
                        # The final part captures everything up to the EOF
                        end_byte = total_bytes
                    else:
                        # All intermediate parts are forced to exactly 5MB
                        end_byte = start_byte + MIN_PART_SIZE
                        
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
                self._execute_with_retry(
                    f"Commit File {s3_key}",
                    s3_client.complete_multipart_upload,
                    Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
                )
            print("Pipeline execution completed successfully.")

        except Exception as pipeline_error:
            print(f"\n[CRITICAL FAILURE] Pipeline interrupted: {pipeline_error}. Commencing atomic abort/rollback...")
            for s3_key, upload_id, _ in pending_uploads:
                try:
                    self._execute_with_retry(
                        f"Abort Upload for {s3_key}",
                        s3_client.abort_multipart_upload, Bucket=self.output_bucket, Key=s3_key, UploadId=upload_id
                    )
                except Exception as abort_fault:
                    print(f"[FATAL ORPHAN SEVERITY] Abandoned upload footprint tracking lost on {s3_key}: {abort_fault}")
            raise pipeline_error

        staged_partitions = {}




class KafkaAtomicJsonWriter(BaseWriter):
    def __init__(self):
        self.bootstrap_servers = None
        self.topic_prefix = None
        self.transactional_id = None
        self.producer = None
        self.configure_from_env()

    def configure_from_env(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic_prefix = os.getenv("KAFKA_TOPIC_PREFIX", "analytics.events.")
        # Kafka requires a static, unique transactional ID per application instance
        self.transactional_id = os.getenv("KAFKA_TRANSACTIONAL_ID", "etl_geo_job_001")

    def _init_producer_with_retry(self):
        """Initializes the transactional Kafka producer with up to 3 retries."""
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'transactional.id': self.transactional_id,
            'enable.idempotence': True, # Required for transactional safety
            'acks': 'all'               # Guarantees all cluster brokers verify the write
        }
        
        for attempt in range(1, 4):
            try:
                producer = Producer(conf)
                # Register this transactional context with the Kafka cluster coordinator
                producer.init_transactions(timeout=5.0)
                return producer
            except KafkaException as e:
                print(f"[Attempt {attempt}/3] Failed to initialize Kafka Transaction Coordinator: {e}")
                if attempt == 3: raise e
                time.sleep(2 ** attempt)

    @profile_etl_step("Kafka Transactional Event Streaming")
    def write_stream(self, chunk_stream):
        # Initialize the native transactional engine
        self.producer = self._init_producer_with_retry()
        
        try:
            print("\n--- BEGINNING KAFKA ATOMIC TRANSACTION BOUNDARY ---")
            self.producer.begin_transaction()

            # 1. Lazily consume the DataFrame stream chunk-by-chunk
            for df in chunk_stream:
                # Group records by date dynamically so they route to separate date-based topics
                for date_val, group in df.groupby("date_partition"):
                    # Dynamically compute target topic name based on date (replaces partition folders)
                    target_topic = f"{self.topic_prefix}{date_val.replace('-', '.')}"
                    
                    # Drop partition tracker before payload transmission
                    clean_group = group.drop(columns=["date_partition"])
                    
                    # Convert the rows into structured dictionaries to stream
                    records = clean_group.to_dict(orient="records")
                    
                    for record in records:
                        payload = json.dumps(record).encode("utf-8")
                        
                        # Use event_id as the message key to maintain sequence ordering inside Kafka partitions
                        message_key = str(record.get("event_id", "")).encode("utf-8")
                        
                        # Stream the record asynchronously into Kafka's internal cluster buffer
                        self.producer.produce(
                            topic=target_topic,
                            key=message_key,
                            value=payload
                        )
                
                # Flush the memory chunk buffer over the network to keep your Python RAM flat
                self.producer.flush(timeout=0)

            # 2. THE ATOMIC JOB COMMIT PHASE (Executed only if the entire stream drains successfully)
            print("\n--- COMMITTING ALL STREAMED EVENT BATCHES TO KAFKA ---")
            self.producer.commit_transaction(timeout=10.0)
            print("Kafka stream transaction committed cleanly.")

        except Exception as pipeline_error:
            print(f"\n[CRITICAL FAILURE] Kafka pipeline aborted due to: {pipeline_error}. Commencing cluster rollback...")
            try:
                # 3. THE ATOMIC JOB ABORT PHASE (Drops all records produced during this run)
                self.producer.abort_transaction(timeout=5.0)
                print("Kafka cluster transaction successfully aborted. Zero records exposed to consumers.")
            except KafkaException as abort_fault:
                print(f"[FATAL STRANDED TRANSACTION RISK] Could not abort transaction on Kafka cluster: {abort_fault}")
            raise pipeline_error

class OpenSearchHistoricalAtomicWriter(BaseWriter):
    def __init__(self):
        self.hosts = None
        self.target_index = None
        self.client = None
        self.pipeline_run_id = None
        self.configure_from_env()

    def configure_from_env(self):
        self.hosts = [{"host": os.getenv("OPENSEARCH_HOST", "localhost"), "port": int(os.getenv("OPENSEARCH_PORT", 9200))}]
        self.target_index = os.getenv("OPENSEARCH_INDEX", "historical_telemetry")
        # Generate a unique tracking token for this specific pipeline execution run
        self.pipeline_run_id = f"run_{uuid.uuid4().hex[:12]}"

    def _execute_with_retry(self, action_name, func, *args, **kwargs):
        """Standard 3x retry runner with exponential backoff for OpenSearch network calls."""
        for attempt in range(1, 4):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"[Attempt {attempt}/3] Failed {action_name}: {str(e)}")
                if attempt == 3: raise e
                time.sleep(2 ** attempt)

    @profile_etl_step("OpenSearch Historical Transactional Bulk Write")
    def write_stream(self, chunk_stream):
        # Initialize direct unencrypted HTTP OpenSearch connection
        self.client = OpenSearch(
            hosts=self.hosts,
            use_ssl=False,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        # Track active bulk actions across chunk iterations
        indexed_count = 0

        try:
            print(f"\n--- BEGINNING OPEN-SEARCH HISTORICAL TRANSACTION BOUNDARY ---")
            print(f"Assigning Transaction Run ID: {self.pipeline_run_id}")

            # 1. Lazily consume the streaming DataFrames
            for df in chunk_stream:
                # Drop partition metadata columns since OpenSearch prefers single indices
                if "date_partition" in df.columns:
                    df = df.drop(columns=["date_partition"])
                
                # Convert DataFrame rows into OpenSearch bulk action dictionaries
                bulk_actions = []
                for record in df.to_dict(orient="records"):
                    # Inject transaction isolation metadata flags into the historical document
                    record["pipeline_run_id"] = self.pipeline_run_id
                    record["status"] = "uncommitted"  # <-- Hidden from standard consumer queries
                    
                    bulk_actions.append({
                        "_index": self.target_index,
                        "_source": record
                    })
                
                # Execute the bulk chunk upload over the network (3x Retries)
                # This flushes RAM continuously, maintaining a flat memory profile
                success, errors = self._execute_with_retry(
                    f"Bulk Uploading Chunk Segment ({len(bulk_actions)} rows)",
                    bulk, self.client, bulk_actions, stats_only=True
                )
                indexed_count += success
                
                if errors > 0:
                    raise RuntimeError(f"OpenSearch internal chunk errors encountered: {errors}")

            # 2. THE ATOMIC COMMIT PHASE (Fires only if the whole file streamed successfully)
            print(f"\n--- COMMITTING ALL {indexed_count:,} STREAMED DOCUMENTS ---")
            commit_query = {
                "script": {
                    "source": "ctx._source.status = 'committed'",
                    "lang": "painless"
                },
                "query": {
                    "term": {
                        "pipeline_run_id": self.pipeline_run_id
                    }
                }
            }
            # Flip status from 'uncommitted' to 'committed' globally across the target ID (3x Retries)
            self._execute_with_retry(
                "Finalizing Status Update Commit",
                self.client.update_by_query,
                index=self.target_index,
                body=commit_query,
                wait_for_completion=True
            )
            print("Success! Historical transaction committed safely.")

        except Exception as pipeline_error:
            print(f"\n[CRITICAL FAILURE] Pipeline failed. Commencing historical rollback/cleanup...")
            
            abort_query = {
                "query": {
                    "term": {
                        "pipeline_run_id": self.pipeline_run_id
                    }
                }
            }
            # 3. THE ATOMIC ABORT PHASE: Completely purge the dirty streaming footprint (3x Retries)
            try:
                self._execute_with_retry(
                    "Rolling back uncommitted records via Delete-By-Query",
                    self.client.delete_by_query,
                    index=self.target_index,
                    body=abort_query,
                    wait_for_completion=True
                )
                print("Rollback successful. Historical index remains completely clean.")
            except Exception as abort_fault:
                print(f"[FATAL STRANDED TRANSACTION RISK] Ghost data remains in historical index! Abort failed: {abort_fault}")
            
            raise pipeline_error


class JobManager:
    """
    Orchestration Engine.
    Dynamically executes any combination of Ingest, Transform, and Writer classes.
    """
    def __init__(self, ingest_cls: type, transform_cls: type, writer_cls: type):
        # Instantiate objects dynamically based on architectural plugin injection
        self.ingestor = ingest_cls()
        self.transformer = transform_cls()
        self.writer = writer_cls()

    @profile_etl_step("Run job")
    #@profile
    def run_job(self):
        print("Starting Job Pipeline Coordination...")
        
        # Step 1: Initialize Ingestion Stream Generator
        raw_stream = self.ingestor.stream_chunks()
        
        # Step 2: Pass stream generator into Transformation Plugin
        transformed_stream = self.transformer.transform(raw_stream)
        
        # Step 3: Pass final stream generator into Storage Engine
        self.writer.write_stream(transformed_stream)
        
        print("Job Manager Pipeline finalized successfully.")

            

if __name__ == "__main__":
    # Simulate the environment configurations supplied to your host system
    os.environ["INGEST_S3_PATH"] = "s3://input/mock_geo_data_100mb.json" # Auto-detects JSON
    os.environ["OUTPUT_S3_BUCKET"] = "output"
    os.environ["OUTPUT_S3_PREFIX"] = "nospark/"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
    os.environ["AWS_ENDPOINT_URL"] = "http://minio:9000" # MinIO / LocalStack compatible
    os.environ["TRANSFORM_STATUS_CASE"] = "UPPER"

    # 2. NEW Sink Configurations (Plugging in Kafka environment settings)
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    os.environ["KAFKA_TOPIC_PREFIX"] = "telemetry.geo.date."
    os.environ["KAFKA_TRANSACTIONAL_ID"] = "prod_geo_stream_pipeline"

    os.environ["OPENSEARCH_HOST"] = "localhost"
    os.environ["OPENSEARCH_PORT"] = "9200"
    os.environ["OPENSEARCH_INDEX"] = "company_historical_logs"

    # The Job Manager instantiates the classes dynamically based on your chosen plugins
    pipeline_job = JobManager(
        ingest_cls=S3StreamIngestor,
        transform_cls=AnalyticsTransformer,
        writer_cls=S3AtomicJsonWriter
    )
    
    # Execute the end-to-end streamed transaction
    pipeline_job.run_job()
