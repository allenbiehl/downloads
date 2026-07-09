# import json
# from pathlib import Path
# import shutil
# import uuid
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, input_file_name, concat_ws, date_format, concat, lit

# DRIVER_HOST = "spark-submit-client"

# SPARK_JARS = ",".join([
#     "/opt/spark/jars/hadoop-aws-3.3.4.jar",
#     "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
# ])

# spark = (
#     SparkSession.builder
#     .appName("test")
#     .master("spark://spark-master:7077")
#     .config("spark.blockManager.port", "6002")    
#     .config("spark.cores.max", "6")
#     .config("spark.driver.host", DRIVER_HOST)
#     .config("spark.driver.bindAddress", "0.0.0.0")
#     .config("spark.driver.port", "6001")
#     .config("spark.driver.memory", "1g")
#     .config("spark.driver.maxResultSize", "1g")    
#     .config("spark.executor.memory", "1g")
#     .config("spark.executor.pyspark.memory", "1g")
#     .config("spark.executor.cores", "2")

#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#     .config("spark.hadoop.fs.s3a.access.key", "admin")
#     .config("spark.hadoop.fs.s3a.secret.key", "password")
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
#     .config("spark.hadoop.fs.s3a.attempts.maximum", "3")

#     .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
#     .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/data/buffer")
#     .config("spark.hadoop.fs.s3a.committer.name", "staging")
#     .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/data/staging")
#     .config("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true")
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .config("spark.hadoop.fs.s3a.connection.timeout", "60000")

#     .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
#                 "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
#     .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
#     .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
#     .config("spark.sql.parquet.output.committer.class",
#                 "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
#     .config("spark.sql.sources.commitProtocolClass",
#                 "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")

#     .config("spark.jars", SPARK_JARS)
#     .getOrCreate()
# )

# # spark.sparkContext.setLogLevel("TRACE")

# print("--> Downloading file")
# json_df = spark.read \
#     .option("multiLine", "false") \
#     .option("dateFormat", "yyyy-MM-dd") \
#     .json("s3a://input/example.json")
# print("Input files", json_df.inputFiles())

# print("--> Add date partition column")
# processed_df = json_df.withColumn("partition_date", to_date(col("event_date")))

# print("--> Writing partitioned files")
# processed_df \
#     .write \
#     .mode("overwrite") \
#     .partitionBy("partition_date") \
#     .parquet("s3a://output/data")

# file_paths = [row[0] for row in processed_df.select("_metadata.file_name").distinct().collect()]

# for file in file_paths:
#     print(file)

# spark.stop()



import io
import time
import math
import boto3
import pandas as pd
from botocore.exceptions import ClientError

def execute_with_retry(action_name, func, *args, max_attempts=3, **kwargs):
    """Utility helper to execute any S3 network function with up to 3 retries."""
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            print(f"[Attempt {attempt}/{max_attempts}] Failed {action_name}: {e.response['Error']['Message']}")
            if attempt == max_attempts:
                raise e
            time.sleep(2 ** attempt) # Exponential backoff

def s3a_resilient_committer_write(
    df: pd.DataFrame, 
    bucket: str, 
    base_prefix: str, 
    partition_cols: list, 
    s3_client
):
    """
    Spark S3A Magic/Staging Committer Emulator.
    Features:
      - Dynamic chunk sizing (Handles any file size; fits single-part loophole under 5MB).
      - Strict 3x Retry logic on Uploads, Commits, and Aborts.
      - Atomic transaction boundary (All-or-Nothing).
    """
    pending_uploads = [] 
    MIN_PART_SIZE = 5 * 1024 * 1024 

    try:
        print(f"--- STAGE PHASE STARTED ---")
        
        # 1. Slice and group data by structural partitions
        for partition_values, group in df.groupby(partition_cols):
            if group.empty:
                continue
            
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)
                
            partition_path = "/".join(f"{col}={val}" for col, val in zip(partition_cols, partition_values))
            file_df = group.drop(columns=partition_cols)
            if file_df.empty:
                continue

            key = f"{base_prefix.strip('/')}/{partition_path}/part-0000.json"
            
            # Initiate Multipart Upload (Retried up to 3 times)
            init_res = execute_with_retry(
                f"Initiate Upload for {key}", 
                s3_client.create_multipart_upload, Bucket=bucket, Key=key
            )
            upload_id = init_res["UploadId"]
            
            # In-memory payload serialization
            buffer = io.BytesIO()
            file_df.to_json(buffer, orient="records", lines=True)
            buffer.seek(0)
            file_bytes = buffer.read()
            total_bytes = len(file_bytes)
            
            # S3 Loophole handling: If total size < 5MB, make it a 1-part upload.
            # Otherwise, split it into chunks where all preceding chunks are >= 5MB.
            if total_bytes < MIN_PART_SIZE:
                num_parts = 1
                part_size = total_bytes
            else:
                num_parts = math.ceil(total_bytes / MIN_PART_SIZE)
                part_size = math.ceil(total_bytes / num_parts)
            
            parts_manifest = []
            
            # Upload data parts
            for part_num in range(1, num_parts + 1):
                start_byte = (part_num - 1) * part_size
                end_byte = min(start_byte + part_size, total_bytes)
                byte_chunk = file_bytes[start_byte:end_byte]
                
                if not byte_chunk:
                    break
                
                # Upload single payload segment (Retried up to 3 times)
                part_res = execute_with_retry(
                    f"Upload Part {part_num} for {key}",
                    s3_client.upload_part,
                    Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part_num, Body=byte_chunk
                )
                parts_manifest.append({"ETag": part_res["ETag"], "PartNumber": part_num})
            
            # Cache active metadata token
            pending_uploads.append((key, upload_id, parts_manifest))
            
        # 2. JOB COMMIT PHASE (All parts must succeed to reach this block)
        print(f"\n--- COMMIT PHASE STARTED ---")
        print(f"Staged {len(pending_uploads)} files successfully. Finalizing transaction...")
        
        for key, upload_id, parts in pending_uploads:
            # Complete transaction flag (Retried up to 3 times)
            execute_with_retry(
                f"Commit File {key}",
                s3_client.complete_multipart_upload,
                Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )
        print("Success! All transactional data files committed cleanly to S3.")
        
    except Exception as job_error:
        # 3. JOB ABORT PHASE (Triggered if any step above throws an error)
        print(f"\n--- TRANSACTION FAILED ---")
        print(f"Reason: {job_error}")
        print(f"Aborting and rolling back {len(pending_uploads)} staged file uploads...")
        
        for key, upload_id, _ in pending_uploads:
            try:
                # Cancel pending transaction footprint (Retried up to 3 times)
                execute_with_retry(
                    f"Abort Upload for {key}",
                    s3_client.abort_multipart_upload,
                    Bucket=bucket, Key=key, UploadId=upload_id
                )
            except Exception as abort_critical_err:
                print(f"[CRITICAL FAILURE] Permanent orphan risk for {key}: {abort_critical_err}")
        
        # Bubble error up to target orchestration framework (Glue, Lambda, Airflow)
        raise job_error


# import awswrangler as wr
# import boto3
# import pandas as pd

storage_options = {
    "key": "MINIO_ROOT_USER",               # Your MinIO Access Key
    "secret": "MINIO_ROOT_PASSWORD",       # Your MinIO Secret Key
    "client_kwargs": {
        "endpoint_url": "http://localhost:9000" # Your MinIO Server URL
    }
}


# wr.config.s3_endpoint_url = "http://minio:9000"


s3_client = boto3.client(
    service_name="s3",
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    endpoint_url="http://minio:9000",
    verify=False,
)

# 2. Extract Bucket and Key names from your path ("s3://input/example.json")
BUCKET_NAME = "input"
KEY_NAME = "example.json"

# 3. Stream the file directly into memory
response = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY_NAME)
file_stream = io.BytesIO(response["Body"].read())

# 4. Read into Pandas using the in-memory stream (Requires zero dependencies!)
json_df = pd.read_json(file_stream, lines=True)

json_df = pd.read_json("s3://input/example.json", lines=True,  storage_options=storage_options)

# wr.s3.to_parquet(
#     df=df,
#     path="s3://output/spark",
#     dataset=True,
#     mode="append",
#     partition_cols=["event_date"],
#     boto3_session=custom_session
# )

# processed_df = json_df.withColumn("partition_date", to_date(col("event_date")))

json_df["partition_date"] = pd.to_datetime(json_df["event_date"])

# Execute the atomic commit operation safely
s3a_resilient_committer_write(
    df=json_df,
    bucket="output",
    base_prefix="spark/",
    partition_cols=["partition_date"],
    s3_client=s3_client
)