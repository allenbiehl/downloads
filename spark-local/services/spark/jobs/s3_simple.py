import json
from pathlib import Path
import shutil
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, input_file_name, concat_ws, date_format, concat, lit, to_timestamp
from memory_profiler import profile

import functools
import types
import time

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


def iteration(spark):
    print("--> Downloading file")
    json_df = spark.read \
        .option("multiLine", "false") \
        .option("dateFormat", "yyyy-MM-dd") \
        .json("s3a://input/mock_geo_data_1mb.json")
    print("Input files", json_df.inputFiles())

    print("--> Add date partition column")
    processed_df = json_df.withColumn(
        "partition_date", 
        date_format(to_timestamp(col("event_time"), "yyyy/MM/dd HH:mm:ss"), "yyyy-MM-dd")
    )

    print("--> Writing partitioned files")
    processed_df \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .partitionBy("partition_date") \
        .json("s3a://output/spark")    

#@profile_etl_step("main")
@profile
def main():
    DRIVER_HOST = "spark-submit-client"

    SPARK_JARS = ",".join([
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    ])

    spark = (
        SparkSession.builder
        .appName("test")
        .master("spark://spark-master:7077")
        .config("spark.blockManager.port", "6002")    
        .config("spark.cores.max", "6")
        .config("spark.driver.host", DRIVER_HOST)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "6001")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")    
        .config("spark.executor.memory", "1g")
        .config("spark.executor.pyspark.memory", "1g")
        .config("spark.executor.cores", "2")

        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")

        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/data/buffer")
        .config("spark.hadoop.fs.s3a.committer.name", "staging")
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/data/staging")
        .config("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")

        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
                    "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.sql.parquet.output.committer.class",
                    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
        .config("spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")

        .config("spark.jars", SPARK_JARS)
        .getOrCreate()
    )

    # spark.sparkContext.setLogLevel("TRACE")

    iteration(spark)

    # file_paths = [row[0] for row in processed_df.select("_metadata.file_name").distinct().collect()]

    # for file in file_paths:
    #     print(file)

    spark.stop()

if __name__ == "__main__":
    main()
