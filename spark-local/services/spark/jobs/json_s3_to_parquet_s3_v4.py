import json
from pathlib import Path
import shutil
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, input_file_name, concat_ws, date_format, concat, lit
from utils import session_info

DRIVER_HOST = "spark-submit-client"

SPARK_JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/jars/app-spark-common-1.0.jar",    
])

spark = (
    SparkSession.builder
    .appName("Remote-PySpark-Job")
    .master("spark://spark-master:7077")

    .config("spark.driver.extraJavaOptions", "-Dlog4j.configiguration=log4j2.properties")
    .config("spark.driver.logLevel", "TRACE")
    .config("spark.executor.logLevel", "TRACE")

    .config("spark.blockManager.port", "6002")
    .config("spark.cores.max", "1")    
    .config("spark.driver.bindAddress", "0.0.0.0")    
    .config("spark.driver.host", DRIVER_HOST)
    .config("spark.driver.maxResultSize", "1g")    
    .config("spark.driver.memory", "1g")    
    .config("spark.driver.port", "6001")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "1g")    
    .config("spark.executor.pyspark.memory", "1g")

    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "spark")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")

    .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
    .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/data/buffer")
    .config("spark.hadoop.fs.s3a.committer.name", "staging")
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/data/staging")
    .config("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") 

    .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")    
    # .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.HadoopMapReduceCommitProtocolCustom")
    .config("spark.jars", SPARK_JARS)
    .getOrCreate()
)

session_info(spark)

json_df = spark.read \
    .option("multiLine", "false") \
    .option("dateFormat", "yyyy-MM-dd") \
    .json("s3a://input/example.json")

processed_df = json_df.withColumn("partition_date", to_date(col("event_date")))

print("--> Writing partitioned files")
STAGING_DIR = f"/opt/spark/work-dir/data/staging/{uuid.uuid4().hex}"
processed_df.repartition("partition_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("partition_date") \
    .parquet(STAGING_DIR)

print("--> Loading partitions")
partitions = processed_df.select("partition_date").distinct().collect()
partition_dates = [row['partition_date'] for row in partitions]

for partition_date in sorted(partition_dates):
    print(f"Copying partitioned records for '{partition_date}'")
    (
        processed_df
            .filter(col("partition_date") == partition_date)
            .drop("partition_date")
            .write
            .mode("append")
            .parquet(f"s3a://output/{partition_date:%Y/%m/%d}")
    )        
    # try:
    #     date_prefix = partition_date.strftime("%Y/%m/%d")
    #     s3_dir = f"s3a://output/{date_prefix}"
    #     partition_file_path = f"{STAGING_DIR}/partition_date={partition_date}/"
    #     partition_df = spark.read.parquet(partition_file_path)
    #     partition_df.write \
    #         .mode("append") \
    #         .parquet(f"s3a://output/{date_prefix}")
    #     print(f"Successfully copied partioned records to '{s3_dir}'")
    #     partition_df.show()

    #     print("Output files", partition_df.summary())
    # except Exception as err:
    #     print(f"Failed to copy partioned records to '{s3_dir}'")



spark.stop()
