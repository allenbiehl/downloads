# Capabilities
# - Correctly uses shared staging directory
# - Does not write _temporary directory to s3
# - Correctly writes s3 files using yyyy/mm/dd/part_file format
#
import uuid
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from utils import session_info

DRIVER_HOST = "spark-submit-client"
SPARK_JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
])

spark = (
    SparkSession.builder
    .appName("Remote-PySpark-Job")
    .master("spark://spark-master:7077")
    .config("spark.blockManager.port", "6002")
    # NOTE: this was capping the whole app to 1 core cluster-wide, which
    # makes multi-worker parallelism impossible no matter what the rest of
    # the code does. Raise both of these to actually use multiple workers.
    .config("spark.cores.max", "4")
    .config("spark.executor.cores", "2")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.host", DRIVER_HOST)
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.driver.port", "6001")
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
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
    .config("spark.jars", SPARK_JARS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # TRACE is extremely noisy for normal runs; dial back up only when debugging
session_info(spark)

json_df = spark.read \
    .option("multiLine", "false") \
    .option("dateFormat", "yyyy-MM-dd") \
    .json("s3a://input/example.json")

processed_df = json_df.withColumn("partition_date", to_date(col("event_date")))

print("--> Writing partitioned files to local staging")
STAGING_DIR = f"/opt/spark/work-dir/data/staging/{uuid.uuid4().hex}"
processed_df.repartition("partition_date") \
    .write \
    .mode("overwrite") \
    .partitionBy("partition_date") \
    .parquet(STAGING_DIR)

print("--> Loading partitions")
partitions = processed_df.select("partition_date").distinct().collect()
partition_dates = [row["partition_date"] for row in partitions]

for partition_date in sorted(partition_dates):
    date_prefix = partition_date.strftime("%Y/%m/%d")
    s3_dir = f"s3a://output/{date_prefix}"   # computed up front so it's always defined for the except block
    print(f"Copying partitioned records for '{partition_date}' -> '{s3_dir}'")
    try:
        partition_file_path = f"{STAGING_DIR}/partition_date={partition_date}/"
        partition_df = spark.read.parquet(partition_file_path)
        partition_df.write \
            .mode("append") \
            .parquet(s3_dir)
        print(f"Successfully copied partitioned records to '{s3_dir}'")
    except Exception as err:
        print(f"Failed to copy partitioned records to '{s3_dir}': {err}")
        raise  # don't silently swallow failures -- a partial run should surface loudly

# Clean up local staging so repeated runs don't accumulate disk usage.
shutil.rmtree(STAGING_DIR, ignore_errors=True)

spark.stop()