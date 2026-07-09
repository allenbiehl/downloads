# Capabilities
# - Correctly uses shared staging directory
# - Does not write _temporary directory to s3
# - Requires writing to a prefix "spark" 
# - Uses hadoop file paths (date_prefix=yyyy/mm/dd) instead of nested paths (/yyyy/MM/dd/part_file.parquet)
# - Dynamically determines partitioning via spark.sql.adaptive.enabled
# - Ensures all files are written or aborted in a single transaction

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

SPARK_JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
])

config = SparkConf()

config.set("spark.blockManager.port", "6002")

config.set("spark.cores.max", "6")
config.set("spark.driver.bindAddress", "0.0.0.0")
config.set("spark.driver.host", "spark-submit-client")
config.set("spark.driver.maxResultSize", "1g")
config.set("spark.driver.memory", "1g")
config.set("spark.driver.port", "6001")
config.set("spark.executor.cores", "2")
config.set("spark.executor.memory", "1g")
config.set("spark.executor.pyspark.memory", "1g")

config.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
config.set("spark.hadoop.fs.s3a.access.key", "spark")
config.set("spark.hadoop.fs.s3a.secret.key", "password")  
config.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
config.set("spark.hadoop.fs.s3a.path.style.access", "true")

config.set("spark.hadoop.fs.s3a.attempts.maximum", "1")
config.set("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/data/buffer")
config.set("spark.hadoop.fs.s3a.committer.name", "staging")
config.set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/data/staging")
config.set("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true")
config.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
config.set("spark.hadoop.fs.s3a.connection.timeout", "60000")

config.set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
config.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
config.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
config.set("spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
config.set("spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")

# Use adaptive to determine whether to use coalesce or repartition
config.set("spark.sql.adaptive.enabled", "true")
config.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
config.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1")
config.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")

config.set("spark.jars", SPARK_JARS)

spark = (
    SparkSession.builder
    .appName("Remote-PySpark-Job")
    .master("spark://spark-master:7077")
    .config(conf=config)
    .getOrCreate()
)

json_df = spark.read \
    .option("multiLine", "false") \
    .option("dateFormat", "yyyy-MM-dd") \
    .json("s3a://input/example.json")

date_prefix_df = json_df.withColumn("date_prefix", F.date_format(F.col("event_date"), "yyyy/MM/dd"))

date_prefix_df.write \
    .mode("append") \
    .partitionBy("date_prefix") \
    .parquet("s3a://output/spark")

spark.stop()
