import json
from pathlib import Path
import shutil
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, input_file_name, concat_ws, date_format, concat, lit

DRIVER_HOST = "spark-submit-client"

SPARK_JARS = ",".join([
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar"
])

spark = (
    SparkSession.builder
    .appName("test")
    .master("spark://spark-master:7077")
    .config("spark.blockManager.port", "6002")    
    .config("spark.cores.max", "1")
    .config("spark.driver.host", DRIVER_HOST)
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", "6001")
    .config("spark.driver.memory", "1g")
    .config("spark.driver.maxResultSize", "1g")    
    .config("spark.executor.memory", "1g")
    .config("spark.executor.pyspark.memory", "1g")
    .config("spark.executor.cores", "1")

    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")

    .config("spark.jars", SPARK_JARS)
    .getOrCreate()
)

print("--> Downloading file")
json_df = spark.read \
    .option("multiLine", "false") \
    .option("dateFormat", "yyyy-MM-dd") \
    .json("s3a://input/example.json")
print("Input files", json_df.inputFiles())

print("--> Add date partition column")
processed_df = json_df.withColumn("partition_date", to_date(col("event_date")))

print("--> Writing partitioned files")
processed_df \
    .write \
    .mode("overwrite") \
    .partitionBy("partition_date") \
    .parquet("s3a://output/data")

file_paths = [row[0] for row in processed_df.select("_metadata.file_name").distinct().collect()]

for file in file_paths:
    print(file)

spark.stop()
