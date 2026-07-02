from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.functions import expr

DRIVER_HOST = "spark-submit-client"

SPARK_JARS = ",".join([
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/opt/spark/jars/kafka-clients-3.4.1.jar",    
    "/opt/spark/jars/commons-pool2-2.11.1.jar"
])

spark = (
    SparkSession.builder
    .appName("Remote-PySpark-Job")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", DRIVER_HOST)
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", "6001")
    .config("spark.blockManager.port", "6002")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.pyspark.memory", "1g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.cores.max", "1")
    .config("spark.executor.cores", "1")
    .config("spark.jars", SPARK_JARS)
    .getOrCreate()
)

print("--> Downloading files")
json_df = spark.read \
    .json("/opt/spark/work-dir/data/input/silver-to-gold") \
    .withColumn("id", expr("uuid()"))
json_df.show(truncate=False)

kafka_df = json_df.select(
    # to_json(struct(col("id").alias("_id"))).alias("key"), # Maps to document _id
    col("id").cast("string").alias("key"),
    to_json(struct("*")).alias("value")
)
kafka_df.show()

# streaming
# query = (kafka_df.writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "localhost:9092")
#     .option("topic", "my_target_topic")
#     .option("checkpointLocation", "/path/to/checkpoint/dir")  # Required for streaming
#     .start())

# batch
(kafka_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "silver_to_gold")
    .save())

spark.stop()
