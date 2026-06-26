from pyspark.sql import SparkSession

DRIVER_HOST = "spark-submit-client"

spark = SparkSession.builder \
    .appName("Remote-PySpark-Job") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", DRIVER_HOST) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.port", "6001") \
    .config("spark.blockManager.port", "6002") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.pyspark.memory", "1g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

print("--> Successfully connected to the Spark Cluster!")

# Run a simple parallelized workload
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = spark.sparkContext.parallelize(data)
result = rdd.map(lambda x: x * 2).collect()

print(f"--> Execution Result: {result}")

spark.stop()
