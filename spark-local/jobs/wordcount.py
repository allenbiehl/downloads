import uuid
from pyspark.sql import SparkSession

DRIVER_HOST = "spark-submit-client"

spark = SparkSession.builder \
    .appName("WordCount") \
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

# Read a text file and count word frequencies
text_rdd = spark.sparkContext.textFile("/opt/spark/work-dir/data/input/wordcount.txt")
counts = text_rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print(f"--> Execution Result: {counts}")

# Save results
random_uuid = uuid.uuid4()
counts.saveAsTextFile(f"/opt/spark/work-dir/data/output/{random_uuid.hex}")
spark.stop()
