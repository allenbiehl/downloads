 
def session_info(spark) -> None:
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    for key in [
        "mapreduce.outputcommitter.factory.scheme.s3a",
        "fs.s3a.committer.name",
        "fs.s3a.committer.staging.tmp.path",
        "mapreduce.fileoutputcommitter.algorithm.version",
        "spark.sql.sources.commitProtocolClass",
    ]:
        print(f"{key} = {conf.get(key)}")

    for key in [
        "spark.sql.sources.commitProtocolClass",
        "spark.sql.parquet.output.committer.class",
    ]:
        try:
            print(key, spark.conf.get(key))
        except Exception:
            print(key, "<not set>")