# Spark Local

https://oneuptime.com/blog/post/2026-02-08-how-to-run-apache-spark-in-docker/view

## Dependencies

### S3 Spark jars

https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

## Jobs

### Simple

1. Start cluster

```bash
docker compose up
```

2. Execute job

```bash
docker exec -it spark-submit-client python3 simple.py
```

### Wordcount

1. Start cluster

```bash
docker compose up
```

2. Copy input file

```bash
docker cp ./jobs/wordcount.txt spark-master:/opt/spark/work-dir/data/input/wordcount.txt
```

3. Execute job

```bash
docker exec -it spark-submit-client python3 wordcount.py
```

### S3 xml -> parquet

1. Start cluster

```bash
docker compose up
```

2. Copy input files to minio

- resources/spark/data/input/2026/06/25 -> input/2026/06/25
- resources/spark/data/input/2026/06/26 -> input/2026/06/26

3. Execute job

```bash
docker exec -it spark-submit-client python3 s3_xml_parquet.py
```

### S3 json -> parquet

1. Start cluster

```bash
docker compose up
```

2. Copy input files to minio

- resources/spark/data/input/example.json -> input/example.json

3. Execute job

```bash
docker exec -it spark-submit-client python3 s3_json_parquet.py
```