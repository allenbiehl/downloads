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
docker compose -f docker-compose-spark.yaml --profile backend-services up
```

2. Execute job

```bash
docker exec -it spark-submit-client python3 simple.py
```

### Wordcount

1. Start cluster

```bash
docker compose -f docker-compose-spark.yaml --profile backend-services up
```

2. Execute job

```bash
docker exec -it spark-submit-client python3 wordcount.py
```

### XML S3 to Parquet S3

1. Start cluster

```bash
docker compose -f docker-compose-spark.yaml --profile backend-services up
```

2. Copy input files to minio

- resources/spark/data/input/2026/06/25 -> input/2026/06/25
- resources/spark/data/input/2026/06/26 -> input/2026/06/26

3. Execute job

```bash
docker exec -it spark-submit-client python3 xml_s3_to_parquet_s3.py
```

### JSON S3 to Parquet S3

1. Start cluster

```bash
docker compose -f docker-compose-spark.yaml --profile backend-services up
```

1. Start cluster

```bash
scripts/dev/start_spark_service.sh
```

2. Copy input files to minio

- resources/spark/data/input/example.json -> input/example.json

3. Execute job

```bash
docker exec -it spark-submit-client python3 json_s3_to_parquet_s3.py
```

### Silver FS to Gold Kafka

1. Start cluster

```bash
docker compose -f docker-compose-spark.yaml --profile backend-services up
```

2. Execute job

```bash
docker exec -it spark-submit-client python3 silver_fs_to_gold_kafka.py
```

## Download jars

1. Download pom

```bash
curl -s https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.4/spark-sql-kafka-0-10_2.12-3.3.4.pom > pom.xml
```

2. Download dependencies

```bash
mvn dependency:copy-dependencies -DoutputDirectory=./output
```

3. Download library jar

```bash
mvn dependency:copy -Dartifact=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4 -DoutputDirectory=/Users/evanbiehl/Projects/spark-local/output
```

4. Cleanup

```bash
rm pom.xml
```


###

It is easy to confuse native internal pipelines with external tools. Keep these distinctions in mind:Internal 

(Ingest Pipelines): These run on your OpenSearch nodes. They are best for quick transformations, script-based changes, or renaming fields using ⁠standard ingest processors.External 

(Data Prepper / Amazon OpenSearch Ingestion): If you use ⁠OpenSearch Data Prepper or the fully managed ⁠Amazon OpenSearch Ingestion (OSI), these pipelines run completely outside of your core OpenSearch nodes. They process, filter, and buffer data on separate serverless compute layers before sending the final results to your cluster.

If you are planning your cluster architecture, let me know if you are setting up self-managed hardware or using a cloud service like AWS, and I can provide the exact configuration rules for optimizing your node roles.

###

OpenSearch Ingestion pipelines (the managed service powered by Data Prepper) are generally faster and more scalable for high-throughput Kafka-to-OpenSearch workloads, largely because they handle HTTP bulk overhead and automatic scaling natively. However, the better choice depends on your architecture, transformation needs, and hosting environment.

How They Compare for Kafka Ingestion

OpenSearch Ingestion (OSI): This is a fully managed, serverless pipeline service. It features native pull-based ingestion (allowing the service to pull directly from Kafka) and eliminates the manual provisioning of resources. Because the service automatically scales and directly manages buffer limits and batching (often bypassing standard HTTP-based bulk API bottlenecks), it easily handles massive, high-volume streaming data.

Data Prepper: This is the open-source, self-managed data collector that powers OSI. It runs externally to your OpenSearch cluster and requires you to provision, scale, and tune your own servers. While highly performant (often sustaining immense throughput in simulated environments), configuring and scaling its Kafka consumer groups and buffer sizes falls squarely on your team's shoulders.

###

No, OpenSearch Ingestion pipelines do not only run in AWS.

While Amazon OpenSearch Ingestion (OSI) is a proprietary, fully managed AWS service, the underlying technology that powers it is Data Prepper, an open-source tool developed by the OpenSearch community. Because of this open-source core, you can run identical ingestion pipelines anywhere.

The Core Technology: Data Prepper

AWS built its managed service using ⁠OpenSearch Data Prepper. If you want to use these pipelines outside of AWS, you can deploy Data Prepper yourself:

On-Premises: Run it on your own bare-metal servers or local VMs.

Kubernetes (EKS, GKE, AKS): Deploy it using Docker containers in any cloud or local cluster.

Other Clouds: Host it on Google Cloud Platform (GCP) or Microsoft Azure.

Two Types of OpenSearch Pipelines

Depending on your architecture, you might also be referring to native features that have zero dependency on AWS:

In-Cluster Ingest Pipelines: OpenSearch has a built-in feature called ⁠Ingest Pipelines. These process data inside the cluster nodes using simple processors (like Grok or JSON parsing). This works natively in any OpenSearch cluster, whether self-hosted or managed.

Search Pipelines: OpenSearch also supports ⁠Search Pipelines natively to handle query and results transformations locally on any server.

Where Amazon OpenSearch Ingestion (OSI) is Unique

If you choose to use the specific AWS-managed variant (Amazon OpenSearch Ingestion), the pipeline execution happens on AWS infrastructure. However, its data collection boundaries are global:

External Sources: It can pull or receive data from on-premises servers, third-party applications, or remote OpenTelemetry Collectors.

AWS Destinations Only: The managed AWS service restricts you to routing the finalized data into Amazon OpenSearch Service domains or Serverless collections. It cannot write to self-managed OpenSearch clusters outside AWS.

###

Ingest Pipelines

https://docs.opensearch.org/latest/ingest-pipelines/


```
version: "2"
my-kafka-pipeline:
  source:
    kafka:
      bootstrap_servers:
        - "kafka-broker-1:9092"
        - "kafka-broker-2:9092"
      topics:
        - name: "topic-a"
          group_id: "my-consumer-group"
        - name: "topic-b"
          group_id: "my-consumer-group"
      # Extracts the original Kafka topic name to a metadata key
      schema_registry_url: "http://localhost:8081" # Optional if not using Avro
      include_key: false

  processor:
    # Extracts the current timestamp and maps it to a metadata field
    - date_printer:
        from_time_received: true
        key: "ingest_date"
        format: "yyyy.MM.dd"

  sink:
    - opensearch:
        hosts:
          - "https://search-opensearch-domain.com:443"
        username: "admin"
        password: "your-password"
        # Uses the extracted topic and date to route to dynamic indexes
        index: "logs-${getEventMetadata(\"kafka_topic_name\")}-${ingest_date}"

```

```
version: "2"
kafka-to-opensearch-pipeline:
  source:
    kafka:
      bootstrap_servers:
        - "your-kafka-broker:9092"
      topics:
        - name: "your-kafka-topic"
      group_id: "data-prepper-group"
      # (Optional) Add SSL encryption or SASL authentication if your Kafka cluster requires it

  processor:
    # Optional: Use a grok or parse processor if your message isn't already JSON
    # Or use the date processor to ensure the field is recognized as a standardized timestamp

  sink:
    - opensearch:
        hosts: [ "https://your-opensearch-domain:9200" ]
        username: "your-username"
        password: "your-password"
        # Uses the extracted message date field and appends year and month
        index: "my-index-${/internal_message_date_field:yyyy.MM}"
```

```
version: "2"
my-kafka-pipeline:
  source:
    kafka:
      bootstrap_servers:
        - "your-kafka-broker:9092"
      topics:
        - name: "topic-1"
        - name: "topic-2"
        # ... list all 30 topics
      group_id: "opensearch-ingest-group"
  processor:
    - add_entries:
        entries:
          - key: "index_name"
            # In Data Prepper, use metadata to tag the destination index dynamically
            value: "${getMetadata.kafka.topic}" 
  sink:
    - opensearch:
        hosts: [ "https://your-opensearch-domain:9200" ]
        index: "${getMetadata.kafka.topic}-index"
        username: "your-admin-username"
        password: "your-admin-password"
```

```
# Pipeline 1: Topics 1-10
topic-group-1:
  source:
    kafka:
      bootstrap_servers: ["kafka-broker-1:9092"]
      topics:
        - name: "topic_1"
        - name: "topic_2"
        # ... up to 10 topics
      group_id: "dp-group-1"
  sink:
    - pipeline:
        name: "central-pipeline"

# Pipeline 2: Topics 11-20
topic-group-2:
  source:
    kafka:
      bootstrap_servers: ["kafka-broker-1:9092"]
      topics:
        - name: "topic_11"
        - name: "topic_20"
      group_id: "dp-group-2"
  sink:
    - pipeline:
        name: "central-pipeline"

# Central pipeline for processing and storage
central-pipeline:
  source:
    pipeline:
      name: ["topic-group-1", "topic-group-2"]
  processor:
    # E.g., Grok, Date parsing, Remap
  sink:
    - opensearch:
        # OpenSearch configuration

```

## Kafka Connect OpenSearch

1. Start services

```bash
 docker compose -f docker-compose-kc.yaml up
```

2. Configuring connector

2.1. Create connector

```bash
DATA=$(cat ./resources/kafka-connect/topics/topic-1.json)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data "${DATA}"
```

2.2. Update connector

```bash
DATA=$(cat ./resources/kafka-connect/topics/topic-1.json | jq -c '.config')
curl -X PUT http://localhost:8083/connectors/topic-1/config \
  -H "Content-Type: application/json" \
  --data "${DATA}"
```

3. Delete connector

```bash
curl -X DELETE http://localhost:8083/connectors/topic-1
```

### Build kafka connect transformer

1. Verify you're running java 21

```
java --version
```

2. Run tests

```bash
cd plugins/cdet-kafka-connect-transforms
gradle clean test
```

3. Build jars

```bash
cd plugins/cdet-kafka-connect-transforms
gradle clean jar
```

4. Copy plugin jars to 

```bash
mkdir -p ./containers/kc/resources/plugins/cdet-kafka-connect-transforms
cp ./plugins/cdet-kafka-connect-transforms/build/libs/cdet-kafka-connect-transforms-1.0.0.jar \
   ./containers/kc/resources/plugins/cdet-kafka-connect-transforms
```

5. Delete temporary directories

```bash
rm -rf ./plugins/cdet-kafka-connect-transforms/.gradle
rm -rf ./plugins/cdet-kafka-connect-transforms/build
```

# automatically delete multi part uploads not committed after a period of time

The Solution: Configure an Object Lifecycle PolicyModern data lakes solve this problem by establishing a bucket-level rule that tells the MinIO or S3 cluster storage nodes to scan the tracking ledger and automatically delete any multipart upload session that has been abandoned for more than a set number of days.Method 1: Using the MinIO Client CLI (mc)You can apply a lifecycle policy to your target bucket using the official MinIO command-line tool. Run the following command to automatically purge any abandoned multipart uploads after 7 days:bash# 1. Alias your local unencrypted MinIO deployment cluster
mc alias set myminio http://localhost:9000 minioadmin minioadmin

# 2. Add an automatic lifecycle rule to target incomplete multipart uploads
mc ilm rule add myminio/production-data-lake --abort-incomplete-multipart-days 7
Use code with caution.Method 2: Injecting a Standard S3 XML Lifecycle RuleIf you prefer to configure your buckets programmatically inside an infrastructure-as-code script or a pre-flight automation hook, you can pass a standard S3 lifecycle configuration XML block.The explicit specification schema layout required to automate this cleanup looks like this:xml<LifecycleConfiguration>
    <Rule>
        <ID>PurgeAbandonedMultipartUploads</ID>
        <Status>Enabled</Status>
        <Prefix></Prefix> <!-- Applies to all folders and keys inside the bucket -->
        <AbortIncompleteMultipartUpload>
            <DaysAfterInitiation>7</DaysAfterInitiation> <!-- Deletes parts after 7 days -->
        </AbortIncompleteMultipartUpload>
    </Rule>
</LifecycleConfiguration>