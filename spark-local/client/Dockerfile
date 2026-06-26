FROM apache/spark:3.5.1

# ENV PYSPARK_PYTHON=python3
# ENV PYSPARK_DRIVER_PYTHON=python3

RUN wget \
    -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar && \
    wget \
    -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget \
    -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    wget \
    -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar

USER root

RUN python3 -m pip install pyspark "pyspark[sql]" pandas

USER spark