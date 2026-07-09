FROM confluentinc/cp-kafka-connect-base:8.2.2

USER root

RUN microdnf install -y wget tar && microdnf clean all

RUN mkdir -p /usr/share/kafka/plugins/aiven-opensearch-connector && \
    wget -qO- \
    https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v3.1.1/opensearch-connector-for-apache-kafka-3.1.1.tar | \
    tar -xvf - --strip-components=1 -C /usr/share/kafka/plugins/aiven-opensearch-connector && \
    rm -rf /usr/share/kafka/plugins/aiven-opensearch-connector/jackson-databind-*.jar && \
    rm -rf /usr/share/kafka/plugins/aiven-opensearch-connector/jackson-core-*.jar && \
    rm -rf /usr/share/kafka/plugins/aiven-opensearch-connector/jackson-annotations-*.jar

RUN mkdir -p /usr/share/kafka/plugins/aiven-opensearch-transform && \
    wget -qO- \
    https://github.com/Aiven-Open/transforms-for-apache-kafka-connect/releases/download/v1.6.0/transforms-for-apache-kafka-connect-1.6.0.tar | \
    tar -xvf - --strip-components=1 -C /usr/share/kafka/plugins/aiven-opensearch-transform && \
    rm -rf /usr/share/kafka/plugins/aiven-opensearch-transform/jackson-databind-*.jar && \
    rm -rf /usr/share/kafka/plugins/aiven-opensearch-transform/jackson-core-*.jar && \
    rm -rf /usr/share/kafka/plugins/aiven-opensearch-transform/jackson-annotations-*.jar

COPY ./resources/plugins/cdet-kafka-connect-transforms /usr/share/kafka/plugins/cdet-kafka-connect-transforms

USER appuser