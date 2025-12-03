ARG VERSION

FROM docker.io/confluentinc/cp-kafka-connect:${VERSION}

# Create plugin directory
RUN mkdir -p /usr/share/java/iceberg-connect

RUN wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar \
        -O /usr/share/java/iceberg-connect/postgresql-42.7.8.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-aws-bundle-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.31.69/s3-2.31.69.jar \
        -O /usr/share/java/iceberg-connect/s3-2.31.69.jar && \
    wget https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.31.69/s3-transfer-manager-2.31.69.jar \
        -O /usr/share/java/iceberg-connect/s3-transfer-manager-2.31.69.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/1.10.0/iceberg-api-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-api-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-bundled-guava/1.10.0/iceberg-bundled-guava-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-bundled-guava-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-common/1.10.0/iceberg-common-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-common-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/1.10.0/iceberg-core-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-core-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-data/1.10.0/iceberg-data-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-data-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-metastore/1.10.0/iceberg-hive-metastore-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-hive-metastore-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect/1.10.0/iceberg-kafka-connect-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-kafka-connect-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect-events/1.10.0/iceberg-kafka-connect-events-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-kafka-connect-events-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect-transforms/1.10.0/iceberg-kafka-connect-transforms-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-kafka-connect-transforms-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-parquet/1.10.0/iceberg-parquet-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-parquet-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-orc/1.10.0/iceberg-orc-1.10.0.jar \
        -O /usr/share/java/iceberg-connect/iceberg-orc-1.10.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
        -O /usr/share/java/iceberg-connect/hadoop-aws-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar \
        -O /usr/share/java/iceberg-connect/hadoop-common-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.4.1/hadoop-hdfs-client-3.4.1.jar \
        -O /usr/share/java/iceberg-connect/hadoop-hdfs-client-3.4.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.3.0/hadoop-shaded-guava-1.3.0.jar \
        -O /usr/share/java/iceberg-connect/hadoop-shaded-guava-1.3.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.4/avro-1.11.4.jar \
        -O /usr/share/java/iceberg-connect/avro-1.11.4.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.11.0/commons-configuration2-2.11.0.jar \
        -O /usr/share/java/iceberg-connect/commons-configuration2-2.11.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-text/1.13.0/commons-text-1.13.0.jar \
        -O /usr/share/java/iceberg-connect/commons-text-1.13.0.jar

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"