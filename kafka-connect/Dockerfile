ARG VERSION
FROM confluentinc/cp-kafka-connect:${VERSION}

# Iceberg version you want
ARG ICEBERG_VERSION=1.10.0

# Create plugin directory
RUN mkdir -p /usr/share/java/iceberg-connect

# Install Iceberg Kafka Connect sink + dependencies
RUN curl -L -o /usr/share/java/iceberg-connect/iceberg-kafka-connect.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-kafka-connect/${ICEBERG_VERSION}/iceberg-kafka-connect-${ICEBERG_VERSION}.jar && \
    curl -L -o /usr/share/java/iceberg-connect/iceberg-api.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/${ICEBERG_VERSION}/iceberg-api-${ICEBERG_VERSION}.jar && \
    curl -L -o /usr/share/java/iceberg-connect/iceberg-core.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/${ICEBERG_VERSION}/iceberg-core-${ICEBERG_VERSION}.jar && \
    curl -L -o /usr/share/java/iceberg-connect/iceberg-common.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-common/${ICEBERG_VERSION}/iceberg-common-${ICEBERG_VERSION}.jar && \
    curl -L -o /usr/share/java/iceberg-connect/iceberg-parquet.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-parquet/${ICEBERG_VERSION}/iceberg-parquet-${ICEBERG_VERSION}.jar

# OPTIONAL: S3 / MinIO Support
RUN curl -L -o /usr/share/java/iceberg-connect/iceberg-aws.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"
