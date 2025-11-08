# Use Apache Spark 4.0.1 Python image
ARG VERSION
FROM docker.io/apache/spark:${VERSION}-python3

# Switch to root to install packages
USER root

# Install curl (used to download JAR)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install PySpark for Python
ARG VERSION
COPY requirements.txt .
RUN pip install --no-cache-dir pyspark==${VERSION}
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/scripts:${PYTHONPATH}"

# Create directories for scripts and Ivy cache
RUN mkdir -p ${SPARK_HOME}/scripts /tmp/.ivy2.5.2

# Copy Python scripts into the container
COPY scripts/ ${SPARK_HOME}/scripts/
RUN chmod +x ${SPARK_HOME}/scripts/*.py || true

# Download Spark connector JARs and dependencies
RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/kafka-clients-3.9.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/commons-pool2-2.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-streaming-kafka-0-10_2.13-4.0.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.13/4.0.1/spark-streaming-kafka-0-10_2.13-4.0.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/iceberg-aws-bundle-1.10.0.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/s3-2.31.69.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.31.69/s3-2.31.69.jar && \
    curl -L -o ${SPARK_HOME}/jars/s3-transfer-manager-2.31.69.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.31.69/s3-transfer-manager-2.31.69.jar

# Switch back to Spark user (UID 185)
USER 185

# Set working directory
WORKDIR ${SPARK_HOME}

# Default command
CMD ["bash"]
