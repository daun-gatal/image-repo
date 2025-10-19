# Use the official Apache Spark 4.0.1 Python image (v1)
FROM docker.io/apache/spark:4.0.1

# Switch to root for installing packages
USER root

# Install curl (needed to download jars)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Switch back to the spark user for safety
USER 185  # (Spark runs as UID 185 in official images)

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/scripts:${PYTHONPATH}"

# Create scripts + ivy dirs
RUN mkdir -p ${SPARK_HOME}/scripts /tmp/.ivy2.5.2

# Copy scripts
COPY scripts/ ${SPARK_HOME}/scripts/
RUN chmod +x ${SPARK_HOME}/scripts/*.py || true

# Download connector jars
USER root
RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -L -o ${SPARK_HOME}/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar && \
    curl -L -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.13-4.0.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar

# Switch back again to non-root user
USER 185

# Working directory
WORKDIR ${SPARK_HOME}

# Default command
CMD ["bash"]
