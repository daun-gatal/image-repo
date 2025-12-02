ARG VERSION

# ==============================
# Stage 1: Build Iceberg from source
# ==============================
FROM eclipse-temurin:21-jdk AS iceberg-builder

# Install dependencies
RUN apt-get update && \
    apt-get install -y git wget unzip && \
    rm -rf /var/lib/apt/lists/*

# Install Gradle
RUN wget https://services.gradle.org/distributions/gradle-8.14.3-bin.zip && \
    unzip gradle-8.14.3-bin.zip -d /opt && \
    rm gradle-8.14.3-bin.zip
ENV PATH="/opt/gradle-8.14.3/bin:${PATH}"

# Build Iceberg
WORKDIR /workspace
RUN git clone --depth 1 https://github.com/apache/iceberg.git && \
    cd iceberg && \
    ./gradlew -x test -x integrationTest clean build

# Extract Iceberg Kafka Connect runtime ZIP
RUN mkdir -p /workspace/kafka-connect-lib /workspace/final-lib

# Copy and extract the ZIP
WORKDIR /workspace/kafka-connect-lib
RUN cp /workspace/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*.zip . && \
    for zip in iceberg-kafka-connect-runtime-*.zip ; do unzip -o "$zip" ; done

# Debug: Show what was extracted
RUN echo "=== Extracted contents ===" && \
    find /workspace/kafka-connect-lib -name "*.jar" -type f && \
    echo "==========================="

# Copy ALL JARs using find (more robust than glob)
RUN find /workspace/kafka-connect-lib -name "*.jar" -type f -exec cp -v {} /workspace/final-lib/ \;

# Verify iceberg-api JAR exists
RUN echo "=== Verifying iceberg-api JAR ===" && \
    ls -la /workspace/final-lib/iceberg-api*.jar && \
    echo "=================================="

# Download PostgreSQL JDBC driver
RUN wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar \
    -O /workspace/final-lib/postgresql-42.7.8.jar

# Add Hadoop AWS dependency
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    -O /workspace/final-lib/hadoop-aws-3.4.2.jar

# Add AWS SDK S3 Transfer Manager (not included in bundle)
RUN wget https://repo1.maven.org/maven2/software/amazon/awssdk/s3-transfer-manager/2.29.52/s3-transfer-manager-2.29.52.jar \
    -O /workspace/final-lib/s3-transfer-manager-2.29.52.jar

# Add AWS SDK bundle (often needed with hadoop-aws)
RUN wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    -O /workspace/final-lib/bundle-2.29.52.jar

# ==============================
# Stage 2: Final Kafka Connect image
# ==============================
FROM docker.io/confluentinc/cp-kafka-connect:${VERSION}

# Create plugin directory
RUN mkdir -p /usr/share/java/iceberg-connect

# Copy built libraries from builder stage
COPY --from=iceberg-builder /workspace/final-lib /usr/share/java/iceberg-connect/

# Verify the iceberg-api JAR is present (this is the one with IcebergBuild)
RUN echo "=== Final JARs ===" && \
    ls -la /usr/share/java/iceberg-connect/ && \
    echo "=== Checking for iceberg-api ===" && \
    ls /usr/share/java/iceberg-connect/iceberg-api*.jar

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"
