ARG VERSION

# =============================================================================
# Build stage - compile Iceberg Kafka Connect
# =============================================================================
FROM eclipse-temurin:17-jdk AS builder

RUN apt-get update && \
    apt-get install -y git unzip wget && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Download and setup Gradle
RUN wget -q https://services.gradle.org/distributions/gradle-8.14.3-bin.zip && \
    unzip -q gradle-8.14.3-bin.zip -d /opt && \
    rm gradle-8.14.3-bin.zip

ENV PATH="/opt/gradle-8.14.3/bin:$PATH"

# Clone and build Iceberg
RUN git clone --depth 1 https://github.com/apache/iceberg.git && \
    cd iceberg && \
    ./gradlew -x test -x integrationTest clean build

# Extract the connector runtime
RUN mkdir -p /build/iceberg-connect && \
    # Extract each zip file separately using shell loop
    for zip in /build/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/*.zip; do \
        echo "Extracting: $zip"; \
        unzip -o "$zip" -d /build/iceberg-connect/; \
    done && \
    echo "=========================================================" && \
    echo "EXTRACTED CONTENTS:" && \
    find /build/iceberg-connect -type f | head -30 && \
    echo "=========================================================" && \
    # Flatten all nested directories
    find /build/iceberg-connect -mindepth 2 -type f -exec mv {} /build/iceberg-connect/ \; 2>/dev/null || true && \
    # Also check for lib folders and move their contents
    find /build/iceberg-connect -type d -name "lib" -exec sh -c 'mv {}/* /build/iceberg-connect/' \; 2>/dev/null || true && \
    echo "========================================================="

RUN set -ex && \
    DEST="/build/iceberg-connect" && \
    MAVEN_REPO="https://repo1.maven.org/maven2" && \
    # =================== \
    # Extract versions dynamically from existing JARs \
    # =================== \
    # Get Hadoop version from hadoop-common-*.jar \
    HADOOP_VERSION=$(ls ${DEST}/hadoop-common-*.jar 2>/dev/null | sed 's/.*hadoop-common-\(.*\)\.jar/\1/' | head -1) && \
    echo "Detected Hadoop version: ${HADOOP_VERSION}" && \
    # Get AWS SDK version from aws-core-*.jar \
    AWS_SDK_VERSION=$(ls ${DEST}/aws-core-*.jar 2>/dev/null | sed 's/.*aws-core-\(.*\)\.jar/\1/' | head -1) && \
    echo "Detected AWS SDK version: ${AWS_SDK_VERSION}" && \
    # Get PostgreSQL version from existing jar or use default \
    POSTGRES_VERSION=42.7.8 && \
    echo "Detected PostgreSQL version: ${POSTGRES_VERSION}" && \
    # Get Iceberg version from iceberg-core-*.jar \
    ICEBERG_VERSION=1.10.0 && \
    echo "Detected Iceberg version: ${ICEBERG_VERSION}" && \
    # Download Hadoop AWS if not present \
    if [ ! -f "${DEST}/hadoop-aws-${HADOOP_VERSION}.jar" ]; then \
        echo "Downloading hadoop-aws-${HADOOP_VERSION}.jar" && \
        wget -q "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
            -O "${DEST}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    fi && \
    # Download S3 Transfer Manager if not present \
    if [ ! -f "${DEST}/s3-transfer-manager-${AWS_SDK_VERSION}.jar" ]; then \
        echo "Downloading s3-transfer-manager-${AWS_SDK_VERSION}.jar" && \
        wget -q "${MAVEN_REPO}/software/amazon/awssdk/s3-transfer-manager/${AWS_SDK_VERSION}/s3-transfer-manager-${AWS_SDK_VERSION}.jar" \
            -O "${DEST}/s3-transfer-manager-${AWS_SDK_VERSION}.jar"; \
    fi && \
    # Download PostgreSQL JDBC if not present \
    if [ ! -f "${DEST}/postgresql-${POSTGRES_VERSION}.jar" ]; then \
        echo "Downloading postgresql-${POSTGRES_VERSION}.jar" && \
        wget -q "${MAVEN_REPO}/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" \
            -O "${DEST}/postgresql-${POSTGRES_VERSION}.jar"; \
    fi

# =============================================================================
# Runtime stage - Kafka Connect with Iceberg plugin
# =============================================================================
FROM docker.io/confluentinc/cp-kafka-connect:${VERSION}

# Copy ALL JARs directly (not from lib/ subdirectory)
COPY --from=builder /build/iceberg-connect/*.jar /usr/share/java/iceberg-connect/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"

RUN echo "=========================================================" && \
    echo "FINAL PLUGIN DIRECTORY:" && \
    ls -la /usr/share/java/iceberg-connect/ && \
    echo "Total JARs: $(ls /usr/share/java/iceberg-connect/*.jar 2>/dev/null | wc -l)" && \
    echo "========================================================="