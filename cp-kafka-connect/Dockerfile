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
    # ===================
    # Version definitions
    # ===================
    ICEBERG_VERSION=1.10.0 && \
    HADOOP_AWS_VERSION=3.4.2 && \
    POSTGRES_VERSION=42.7.8 && \
    # ===================
    DEST="/build/iceberg-connect" && \
    MAVEN_REPO="https://repo1.maven.org/maven2" && \
    # Delete all existing iceberg JARs
    rm -f ${DEST}/iceberg-*.jar && \
    
    # Download Iceberg JARs
    for module in \
        iceberg-aws-bundle \
        iceberg-api \
        iceberg-bundled-guava \
        iceberg-common \
        iceberg-core \
        iceberg-data \
        iceberg-hive-metastore \
        iceberg-kafka-connect \
        iceberg-kafka-connect-events \
        iceberg-kafka-connect-transforms \
        iceberg-parquet \
        iceberg-orc; \
    do \
        echo "Downloading ${module}-${ICEBERG_VERSION}.jar" && \
        wget -q "${MAVEN_REPO}/org/apache/iceberg/${module}/${ICEBERG_VERSION}/${module}-${ICEBERG_VERSION}.jar" \
            -O "${DEST}/${module}-${ICEBERG_VERSION}.jar"; \
    done && \
    wget -q "${MAVEN_REPO}/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" \
        -O "${DEST}/postgresql-${POSTGRES_VERSION}.jar" && \
    wget -q "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -O "${DEST}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"


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