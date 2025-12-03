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
    echo "=========================================================" && \
    echo "ZIP FILES FOUND:" && \
    ls -la /build/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/ && \
    echo "=========================================================" && \
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
    echo "=========================================================" && \
    echo "FINAL STRUCTURE:" && \
    ls -la /build/iceberg-connect/ && \
    echo "=========================================================" && \
    echo "JAR COUNT: $(find /build/iceberg-connect -maxdepth 1 -name '*.jar' | wc -l)" && \
    echo "========================================================="

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