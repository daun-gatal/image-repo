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
# Zip location: /build/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/
RUN mkdir -p /build/iceberg-connect && \
    unzip -o /build/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*.zip -d /build/iceberg-connect && \
    mv /build/iceberg-connect/iceberg-kafka-connect-runtime-*/* /build/iceberg-connect/ 2>/dev/null || true && \
    rmdir /build/iceberg-connect/iceberg-kafka-connect-runtime-* 2>/dev/null || true

# =============================================================================
# Runtime stage - Kafka Connect with Iceberg plugin
# =============================================================================
FROM docker.io/confluentinc/cp-kafka-connect:${VERSION}

# Copy the built JARs from builder stage
COPY --from=builder /build/iceberg-connect/lib/ /usr/share/java/iceberg-connect/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"