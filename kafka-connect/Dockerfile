FROM eclipse-temurin:17-jdk AS iceberg-builder

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
RUN mkdir -p /workspace/iceberg-connect
RUN cp /workspace/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/*.zip /workspace/iceberg-connect/

WORKDIR /workspace/iceberg-connect
RUN for zip in *.zip ; do unzip -o "$zip" ; done

ARG VERSION
FROM docker.io/confluentinc/cp-kafka-connect:${VERSION}

# Create plugin directory
RUN mkdir -p /usr/share/java/iceberg-connect

# Copy built libraries from builder stage
COPY --from=iceberg-builder /workspace/iceberg-connect/lib /usr/share/java/iceberg-connect/

RUN ls -l /usr/share/java/iceberg-connect/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"
