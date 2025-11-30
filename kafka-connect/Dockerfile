ARG VERSION

# ==============================
# Stage 1: Build Iceberg from source
# ==============================
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
RUN mkdir -p /workspace/kafka-connect-lib
RUN cp /workspace/iceberg/kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-*.zip /workspace/kafka-connect-lib/

WORKDIR /workspace/kafka-connect-lib
RUN for zip in iceberg-kafka-connect-runtime-*.zip ; do unzip -o "$zip" ; done

RUN mkdir -p /workspace/final-lib
RUN echo "Flattening all lib folders into /workspace/kafka-connect-lib ..." && \
    for d in /workspace/kafka-connect-lib/*/ ; do \
        if [ -d "$d/lib" ]; then \
            echo "Copying libs from $d/lib ..."; \
            cp -v "$d/lib"/*.jar /workspace/final-lib/ ; \
        fi; \
    done

# Download PostgreSQL JDBC driver
RUN wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar \
    -O /workspace/final-lib/postgresql-42.7.8.jar

# Add Hadoop AWS dependency
RUN wget -https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    -O /workspace/final-lib/hadoop-aws-3.4.2.jar 
    

# ==============================
# Stage 2: Final Kafka Connect image
# ==============================
FROM docker.io/confluentinc/cp-kafka-connect:${VERSION}

# Create plugin directory
RUN mkdir -p /usr/share/java/iceberg-connect

# Copy built libraries from builder stage
COPY --from=iceberg-builder /workspace/final-lib /usr/share/java/iceberg-connect/

# After copying your built artifacts into the image
RUN ls -l /usr/share/java/iceberg-connect/

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/java/iceberg-connect,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"
