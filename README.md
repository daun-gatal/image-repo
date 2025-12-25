# Custom Data Platform Images

This repository contains the Docker build configurations for a comprehensive Data Lakehouse platform. It provides custom, optimized images for **Apache Airflow**, **PySpark**, and **Kafka Connect**, pre-configured to work seamlessly with **Apache Iceberg**, **AWS S3** (MinIO/LocalStack), and **PostgreSQL**.

## 🏗️ Architecture & Components

The project is structured into three main image builds, each tailored for a specific role in the data engineering lifecycle:

### 1. Airflow (`/airflow`)
*   **Base Image**: `apache/airflow`
*   **Purpose**: Orchestration of data pipelines.
*   **Customization**:
    *   Pre-installed system dependencies (`build-essential`) to support compiling custom providers.
    *   Injects a `requirements.txt` for Python dependencies.
    *   Optimized multi-stage build to keep the final image size minimal.

### 2. PySpark (`/pyspark`)
*   **Base Image**: `apache/spark` (v4.0.1 Python 3)
*   **Purpose**: Distributed data processing and transformation.
*   **Key Integrations**:
    *   **Apache Iceberg**: Pre-loaded with `iceberg-spark-runtime`.
    *   **AWS S3 Support**: Includes `hadoop-aws`, `aws-java-sdk-bundle` (via `s3` and `s3-transfer-manager` jars).
    *   **Kafka**: `spark-sql-kafka` and `spark-streaming-kafka` for structured streaming.
    *   **PostgreSQL**: JDBC driver included for relational database connectivity.
*   **Extras**: Helper scripts are copied into `/opt/spark/scripts` for standardized job execution.

### 3. Kafka Connect (`/kafka-connect`)
*   **Base Image**: `confluentinc/cp-kafka-connect`
*   **Purpose**: Ingesting data into the Lakehouse (Iceberg Sink).
*   **Build Process**:
    *   **Source Build**: Uses a multi-stage process where the **Iceberg Kafka Connect** sink is built from source using Gradle (`eclipse-temurin:17-jdk` builder image).
    *   **Plugin Install**: The built artifacts are installed into the Confluent Connect plugin path.
*   **Drivers**: Pre-packaged with PostgreSQL JDBC drivers.

## 🚀 Key Features

*   **Lakehouse Ready**: All components are specifically version-matched to support Apache Iceberg tables.
*   **Cloud Native**: Configured with necessary Hadoop-AWS bundles to talk to S3-compatible storage out of the box.
*   **Streaming Support**: PySpark and Kafka Connect images are ready for real-time data ingestion and processing.
*   **CI/CD Pipeline**:
    *   Leverages GitLab CI/CD `child pipelines` to trigger builds only when relevant files change in each subdirectory.
    *   Uses **Kaniko** (in `ci-templates`) for secure, unprivileged container builds inside Kubernetes.

## 🛠️ Build & Usage

The repository uses specific `Dockerfile`s for each service. To build locally:

```bash
# Build Airflow
docker build -t my-airflow:latest ./airflow --build-arg VERSION=2.9.0

# Build PySpark
docker build -t my-pyspark:latest ./pyspark --build-arg VERSION=4.0.1

# Build Kafka Connect
docker build -t my-kafka-connect:latest ./kafka-connect --build-arg VERSION=7.6.0
```

## ⚙️ CI/CD Logic

The `.gitlab-ci.yml` entry point acts as a orchestrator:
1.  **Detection**: It detects changes in subdirectories.
2.  **Trigger**: Triggers the corresponding child pipeline (`airflow/.gitlab-ci.yml`, `pyspark/.gitlab-ci.yml`, etc.).
3.  **Build**: Each child pipeline uses the shared templates to build and push images to the registry.

## 📚 Tech Stack

*   **Containerization**: Docker
*   **Orchestration**: GitLab CI/CD, Kubernetes
*   **Data Processing**: Apache Spark (PySpark)
*   **Workflow Engine**: Apache Airflow
*   **Streaming**: Apache Kafka, Kafka Connect
*   **Storage Format**: Apache Iceberg
