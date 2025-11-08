# ===============================
# Stage 1: Builder
# ===============================
# Accept version as a build argument
ARG VERSION
FROM docker.io/apache/airflow:${VERSION} AS builder

USER root

# Install build dependencies (optional, if your packages require compilation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install extra Python packages into a temp directory
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ===============================
# Stage 2: Final Image
# ===============================
# Accept version as a build argument
ARG VERSION
FROM docker.io/apache/airflow:${VERSION}

USER root

# Copy only installed packages from builder stage (reduces image size)
COPY --from=builder /home/airflow/.local /home/airflow/.local

# Fix PATH so that airflow finds the pip-installed packages
ENV PATH=/home/airflow/.local/bin:$PATH

USER airflow

# Optional: verify installation
RUN airflow providers list

# Add comments about the image