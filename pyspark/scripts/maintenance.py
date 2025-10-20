import sys
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

"""
Iceberg Table Maintenance Job
Run this script daily/weekly via cron or scheduler to:
- Expire old snapshots
- Remove orphan files
- Compact small files (optional)
"""

try:
    # Spark Connect URL - can be configured via environment variable
    spark_connect_url = os.getenv("SPARK_CONNECT_URL")
    catalog_name = os.getenv("CATALOG_NAME")
    table_name_only = os.getenv("TABLE_NAME")

    if not spark_connect_url:
        print("[ERROR] SPARK_CONNECT_URL environment variable is required")
        sys.exit(1)
    if not catalog_name:
        print("[ERROR] CATALOG_NAME environment variable is required")
        sys.exit(1)
    if not table_name_only:
        print("[ERROR] TABLE_NAME environment variable is required")
        sys.exit(1)

    # Initialize Spark Connect session
    spark = (
        SparkSession.builder.appName(
            f"IcebergMaintenanceFor{table_name_only.replace('.','_')}"
        )
        .remote(spark_connect_url)
        .getOrCreate()
    )
    print(f"[INFO] Spark Connect session started: {spark_connect_url}")
    print("[INFO] Connected for maintenance")

    # Get catalog and table name from environment variables
    table_name = f"{catalog_name}.{table_name_only}"

    print(f"[INFO] Catalog: {catalog_name}")
    print(f"[INFO] Table: {table_name_only}")
    print(f"[INFO] Full table path: {table_name}")

    # Configure retention period (7 days)
    retention_days = 7
    older_than = (datetime.now() - timedelta(days=retention_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(f"[INFO] Starting maintenance for {table_name}")
    print(f"[INFO] Retention period: {retention_days} days")
    print(f"[INFO] Expiring data older than: {older_than}")

    # 1. Expire old snapshots
    print("\n[STEP 1] Expiring old snapshots...")
    try:
        result = spark.sql(
            f"""
            CALL {catalog_name}.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{older_than}',
                retain_last => 5
            )
        """
        )
        result.show(truncate=False)
        print(f"[SUCCESS] Expired snapshots older than {older_than}")
    except Exception as e:
        print(f"[WARNING] Snapshot expiration: {e}")

    # 2. Remove orphan files
    print("\n[STEP 2] Removing orphan files...")
    try:
        result = spark.sql(
            f"""
            CALL {catalog_name}.system.remove_orphan_files(
                table => '{table_name}',
                older_than => TIMESTAMP '{older_than}'
            )
        """
        )
        result.show(truncate=False)
        print(f"[SUCCESS] Removed orphan files older than {older_than}")
    except Exception as e:
        print(f"[WARNING] Orphan file removal: {e}")

    # 3. Optional: Rewrite data files (compaction)
    # Uncomment if you want to compact small files
    print("\n[STEP 3] Compacting small files...")
    try:
        spark.sql(
            f"""
            CALL {catalog_name}.system.rewrite_data_files(
                table => '{table_name}',
                options => map('target-file-size-bytes', '134217728')  -- 128MB
            )
        """
        ).show(truncate=False)
        print("[SUCCESS] Data files compacted")
    except Exception as e:
        print(f"[WARNING] Compaction: {e}")

    # 4. Optional: Rewrite manifests
    # Uncomment to optimize metadata
    print("\n[STEP 4] Rewriting manifests...")
    try:
        spark.sql(
            f"""
            CALL {catalog_name}.system.rewrite_manifests(
                table => '{table_name}'
            )
        """
        ).show(truncate=False)
        print("[SUCCESS] Manifests rewritten")
    except Exception as e:
        print(f"[WARNING] Manifest rewrite: {e}")

    # Show table statistics
    print("\n[INFO] Table Statistics:")
    spark.sql(f"DESCRIBE EXTENDED {table_name}").show(100, False)

    print("\n[SUCCESS] Maintenance completed successfully!")

except Exception as e:
    print(f"[ERROR] Maintenance failed: {e}")
    raise

finally:
    if "spark" in locals():
        spark.stop()
        print("[INFO] Spark session stopped")
