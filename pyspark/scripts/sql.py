import os
import sys
from pyspark.sql import SparkSession

try:
    # --- Read environment variables ---
    spark_connect_url = os.getenv("SPARK_CONNECT_URL")
    sql_query = os.getenv("SQL_QUERY")

    # --- Validate required variables ---
    if not spark_connect_url:
        print("[ERROR] SPARK_CONNECT_URL environment variable is required")
        sys.exit(1)
    if not sql_query:
        print("[ERROR] SQL_QUERY environment variable is required")
        sys.exit(1)

    # --- Initialize Spark session ---
    spark = (
        SparkSession.builder.appName(f"IcebergJob_SQLExecution")
        .remote(spark_connect_url)
        .getOrCreate()
    )
    print(f"[INFO] Spark Connect session started at: {spark_connect_url}")

    # --- Execute SQL query ---
    df = spark.sql(sql_query)

    # --- Display results ---
    print("[INFO] Query executed successfully. Preview of results:")
    df.show(truncate=True)

except Exception as e:
    print(f"[ERROR] PySpark execution failed: {e}")
    raise

finally:
    if "spark" in locals():
        spark.stop()
        print("[INFO] Spark session stopped")
