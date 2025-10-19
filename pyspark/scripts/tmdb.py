from pyspark.sql import SparkSession


try:
    spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()
    print("✅ Connected to Spark with Kafka package loaded.")

    df = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
        )
        .option("subscribe", "tmdb")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    query = (
        df_parsed.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if "spark" in locals():
        spark.stop()
        print("🛑 SparkSession stopped.")
