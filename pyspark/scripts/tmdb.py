from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import *


try:
    spark = SparkSession.builder.appName("KafkaSpark").getOrCreate()
    print("✅ Connected to Spark with Kafka package loaded.")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS datalake.tmdb")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS datalake.tmdb.movies (
            adult BOOLEAN,
            backdrop_path STRING,
            belongs_to_collection STRING,
            budget BIGINT,
            genres ARRAY<STRUCT<id: INT, name: STRING>>,
            homepage STRING,
            id BIGINT,
            imdb_id STRING,
            origin_country ARRAY<STRING>,
            original_language STRING,
            original_title STRING,
            overview STRING,
            popularity DOUBLE,
            poster_path STRING,
            production_companies ARRAY<STRUCT<
                id: BIGINT,
                logo_path: STRING,
                name: STRING,
                origin_country: STRING
            >>,
            production_countries ARRAY<STRUCT<
                iso_3166_1: STRING,
                name: STRING
            >>,
            release_date DATE,
            revenue BIGINT,
            runtime INT,
            spoken_languages ARRAY<STRUCT<
                english_name: STRING,
                iso_639_1: STRING,
                name: STRING
            >>,
            status STRING,
            tagline STRING,
            title STRING,
            video BOOLEAN,
            vote_average DOUBLE,
            vote_count INT
        )
        USING iceberg
        PARTITIONED BY (days(release_date))
    """
    )

    # Configure expiration properties for the table (run once at setup)
    spark.sql(
        """
        ALTER TABLE datalake.tmdb.movies 
        SET TBLPROPERTIES (
            'history.expire.max-snapshot-age-ms' = '604800000',
            'history.expire.min-snapshots-to-keep' = '5',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '5'
        )
    """
    )
    print("Expiration properties configured...")

    movie_schema = StructType(
        [
            StructField("adult", BooleanType()),
            StructField("backdrop_path", StringType()),
            StructField("belongs_to_collection", StringType()),  # null in sample
            StructField("budget", LongType()),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", IntegerType()),
                            StructField("name", StringType()),
                        ]
                    )
                ),
            ),
            StructField("homepage", StringType()),
            StructField("id", LongType()),
            StructField("imdb_id", StringType()),
            StructField("origin_country", ArrayType(StringType())),
            StructField("original_language", StringType()),
            StructField("original_title", StringType()),
            StructField("overview", StringType()),
            StructField("popularity", DoubleType()),
            StructField("poster_path", StringType()),
            StructField(
                "production_companies",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType()),
                            StructField("logo_path", StringType()),
                            StructField("name", StringType()),
                            StructField("origin_country", StringType()),
                        ]
                    )
                ),
            ),
            StructField(
                "production_countries",
                ArrayType(
                    StructType(
                        [
                            StructField("iso_3166_1", StringType()),
                            StructField("name", StringType()),
                        ]
                    )
                ),
            ),
            StructField("release_date", StringType()),
            StructField("revenue", LongType()),
            StructField("runtime", IntegerType()),
            StructField(
                "spoken_languages",
                ArrayType(
                    StructType(
                        [
                            StructField("english_name", StringType()),
                            StructField("iso_639_1", StringType()),
                            StructField("name", StringType()),
                        ]
                    )
                ),
            ),
            StructField("status", StringType()),
            StructField("tagline", StringType()),
            StructField("title", StringType()),
            StructField("video", BooleanType()),
            StructField("vote_average", DoubleType()),
            StructField("vote_count", IntegerType()),
        ]
    )

    df = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
        )
        .option("subscribe", "tmdb")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        df.select(from_json(col("value").cast("string"), movie_schema).alias("data"))
        .select("data.*")
        .withColumn("release_date", to_date(col("release_date")))
    )

    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("fanout-enabled", "true")  # parallel writers for performance
        .toTable("datalake.tmdb.movies")  # catalog.database.table
    )

    query.awaitTermination()

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if "spark" in locals():
        spark.stop()
        print("🛑 SparkSession stopped.")
