from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit, lpad, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
)

spark = (
    SparkSession.builder.appName("CHIP-Task3-Harmonizer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


NIH_SCHEMA = StructType(
    [
        StructField("record_id", StringType()),
        StructField("source", StringType()),
        StructField("epi_year", IntegerType()),
        StructField("epi_week", IntegerType()),
        StructField("district", StringType()),
        StructField("district_canonical", StringType()),
        StructField("province", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("disease", StringType()),
        StructField("case_count", IntegerType()),
        StructField("idsr_compliance_pct", DoubleType()),
        StructField("ingestion_ts", StringType()),
    ]
)

WEATHER_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("district", StringType()),
        StructField("district_canonical", StringType()),
        StructField("province", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("epi_year", IntegerType()),
        StructField("epi_week", IntegerType()),
        StructField("temperature_c", DoubleType()),
        StructField("humidity_pct", DoubleType()),
        StructField("wind_speed_kmh", DoubleType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("timestamp", DoubleType()),
    ]
)

NDMA_SCHEMA = StructType(
    [
        StructField("record_id", StringType()),
        StructField("alert_id", StringType()),
        StructField("source", StringType()),
        StructField("event_type", StringType()),
        StructField("district", StringType()),
        StructField("district_canonical", StringType()),
        StructField("province", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("severity", StringType()),
        StructField("epi_year", IntegerType()),
        StructField("epi_week", IntegerType()),
        StructField("issued_at", StringType()),
        StructField("description", StringType()),
    ]
)

POLICY_SCHEMA = StructType(
    [
        StructField("record_id", StringType()),
        StructField("source", StringType()),
        StructField("title", StringType()),
        StructField("policy_type", StringType()),
        StructField("status", StringType()),
        StructField("year", IntegerType()),
        StructField("summary", StringType()),
        StructField("sectors", ArrayType(StringType())),
        StructField("report_month", IntegerType()),
        StructField("report_year", IntegerType()),
        StructField("report_month_label", StringType()),
        StructField("ingestion_ts", StringType()),
    ]
)

NEWS_SCHEMA = StructType(
    [
        StructField("source", StringType()),
        StructField("title", StringType()),
        StructField("link", StringType()),
        StructField("published", StringType()),
    ]
)


def add_epi_week_key(df, year_col="epi_year", week_col="epi_week"):
    return df.withColumn(
        "epi_week_key",
        concat(
            col(year_col).cast("string"),
            lit("-W"),
            lpad(col(week_col).cast("string"), 2, "0"),
        ),
    )


def read_topic(topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def build_pipeline(topic_name, schema, output_name):
    raw = read_topic(topic_name)
    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("d")
    ).select("d.*")
    harmonized = add_epi_week_key(parsed).withColumn(
        "processed_at", current_timestamp()
    )

    parquet_query = (
        harmonized.writeStream.format("parquet")
        .option("path", f"data/sample_output/{output_name}_harmonized")
        .option("checkpointLocation", f"data/checkpoints/{output_name}")
        .partitionBy("epi_week_key")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    console_query = (
        harmonized.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .queryName(f"{output_name}_console")
        .start()
    )

    print(
        f"✓ Streaming '{topic_name}' -> data/sample_output/{output_name}_harmonized (+ console)"
    )
    return parquet_query, console_query


def build_policy_pipeline(topic_name, schema, output_name):
    """
    Separate pipeline for month-level sources. Partitions by
    report_month_label instead of epi_week_key — policy data
    is periodic, not weekly, per assessment requirements.
    """
    raw = read_topic(topic_name)
    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("d")
    ).select("d.*")
    harmonized = parsed.withColumn("processed_at", current_timestamp())

    parquet_query = (
        harmonized.writeStream.format("parquet")
        .option("path", f"data/sample_output/{output_name}_harmonized")
        .option("checkpointLocation", f"data/checkpoints/{output_name}")
        .partitionBy("report_month_label")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    console_query = (
        harmonized.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .queryName(f"{output_name}_console")
        .start()
    )

    print(
        f"✓ Streaming '{topic_name}' -> data/sample_output/{output_name}_harmonized (+ console)"
    )
    return parquet_query, console_query


def build_simple_pipeline(topic_name, schema, output_name):
    raw = read_topic(topic_name)
    parsed = (
        raw.select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .withColumn("processed_at", current_timestamp())
    )
    pq = (
        parsed.writeStream.format("parquet")
        .option("path", f"data/sample_output/{output_name}_harmonized")
        .option("checkpointLocation", f"data/checkpoints/{output_name}")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"✓ Streaming '{topic_name}' -> data/sample_output/{output_name}_harmonized")
    return pq


nih_pq, nih_console = build_pipeline("nih-surveillance-topic", NIH_SCHEMA, "nih")
weather_pq, weather_console = build_pipeline("weather-topic", WEATHER_SCHEMA, "weather")
ndma_pq, ndma_console = build_pipeline("ndma-alerts-topic", NDMA_SCHEMA, "ndma")
policy_pq, policy_console = build_policy_pipeline(
    "policy-topic", POLICY_SCHEMA, "policy"
)
news_pq = build_simple_pipeline("news-topic", NEWS_SCHEMA, "news")

print(
    "\n✓ All 5 streams active (NIH, Weather, NDMA, Policy, News). Awaiting data... (Ctrl+C to stop)\n"
)
spark.streams.awaitAnyTermination()
