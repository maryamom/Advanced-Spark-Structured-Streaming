#!/usr/bin/env python3
"""
Spark Structured Streaming: read from Kafka, handle dirty data (valid/invalid),
event time + watermark, windowed aggregations, write to console and optional sinks.
Does not crash on bad data.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema import EVENTS_SCHEMA, EVENT_TIME_COL

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("EVENTS_TOPIC", "events")
WATERMARK_MINUTES = int(os.environ.get("WATERMARK_MINUTES", "10"))
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "5"))


def main():
    spark = (
        SparkSession.builder.appName("AdvancedSparkStreaming")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint-events")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka (value as string)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    value_str = F.col("value").cast("string")

    # Parse JSON safely: from_json returns null for malformed rows
    parsed = raw.select(
        F.from_json(value_str, EVENTS_SCHEMA).alias("parsed"),
        value_str,
    )
    with_parsed = parsed.select("parsed.*", "value_str")

    # Event time from string (event_time column from JSON)
    with_ts = with_parsed.withColumn(
        EVENT_TIME_COL,
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
    ).withWatermark(EVENT_TIME_COL, f"{WATERMARK_MINUTES} minutes")

    # Valid: device_id and country non-null and non-empty, temperature numeric and not sentinel -999
    valid_filter = (
        F.col("device_id").isNotNull()
        & (F.trim(F.col("device_id")) != "")
        & F.col("country").isNotNull()
        & (F.trim(F.col("country")) != "")
        & F.col("temperature").isNotNull()
        & (F.col("temperature") != -999)
        & F.col(EVENT_TIME_COL).isNotNull()
    )
    valid = with_ts.filter(valid_filter)
    invalid = with_ts.filter(~valid_filter)

    # Windowed aggregations on valid stream (event-time windows)
    windowed = valid.groupBy(
        F.window(F.col(EVENT_TIME_COL), f"{WINDOW_MINUTES} minutes"),
        F.col("country"),
    ).agg(
        F.count("*").alias("event_count"),
        F.avg("temperature").alias("avg_temperature"),
    )

    # Write valid aggregates to console (update mode for windowed)
    q_window = (
        windowed.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )
    # Invalid to console (append)
    q_invalid = (
        invalid.select("value_str")
        .writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )
    q_window.awaitTermination()


if __name__ == "__main__":
    main()
