"""
Expected schema of streaming JSON events.
Used in streaming_app.py to parse raw JSON, enable event-time processing,
and detect malformed or incomplete records.
"""
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Raw value can have device_id, event_time, temperature, country (and optional meta/temp, this_is, etc.)
EVENTS_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("country", StringType(), True),
])

# Event time column name for watermarks and windows
EVENT_TIME_COL = "event_time_ts"
