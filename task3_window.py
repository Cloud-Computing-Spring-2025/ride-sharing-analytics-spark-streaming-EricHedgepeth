from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, sum, to_timestamp,
    window, date_format
)
from pyspark.sql.types import StructType, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder.appName("WindowedFareAggregation").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Read stream from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse and convert timestamp
parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed = parsed.withColumn("event_time", to_timestamp("timestamp"))

# ✅ Add watermark + sliding window (5 min window, 1 min slide)
windowed_agg = parsed \
    .withWatermark("event_time", "15 seconds") \
    .groupBy(window(col("event_time"), "1 minutes", "30 seconds")) \
    .agg(sum("fare_amount").alias("total_fare"))

# ✅ Flatten the window struct for CSV output
from pyspark.sql.functions import date_format
flattened = windowed_agg \
    .withColumn("window_start", date_format("window.start", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("window_end", date_format("window.end", "yyyy-MM-dd HH:mm:ss")) \
    .drop("window")

# ✅ Write to console in 'update' mode (shows partial updates)
console_query = windowed_agg.writeStream.outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# ✅ Write flattened results to CSV in 'append' mode
csv_query = flattened.writeStream.outputMode("append") \
    .format("csv") \
    .option("path", "output/windowed_fares") \
    .option("checkpointLocation", "checkpoints/windowed_fares") \
    .option("header", True) \
    .start()

# Wait for either stream to stop
spark.streams.awaitAnyTermination()
