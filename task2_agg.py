from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, avg, to_timestamp, window, date_format
from pyspark.sql.types import StructType, StringType, DoubleType

# Spark session
spark = SparkSession.builder.appName("DriverAggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Read and parse stream
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost").option("port", 9999).load()

parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed = parsed.withColumn("event_time", to_timestamp("timestamp"))

# ✅ Watermark + group by time window + driver_id
aggregated = parsed \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("driver_id")
    ) \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# ✅ Flatten the window for CSV output
flattened = aggregated \
    .withColumn("window_start", date_format("window.start", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("window_end", date_format("window.end", "yyyy-MM-dd HH:mm:ss")) \
    .drop("window")

# ✅ Start both writers
console_query = aggregated.writeStream.outputMode("update") \
    .format("console").option("truncate", False).start()

csv_query = flattened.writeStream.outputMode("append") \
    .format("csv") \
    .option("path", "output/driver_aggregates") \
    .option("checkpointLocation", "checkpoints/driver_aggregates") \
    .option("header", True) \
    .start()

#  Wait for either stream to finish
spark.streams.awaitAnyTermination()
