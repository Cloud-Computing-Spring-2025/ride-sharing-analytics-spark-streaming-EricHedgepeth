from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Start Spark session
spark = SparkSession.builder.appName("IngestRideStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schema to match your generated JSON
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Read stream from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost").option("port", 9999).load()

# Parse JSON
parsed = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Print parsed records to console
query = parsed.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
