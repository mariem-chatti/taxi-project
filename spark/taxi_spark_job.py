"""
STEP 3 — SPARK STRUCTURED STREAMING JOB
=========================================
This script runs inside the Spark container. It:

  1. Reads the 'taxi-raw' Kafka topic as a continuous stream
  2. Parses and cleans each JSON record
  3. Detects anomalies (impossible fares, distances, passengers)
  4. Joins every trip with the zone lookup CSV (PU and DO boroughs)
  5. Writes clean trips  → HDFS /taxi/clean/
     Writes anomalies    → HDFS /taxi/anomalies/
     Writes joined data  → HDFS /taxi/joined/   (for MapReduce)

Run this inside the spark-master container:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    /opt/spark-apps/taxi_spark_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType
)

# ── 1. CREATE SPARK SESSION ───────────────────────────────────────────────────
# The session is the entry point to all Spark functionality.
# We configure it to talk to our local HDFS namenode.
spark = (
    SparkSession.builder
    .appName("TaxiPipelineStreaming")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    # Reduce shuffle partitions for small cluster (default 200 is too many)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session started")

# ── 2. DEFINE THE JSON SCHEMA ─────────────────────────────────────────────────
# Telling Spark the exact schema avoids an expensive schema-inference scan.
# Every field matches what the Python producer serialises.
TAXI_SCHEMA = StructType([
    StructField("vendor_id",             IntegerType(), True),
    StructField("pickup_datetime",       StringType(),  True),
    StructField("dropoff_datetime",      StringType(),  True),
    StructField("passenger_count",       IntegerType(), True),
    StructField("trip_distance",         FloatType(),   True),
    StructField("rate_code_id",          IntegerType(), True),
    StructField("store_and_fwd_flag",    StringType(),  True),
    StructField("pu_location_id",        IntegerType(), True),
    StructField("do_location_id",        IntegerType(), True),
    StructField("payment_type",          IntegerType(), True),
    StructField("fare_amount",           FloatType(),   True),
    StructField("extra",                 FloatType(),   True),
    StructField("mta_tax",               FloatType(),   True),
    StructField("tip_amount",            FloatType(),   True),
    StructField("tolls_amount",          FloatType(),   True),
    StructField("improvement_surcharge", FloatType(),   True),
    StructField("total_amount",          FloatType(),   True),
    StructField("congestion_surcharge",  FloatType(),   True),
])

# ── 3. READ FROM KAFKA ────────────────────────────────────────────────────────
# Spark Structured Streaming treats Kafka as an infinite table.
# Each new Kafka message becomes a new row automatically.
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "taxi-raw")
    # "earliest" = start from the very first message in the topic,
    # useful so we don't miss records published before Spark started.
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# Kafka gives us a 'value' column as raw bytes — decode it to a string first.
json_stream = raw_stream.select(
    F.col("value").cast("string").alias("json_str"),
    F.col("timestamp").alias("kafka_timestamp"),   # when Kafka received it
)

# Parse the JSON string into typed columns using our schema
parsed = json_stream.select(
    F.from_json(F.col("json_str"), TAXI_SCHEMA).alias("data"),
    F.col("kafka_timestamp")
).select("data.*", "kafka_timestamp")

print("✅ Kafka stream connected and parsed")

# ── 4. DATA CLEANING ──────────────────────────────────────────────────────────
# Drop rows that are structurally unusable:
#   • Any required field is null
#   • Trip distance is zero or negative (the taxi didn't move)
#   • Fare is zero or negative (free rides aren't real taxi data)
#   • Passenger count is zero (nobody in the cab)

required_cols = [
    "vendor_id", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance", "pu_location_id",
    "do_location_id", "fare_amount", "total_amount"
]

cleaned = parsed.dropna(subset=required_cols)

# Cast datetime strings to proper timestamp type for time-based analytics
cleaned = cleaned \
    .withColumn("pickup_ts",  F.to_timestamp("pickup_datetime",  "M/d/yyyy H:mm")) \
    .withColumn("dropoff_ts", F.to_timestamp("dropoff_datetime", "M/d/yyyy H:mm")) \
    .withColumn("trip_duration_min",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60
    )

# Compute tip percentage for analytics
cleaned = cleaned.withColumn(
    "tip_pct",
    F.when(F.col("fare_amount") > 0,
           (F.col("tip_amount") / F.col("fare_amount")) * 100
    ).otherwise(0.0)
)

print("✅ Data cleaning applied")

# ── 5. ANOMALY DETECTION ──────────────────────────────────────────────────────
# Flag records that are statistically suspicious.
# These are NOT deleted — they're written to a separate HDFS path so analysts
# can review them later.
#
# Rules (domain knowledge from NYC TLC data):
#   fare > $500   → almost certainly a data entry error
#   distance > 100 miles → impossible within NYC metro
#   passengers > 6  → legal max for yellow cabs
#   duration > 180 min or < 1 min → unrealistic trip times
#   negative amounts → corrupt data

anomaly_flags = (
    (F.col("fare_amount")      > 500)   |
    (F.col("fare_amount")      <= 0)    |
    (F.col("trip_distance")    > 100)   |
    (F.col("trip_distance")    <= 0)    |
    (F.col("passenger_count")  > 6)     |
    (F.col("passenger_count")  <= 0)    |
    (F.col("tip_amount")       < 0)     |
    (F.col("total_amount")     < 0)     |
    (F.col("trip_duration_min") > 180)  |
    (F.col("trip_duration_min") < 1)
)

# Label every row — we'll split into two streams below
labelled = cleaned.withColumn(
    "is_anomaly", F.when(anomaly_flags, True).otherwise(False)
)

# Two logical sub-streams derived from the same labelled stream
normal_trips  = labelled.filter(F.col("is_anomaly") == False).drop("is_anomaly")
anomaly_trips = labelled.filter(F.col("is_anomaly") == True)

print("✅ Anomaly detection rules applied")

# ── 6. JOIN WITH ZONE LOOKUP ──────────────────────────────────────────────────
# The zone CSV is small (265 rows) → load it as a regular (static) DataFrame.
# Spark is smart enough to broadcast it to all workers so every partition
# can join locally without shuffling the large stream across the network.

zone_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://namenode:9000/taxi/taxi_zone_lookup.csv")
    .select(
        F.col("LocationID").alias("zone_location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
        F.col("service_zone"),
    )
)

# Broadcast hint tells Spark to send the small zone table to every executor
zone_broadcast = F.broadcast(zone_df)

# Join pickup location
joined = normal_trips.join(
    zone_broadcast.select(
        F.col("zone_location_id").alias("pu_zone_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone_name").alias("pu_zone"),
    ),
    normal_trips["pu_location_id"] == F.col("pu_zone_id"),
    how="left"
).drop("pu_zone_id")

# Join dropoff location
joined = joined.join(
    zone_broadcast.select(
        F.col("zone_location_id").alias("do_zone_id"),
        F.col("borough").alias("do_borough"),
        F.col("zone_name").alias("do_zone"),
    ),
    joined["do_location_id"] == F.col("do_zone_id"),
    how="left"
).drop("do_zone_id")

print("✅ Zone lookup join configured")

# ── 7. WRITE TO HDFS ──────────────────────────────────────────────────────────
# Structured Streaming writes in micro-batches (every `triggerInterval`).
# We use "append" output mode: once a batch is written it's never changed,
# matching how HDFS works (write-once, read-many).

HDFS_BASE = "hdfs://namenode:9000/taxi"

def write_stream(df, path: str, checkpoint: str, trigger_secs: int = 30):
    """Helper: write a streaming DataFrame to HDFS as Parquet."""
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{HDFS_BASE}/{path}")
        .option("checkpointLocation", f"{HDFS_BASE}/checkpoints/{checkpoint}")
        # Trigger every N seconds — collects many messages into one file batch
        .trigger(processingTime=f"{trigger_secs} seconds")
        .start()
    )

# Stream 1: Clean joined trips (for analytics and MapReduce input)
query_joined = write_stream(joined,       "joined",    "joined_ckpt")

# Stream 2: Raw clean trips (fast path, no zone join)
query_clean  = write_stream(normal_trips, "clean",     "clean_ckpt")

# Stream 3: Anomalies (for review)
query_anomaly = write_stream(anomaly_trips, "anomalies", "anomaly_ckpt")

print("✅ Streaming writes to HDFS started")
print(f"   → Clean trips  : {HDFS_BASE}/clean/")
print(f"   → Joined trips : {HDFS_BASE}/joined/")
print(f"   → Anomalies    : {HDFS_BASE}/anomalies/")

# Block until all streams stop (Ctrl+C or error)
spark.streams.awaitAnyTermination()
