"""
MAPREDUCE JOB 1 — TRIPS PER BOROUGH
=====================================
Reads Parquet files from HDFS /taxi/joined/ and counts how many trips
originated (pickup) in each NYC borough.

How MapReduce works:
  MAP phase   → each record emits (borough, 1)
  SHUFFLE     → Hadoop groups all (borough, *) pairs together
  REDUCE phase → sum the 1s for each borough → (borough, total_count)

Run inside the namenode container:
  python /data/mapreduce/mr_trips_per_borough.py \
    hdfs://namenode:9000/taxi/joined/ \
    hdfs://namenode:9000/taxi/output/trips_per_borough/

Or with Hadoop Streaming (alternative):
  hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input  /taxi/joined/ \
    -output /taxi/output/trips_per_borough \
    -mapper  "python mr_trips_per_borough.py --mapper" \
    -reducer "python mr_trips_per_borough.py --reducer"
"""

import sys
import argparse
from collections import defaultdict

# ── Pure Python implementation (no Hadoop Streaming needed) ──────────────────
# We use PySpark in batch mode to read the Parquet files from HDFS,
# then apply the classic MapReduce pattern manually so you can see every step.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def map_phase(row):
    """
    MAP: Extract the pickup borough from each row.
    Emits a (key, value) pair → (borough_name, 1)
    If borough is null/unknown, emit ("Unknown", 1) so we don't lose the trip.
    """
    borough = row["pu_borough"] if row["pu_borough"] else "Unknown"
    return (borough, 1)


def reduce_phase(pairs):
    """
    REDUCE: Group by key (borough) and sum the values (counts).
    Returns a dict: { borough: total_trips }
    """
    totals = defaultdict(int)
    for borough, count in pairs:
        totals[borough] += count
    return totals


def main():
    spark = (
        SparkSession.builder
        .appName("MapReduce_TripsPerBorough")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    INPUT_PATH  = "hdfs://namenode:9000/taxi/joined/"
    OUTPUT_PATH = "hdfs://namenode:9000/taxi/output/trips_per_borough"

    print("=== MapReduce Job 1: Trips Per Borough ===")

    # ── READ ───────────────────────────────────────────────────────────────────
    df = spark.read.parquet(INPUT_PATH)
    print(f"Total rows read: {df.count():,}")

    # ── MAP ────────────────────────────────────────────────────────────────────
    # rdd.map() applies our map_phase function to every row in parallel
    mapped_rdd = df.rdd.map(map_phase)

    # ── SHUFFLE (automatic in Spark) ───────────────────────────────────────────
    # reduceByKey groups all pairs with the same key and applies the function
    # This is equivalent to Hadoop's shuffle+sort+reduce in one step
    reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

    # ── COLLECT & DISPLAY ──────────────────────────────────────────────────────
    results = reduced_rdd.collect()
    results_sorted = sorted(results, key=lambda x: x[1], reverse=True)

    print("\n📊 TRIPS PER PICKUP BOROUGH:")
    print(f"{'Borough':<20} {'Trip Count':>12}")
    print("-" * 34)
    for borough, count in results_sorted:
        print(f"{borough:<20} {count:>12,}")

    # ── WRITE RESULT TO HDFS ───────────────────────────────────────────────────
    result_df = spark.createDataFrame(results_sorted, ["borough", "trip_count"])
    result_df.write.mode("overwrite").csv(OUTPUT_PATH, header=True)
    print(f"\n✅ Results saved to HDFS: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
