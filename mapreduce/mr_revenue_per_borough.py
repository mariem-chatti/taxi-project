"""
MAPREDUCE JOB 2 — REVENUE PER BOROUGH
========================================
Reads Parquet files from HDFS /taxi/joined/ and computes:
  - Total revenue per pickup borough
  - Average fare per trip per borough
  - Total tips per borough

MAP phase   → each record emits (borough, (total_amount, fare_amount, tip_amount))
SHUFFLE     → Hadoop groups all tuples by borough
REDUCE phase → sum all monetary fields per borough, compute averages

Run inside the namenode container:
  spark-submit /data/mapreduce/mr_revenue_per_borough.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from collections import defaultdict


def map_phase(row):
    """
    MAP: For each trip, emit the borough and its revenue components.
    Returns (borough, (total_amount, fare_amount, tip_amount, trip_count=1))
    """
    borough      = row["pu_borough"] if row["pu_borough"] else "Unknown"
    total_amount = float(row["total_amount"] or 0)
    fare_amount  = float(row["fare_amount"]  or 0)
    tip_amount   = float(row["tip_amount"]   or 0)
    return (borough, (total_amount, fare_amount, tip_amount, 1))


def reduce_phase(a, b):
    """
    REDUCE: Combine two tuples by summing each component.
    (total_a, fare_a, tip_a, count_a) + (total_b, fare_b, tip_b, count_b)
    """
    return (
        a[0] + b[0],   # sum total_amount
        a[1] + b[1],   # sum fare_amount
        a[2] + b[2],   # sum tip_amount
        a[3] + b[3],   # sum trip count
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MapReduce_RevenuePerBorough")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    INPUT_PATH  = "hdfs://namenode:9000/taxi/joined/"
    OUTPUT_PATH = "hdfs://namenode:9000/taxi/output/revenue_per_borough"

    print("=== MapReduce Job 2: Revenue Per Borough ===")

    # ── READ ───────────────────────────────────────────────────────────────────
    df = spark.read.parquet(INPUT_PATH)
    print(f"Total rows read: {df.count():,}")

    # ── MAP ────────────────────────────────────────────────────────────────────
    mapped_rdd = df.rdd.map(map_phase)

    # ── SHUFFLE + REDUCE ───────────────────────────────────────────────────────
    reduced_rdd = mapped_rdd.reduceByKey(reduce_phase)

    # ── POST-REDUCE: compute averages ──────────────────────────────────────────
    final_rdd = reduced_rdd.map(lambda kv: (
        kv[0],                        # borough
        round(kv[1][0], 2),           # total_revenue
        round(kv[1][1], 2),           # total_fares
        round(kv[1][2], 2),           # total_tips
        kv[1][3],                     # trip_count
        round(kv[1][0] / kv[1][3], 2) if kv[1][3] > 0 else 0.0,  # avg_revenue_per_trip
    ))

    # ── COLLECT & DISPLAY ──────────────────────────────────────────────────────
    results = final_rdd.collect()
    results_sorted = sorted(results, key=lambda x: x[1], reverse=True)

    print("\n💰 REVENUE PER PICKUP BOROUGH:")
    print(f"{'Borough':<20} {'Total Revenue':>15} {'Total Fares':>13} "
          f"{'Total Tips':>12} {'Trips':>8} {'Avg/Trip':>10}")
    print("-" * 82)
    for r in results_sorted:
        print(f"{r[0]:<20} ${r[1]:>14,.2f} ${r[2]:>12,.2f} "
              f"${r[3]:>11,.2f} {r[4]:>8,} ${r[5]:>9,.2f}")

    # ── WRITE TO HDFS ──────────────────────────────────────────────────────────
    result_df = spark.createDataFrame(
        results_sorted,
        ["borough", "total_revenue", "total_fares", "total_tips",
         "trip_count", "avg_revenue_per_trip"]
    )
    result_df.write.mode("overwrite").csv(OUTPUT_PATH, header=True)
    print(f"\n✅ Results saved to HDFS: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
