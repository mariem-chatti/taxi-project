"""
MAPREDUCE JOB 3 — ANOMALY COUNTS PER BOROUGH
==============================================
Reads anomaly Parquet files from HDFS /taxi/anomalies/ and counts:
  - How many anomalous trips originated in each borough
  - Breakdown by anomaly TYPE so analysts know what's going wrong

MAP phase   → each record emits (borough, anomaly_type)
SHUFFLE     → group by (borough, anomaly_type)
REDUCE phase → count occurrences of each (borough, anomaly_type) pair

Run inside the namenode container:
  spark-submit /data/mapreduce/mr_anomaly_counts.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def classify_anomaly(row):
    """
    MAP: Determine which anomaly rule(s) a record triggered.
    Returns a list of (borough, anomaly_type) pairs — one row can hit
    multiple rules, so we flatMap rather than map.
    """
    borough = row["pu_borough"] if (row.get("pu_borough")) else "Unknown"
    anomalies = []

    fare = float(row["fare_amount"] or 0)
    dist = float(row["trip_distance"] or 0)
    pax  = int(row["passenger_count"] or 0)
    tip  = float(row["tip_amount"] or 0)
    tot  = float(row["total_amount"] or 0)
    dur  = float(row.get("trip_duration_min") or 0)

    if fare > 500:
        anomalies.append((borough, "fare_too_high"))
    if fare <= 0:
        anomalies.append((borough, "fare_zero_or_negative"))
    if dist > 100:
        anomalies.append((borough, "distance_too_high"))
    if dist <= 0:
        anomalies.append((borough, "distance_zero_or_negative"))
    if pax > 6:
        anomalies.append((borough, "too_many_passengers"))
    if pax <= 0:
        anomalies.append((borough, "no_passengers"))
    if tip < 0:
        anomalies.append((borough, "negative_tip"))
    if tot < 0:
        anomalies.append((borough, "negative_total"))
    if dur > 180:
        anomalies.append((borough, "duration_too_long"))
    if 0 < dur < 1:
        anomalies.append((borough, "duration_too_short"))

    # If somehow nothing matched (shouldn't happen), label generically
    if not anomalies:
        anomalies.append((borough, "unknown_anomaly"))

    return anomalies


def main():
    spark = (
        SparkSession.builder
        .appName("MapReduce_AnomalyCounts")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    INPUT_PATH  = "hdfs://namenode:9000/taxi/anomalies/"
    OUTPUT_PATH = "hdfs://namenode:9000/taxi/output/anomaly_counts"

    print("=== MapReduce Job 3: Anomaly Counts Per Borough ===")

    # ── READ ───────────────────────────────────────────────────────────────────
    df = spark.read.parquet(INPUT_PATH)
    total_anomalies = df.count()
    print(f"Total anomaly rows: {total_anomalies:,}")

    # ── MAP (flatMap because one row → multiple anomaly types) ─────────────────
    # flatMap "flattens" the list returned by classify_anomaly into individual
    # (borough, anomaly_type) pairs
    mapped_rdd = df.rdd.flatMap(classify_anomaly)

    # ── SHUFFLE + REDUCE ───────────────────────────────────────────────────────
    # Each (borough, anomaly_type) pair gets counted
    reduced_rdd = mapped_rdd.map(lambda pair: (pair, 1)) \
                            .reduceByKey(lambda a, b: a + b)

    # Reformat to flat tuples for display and saving
    final_rdd = reduced_rdd.map(lambda kv: (kv[0][0], kv[0][1], kv[1]))

    # ── COLLECT & DISPLAY ──────────────────────────────────────────────────────
    results = final_rdd.collect()
    results_sorted = sorted(results, key=lambda x: (x[0], -x[2]))

    print("\n🚨 ANOMALY COUNTS BY BOROUGH AND TYPE:")
    print(f"{'Borough':<20} {'Anomaly Type':<30} {'Count':>8}")
    print("-" * 62)

    current_borough = None
    borough_total = 0
    for borough, atype, count in results_sorted:
        if borough != current_borough:
            if current_borough:
                print(f"{'':>20} {'SUBTOTAL':<30} {borough_total:>8,}")
                print()
            current_borough = borough
            borough_total = 0
        print(f"{borough:<20} {atype:<30} {count:>8,}")
        borough_total += count
    # Print last borough subtotal
    if current_borough:
        print(f"{'':>20} {'SUBTOTAL':<30} {borough_total:>8,}")

    print(f"\n{'GRAND TOTAL':<52} {total_anomalies:>8,}")

    # ── WRITE TO HDFS ──────────────────────────────────────────────────────────
    result_df = spark.createDataFrame(results_sorted, ["borough", "anomaly_type", "count"])
    result_df.write.mode("overwrite").csv(OUTPUT_PATH, header=True)
    print(f"\n✅ Results saved to HDFS: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
