# NYC Taxi Big Data Pipeline — Expert Analysis

## Project Overview
This is an **end-to-end batch/streaming hybrid data pipeline** for processing NYC Yellow Taxi trip data. It demonstrates core distributed systems concepts using a realistic taxi dataset (~50,000 records).

## Architecture Flow

```
CSV File → Kafka Producer → Kafka Topic → Spark Structured Streaming → HDFS → MapReduce → Output CSVs
                     ↑                                               ↓
               (JSON records)                              3 Parquet outputs:
                                                             - /taxi/clean/
                                                             - /taxi/joined/ (with borough names)
                                                             - /taxi/anomalies/
```

## Infrastructure (8 Docker Containers)

| Component | Purpose |
|-----------|---------|
| **Zookeeper** | Kafka coordinator for broker leadership |
| **Kafka Broker** | Message bus - holds `taxi-raw` topic |
| **Kafka UI** | Web UI to inspect topics (port 8080) |
| **NameNode** | HDFS master, manages namespace (port 9870) |
| **DataNode** | HDFS worker, stores actual blocks |
| **Spark Master** | Coordinates job distribution (port 8090) |
| **2 Spark Workers** | Execute tasks (2GB RAM, 2 cores each) |

## Key Big Data Patterns Demonstrated

### 1. **Ingestion Layer** ([`producer/producer.py`](producer/producer.py:1))
- Reads CSV line-by-line (not loading entire file into memory)
- Converts to JSON → Kafka with 100ms delays (controlled throughput)
- Retry logic with 30s initial wait for Kafka readiness

### 2. **Stream Processing** ([`spark/taxi_spark_job.py`](spark/taxi_spark_job.py:1))
- **Spark Structured Streaming** — continuous micro-batch processing every 30 seconds
- Schema explicitly defined to avoid inference overhead
- Data cleaning: null drops, datetime parsing, derived columns (trip_duration_min, tip_pct)

### 3. **Anomaly Detection** (10 rules in [`taxi_spark_job.py`](spark/taxi_spark_job.py:132))
- Domain-based flags: fare > $500, distance > 100 miles, duration > 180 min, negative amounts
- **flatMap pattern** — one record can trigger multiple anomalies
- Anomalies separated to dedicated HDFS path for analyst review

### 4. **Broadcast Join** ([`taxi_spark_job.py`](spark/taxi_spark_job.py:186))
- Zone lookup CSV (265 rows) broadcast to all executors
- Avoids shuffling the large streaming DataFrame across network

### 5. **MapReduce Batch Analytics** (3 jobs in [`spark/mapreduce/`](spark/mapreduce/))
- **mr_trips_per_borough.py** — count trips per borough (Map: borough→1, Reduce: sum)
- **mr_revenue_per_borough.py** — aggregate revenue metrics per borough
- **mr_anomaly_counts.py** — flatMap for multi-label anomaly counting

## Data Formats
- **Input**: Tab-separated CSV (`taxi_sample.csv`)
- **Transport**: JSON over Kafka
- **Storage**: Parquet (columnar, compressed, fast analytics)
- **Output**: CSV from MapReduce

## Strengths
1. Complete Lambda Architecture (streaming + batch layers)
2. Separation of clean data vs anomalies for data quality
3. Broadcast join optimization for small dimension tables
4. Checkpointing for exactly-once streaming semantics

## Potential Improvements
1. No schema registry — schema embedded in producer code
2. Single Kafka partition — bottleneck for parallel consumers
3. No dead-letter queue for malformed Kafka messages
4. Could use Spark's continuous mode for sub-second latency