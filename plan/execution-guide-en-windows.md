# NYC Taxi Big Data Pipeline - Execution Guide (Windows)

## Project Structure

```
taxi-project-exam/
├── docker-compose.yml          # Orchestration of 8 containers
├── README.md                   # Complete documentation
├── plan/                      # Analysis folder
│   └── taxi-project-analysis.md
├── data/
│   ├── taxi_sample.csv         # 50,000 trip records
│   └── taxi_zone_lookup.csv    # NYC zone reference
├── producer/                   # Kafka Producer in Python
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
└── spark/                      # Spark Jobs
    ├── taxi_spark_job.py       # Structured Streaming
    └── mapreduce/              # 3 MapReduce jobs
        ├── mr_trips_per_borough.py
        ├── mr_revenue_per_borough.py
        └── mr_anomaly_counts.py
```

## Pipeline Architecture

```
CSV → Kafka Producer → Kafka (taxi-raw) → Spark Structured Streaming
                                              ↓
                           ┌─────────────────┼─────────────────┐
                           ↓                 ↓                 ↓
                    /taxi/clean/      /taxi/joined/     /taxi/anomalies/
                           └─────────────────┼─────────────────┘
                                              ↓
                                    MapReduce Batch Jobs
                                              ↓
                                    /taxi/output/
```

## Step 1 — Start Docker Infrastructure

### 1.1 Stop services occupying port 9000

```powershell
docker stop deployment_alliance-minio-1
```

### 1.2 Start the 8 containers

```powershell
cd C:\path\to\taxi-project-exam
docker compose up -d zookeeper kafka kafka-ui namenode datanode spark-master spark-worker spark-worker-2
```

**Explanation:**
- Zookeeper coordinates Kafka
- Kafka broker receives messages
- Kafka-UI allows visualizing topics (port 8080)
- NameNode + DataNode = HDFS (distributed file system)
- Spark Master + 2 Workers = Spark cluster

### 1.3 Verify all containers are running

```powershell
docker ps
```

Wait for all containers to show "Up" (about 45 seconds).

---

## Step 2 — Prepare HDFS

### 2.1 Fix DNS issue if needed

If namenode fails to start due to "UnknownHostException", run:

```powershell
docker rm namenode 2>$null
docker run -d --name namenode `
  --network taxi-project-exam_taxi-net `
  -p 9870:9870 -p 9000:9000 `
  -v hadoop_namenode:/hadoop/dfs/name `
  -v ./data:/data `
  -e CLUSTER_NAME=taxi-cluster `
  -e CORE_CONF_fs_defaultFS=hdfs://namenode:9000 `
  --add-host "namenode:127.0.0.1" `
  --add-host "%COMPUTERNAME%:127.0.0.1" `
  bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
```

### 2.2 Create HDFS directories

```powershell
docker exec namenode hdfs dfs -mkdir -p /taxi/clean
docker exec namenode hdfs dfs -mkdir -p /taxi/anomalies
docker exec namenode hdfs dfs -mkdir -p /taxi/joined
docker exec namenode hdfs dfs -mkdir -p /taxi/output
docker exec namenode hdfs dfs -mkdir -p /taxi/checkpoints
```

### 2.3 Load the zone reference file

```powershell
docker exec namenode hdfs dfs -put /data/taxi_zone_lookup.csv /taxi/
```

### 2.4 Verify directories

```powershell
docker exec namenode hdfs dfs -ls /taxi/
```

**Expected result:**
```
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/anomalies
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/checkpoints
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/clean
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/joined
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/output
-rw-r--r--   3 root supergroup      12065 2026-06-05 00:49 /taxi/taxi_zone_lookup.csv
```

### 2.5 Set permissions

```powershell
docker exec namenode hdfs dfs -chmod -R 777 /taxi
```

---

## Step 3 — Create Kafka Topic

```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic taxi-raw --partitions 1 --replication-factor 1
```

---

## Step 4 — Start Spark Structured Streaming Job

### 4.1 Submit the job to Spark cluster

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --conf spark.executor.cores=1 `
  --conf spark.executor.memory=1g `
  --conf spark.sql.shuffle.partitions=4 `
  /opt/spark-apps/taxi_spark_job.py
```

**Wait for the message:** `✅ Spark session started`

### 4.2 Reduce worker resources (if running MapReduce in parallel)

If you need to run MapReduce jobs while streaming is running, reduce worker resources to avoid conflicts:

```powershell
# Edit docker-compose.yml: spark-worker and spark-worker-2
# --memory "1G" --cores "1" instead of --memory "2G" --cores "2"

docker compose up -d --force-recreate spark-worker spark-worker-2
```

### 4.3 Check the web interfaces

- Spark Master UI: http://localhost:8090
- HDFS NameNode UI: http://localhost:9870

---

## Step 5 — Start Kafka Producer

### 5.1 Build the producer Docker image

```powershell
docker compose build producer
```

### 5.2 Start the producer container

```powershell
docker compose up -d producer
```

### 5.3 Monitor the logs

```powershell
docker logs -f taxi-producer
```

**Expected messages:**
```
2026-06-05 00:xx:xx [PRODUCER] Waiting 30s for Kafka to initialise...
2026-06-05 00:xx:xx [PRODUCER] Connected to Kafka at kafka:29092
2026-06-05 00:xx:xx [PRODUCER] Streaming '/data/taxi_sample.csv' → Kafka topic 'taxi-raw'
2026-06-05 00:xx:xx [PRODUCER] Sent 1,000 messages | Skipped 0 bad rows
...
```

### 5.4 Check Kafka UI

- URL: http://localhost:8080
- Topic: taxi-raw → Messages

---

## Step 6 — Verify Data Arrives in HDFS

Wait 5 minutes, then verify:

```powershell
# Check joined folder (used by MapReduce)
docker exec namenode hdfs dfs -ls /taxi/joined/

# Check anomalies folder
docker exec namenode hdfs dfs -ls /taxi/anomalies/

# Check clean folder
docker exec namenode hdfs dfs -ls /taxi/clean/

# View space used
docker exec namenode hdfs dfs -du -h /taxi/
```

**Expected result:** Parquet files named `part-00000-*.parquet`

---

## Step 7 — Run MapReduce Jobs

### 7.1 Job 1: Count trips per borough

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/mapreduce/mr_trips_per_borough.py
```

**Read the result:**
```powershell
docker exec namenode hdfs dfs -cat /taxi/output/trips_per_borough/*.csv
```

### 7.2 Job 2: Revenue per borough

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/mapreduce/mr_revenue_per_borough.py
```

**Read the result:**
```powershell
docker exec namenode hdfs dfs -cat /taxi/output/revenue_per_borough/*.csv
```

### 7.3 Job 3: Count anomalies per borough

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/mapreduce/mr_anomaly_counts.py
```

**Read the result:**
```powershell
docker exec namenode hdfs dfs -cat /taxi/output/anomaly_counts/*.csv
```

---

## Useful Commands Summary

| Action | Command |
|--------|---------|
| View all containers | `docker ps` |
| View container logs | `docker logs -f <name>` |
| List HDFS | `docker exec namenode hdfs dfs -ls /taxi/` |
| View HDFS size | `docker exec namenode hdfs dfs -du -h /taxi/` |
| Read HDFS file | `docker exec namenode hdfs dfs -cat /taxi/output/...` |
| Stop everything | `docker compose down` |

## Available Web Services

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8090 |
| HDFS NameNode UI | http://localhost:9870 |

## Troubleshooting

### Problem: Port 9000 already in use
```powershell
docker stop deployment_alliance-minio-1
```

### Problem: Spark Workers monopolized by streaming

By default, workers have 2 cores and 2GB. To allow MapReduce jobs to run in parallel:

```powershell
# Edit in docker-compose.yml:
# spark-worker: --memory "1G" --cores "1"
# spark-worker-2: --memory "1G" --cores "1"

docker compose up -d --force-recreate spark-worker spark-worker-2
```

### Problem: Kafka topic doesn't exist
```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic taxi-raw --partitions 1 --replication-factor 1