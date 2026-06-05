# NYC Taxi Big Data Pipeline
# Full Manual Guide — Windows PowerShell
# =========================================================
# Run every command yourself so you understand and can explain
# each step to your professor.
# =========================================================


## PROJECT STRUCTURE

    taxi-pipeline\
    |-- docker-compose.yml
    |-- data\
    |   |-- taxi_sample.csv
    |   +-- taxi_zone_lookup.csv
    |-- producer\
    |   |-- Dockerfile
    |   |-- requirements.txt
    |   +-- producer.py
    +-- spark\
        |-- taxi_spark_job.py
        +-- mapreduce\
            |-- mr_trips_per_borough.py
            |-- mr_revenue_per_borough.py
            +-- mr_anomaly_counts.py


## ARCHITECTURE (what you will explain to your professor)

    taxi_sample.csv  (50,000 rows)
          |
          | Python reads line by line
          v
    [ Python Producer ]
          |
          | sends JSON messages over the network
          v
    [ Kafka - topic: taxi-raw ]
          |
          | Spark subscribes to this topic
          v
    [ Apache Spark Structured Streaming ]
          | - parse JSON
          | - clean bad rows
          | - detect anomalies
          | - join with zone lookup (add borough names)
          |
          +-------> HDFS /taxi/clean/       (good records, no borough)
          +-------> HDFS /taxi/joined/      (good records + borough names)
          +-------> HDFS /taxi/anomalies/   (flagged bad records)
                            |
                            | MapReduce reads /taxi/joined/ and /taxi/anomalies/
                            v
                    [ MapReduce Job 1 ] -> Trips per Borough
                    [ MapReduce Job 2 ] -> Revenue per Borough
                    [ MapReduce Job 3 ] -> Anomaly Counts per Borough
                            |
                            v
                    HDFS /taxi/output/   (final results as CSV)


## PREREQUISITES

1. Docker Desktop for Windows must be installed and RUNNING
   - Check: the whale icon must be visible in the system tray
   - Download: https://www.docker.com/products/docker-desktop/
   - In Docker Desktop -> Settings -> Resources -> Memory: set to 8 GB minimum

2. Your two CSV files must be inside the data\ folder:
      taxi-pipeline\data\taxi_sample.csv
      taxi-pipeline\data\taxi_zone_lookup.csv

3. Open ONE PowerShell window and navigate to your project folder:
      cd C:\path\to\taxi-pipeline


## =========================================================
## PHASE 1 — START THE INFRASTRUCTURE
## =========================================================

WHAT THIS DOES:
Starts 7 Docker containers. Each container is an isolated service.
Docker Compose reads docker-compose.yml and starts them all together.
The services are: Zookeeper, Kafka, Kafka-UI, NameNode, DataNode,
Spark Master, Spark Worker.
We do NOT start the producer yet — it needs Kafka to be ready first.
If you want to avoid Spark failing before the Kafka topic exists, create the topic first or start the producer before submitting Spark.

COMMAND:
    docker compose up -d zookeeper kafka kafka-ui namenode datanode spark-master spark-worker spark-worker-2

EXPLAIN TO PROFESSOR:
- We start two Spark workers so the streaming pipeline and the later MapReduce batch jobs can both acquire resources.

NOTE: The Spark services now use the official `apache/spark:3.4.1` image. If you see a Compose warning about `version` being obsolete, it is safe to remove that top-level `version:` line from `docker-compose.yml`.

EXPLAIN TO PROFESSOR:
- "-d" means detached (runs in background, gives you the terminal back)
- Zookeeper starts first because Kafka depends on it
- NameNode + DataNode together form HDFS (the distributed filesystem)
- Spark Master coordinates jobs; Spark Worker executes them

WAIT 45 SECONDS then verify everything is running:
    docker ps

You should see 7 containers with status "Up".


## =========================================================
## PHASE 2 — PREPARE HDFS
## =========================================================

WHAT THIS DOES:
HDFS starts as an empty filesystem. We must create the directory
structure before Spark tries to write to it. Then we upload the
zone lookup CSV so Spark can read it from inside the cluster.

-- Create the directory tree inside HDFS --

    docker exec namenode hdfs dfs -mkdir -p /taxi/clean
    docker exec namenode hdfs dfs -mkdir -p /taxi/anomalies
    docker exec namenode hdfs dfs -mkdir -p /taxi/joined
    docker exec namenode hdfs dfs -mkdir -p /taxi/output
    docker exec namenode hdfs dfs -mkdir -p /taxi/checkpoints

EXPLAIN TO PROFESSOR:
- "docker exec namenode" means: run the next command INSIDE the namenode container
- "hdfs dfs" is the HDFS command-line client
- "-mkdir -p" creates the full path including any missing parent directories
- These paths are on the HDFS filesystem, not on your Windows hard drive

-- Upload the zone lookup file to HDFS --

    docker exec namenode hdfs dfs -put /data/taxi_zone_lookup.csv /taxi/

EXPLAIN TO PROFESSOR:
- The data\ folder on your machine is mounted into the namenode container at /data/
- "-put" copies a local file (inside the container) into HDFS
- Spark will read this file from HDFS at /taxi/taxi_zone_lookup.csv during the join step

-- Verify the directories were created --

    docker exec namenode hdfs dfs -ls /taxi/

You should see: clean, anomalies, joined, output, checkpoints, taxi_zone_lookup.csv

If Spark later fails with HDFS write permission errors, fix it by making `/taxi` writable and ensuring the Spark user owns the directory:

    docker exec namenode hdfs dfs -chmod -R 777 /taxi
    docker exec namenode hdfs dfs -chown -R spark:supergroup /taxi

If you still see permission denied for `/taxi/joined`, rerun the exact commands above and restart the streaming job.

-- Open HDFS Web UI in your browser --
    URL: http://localhost:9870
    Click "Utilities" -> "Browse the file system" -> navigate to /taxi/


## =========================================================
## PHASE 3 — SUBMIT THE SPARK STREAMING JOB
## =========================================================

WHAT THIS DOES:
Submits our Python Spark script to the Spark cluster.
Spark will connect to Kafka, read every message from the "taxi-raw"
topic, process it (clean + anomaly detection + zone join), and write
results to HDFS in micro-batches every 30 seconds.

OPEN A NEW POWERSHELL WINDOW for this command so you can see the
Spark logs while the rest of the pipeline runs.

NOTE: If the Kafka topic `taxi-raw` does not exist yet, create it before
starting Spark, or start the producer first so the topic is created.

    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic taxi-raw --partitions 1 --replication-factor 1

COMMAND (run in the new PowerShell window):
    docker exec spark-master /opt/spark/bin/spark-submit `
      --master spark://spark-master:7077 `
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 `
      --conf spark.jars.ivy=/tmp/.ivy2 `
      --conf spark.executor.cores=1 `
      --conf spark.executor.memory=1g `
      --conf spark.sql.shuffle.partitions=4 `
      /opt/spark-apps/taxi_spark_job.py

NOTE:
- If Spark still fails while resolving packages, create the Ivy cache directory first:
    docker exec spark-master mkdir -p /tmp/.ivy2/cache

EXPLAIN TO PROFESSOR:
- "spark-submit" is the standard way to launch any Spark application
- When using `docker exec`, you can now run `spark-submit` directly because the Compose config adds `/opt/spark/bin` to the container `PATH`
- If that does not work for any reason, use the full path `/opt/spark/bin/spark-submit`
- "--master spark://spark-master:7077" tells Spark where the cluster master is
- "--packages ..." downloads the Kafka connector JAR automatically
- "--conf spark.sql.shuffle.partitions=4" reduces partitions for our small cluster
  (default is 200 which would create too many small files)
- The script path /opt/spark-apps/ is where docker-compose mounts our spark\ folder

WAIT for the line: "Spark session started" to appear in the logs
This means Spark is running and waiting for Kafka messages.

-- Open Spark Web UI in your browser --
    URL: http://localhost:8090
    You will see the streaming query listed under "Streaming" once data flows


## =========================================================
## PHASE 4 — BUILD AND START THE KAFKA PRODUCER
## =========================================================

WHAT THIS DOES:
First builds a Docker image from producer\Dockerfile (installs Python
and kafka-python library). Then starts the container which runs
producer.py — it opens the CSV, reads it row by row, converts each
row to JSON, and sends it to the Kafka topic "taxi-raw".

Go back to your FIRST PowerShell window.

-- Step 4a: Build the producer image --

    docker compose build producer

EXPLAIN TO PROFESSOR:
- This reads producer\Dockerfile and creates a Docker image
- It installs the "kafka-python" library inside the image
- You only need to build once; after that it is cached

-- Step 4b: Start the producer container --

    docker compose up -d producer

EXPLAIN TO PROFESSOR:
- The container starts and immediately begins running producer.py
- producer.py waits 30 seconds internally (lets Kafka be fully ready)
- Then it opens taxi_sample.csv and sends one record every 100ms
- 50,000 records at 100ms each = about 83 minutes total
- You can increase speed by changing DELAY_MS in docker-compose.yml to "10"

-- Step 4c: Watch the producer logs in real time --

    docker logs -f taxi-producer

EXPLAIN TO PROFESSOR:
- "-f" means "follow" — it streams new log lines as they appear
- You will see "Sent 1,000 messages", "Sent 2,000 messages" etc.
- Press Ctrl+C to stop following logs (does NOT stop the container)

-- Open Kafka UI in your browser --
    URL: http://localhost:8080
    Click "taxi-raw" topic -> "Messages" to see records flowing in


## =========================================================
## PHASE 5 — VERIFY DATA IS LANDING IN HDFS
## =========================================================

WHAT THIS DOES:
Confirms that Spark has consumed messages from Kafka, processed them,
and written Parquet files to HDFS. Wait about 5 minutes after the
producer starts before running these checks.

-- Check the joined folder (main output used by MapReduce) --

    docker exec namenode hdfs dfs -ls /taxi/joined/

-- Check the anomalies folder --

    docker exec namenode hdfs dfs -ls /taxi/anomalies/

-- Check the clean folder --

    docker exec namenode hdfs dfs -ls /taxi/clean/

EXPLAIN TO PROFESSOR:
- Spark writes in micro-batches every 30 seconds
- Each batch creates a new Parquet file (you will see files named part-00000-*.parquet)
- Parquet is a columnar binary format — much faster than CSV for analytics
- The "joined" folder contains trips enriched with borough names from the zone lookup

-- See total size of data written --

    docker exec namenode hdfs dfs -du -h /taxi/


## =========================================================
## PHASE 6 — RUN MAPREDUCE JOB 1: TRIPS PER BOROUGH
## =========================================================

WHAT THIS DOES:
Reads all Parquet files from /taxi/joined/, applies the MapReduce
pattern (Map -> Shuffle -> Reduce) and counts how many trips
started in each NYC borough. Saves the result to /taxi/output/trips_per_borough/.

COMMAND:
    docker exec spark-master spark-submit `
      --master spark://spark-master:7077 `
      /opt/spark-apps/mapreduce/mr_trips_per_borough.py

EXPLAIN TO PROFESSOR (the MapReduce steps inside the script):
  MAP phase:
    Each row is transformed into a (key, value) pair.
    key   = pickup borough name  (e.g. "Manhattan")
    value = 1
    Example: row for a Manhattan trip -> ("Manhattan", 1)

  SHUFFLE phase (automatic):
    Spark groups all pairs with the same key together.
    ("Manhattan", [1, 1, 1, 1, ...])
    ("Brooklyn",  [1, 1, 1, ...])

  REDUCE phase:
    For each key, sum all the 1s.
    ("Manhattan", 32000)
    ("Brooklyn",  8000)

-- Read the result --

    docker exec namenode hdfs dfs -cat /taxi/output/trips_per_borough/*.csv


## =========================================================
## PHASE 7 — RUN MAPREDUCE JOB 2: REVENUE PER BOROUGH
## =========================================================

WHAT THIS DOES:
Same MapReduce pattern but the value is a tuple of monetary fields.
Computes total revenue, total fares, total tips, and average revenue
per trip for each borough.

COMMAND:
    docker exec spark-master spark-submit `
      --master spark://spark-master:7077 `
      /opt/spark-apps/mapreduce/mr_revenue_per_borough.py

EXPLAIN TO PROFESSOR:
  MAP phase:
    key   = pickup borough
    value = (total_amount, fare_amount, tip_amount, 1)

  SHUFFLE phase (automatic):
    Groups all tuples by borough.

  REDUCE phase:
    Sums each position in the tuple independently:
    (sum_total, sum_fare, sum_tip, count)
    Then divides sum_total / count to get average per trip.

-- Read the result --

    docker exec namenode hdfs dfs -cat /taxi/output/revenue_per_borough/*.csv


## =========================================================
## PHASE 8 — RUN MAPREDUCE JOB 3: ANOMALY COUNTS PER BOROUGH
## =========================================================

WHAT THIS DOES:
Reads /taxi/anomalies/ and counts how many anomalous records came
from each borough, broken down by anomaly type. Uses flatMap because
one record can trigger multiple anomaly rules simultaneously.

COMMAND:
    docker exec spark-master spark-submit `
      --master spark://spark-master:7077 `
      /opt/spark-apps/mapreduce/mr_anomaly_counts.py

EXPLAIN TO PROFESSOR:
  The anomaly rules Spark applied earlier:
    fare > $500            -> fare_too_high
    fare <= 0              -> fare_zero_or_negative
    distance > 100 miles   -> distance_too_high
    distance <= 0          -> distance_zero_or_negative
    passengers > 6         -> too_many_passengers
    passengers <= 0        -> no_passengers
    tip < 0                -> negative_tip
    total < 0              -> negative_total
    duration > 180 min     -> duration_too_long
    duration < 1 min       -> duration_too_short

  Why flatMap instead of map?
    One record can break MORE THAN ONE rule (e.g. fare=0 AND distance=0).
    map()    -> one record produces exactly one output pair
    flatMap  -> one record can produce multiple output pairs
    This gives a detailed breakdown rather than just a binary flag.

-- Read the result --

    docker exec namenode hdfs dfs -cat /taxi/output/anomaly_counts/*.csv


## =========================================================
## USEFUL COMMANDS DURING YOUR PRESENTATION
## =========================================================

-- See all running containers --
    docker ps

-- See logs of any container --
    docker logs kafka
    docker logs spark-master
    docker logs taxi-producer

-- See how much data is in HDFS --
    docker exec namenode hdfs dfs -du -h /taxi/

-- Count rows in a specific HDFS folder --
    docker exec spark-master spark-submit --master spark://spark-master:7077 --conf spark.sql.shuffle.partitions=4 -e "spark.read.parquet('hdfs://namenode:9000/taxi/joined/').count()"

-- List all topics in Kafka --
    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list

-- Describe the taxi-raw Kafka topic --
    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic taxi-raw

-- See latest messages in Kafka topic (shows last 5) --
    docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic taxi-raw --from-beginning --max-messages 5


## =========================================================
## SHUT DOWN EVERYTHING AFTER THE EXAM
## =========================================================

-- Stop all containers but KEEP the HDFS data (volumes survive) --
    docker compose down

-- Stop everything AND delete all data (full reset) --
    docker compose down -v


## =========================================================
## WEB DASHBOARDS — OPEN THESE DURING YOUR PRESENTATION
## =========================================================

    Kafka UI  -> http://localhost:8080   (topics, messages, consumer lag)
    Spark UI  -> http://localhost:8090   (streaming queries, batch timing)
    HDFS UI   -> http://localhost:9870   (filesystem, file sizes, block info)

