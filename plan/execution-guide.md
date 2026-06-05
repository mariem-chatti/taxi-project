# Guide d'Exécution du Pipeline NYC Taxi Big Data

## Structure du Projet

```
taxi-project-exam/
├── docker-compose.yml          # Orchestration des 8 conteneurs
├── README.md                   # Documentation complète
├── plan/                      # Dossier pour les analyses
│   └── taxi-project-analysis.md
├── data/
│   ├── taxi_sample.csv         # 50,000 enregistrements de courses
│   └── taxi_zone_lookup.csv    # Référence des zones NYC
├── producer/                   # Producteur Kafka en Python
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
└── spark/                      # Jobs Spark
    ├── taxi_spark_job.py       # Streaming structuré
    └── mapreduce/              # 3 jobs MapReduce
        ├── mr_trips_per_borough.py
        ├── mr_revenue_per_borough.py
        └── mr_anomaly_counts.py
```

## Architecture du Pipeline

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

## Étape 1 — Démarrer l'Infrastructure Docker

### 1.1 Arrêter les services qui occupent le port 9000

```bash
docker stop deployment_alliance-minio-1
```

### 1.2 Démarrer les 8 conteneurs

```bash
cd /home/jenebsalem/Documents/Mariem_projects/taxi-project-exam
docker compose up -d zookeeper kafka kafka-ui namenode datanode spark-master spark-worker spark-worker-2
```

**Explication :**
- Zookeeper coordonne Kafka
- Kafka broker reçoit les messages
- Kafka-UI permet de visualiser les topics (port 8080)
- NameNode + DataNode = HDFS (système de fichiers distribué)
- Spark Master + 2 Workers = cluster Spark

### 1.3 Vérifier que tous les conteneurs sont 运行

```bash
docker ps
```

Attendre que tous les conteneurs affichent "Up" (45 secondes).

---

## Étape 2 — Préparer HDFS

### 2.1 Corriger le problème DNS si nécessaire

Si le namenode ne démarre pas à cause de "UnknownHostException", exécuter :

```bash
docker rm namenode 2>/dev/null
docker run -d --name namenode \
  --network taxi-project-exam_taxi-net \
  -p 9870:9870 -p 9000:9000 \
  -v hadoop_namenode:/hadoop/dfs/name \
  -v ./data:/data \
  -e CLUSTER_NAME=taxi-cluster \
  -e CORE_CONF_fs_defaultFS=hdfs://namenode:9000 \
  --add-host "namenode:127.0.0.1" \
  --add-host "$(hostname):127.0.0.1" \
  bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
```

### 2.2 Créer les répertoires HDFS

```bash
docker exec namenode hdfs dfs -mkdir -p /taxi/clean
docker exec namenode hdfs dfs -mkdir -p /taxi/anomalies
docker exec namenode hdfs dfs -mkdir -p /taxi/joined
docker exec namenode hdfs dfs -mkdir -p /taxi/output
docker exec namenode hdfs dfs -mkdir -p /taxi/checkpoints
```

### 2.3 Charger le fichier de référence des zones

```bash
docker exec namenode hdfs dfs -put /data/taxi_zone_lookup.csv /taxi/
```

### 2.4 Vérifier les répertoires

```bash
docker exec namenode hdfs dfs -ls /taxi/
```

**Résultat attendu :**
```
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/anomalies
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/checkpoints
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/clean
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/joined
drwxr-xr-x   - root supergroup          0 2026-06-05 00:48 /taxi/output
-rw-r--r--   3 root supergroup      12065 2026-06-05 00:49 /taxi/taxi_zone_lookup.csv
```

### 2.5 Donner les permissions

```bash
docker exec namenode hdfs dfs -chmod -R 777 /taxi
```

---

## Étape 3 — Créer le Topic Kafka

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic taxi-raw --partitions 1 --replication-factor 1
```

---

## Étape 4 — Démarrer le Job Spark Structured Streaming

### 4.1 Soumettre le job au cluster Spark

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/spark-apps/taxi_spark_job.py
```

**Attendre le message :** `✅ Spark session started`

### 4.2 Réduire les ressources des workers (si vous exécutez MapReduce en parallèle)

Si vous devez exécuter des jobs MapReduce en même temps que le streaming, réduisez les ressources des workers pour éviter les conflits :

```bash
# Modifier docker-compose.yml: spark-worker et spark-worker-2
# --memory "1G" --cores "1" au lieu de --memory "2G" --cores "2"

docker compose up -d --force-recreate spark-worker spark-worker-2
```

### 4.3 Vérifier sur les interfaces web

- Spark Master UI : http://localhost:8090
- HDFS NameNode UI : http://localhost:9870

---

## Étape 5 — Démarrer le Producteur Kafka

### 5.1 Construire l'image Docker du producteur

```bash
docker compose build producer
```

### 5.2 Démarrer le conteneur producer

```bash
docker compose up -d producer
```

### 5.3 Surveiller les logs

```bash
docker logs -f taxi-producer
```

**Messages attendus :**
```
2026-06-05 00:xx:xx [PRODUCER] Waiting 30s for Kafka to initialise...
2026-06-05 00:xx:xx [PRODUCER] Connected to Kafka at kafka:29092
2026-06-05 00:xx:xx [PRODUCER] Streaming '/data/taxi_sample.csv' → Kafka topic 'taxi-raw'
2026-06-05 00:xx:xx [PRODUCER] Sent 1,000 messages | Skipped 0 bad rows
...
```

### 5.4 Vérifier sur Kafka UI

- URL : http://localhost:8080
- Topic : taxi-raw → Messages

---

## Étape 6 — Vérifier que les Données Arrivent dans HDFS

Attendre 5 minutes, puis vérifier :

```bash
# Vérifier le dossier joined (utilisé par MapReduce)
docker exec namenode hdfs dfs -ls /taxi/joined/

# Vérifier le dossier anomalies
docker exec namenode hdfs dfs -ls /taxi/anomalies/

# Vérifier le dossier clean
docker exec namenode hdfs dfs -ls /taxi/clean/

# Voir l'espace utilisé
docker exec namenode hdfs dfs -du -h /taxi/
```

**Résultat attendu :** Des fichiers Parquet nommés `part-00000-*.parquet`

---

## Étape 7 — Exécuter les Jobs MapReduce

### 7.1 Job 1 : Compter les courses par borough

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/mapreduce/mr_trips_per_borough.py
```

**Lire le résultat :**
```bash
docker exec namenode hdfs dfs -cat /taxi/output/trips_per_borough/*.csv
```

### 7.2 Job 2 : Revenus par borough

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/mapreduce/mr_revenue_per_borough.py
```

**Lire le résultat :**
```bash
docker exec namenode hdfs dfs -cat /taxi/output/revenue_per_borough/*.csv
```

### 7.3 Job 3 : Compter les anomalies par borough

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/mapreduce/mr_anomaly_counts.py
```

**Lire le résultat :**
```bash
docker exec namenode hdfs dfs -cat /taxi/output/anomaly_counts/*.csv
```

---

## Résumé des Commandes Utiles

| Action | Commande |
|--------|----------|
| Voir tous les conteneurs | `docker ps` |
| Voir les logs d'un conteneur | `docker logs -f <nom>` |
| Lister HDFS | `docker exec namenode hdfs dfs -ls /taxi/` |
| Voir taille HDFS | `docker exec namenode hdfs dfs -du -h /taxi/` |
| Lire un fichier HDFS | `docker exec namenode hdfs dfs -cat /taxi/output/...` |
| Arrêter tout | `docker compose down` |

## Services Web Disponibles

| Service | URL |
|---------|-----|
| Kafka UI | http://localhost:8080 |
| Spark Master UI | http://localhost:8090 |
| HDFS NameNode UI | http://localhost:9870 |

## Dépannage

### Problème : Port 9000 déjà occupé
```bash
docker stop deployment_alliance-minio-1
```

### Problème : Workers Spark monopolisés par le streaming

Par défaut, les workers ont 2 cores et 2GB. Pour permettre aux jobs MapReduce de s'exécuter en parallèle :

```bash
# Modifier dans docker-compose.yml:
# spark-worker: --memory "1G" --cores "1"
# spark-worker-2: --memory "1G" --cores "1"

docker compose up -d --force-recreate spark-worker spark-worker-2
```

### Problème : Topic Kafka n'existe pas
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic taxi-raw --partitions 1 --replication-factor 1