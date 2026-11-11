# Kafka → Iceberg with Spark Structured Streaming

Step-by-step guide to continuously write the `digipay.transactions` Kafka topic (produced by `ingestion.py`) into an Iceberg table, using the Spark standalone cluster in `docker-compose.yml`.

> The code in this guide was written against the repo's compose file and `ingestion.py` but has not been run end-to-end. Each step has a verification command. Run them in order and fix problems as they appear.

## 1. Architecture

```
ingestion.py (host)
   │  confluent-kafka, idempotent, acks=all, key = user_id
   ▼
Kafka  kafka-1 / kafka-2   topic: digipay.transactions (6 partitions, RF 2)
   │
   ▼
Spark Structured Streaming job  (stream_to_iceberg.py)
   driver   : container `spark-streaming` (or `spark-iceberg` for dev)
   master   : spark://spark-master:7077
   executors: spark-worker-1, spark-worker-2  (4 cores total)
   │  micro-batch every 30 s, one Iceberg commit per batch
   ▼
Iceberg table  polaris.digipay.transactions
   ├─ catalog / metadata pointer : Polaris (REST, Postgres)
   ├─ data + metadata files      : MinIO  s3://warehouse/polaris/digipay/transactions/
   └─ streaming checkpoint       : MinIO  s3a://warehouse/checkpoints/digipay_transactions/
```

### Design decisions

| Concern | Decision | Why |
|---|---|---|
| Engine | Structured Streaming (`readStream` kafka → `writeStream` iceberg) | Offsets are tracked in the checkpoint and committed atomically with the Iceberg snapshot, so you get exactly-once into Iceberg. |
| Delivery semantics | Producer idempotent + `acks=all` (already in `ingestion.py`), checkpointed consumer | No loss, no duplicates on retry. `transaction_id` is kept for dedup downstream. |
| Trigger | `processingTime = 30 seconds` | Every commit writes a snapshot, manifests and data files. Short triggers create tiny files. |
| Checkpoint | MinIO, not container disk | Executors and a restarted driver must see the same state. |
| Schema | Explicit schema with `from_json` | No inference. Corrupt messages are isolated. |
| Layer | Land as a "bronze" table (typed columns + Kafka metadata) | Cleaning and aggregation go in separate batch jobs. |
| Partitioning | `days(event_time)` (hidden partitioning) | Matches time-range queries. Never partition by `user_id`. |
| Maintenance | Separate scheduled job (compaction, expire snapshots, orphan files) | Streaming leaves small files and many snapshots. |

## 2. Prerequisites

1. The stack is up and healthy:

   ```sh
   docker compose up -d
   docker compose ps
   ```

   Needed: `kafka-1`, `kafka-2`, `minio`, `polaris` (healthy), `spark-master`, `spark-worker-1`, `spark-worker-2`, `spark-iceberg`. `polaris-setup` should have exited 0. Check with `docker compose logs polaris-setup`.

2. `.env` contains `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `POLARIS_CLIENT_ID`, `POLARIS_CLIENT_SECRET`, `KAFKA_KRAFT_CLUSTER_ID`.

3. The Spark image has the Kafka source jars (added in `spark/Dockerfile`). Verify:

   ```sh
   docker exec spark-iceberg ls /opt/spark/jars | grep -E "kafka|commons-pool2"
   ```

4. The Spark image can read `s3a://` (needed for the checkpoint). Verify:

   ```sh
   docker exec spark-iceberg ls /opt/spark/jars | grep -E "hadoop-aws|aws-java-sdk-bundle|bundle-2"
   ```

   If `hadoop-aws` is missing, add it to `spark/Dockerfile` (the version must match the Hadoop jars, for example `hadoop-aws-3.3.4.jar` plus `aws-java-sdk-bundle-1.12.262.jar` for Hadoop 3.3.4) and rebuild with `docker compose build spark-iceberg`.

## 3. Step 1: Create the Kafka topic (explicitly)

`ingestion.py` creates the topic if it does not exist, but create it yourself so partitions and retention are deliberate:

```sh
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --if-not-exists \
  --topic digipay.transactions --partitions 6 --replication-factor 2 \
  --config retention.ms=604800000      # 7 days: must exceed worst-case Spark downtime
```

Verify:

```sh
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic digipay.transactions
```

Partitions (6) should be at least the number of streaming cores you give the job. Your cluster has 4 cores, so 6 is fine.

## 4. Step 2: Start the producer

From the host (listeners on `localhost:9094` and `localhost:9095`):

```sh
pip install -r requirements.txt
python ingestion.py --rate 50          # 50 events/s until Ctrl+C
```

Check that messages arrive:

```sh
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic digipay.transactions \
  --from-beginning --max-messages 3
```

Each message is a flat JSON object keyed by `user_id`.

## 5. Step 3: Create the namespace and table in Polaris

Define the table up front so partitioning and properties are explicit. Do not let the stream create it implicitly.

The Spark catalog settings below are used by every command in this guide. Put them in a shell function (same pattern as `iceberg-polaris-test.md`):

```sh
set -a; source .env; set +a

_spark_conf() {
  cat <<EOF
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.polaris.type=rest
--conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog
--conf spark.sql.catalog.polaris.warehouse=polaris_catalog
--conf spark.sql.catalog.polaris.credential=${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}
--conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL
--conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO
--conf spark.sql.catalog.polaris.s3.endpoint=http://minio:9000
--conf spark.sql.catalog.polaris.s3.path-style-access=true
--conf spark.sql.catalog.polaris.client.region=us-east-1
EOF
}
```

Create the table (run on the driver container, in local mode, since it is only DDL):

```sh
docker exec -i spark-iceberg spark-sql $(_spark_conf) -e "
CREATE NAMESPACE IF NOT EXISTS polaris.digipay;

CREATE TABLE IF NOT EXISTS polaris.digipay.transactions (
  transaction_id           STRING    NOT NULL,
  event_time               TIMESTAMP NOT NULL,
  transaction_type         STRING,
  status                   STRING,
  failure_reason           STRING,
  user_id                  STRING,
  counterparty_user_id     STRING,
  merchant_id              STRING,
  merchant_category        STRING,
  city                     STRING,
  amount                   BIGINT,
  currency                 STRING,
  fee                      BIGINT,
  channel                  STRING,
  bill_kind                STRING,
  operator                 STRING,
  bnpl_installments        INT,
  original_transaction_id  STRING,
  -- Kafka lineage
  kafka_topic              STRING,
  kafka_partition          INT,
  kafka_offset             BIGINT,
  kafka_timestamp          TIMESTAMP,
  ingested_at              TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
  'format-version'                 = '2',
  'write.format.default'           = 'parquet',
  'write.parquet.compression-codec'= 'zstd',
  'write.target-file-size-bytes'   = '134217728',
  'write.distribution-mode'        = 'none',
  'history.expire.max-snapshot-age-ms' = '86400000',
  'history.expire.min-snapshots-to-keep' = '10'
);
DESCRIBE EXTENDED polaris.digipay.transactions;"
```

Notes:

- `days(event_time)` is Iceberg hidden partitioning. Queries filter on `event_time` and get partition pruning without a separate date column.
- `write.distribution-mode=none` avoids a shuffle on every micro-batch, which matters on a 4-core cluster.
- The `kafka_*` columns let you trace any row back to its message and detect gaps or duplicates.

Also store bad messages. Create a small dead-letter table:

```sh
docker exec -i spark-iceberg spark-sql $(_spark_conf) -e "
CREATE TABLE IF NOT EXISTS polaris.digipay.transactions_dlq (
  kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP, raw_key STRING, raw_value STRING, ingested_at TIMESTAMP
) USING iceberg PARTITIONED BY (days(ingested_at));"
```

## 6. Step 4: Write the streaming job

Create `spark/jobs/stream_to_iceberg.py`:

```python
"""Kafka (digipay.transactions) -> Iceberg (polaris.digipay.transactions).

Structured Streaming, exactly-once into Iceberg via the checkpoint.
Malformed messages go to polaris.digipay.transactions_dlq.
"""
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "digipay.transactions")
TABLE = "polaris.digipay.transactions"
DLQ_TABLE = "polaris.digipay.transactions_dlq"
CHECKPOINT = os.environ.get(
    "CHECKPOINT", "s3a://warehouse/checkpoints/digipay_transactions")
TRIGGER = os.environ.get("TRIGGER", "30 seconds")
MAX_OFFSETS = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "200000")

# Must match the JSON produced by ingestion.py. event_time arrives as an ISO string.
SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("event_time", StringType()),
    StructField("transaction_type", StringType()),
    StructField("status", StringType()),
    StructField("failure_reason", StringType()),
    StructField("user_id", StringType()),
    StructField("counterparty_user_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("city", StringType()),
    StructField("amount", LongType()),
    StructField("currency", StringType()),
    StructField("fee", LongType()),
    StructField("channel", StringType()),
    StructField("bill_kind", StringType()),
    StructField("operator", StringType()),
    StructField("bnpl_installments", IntegerType()),
    StructField("original_transaction_id", StringType()),
])

spark = SparkSession.builder.appName("kafka-to-iceberg-transactions").getOrCreate()

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")   # only used on the first run (no checkpoint)
    .option("failOnDataLoss", "true")        # fail loudly if retention deleted unread data
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .option("minPartitions", "6")
    .load()
)

parsed = raw.select(
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("key").cast("string").alias("raw_key"),
    F.col("value").cast("string").alias("raw_value"),
).withColumn("j", F.from_json("raw_value", SCHEMA))


def write_batch(df, batch_id):
    df.persist()
    now = F.current_timestamp()

    good = (
        df.where(F.col("j").isNotNull()
                 & F.col("j.transaction_id").isNotNull()
                 & F.to_timestamp("j.event_time").isNotNull())
        .select(
            "j.*",
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        )
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", now)
    )
    bad = (
        df.where(F.col("j").isNull()
                 | F.col("j.transaction_id").isNull()
                 | F.to_timestamp("j.event_time").isNull())
        .select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
                "raw_key", "raw_value")
        .withColumn("ingested_at", now)
    )

    # Idempotent per batch: SparkWrite txn properties let a replayed batch be skipped.
    (good.writeTo(TABLE)
         .option("snapshot-property.kafka-batch-id", str(batch_id))
         .append())
    if bad.take(1):
        bad.writeTo(DLQ_TABLE).append()
    df.unpersist()


query = (
    parsed.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=TRIGGER)
    .queryName("kafka_to_iceberg_transactions")
    .start()
)
query.awaitTermination()
```

Why `foreachBatch` instead of the built-in `format("iceberg")` sink: it lets you route bad records to the DLQ and add lineage columns in one batch. The checkpoint still gives you restart-from-offset behaviour. If you only need a plain append, replace it with:

```python
parsed.select("j.*", ...).writeStream.format("iceberg").outputMode("append") \
    .trigger(processingTime="30 seconds").option("checkpointLocation", CHECKPOINT) \
    .toTable(TABLE)
```

> **Exactly-once caveat with `foreachBatch`:** the built-in `format("iceberg")` sink dedups replayed batches for you. With `foreachBatch`, a crash between the Iceberg commit and the checkpoint commit can replay the last batch, which would append the same rows twice. Two ways to handle it:
> 1. Use `MERGE INTO` on `transaction_id` (or on `kafka_partition, kafka_offset`) inside `write_batch` instead of `append()`. Safer, but costs more per batch.
> 2. Keep `append()` and dedup downstream on `(kafka_partition, kafka_offset)`. Cheap, and acceptable for a bronze table.
>
> Pick one deliberately. This guide uses option 2.

## 7. Step 5: Mount the job into the Spark containers

Add a bind mount so the driver can see the file. In `docker-compose.yml`, on `spark-iceberg`:

```yaml
    volumes:
      - ./spark/jobs:/opt/jobs:ro
```

Then recreate it:

```sh
docker compose up -d spark-iceberg
```

## 8. Step 6: Submit the job to the cluster

Development (client mode, driver runs in `spark-iceberg`, logs in your terminal):

```sh
docker exec -it \
  -e POLARIS_CLIENT_ID -e POLARIS_CLIENT_SECRET \
  spark-iceberg spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name kafka-to-iceberg-transactions \
    --total-executor-cores 4 \
    --executor-memory 768m \
    --conf spark.sql.shuffle.partitions=6 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
    --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
    $(_spark_conf) \
    /opt/jobs/stream_to_iceberg.py
```

Resource sizing (the cluster has 2 workers × 2 cores × 1 GB):

- `--total-executor-cores 4` takes the whole cluster. Use `2` if you also want to run compaction or ad-hoc jobs at the same time.
- `--executor-memory 768m` leaves headroom inside each worker's 1 GB.
- `spark.sql.shuffle.partitions=6` replaces the default 200, which is wasteful here.
- `maxOffsetsPerTrigger` (200000 in the job) caps batch size so a backlog after downtime does not OOM the executors.

Verify the job is running:

- Master UI: http://localhost:8090 shows one running application with 2 executors.
- Driver UI: http://localhost:4040 → Structured Streaming tab (input rate versus process rate, batch duration).
- The log shows a new batch every 30 seconds.

## 9. Step 7: Verify data in Iceberg

```sh
docker exec -i spark-iceberg spark-sql $(_spark_conf) -e "
SELECT count(*) FROM polaris.digipay.transactions;
SELECT transaction_type, status, count(*) c, sum(amount) amt
  FROM polaris.digipay.transactions GROUP BY 1,2 ORDER BY 3 DESC;
SELECT committed_at, snapshot_id, summary['added-records'] added, summary['added-data-files'] files
  FROM polaris.digipay.transactions.snapshots ORDER BY committed_at DESC LIMIT 5;
SELECT count(*) AS dlq FROM polaris.digipay.transactions_dlq;"
```

Check for gaps or duplicates against Kafka:

```sql
-- duplicates (should be 0 rows; see the caveat in step 4)
SELECT kafka_partition, kafka_offset, count(*) FROM polaris.digipay.transactions
GROUP BY 1,2 HAVING count(*) > 1;

-- per-partition progress
SELECT kafka_partition, min(kafka_offset), max(kafka_offset), count(*)
FROM polaris.digipay.transactions GROUP BY 1 ORDER BY 1;
```

For each partition, `count` should equal `max - min + 1`. Compare `max` with the broker's latest offsets:

```sh
docker compose exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka-1:9092 --topic digipay.transactions
```

Check that files landed in MinIO under `s3://warehouse/polaris/digipay/transactions/data/` (console: http://localhost:9001) and checkpoints under `warehouse/checkpoints/digipay_transactions/`.

## 10. Step 8: Run it as a long-lived service

A `docker exec` session dies with your terminal. Add a dedicated driver service so the job restarts automatically and resumes from its checkpoint. Add to `docker-compose.yml`:

```yaml
  spark-streaming:
    <<: *spark-image
    container_name: spark-streaming
    restart: unless-stopped
    depends_on:
      - spark-master
      - spark-worker-1
      - spark-worker-2
      - polaris
      - kafka-1
      - kafka-2
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      AWS_REGION: us-east-1
      POLARIS_CLIENT_ID: ${POLARIS_CLIENT_ID}
      POLARIS_CLIENT_SECRET: ${POLARIS_CLIENT_SECRET}
    volumes:
      - ./spark/jobs:/opt/jobs:ro
    ports:
      - 4040:4040   # driver UI
    entrypoint: ["/bin/bash", "/opt/jobs/submit_streaming.sh"]
```

`spark/jobs/submit_streaming.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name kafka-to-iceberg-transactions \
  --total-executor-cores 2 \
  --executor-memory 768m \
  --conf spark.driver.host=spark-streaming \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key="$AWS_ACCESS_KEY_ID" \
  --conf spark.hadoop.fs.s3a.secret.key="$AWS_SECRET_ACCESS_KEY" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.polaris.type=rest \
  --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
  --conf spark.sql.catalog.polaris.warehouse=polaris_catalog \
  --conf "spark.sql.catalog.polaris.credential=${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" \
  --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.polaris.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.polaris.s3.path-style-access=true \
  --conf spark.sql.catalog.polaris.client.region=us-east-1 \
  /opt/jobs/stream_to_iceberg.py
```

Two points specific to Docker:

- `spark.driver.host` must be the driver's service name. Executors on the workers connect back to the driver, and without it they may try an unreachable container ID or localhost address.
- Because the driver holds `POLARIS_CLIENT_SECRET` in a container environment variable, it is visible via `docker inspect`. This is acceptable for local development. In a real deployment use Docker/Kubernetes secrets.

Start and follow it:

```sh
docker compose up -d spark-streaming
docker compose logs -f spark-streaming
```

Restart test (proves exactly-once resume from the checkpoint):

```sh
docker compose restart spark-streaming
# wait for the next batch, then re-run the duplicate/gap queries from step 7
```

## 11. Step 9: Table maintenance

Streaming creates a lot of small files and snapshots. Run maintenance as a separate job, not inside the stream. Daily is a reasonable start; with a 30 s trigger, run compaction hourly.

```sh
docker exec -i spark-iceberg spark-sql --master spark://spark-master:7077 \
  --total-executor-cores 2 --executor-memory 512m $(_spark_conf) -e "
-- 1. compact small files (only closed partitions; skips the hot partition being written)
CALL polaris.system.rewrite_data_files(
  table => 'digipay.transactions',
  where => 'event_time < current_timestamp() - INTERVAL 1 HOURS',
  options => map('target-file-size-bytes','134217728','min-input-files','5'));

-- 2. rewrite manifests
CALL polaris.system.rewrite_manifests('digipay.transactions');

-- 3. expire old snapshots (keep 1 day of time travel)
CALL polaris.system.expire_snapshots(
  table => 'digipay.transactions',
  older_than => TIMESTAMP '$(date -u -v-1d '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -u -d '1 day ago' '+%Y-%m-%d %H:%M:%S')',
  retain_last => 10);

-- 4. delete orphan files (older than 3 days, to stay clear of in-flight writes)
CALL polaris.system.remove_orphan_files(
  table => 'digipay.transactions',
  older_than => TIMESTAMP '$(date -u -v-3d '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -u -d '3 days ago' '+%Y-%m-%d %H:%M:%S')');"
```

Schedule it with host cron, or a small compose service with a sleep loop. Compaction commits can conflict with the streaming commit, but Iceberg's optimistic concurrency retries them. If you see repeated conflicts, exclude the current day with the `where` clause.

Check the effect:

```sql
SELECT count(*) files, avg(file_size_in_bytes)/1048576 avg_mb FROM polaris.digipay.transactions.files;
SELECT count(*) FROM polaris.digipay.transactions.snapshots;
```

## 12. Operations

### Monitoring

| What | Where | Healthy |
|---|---|---|
| Input vs processed rows/s | Driver UI :4040 → Structured Streaming | Processed ≥ input |
| Batch duration | Same | Shorter than the 30 s trigger |
| Consumer lag | `kafka-get-offsets.sh` versus `max(kafka_offset)` per partition | Small and stable |
| Cluster usage | Master UI :8090 | Cores allocated, no waiting apps |
| Small files | `transactions.files` metadata table | Average near 128 MB after compaction |
| DLQ | `SELECT count(*) FROM polaris.digipay.transactions_dlq` | 0, or explainable |

### Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `Initial job has not accepted any resources` | Cluster cores taken by another app, or executor memory above the worker's 1 GB | Lower `--total-executor-cores` / `--executor-memory`, stop other apps |
| Executors cannot reach driver | Missing `spark.driver.host` | Set it to the driver's service name |
| `ClassNotFoundException: ...KafkaSourceProvider` | Kafka jars missing from the image | Rebuild the Spark image (see `spark/Dockerfile`) |
| `No FileSystem for scheme "s3a"` | `hadoop-aws` missing | See the prerequisites, step 4 |
| `403` / `AccessDenied` from MinIO on executors | Executors lack credentials (Polaris vended credentials are off) | The worker env vars in the compose file must be set |
| `Partition ... offset was changed` / `data loss` | Topic retention deleted unread data, or the topic was recreated | Raise retention. For a recreated topic, remove the checkpoint and the table rows, or start fresh |
| `Table not found` / `Namespace does not exist` | DDL in step 3 not run, or wrong catalog name | Run step 3, check `polaris.` prefix |
| `CommitFailedException` in compaction | Concurrent commit | Re-run, or narrow the `where` clause |
| Rows missing for new columns | Producer added fields not in `SCHEMA` | Add to the Spark schema and the table (below) |

### Schema evolution

When `ingestion.py` adds a field:

1. `ALTER TABLE polaris.digipay.transactions ADD COLUMN new_field STRING;`
2. Add it to `SCHEMA` in `stream_to_iceberg.py`.
3. `docker compose restart spark-streaming`. The checkpoint stays valid.

Add the column to the table first, then deploy the job. Old rows read as `NULL`.

### Changing the job safely

- Stateless query like this one: it is safe to change code and restart with the same checkpoint.
- To reprocess from the start: stop the job, delete the checkpoint path in MinIO, truncate or recreate the table, restart.
- Never share one checkpoint between two queries or tables.

### Stopping gracefully

```sh
docker compose stop -t 60 spark-streaming
```

The in-flight batch finishes (or is replayed on restart), and the offsets stay safe in the checkpoint.

## 13. End-to-end checklist

1. `docker compose up -d` and every service is healthy.
2. Topic `digipay.transactions` exists (6 partitions, RF 2, 7-day retention).
3. `python ingestion.py --rate 50` is producing.
4. Namespace, table and DLQ table exist in Polaris.
5. `spark/jobs/stream_to_iceberg.py` and `submit_streaming.sh` exist and are mounted.
6. The job is running: master UI shows the app, the driver UI shows batches.
7. Row counts and per-partition offset checks match Kafka.
8. Restart test passes with no duplicates and no gaps.
9. A maintenance job is scheduled.

## 14. Possible next steps

- Replace plain `append()` with `MERGE INTO` on `transaction_id` for a deduplicated silver table.
- Build a silver table (by `transaction_type`/`status`) and a gold table (daily aggregates) with batch jobs.
- Use Kafka's Schema Registry with Avro/Protobuf instead of raw JSON, to catch schema drift at the producer.
- Add a Polaris principal with only `TABLE_WRITE_DATA` for the streaming job rather than `root` (see section 6 of `iceberg-polaris-test.md`).
- Move to Spark cluster deploy mode, Kubernetes, or Flink if you need driver HA beyond a restart policy.
