# Digipay transactions: Kafka → Iceberg

A local pipeline that simulates Digipay transactions, streams them through Kafka, and lands them in Apache Iceberg (Polaris catalog, MinIO storage).

| Phase | Status | What |
|-------|--------|------|
| 1 | Done | Simulate transactions and produce them to Kafka (`ingestion.py`) |
| 2 | Planned | Consume from Kafka and write to an Iceberg table via Polaris |

## Stack

Everything runs from [docker-compose.yml](docker-compose.yml):

| Service | Purpose | Host port |
|---------|---------|-----------|
| `kafka-1`, `kafka-2` | 2-node KRaft Kafka cluster | `9094`, `9095` |
| `kafka-ui` | Browse topics and messages | http://localhost:8085 |
| `polaris` (+ `polaris-db`) | Iceberg REST catalog | `8183` |
| `minio` | S3-compatible warehouse storage | `9000`, console `9001` |
| `spark-master`, `spark-worker-1`, `spark-worker-2` | Spark standalone cluster (see below) | UIs `8090`, `8091`, `8092` |
| `spark-iceberg` | Spark driver/client, Jupyter and `spark-submit` | `8888` |

Setup details live in [commands.md](commands.md) (health checks) and [iceberg-polaris-test.md](iceberg-polaris-test.md) (Iceberg + Polaris end-to-end test).

Requires a `.env` with, among others, `KAFKA_KRAFT_CLUSTER_ID`. Generate one with:

```sh
docker run --rm apache/kafka:3.9.2 /opt/kafka/bin/kafka-storage.sh random-uuid
```

```sh
docker compose up -d --build   # first run builds the Spark image (needs internet for Maven Central)
docker compose ps              # kafka-1 and kafka-2 should be healthy
```

## Spark cluster

A standalone cluster for phase 2 and for practicing Spark operations: 1 master and 2 workers, each worker with 2 cores and 1 GB (4 cores and 2 GB in total).

| Container | Role | UI |
|-----------|------|----|
| `spark-master` | Master, `spark://spark-master:7077` | http://localhost:8090 |
| `spark-worker-1` | Worker | http://localhost:8091 |
| `spark-worker-2` | Worker | http://localhost:8092 |
| `spark-iceberg` | Driver (client mode): run `spark-submit` here | Jupyter http://localhost:8888 |

All four use one image built from [spark/Dockerfile](spark/Dockerfile). It is the `spark-iceberg` base image (Spark 3.5.5, Iceberg 1.8.1) plus the Kafka connector jars, so versions are identical on every node.

Submit a job from the driver container:

```sh
docker compose exec spark-iceberg spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-iceberg \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=768m \
  --conf spark.executor.cores=2 --conf spark.cores.max=4 \
  /path/to/job.py
```

Things to know:
- **Client mode only.** Standalone mode doesn't support cluster deploy mode for PySpark, so the driver runs in `spark-iceberg`. Executors connect back to it, which is why `spark.driver.host` is required.
- **Kafka from inside the network:** `kafka-1:9092,kafka-2:9092`, not `localhost:9094`.
- **Polaris catalog config:** the catalog settings are given by the job, as in [iceberg-polaris-test.md](iceberg-polaris-test.md). Vended credentials are off, so executors write to MinIO with the `AWS_*` variables set on the workers in `docker-compose.yml`.
- **Memory:** the master and each worker container are capped at 2 GB (`mem_limit` in `docker-compose.yml`). Workers offer 1 GB to executors (`SPARK_WORKER_MEMORY`), leaving headroom for the JVM and Python overhead. Docker Desktop has about 8 GB here. Keep executor memory at or below the worker memory (1 GB). Raise `SPARK_WORKER_MEMORY` only if Docker has room.
- `spark-iceberg` also runs its own embedded master and worker. They are unused.

Check the cluster:

```sh
curl -s http://localhost:8090/json/ | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['status'], [(w['cores'], w['memory'], w['state']) for w in d['workers']])"
```

Expect `ALIVE` with two workers, each `2 1024 ALIVE`. A job's progress appears in the master UI while it runs.

## Phase 1: simulate ingestion into Kafka

[ingestion.py](ingestion.py) generates synthetic transactions and produces them as JSON to the topic `digipay.transactions`.

### Run

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install confluent-kafka

python ingestion.py                         # ~10 events/s until Ctrl+C
python ingestion.py --rate 200 --count 50000
python ingestion.py --seed 42 --count 1000  # reproducible data
python ingestion.py --dry-run --count 5     # print events, don't touch Kafka
```

The script creates the topic if it is missing (6 partitions, replication factor 2, which matches the 2-broker cluster). It connects through the host listeners `localhost:9094,localhost:9095`. To run it inside the compose network, use `--bootstrap kafka-1:9092,kafka-2:9092`.

| Option | Default | Description |
|--------|---------|-------------|
| `--bootstrap` | `localhost:9094,localhost:9095` | Kafka bootstrap servers |
| `--topic` | `digipay.transactions` | Target topic |
| `--rate` | `10` | Events per second |
| `--count` | `0` | Stop after N events (0 = until Ctrl+C) |
| `--users` | `10000` | Simulated user population |
| `--merchants` | `500` | Simulated merchant population |
| `--partitions` | `6` | Partitions, used only when creating the topic |
| `--replication-factor` | `2` | Used only when creating the topic |
| `--seed` | random | Seed for reproducible output |
| `--dry-run` | off | Print events instead of producing |

Ctrl+C stops cleanly: pending messages are flushed and a summary is printed.

### Producer behavior

- **Key:** `user_id`. All events for one user go to the same partition, so per-user order is preserved.
- **Reliability:** idempotent producer with `acks=all`, so retries don't create duplicates.
- **Value:** JSON, one event per message.
- **Skew:** a few users are much more active than the rest, so the data isn't uniform.

### Event schema

Every event has the same flat set of fields. Fields that don't apply to a transaction type are `null`. This maps directly onto one Iceberg table in phase 2. Amounts are integer **IRR (rials)**.

| Field | Type | Notes |
|-------|------|-------|
| `transaction_id` | string | UUID |
| `event_time` | string | ISO-8601 UTC, millisecond precision |
| `transaction_type` | string | See below |
| `status` | string | `SUCCESS` or `FAILED` |
| `failure_reason` | string/null | Set when `FAILED` (for example `INSUFFICIENT_FUNDS`, `BANK_TIMEOUT`) |
| `user_id` | string | Initiating user |
| `counterparty_user_id` | string/null | `P2P_TRANSFER` only |
| `merchant_id`, `merchant_category` | string/null | `QR_PAYMENT`, `BNPL_PURCHASE`, `REFUND` |
| `city` | string | |
| `amount` | long | IRR |
| `currency` | string | Always `IRR` |
| `fee` | long | IRR |
| `channel` | string | `ANDROID`, `IOS`, `WEB`, `PWA` |
| `bill_kind` | string/null | `BILL_PAYMENT` only |
| `operator` | string/null | `MOBILE_TOPUP` only |
| `bnpl_installments` | int/null | `BNPL_PURCHASE` only (3, 6 or 12) |
| `original_transaction_id` | string/null | `REFUND` only (simulated reference) |

Transaction types: `WALLET_TOPUP`, `P2P_TRANSFER`, `QR_PAYMENT`, `BILL_PAYMENT`, `MOBILE_TOPUP`, `BNPL_PURCHASE`, `BNPL_REPAYMENT`, `WITHDRAWAL`, `REFUND`. Each type has its own frequency, amount range and failure rate. For example, BNPL purchases fail more often than refunds.

Example event:

```json
{"transaction_id": "d79ab6c2-7f03-46be-81f3-0cb5eae3b1bf", "event_time": "2026-11-04T10:54:40.103+00:00",
 "transaction_type": "BNPL_PURCHASE", "status": "SUCCESS", "failure_reason": null,
 "user_id": "u_0006194", "counterparty_user_id": null, "merchant_id": "m_00440",
 "merchant_category": "FUEL", "city": "Qom", "amount": 5985000, "currency": "IRR", "fee": 0,
 "channel": "IOS", "bill_kind": null, "operator": null, "bnpl_installments": 6,
 "original_transaction_id": null}
```

### Verify

Open Kafka UI at http://localhost:8085 and look at the topic `digipay.transactions`, or use the CLI:

```sh
# Message count per partition
docker compose exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic digipay.transactions

# Read a few messages
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic digipay.transactions \
  --from-beginning --max-messages 5
```

## Phase 2: Kafka → Iceberg (planned)

Consume `digipay.transactions` and write to an Iceberg table in the `polaris_catalog` catalog, partitioned by day of `event_time`. The tooling (Spark Structured Streaming or Kafka Connect Iceberg sink) is not chosen yet.
