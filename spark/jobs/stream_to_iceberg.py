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
