#!/usr/bin/env bash
# Iceberg table maintenance for polaris.digipay.transactions.
# Runs the steps once, then sleeps MAINTENANCE_INTERVAL_SECONDS and repeats.
# Set MAINTENANCE_INTERVAL_SECONDS=0 to run once and exit.
set -uo pipefail

TABLE="${MAINTENANCE_TABLE:-digipay.transactions}"
INTERVAL="${MAINTENANCE_INTERVAL_SECONDS:-3600}"
SNAPSHOT_RETENTION="${MAINTENANCE_SNAPSHOT_RETENTION:-1 day ago}"
ORPHAN_RETENTION="${MAINTENANCE_ORPHAN_RETENTION:-3 days ago}"

run_sql() {
  /opt/spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    --name "iceberg-maintenance" \
    --total-executor-cores 2 \
    --executor-memory 512m \
    --conf spark.driver.host=spark-maintenance \
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
    -e "$1"
}

# Each step runs in its own session so one failure does not block the rest.
step() {
  local name="$1" sql="$2"
  echo "[$(date -u '+%F %T')] maintenance: $name"
  run_sql "$sql" || echo "[$(date -u '+%F %T')] maintenance: $name FAILED (continuing)"
}

while true; do
  expire_before=$(date -u -d "$SNAPSHOT_RETENTION" '+%Y-%m-%d %H:%M:%S')
  orphan_before=$(date -u -d "$ORPHAN_RETENTION" '+%Y-%m-%d %H:%M:%S')

  # 1. compact small files (closed partitions only; skips the hot data being written)
  step rewrite_data_files "CALL polaris.system.rewrite_data_files(
    table => '$TABLE',
    where => 'event_time < current_timestamp() - INTERVAL 1 HOURS',
    options => map('target-file-size-bytes','134217728','min-input-files','5'));"

  # 2. rewrite manifests
  step rewrite_manifests "CALL polaris.system.rewrite_manifests('$TABLE');"

  # 3. expire old snapshots (keeps time travel for SNAPSHOT_RETENTION, min 10 snapshots)
  step expire_snapshots "CALL polaris.system.expire_snapshots(
    table => '$TABLE',
    older_than => TIMESTAMP '$expire_before',
    retain_last => 10);"

  # 4. delete orphan files (older than ORPHAN_RETENTION, clear of in-flight writes)
  step remove_orphan_files "CALL polaris.system.remove_orphan_files(
    table => '$TABLE',
    older_than => TIMESTAMP '$orphan_before');"

  # 5. report the effect
  step report "SELECT count(*) AS files, round(avg(file_size_in_bytes)/1048576, 2) AS avg_mb FROM polaris.$TABLE.files;
SELECT count(*) AS snapshots FROM polaris.$TABLE.snapshots;"

  [ "$INTERVAL" -gt 0 ] || exit 0
  echo "[$(date -u '+%F %T')] maintenance: sleeping ${INTERVAL}s"
  sleep "$INTERVAL"
done
