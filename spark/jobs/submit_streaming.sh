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
