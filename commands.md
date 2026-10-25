# Iceberg health checks

Run from the project directory. Requires a `.env` with `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

## 1. Start and check containers

```sh
docker compose up -d
docker compose ps
```

`minio`, `iceberg-rest` and `spark-iceberg` should be `running`. `mc` exits with code 0 after creating the bucket.

```sh
docker compose logs mc
docker compose logs rest --tail 20
```

## 2. MinIO

```sh
curl -sf -o /dev/null -w "minio live: %{http_code}\n" http://localhost:9000/minio/health/live
curl -sf -o /dev/null -w "minio ready: %{http_code}\n" http://localhost:9000/minio/health/ready
```

Both should print `200`. Check the bucket exists:

```sh
docker compose run --rm --entrypoint /bin/sh mc -c \
  'mc alias set m http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && mc ls m'
```

Expect `warehouse/`. Console: http://localhost:9001

## 3. REST catalog

```sh
curl -s http://localhost:8181/v1/config
```

Should return JSON with `defaults` and `overrides`.

```sh
curl -s http://localhost:8181/v1/namespaces
```

Should return `{"namespaces":[...]}`.

## 4. End-to-end test with Spark

Open Spark SQL:

```sh
docker exec -it spark-iceberg spark-sql
```

```sql
CREATE NAMESPACE IF NOT EXISTS demo.health;
CREATE TABLE demo.health.t (id INT, name STRING) USING iceberg;
INSERT INTO demo.health.t VALUES (1, 'a'), (2, 'b');
SELECT * FROM demo.health.t;
SELECT * FROM demo.health.t.snapshots;
```

The `SELECT` should return 2 rows and `snapshots` should show one snapshot.

Same test, non-interactive:

```sh
docker exec spark-iceberg spark-sql -e "
CREATE NAMESPACE IF NOT EXISTS demo.health;
CREATE TABLE IF NOT EXISTS demo.health.t (id INT, name STRING) USING iceberg;
INSERT INTO demo.health.t VALUES (1, 'a');
SELECT count(*) FROM demo.health.t;"
```

Confirm the files landed in MinIO:

```sh
docker compose run --rm --entrypoint /bin/sh mc -c \
  'mc alias set m http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && mc ls --recursive m/warehouse | head'
```

Expect `metadata/*.json` and `data/*.parquet` files.

## 5. Persistence test

```sh
docker compose down
docker compose up -d
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.health.t;"
```

The rows should still be there. (`down -v` deletes the volumes and wipes everything.)

## 6. Cleanup

```sh
docker exec spark-iceberg spark-sql -e "DROP TABLE demo.health.t PURGE; DROP NAMESPACE demo.health;"
```

## Troubleshooting

- `rest` unhealthy or table creation fails: `docker compose logs rest`
- S3 / 403 errors: `AWS_*` values must equal `MINIO_ROOT_*` in `.env`
- Catalog name `demo` not found: check the image's Spark config with
  `docker exec spark-iceberg cat /opt/spark/conf/spark-defaults.conf`
