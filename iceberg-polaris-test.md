# Iceberg + Polaris full feature test

End-to-end test plan for the stack in `docker-compose.yml`: Spark (Iceberg 1.8) → Apache Polaris (REST catalog, Postgres metadata) → MinIO (S3 storage).

Run everything from the project directory in bash or zsh. Each `sp` call starts a fresh Spark session (about 10 s), so the steps are slow but independent.

**Not covered:** vended credentials (MinIO has no STS), Trino/Flink, Polaris policies, catalog federation.

Each step lists the expected result. A step passes only if it matches.

---

## 0. Setup

### 0.1 Start the stack and load secrets

```sh
docker compose up -d
docker compose ps -a
set -a; source .env; set +a
```

Expected: `polaris`, `polaris-db`, `minio`, `iceberg-rest`, `spark-iceberg` are Up. `polaris-bootstrap`, `polaris-setup` and `mc` are `Exited (0)`.

### 0.2 Shell helpers

```sh
BASE=http://localhost:8183
CATALOG=polaris_catalog
NS=itest

# Spark SQL against Polaris. usage: _spark <client_id> <client_secret> [spark-sql args...]
_spark() {
  local id="$1" secret="$2"; shift 2
  docker exec -i spark-iceberg spark-sql \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
    --conf spark.sql.catalog.polaris.warehouse=$CATALOG \
    --conf "spark.sql.catalog.polaris.credential=$id:$secret" \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.polaris.s3.endpoint=http://minio:9000 \
    --conf spark.sql.catalog.polaris.s3.path-style-access=true \
    --conf spark.sql.catalog.polaris.client.region=us-east-1 \
    "$@"
}
sp()  { _spark "${POLARIS_CLIENT_ID:-root}" "$POLARIS_CLIENT_SECRET" -e "$1"; }       # verbose
spq() { _spark "${POLARIS_CLIENT_ID:-root}" "$POLARIS_CLIENT_SECRET" -S -e "$1" 2>/dev/null; }  # rows only, for capturing values

# Polaris REST. usage: mgmt <METHOD> <path under /api/management/v1> [extra curl args...]
mgmt() {
  curl -s -w '\nHTTP %{http_code}\n' -X "$1" "$BASE/api/management/v1$2" \
    -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" \
    -H 'Content-Type: application/json' "${@:3}"
}
# Iceberg REST catalog API
cat_api() {
  curl -s -w '\nHTTP %{http_code}\n' -X "$1" "$BASE/api/catalog/v1$2" \
    -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" \
    -H 'Content-Type: application/json' "${@:3}"
}
```

### 0.3 Get an admin token

```sh
TOKEN=$(curl -s $BASE/api/catalog/v1/oauth/tokens \
  --user "${POLARIS_CLIENT_ID:-root}:$POLARIS_CLIENT_SECRET" \
  -H "Polaris-Realm: POLARIS" \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
echo "${TOKEN:0:20}..."
```

Expected: a non-empty prefix. Tokens expire, so repeat this step if you later get 401.

---

## 1. Platform health

```sh
curl -s http://localhost:8182/q/health          # "status":"UP"
curl -s http://localhost:8182/q/health/ready    # "status":"UP"
curl -s -o /dev/null -w "minio ready: %{http_code}\n" http://localhost:9000/minio/health/ready   # 200
cat_api GET "/config?warehouse=$CATALOG"        # JSON with a "prefix", HTTP 200
```

---

## 2. Polaris: security and API

### 2.1 Authentication is enforced

```sh
curl -s -o /dev/null -w "no token:    %{http_code}\n" $BASE/api/catalog/v1/$CATALOG/namespaces
curl -s -o /dev/null -w "bad token:   %{http_code}\n" -H "Authorization: Bearer nope" $BASE/api/catalog/v1/$CATALOG/namespaces
curl -s -o /dev/null -w "bad secret:  %{http_code}\n" $BASE/api/catalog/v1/oauth/tokens \
  --user root:wrong -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL
```

Expected: `401` for all three.

### 2.2 Catalog definition

```sh
mgmt GET /catalogs
mgmt GET /catalogs/$CATALOG
```

Expected: `polaris_catalog`, `type: INTERNAL`, `storageType: S3`, endpoint `http://minio:9000`, `pathStyleAccess: true`, base location `s3://warehouse/polaris`.

### 2.3 Admin grants

```sh
mgmt GET /catalogs/$CATALOG/catalog-roles
mgmt GET /catalogs/$CATALOG/catalog-roles/catalog_admin/grants
```

Expected: role `catalog_admin` exists and its grants include `CATALOG_MANAGE_CONTENT`. If that grant is missing, `polaris-setup` used the wrong request body. Re-add it with:

```sh
mgmt PUT /catalogs/$CATALOG/catalog-roles/catalog_admin/grants \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
```

If that returns 400, try the flat body `{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}`.

### 2.4 Principals and roles list

```sh
mgmt GET /principals
mgmt GET /principal-roles
```

Expected: `root` principal and the `service_admin` principal role.

---

## 3. Iceberg core features (as root)

### 3.1 Namespace and table

```sh
sp "CREATE NAMESPACE polaris.$NS;
SHOW NAMESPACES IN polaris;"
```

Expected: `itest` listed.

```sh
sp "CREATE TABLE polaris.$NS.orders (
  id BIGINT, customer STRING, amount DECIMAL(10,2), ts TIMESTAMP)
USING iceberg
PARTITIONED BY (bucket(4, id))
TBLPROPERTIES ('format-version'='2',
               'write.delete.mode'='merge-on-read',
               'write.update.mode'='merge-on-read',
               'write.merge.mode'='merge-on-read');
SHOW CREATE TABLE polaris.$NS.orders;
DESCRIBE EXTENDED polaris.$NS.orders;"
```

Expected: partitioning `bucket(4, id)`, `format-version = 2`, location under `s3://warehouse/polaris/itest/orders`.

### 3.2 Insert and read

```sh
sp "INSERT INTO polaris.$NS.orders VALUES
  (1,'alice', 10.50, TIMESTAMP '2026-01-01 10:00:00'),
  (2,'bob',   20.00, TIMESTAMP '2026-01-02 11:00:00'),
  (3,'carol', 30.25, TIMESTAMP '2026-01-03 12:00:00');
SELECT * FROM polaris.$NS.orders ORDER BY id;"
```

Expected: 3 rows.

### 3.3 CTAS and rename

```sh
sp "CREATE TABLE polaris.$NS.orders_copy USING iceberg AS SELECT * FROM polaris.$NS.orders;
SELECT count(*) FROM polaris.$NS.orders_copy;
ALTER TABLE polaris.$NS.orders_copy RENAME TO polaris.$NS.orders_renamed;
SHOW TABLES IN polaris.$NS;"
```

Expected: count 3, tables `orders` and `orders_renamed` (no `orders_copy`).

### 3.4 UPDATE, DELETE, MERGE (row-level, merge-on-read)

```sh
sp "UPDATE polaris.$NS.orders SET amount = amount + 1 WHERE id = 1;
DELETE FROM polaris.$NS.orders WHERE id = 2;
MERGE INTO polaris.$NS.orders t
USING (SELECT 3 AS id, 'carol-new' AS customer, CAST(99.99 AS DECIMAL(10,2)) AS amount, TIMESTAMP '2026-01-03 12:00:00' AS ts
       UNION ALL SELECT 4, 'dave', CAST(40.00 AS DECIMAL(10,2)), TIMESTAMP '2026-01-04 09:00:00') s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
SELECT * FROM polaris.$NS.orders ORDER BY id;"
```

Expected:

| id | customer  | amount |
|----|-----------|--------|
| 1  | alice     | 11.50  |
| 3  | carol-new | 99.99  |
| 4  | dave      | 40.00  |

Row 2 is gone.

```sh
sp "SELECT content, file_path, record_count FROM polaris.$NS.orders.delete_files;"
```

Expected: at least one delete file (merge-on-read in effect). If this is empty, the writes were copy-on-write, so check the table properties.

### 3.5 Schema evolution

```sh
sp "ALTER TABLE polaris.$NS.orders ADD COLUMN country STRING;
ALTER TABLE polaris.$NS.orders RENAME COLUMN customer TO customer_name;
ALTER TABLE polaris.$NS.orders ALTER COLUMN id TYPE BIGINT;
INSERT INTO polaris.$NS.orders VALUES (5,'erin', 50.00, TIMESTAMP '2026-01-05 08:00:00', 'DE');
SELECT * FROM polaris.$NS.orders ORDER BY id;"
```

Expected: old rows show `country = NULL`, the renamed column carries old data, row 5 has `DE`. No data rewrite was needed.

```sh
sp "ALTER TABLE polaris.$NS.orders DROP COLUMN country;
DESCRIBE polaris.$NS.orders;"
```

Expected: `country` is gone.

### 3.6 Partition evolution and hidden partitioning

```sh
sp "ALTER TABLE polaris.$NS.orders ADD PARTITION FIELD days(ts);
INSERT INTO polaris.$NS.orders VALUES (6,'frank', 60.00, TIMESTAMP '2026-02-01 07:00:00');
SELECT * FROM polaris.$NS.orders.partitions;
SELECT * FROM polaris.$NS.orders WHERE ts >= TIMESTAMP '2026-02-01 00:00:00';"
```

Expected: partitions now include both the old `bucket` spec and the new `days(ts)` spec. The filter on `ts` works without any partition column. Old data is not rewritten.

```sh
sp "ALTER TABLE polaris.$NS.orders DROP PARTITION FIELD bucket(4, id);
DESCRIBE EXTENDED polaris.$NS.orders;"
```

Expected: only `days(ts)` remains in the current spec.

### 3.7 Snapshots, history, time travel

```sh
sp "SELECT snapshot_id, committed_at, operation FROM polaris.$NS.orders.snapshots ORDER BY committed_at;
SELECT * FROM polaris.$NS.orders.history;"
```

Expected: one snapshot per write so far, with operations like `append`, `overwrite`, `delete`.

```sh
FIRST=$(spq "SELECT snapshot_id FROM polaris.$NS.orders.snapshots ORDER BY committed_at LIMIT 1" | tail -1)
echo "first snapshot: $FIRST"
sp "SELECT * FROM polaris.$NS.orders VERSION AS OF $FIRST ORDER BY id;"
```

Expected: exactly the 3 original rows (alice 10.50, bob, carol 30.25).

```sh
TS=$(spq "SELECT date_format(committed_at,'yyyy-MM-dd HH:mm:ss.SSS') FROM polaris.$NS.orders.snapshots ORDER BY committed_at LIMIT 1" | tail -1)
sp "SELECT count(*) FROM polaris.$NS.orders TIMESTAMP AS OF '$TS';"
```

Expected: 3. If you get an error that no snapshot exists at that time, the Spark session timezone differs from the displayed value. Use `VERSION AS OF` instead.

### 3.8 Rollback

```sh
LATEST=$(spq "SELECT snapshot_id FROM polaris.$NS.orders.snapshots ORDER BY committed_at DESC LIMIT 1" | tail -1)
sp "CALL polaris.system.rollback_to_snapshot(table => '$NS.orders', snapshot_id => $FIRST);
SELECT count(*) FROM polaris.$NS.orders;"
```

Expected: 3 rows (the original state).

```sh
sp "CALL polaris.system.set_current_snapshot(table => '$NS.orders', snapshot_id => $LATEST);
SELECT count(*) FROM polaris.$NS.orders;"
```

Expected: back to the latest data (5 rows: ids 1, 3, 4, 5, 6).

### 3.9 Branches and tags

```sh
sp "ALTER TABLE polaris.$NS.orders CREATE TAG release_v1;
ALTER TABLE polaris.$NS.orders CREATE BRANCH audit;
SELECT name, type, snapshot_id FROM polaris.$NS.orders.refs;"
```

Expected: refs `main` (BRANCH), `audit` (BRANCH), `release_v1` (TAG).

```sh
sp "INSERT INTO polaris.$NS.orders.branch_audit VALUES (99,'audit-only', 1.00, TIMESTAMP '2026-03-01 00:00:00');
SELECT count(*) AS main_count FROM polaris.$NS.orders;
SELECT count(*) AS audit_count FROM polaris.$NS.orders VERSION AS OF 'audit';
SELECT count(*) AS tag_count   FROM polaris.$NS.orders VERSION AS OF 'release_v1';"
```

Expected: `audit_count` = `main_count` + 1, and the tag count equals the main count at the time of tagging. Writes to the branch are isolated from `main`.

```sh
sp "CALL polaris.system.fast_forward('$NS.orders', 'main', 'audit');
SELECT count(*) FROM polaris.$NS.orders WHERE id = 99;"
```

Expected: 1 (`main` now includes the audit write).

### 3.10 Metadata tables

```sh
sp "SELECT file_path, file_format, record_count FROM polaris.$NS.orders.files;
SELECT path, added_data_files_count FROM polaris.$NS.orders.manifests;
SELECT timestamp, file FROM polaris.$NS.orders.metadata_log_entries;
SELECT * FROM polaris.$NS.orders.all_data_files LIMIT 5;"
```

Expected: rows for each query, with `file_format = PARQUET` and paths under `s3://warehouse/polaris/itest/orders/`.

### 3.11 Views

```sh
sp "CREATE VIEW polaris.$NS.big_orders AS SELECT id, customer_name, amount FROM polaris.$NS.orders WHERE amount > 40;
SELECT * FROM polaris.$NS.big_orders ORDER BY id;
SHOW VIEWS IN polaris.$NS;"
```

Expected: rows with amount above 40 and the view listed. If view creation is unsupported by this client/server combination, note the error and move on.

### 3.12 Table maintenance

Create several small files first:

```sh
sp "INSERT INTO polaris.$NS.orders VALUES (101,'m1',1.00,TIMESTAMP '2026-03-02 00:00:00');
INSERT INTO polaris.$NS.orders VALUES (102,'m2',2.00,TIMESTAMP '2026-03-02 00:00:00');
INSERT INTO polaris.$NS.orders VALUES (103,'m3',3.00,TIMESTAMP '2026-03-02 00:00:00');
SELECT count(*) AS files_before FROM polaris.$NS.orders.files;"
```

```sh
sp "CALL polaris.system.rewrite_data_files(table => '$NS.orders', options => map('min-input-files','2'));
SELECT count(*) AS files_after FROM polaris.$NS.orders.files;"
```

Expected: `rewritten_data_files_count` above 0 in the procedure output, and `files_after` lower than `files_before`. The row count must be unchanged.

```sh
sp "CALL polaris.system.rewrite_manifests('$NS.orders');"
sp "CALL polaris.system.expire_snapshots(table => '$NS.orders', older_than => TIMESTAMP '2100-01-01 00:00:00', retain_last => 2);
SELECT count(*) AS snapshots_left FROM polaris.$NS.orders.snapshots;"
sp "CALL polaris.system.remove_orphan_files(table => '$NS.orders', dry_run => true);"
```

Expected: `snapshots_left` is small (the last 2, plus any snapshots held by branches and tags). `remove_orphan_files` in dry-run mode only lists files and deletes nothing.

Note that `release_v1` and `audit` keep their snapshots alive until you drop those refs.

### 3.13 Other table properties

```sh
sp "CREATE TABLE polaris.$NS.orc_t (id INT, v STRING) USING iceberg TBLPROPERTIES ('write.format.default'='orc');
INSERT INTO polaris.$NS.orc_t VALUES (1,'x');
SELECT file_format FROM polaris.$NS.orc_t.files;
ALTER TABLE polaris.$NS.orc_t SET TBLPROPERTIES ('owner'='qa');
SHOW TBLPROPERTIES polaris.$NS.orc_t;"
```

Expected: `file_format = ORC` and the `owner` property is present.

---

## 4. Data and metadata in MinIO

```sh
docker compose run --rm --entrypoint /bin/sh mc -c \
  'mc alias set m http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls --recursive m/warehouse/polaris | head -30'
```

Expected: `itest/orders/metadata/*.json`, `*.avro` manifests, and `itest/orders/data/*.parquet`. Nothing is written outside `polaris/` (the `demo` catalog from the old REST service lives elsewhere in the bucket).

---

## 5. Iceberg REST API direct (via Polaris)

```sh
cat_api GET "/$CATALOG/namespaces"
cat_api GET "/$CATALOG/namespaces/$NS/tables"
cat_api GET "/$CATALOG/namespaces/$NS/tables/orders"
cat_api GET "/$CATALOG/namespaces/$NS/views"
```

Expected: HTTP 200. The table response contains `metadata-location` (an `s3://warehouse/polaris/...` path), `schemas`, `partition-specs` (with at least two specs after section 3.6) and `snapshots`.

---

## 6. Polaris RBAC (least privilege)

Goal: a read-only principal can read but cannot write or create.

### 6.1 Create principal, role, grants

```sh
# principal (the response contains the credentials, which are shown only once)
RESP=$(mgmt POST /principals -d '{"principal":{"name":"reader"}}')
echo "$RESP"
READER_ID=$(echo "$RESP"     | sed -n 's/.*"clientId":"\([^"]*\)".*/\1/p')
READER_SECRET=$(echo "$RESP" | sed -n 's/.*"clientSecret":"\([^"]*\)".*/\1/p')

mgmt POST /principal-roles -d '{"principalRole":{"name":"reader_role"}}'
mgmt PUT  /principals/reader/principal-roles -d '{"principalRole":{"name":"reader_role"}}'

mgmt POST /catalogs/$CATALOG/catalog-roles -d '{"catalogRole":{"name":"reader_cat_role"}}'
for P in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA VIEW_LIST VIEW_READ_PROPERTIES; do
  mgmt PUT /catalogs/$CATALOG/catalog-roles/reader_cat_role/grants \
    -d "{\"grant\":{\"type\":\"catalog\",\"privilege\":\"$P\"}}" | tail -1
done
mgmt PUT /principal-roles/reader_role/catalog-roles/$CATALOG -d '{"catalogRole":{"name":"reader_cat_role"}}'
```

Expected: HTTP 201 or 200/204 on each call. Review that the grants are listed:

```sh
mgmt GET /catalogs/$CATALOG/catalog-roles/reader_cat_role/grants
mgmt GET /principal-roles/reader_role/catalog-roles/$CATALOG
```

### 6.2 Reader can read

```sh
_spark "$READER_ID" "$READER_SECRET" -e "SHOW NAMESPACES IN polaris; SELECT count(*) FROM polaris.$NS.orders;"
```

Expected: namespaces and a row count are returned.

### 6.3 Reader cannot write

```sh
_spark "$READER_ID" "$READER_SECRET" -e "INSERT INTO polaris.$NS.orders VALUES (500,'x',1.00,TIMESTAMP '2026-04-01 00:00:00');" 2>&1 | grep -i -E "forbidden|not authorized|403" | head -3
_spark "$READER_ID" "$READER_SECRET" -e "CREATE TABLE polaris.$NS.nope (id INT) USING iceberg;" 2>&1 | grep -i -E "forbidden|not authorized|403" | head -3
_spark "$READER_ID" "$READER_SECRET" -e "DROP TABLE polaris.$NS.orders_renamed;" 2>&1 | grep -i -E "forbidden|not authorized|403" | head -3
```

Expected: each command prints a Forbidden / 403 style error, and `itest.nope` is not created. Confirm with:

```sh
sp "SHOW TABLES IN polaris.$NS;"
```

### 6.4 Grant more, then retest

```sh
mgmt PUT /catalogs/$CATALOG/catalog-roles/reader_cat_role/grants \
  -d '{"grant":{"type":"catalog","privilege":"TABLE_WRITE_DATA"}}'
_spark "$READER_ID" "$READER_SECRET" -e "INSERT INTO polaris.$NS.orc_t VALUES (2,'by-reader');"
sp "SELECT * FROM polaris.$NS.orc_t ORDER BY id;"
```

Expected: the insert now succeeds and both rows are visible. Revoking works the same way with `-X PUT .../revoke` (check the Polaris docs for the exact path in your version).

### 6.5 Credential rotation

```sh
mgmt POST /principals/reader/rotate
```

Expected: a new secret is returned, and the old `READER_SECRET` can no longer obtain a token.

---

## 7. Persistence

```sh
docker compose down          # keeps volumes
docker compose up -d
until curl -sf http://localhost:8182/q/health/ready >/dev/null; do sleep 2; done
```

Then repeat step 0.3 (new token) and:

```sh
mgmt GET /catalogs
mgmt GET /principals
sp "SELECT count(*) FROM polaris.$NS.orders; SELECT * FROM polaris.$NS.orders.refs;"
sp "SELECT * FROM polaris.$NS.big_orders;"
```

Expected: the catalog, the `reader` principal, all table data, branches, tags and the view all survive the restart. The old `reader` secret still works.

If `polaris-bootstrap` fails on restart because the realm already exists, that is a bootstrap-idempotency problem. Check `docker compose logs polaris-bootstrap`.

---

## 8. Cleanup

```sh
sp "DROP VIEW IF EXISTS polaris.$NS.big_orders;
DROP TABLE IF EXISTS polaris.$NS.orders PURGE;
DROP TABLE IF EXISTS polaris.$NS.orders_renamed PURGE;
DROP TABLE IF EXISTS polaris.$NS.orc_t PURGE;
DROP NAMESPACE IF EXISTS polaris.$NS;"

mgmt DELETE /principals/reader
mgmt DELETE /principal-roles/reader_role
mgmt DELETE /catalogs/$CATALOG/catalog-roles/reader_cat_role
```

Confirm that `PURGE` removed the files:

```sh
docker compose run --rm --entrypoint /bin/sh mc -c \
  'mc alias set m http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls --recursive m/warehouse/polaris | wc -l'
```

Expected: `0` (or close to it).

Full reset, which deletes **all** data including the Polaris database: `docker compose down -v`.

---

## Checklist

| # | Area | Feature | Pass |
|---|------|---------|------|
| 1 | Platform | Health endpoints, MinIO ready | [ ] |
| 2.1 | Polaris | Unauthenticated / bad credentials rejected | [ ] |
| 2.2-2.4 | Polaris | Catalog, roles, grants, principals exist | [ ] |
| 3.1-3.2 | Iceberg | Create namespace/table, insert, read | [ ] |
| 3.3 | Iceberg | CTAS, rename | [ ] |
| 3.4 | Iceberg | UPDATE / DELETE / MERGE, delete files | [ ] |
| 3.5 | Iceberg | Schema evolution (add, rename, widen, drop) | [ ] |
| 3.6 | Iceberg | Partition evolution, hidden partitioning | [ ] |
| 3.7 | Iceberg | Snapshots, history, time travel | [ ] |
| 3.8 | Iceberg | Rollback and restore snapshot | [ ] |
| 3.9 | Iceberg | Branches and tags | [ ] |
| 3.10 | Iceberg | Metadata tables | [ ] |
| 3.11 | Iceberg | Views | [ ] |
| 3.12 | Iceberg | Compaction, manifest rewrite, expire, orphan scan | [ ] |
| 3.13 | Iceberg | File format and table properties | [ ] |
| 4 | Storage | Files under `s3://warehouse/polaris/` | [ ] |
| 5 | Polaris | Iceberg REST API responses | [ ] |
| 6 | Polaris | RBAC: read-only denied writes, grant, rotate | [ ] |
| 7 | Both | Everything survives restart | [ ] |
| 8 | Both | Cleanup and PURGE | [ ] |

## Troubleshooting

- **Token empty or 401:** the secret in `.env` must match what `polaris-bootstrap` used. If it changed after the first start, run `docker compose down -v` and start again.
- **`NoSuchBucket` or 403 from S3 on writes:** the `AWS_*` values must equal `MINIO_ROOT_*`, and `MINIO_DOMAIN` must be set on `minio`.
- **`UnknownHostException: polaris`:** the `polaris` container crashed. Run `docker compose logs polaris`.
- **`Forbidden` for root on writes:** the `catalog_admin` grant is missing (see 2.3).
- **Procedure or `ALTER TABLE ... BRANCH` syntax errors:** the Iceberg SQL extension must be set, as done in `_spark` above.
- **Slow steps:** each `sp` call starts a new Spark session. For a faster loop, run `docker exec -it spark-iceberg spark-sql ...` once with the same `--conf` flags and paste the SQL into it.
