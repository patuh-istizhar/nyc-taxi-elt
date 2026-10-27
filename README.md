# nyc-taxi-elt

[![CI](https://github.com/patuh-istizhar/nyc-taxi-elt/actions/workflows/ci.yml/badge.svg)](https://github.com/patuh-istizhar/nyc-taxi-elt/actions/workflows/ci.yml)

A small ELT pipeline for learning. Prefect downloads NYC taxi trip files (parquet),
stores them in **S3-compatible storage**, and loads them into a **DuckDB warehouse**.

```
NYC TLC website  --HTTP-->  S3 bucket (raw parquet)  -->  warehouse (DuckDB tables)
```

| | S3 storage | Warehouse | Prefect |
|---|---|---|---|
| **dev**  | RustFS (docker compose) | local file `./data/nyc_taxi.duckdb` | runs on your machine |
| **prod** | Backblaze B2 | MotherDuck `md:nyc_taxi` | Prefect Cloud, Managed work pool |

## Files

```
nyc_taxi_elt/
  config.py          read settings from env vars (or Prefect Secret blocks)
  s3.py              upload a file from a URL to S3, download it again
  warehouse.py       connect to DuckDB, load a parquet file, audit table
  flows/ingest.py    the Prefect flow + command line
  flows/smoke.py     quick check that S3 and the warehouse are reachable
tests/               a few tests (fake S3 with moto, temporary DuckDB file)
prefect.yaml         the prod deployment
docker-compose.yml   RustFS for dev
```

## What the flow does

For every month and taxi type (yellow, green):

1. **Skip** the file if `pipeline_runs` says it is already loaded (unless `--force`).
2. **Upload** `https://d37ci6vzurychx.cloudfront.net/trip-data/<file>` to the S3 bucket.
   If TLC has not published the file yet, it is skipped with a warning.
3. **Load** the file from S3 into `yellow_tripdata` / `green_tripdata`. Old rows of the
   same file are deleted first, so loading twice never creates duplicates.
4. **Record** the load in `pipeline_runs` (file, month, row count, time).

Without a month, the flow checks the last 3 finished months. That is what the
weekly schedule does.

## Dev

```bash
docker compose up -d --wait    # RustFS on :9000, console on :9001 (rustfsadmin / rustfsadmin)
cp .env.example .env
uv sync

uv run --env-file .env python -m nyc_taxi_elt.flows.smoke
uv run --env-file .env python -m nyc_taxi_elt.flows.ingest --taxi-type green --month 2024-01
uv run --env-file .env python -m nyc_taxi_elt.flows.ingest --from 2023-01 --to 2023-12   # backfill
uv run --env-file .env python -m nyc_taxi_elt.flows.ingest --month 2024-01 --force       # load again
uv run --env-file .env python -m nyc_taxi_elt.flows.ingest                               # last 3 months
```

Look at the data (the file and the schema are both called `nyc_taxi`, so write both):

```bash
uv run python -c "import duckdb; con = duckdb.connect('data/nyc_taxi.duckdb'); print(con.sql('SELECT * FROM nyc_taxi.nyc_taxi.pipeline_runs'))"
```

## Prod

Settings live in [`prefect.yaml`](prefect.yaml). Keys are stored in Prefect Secret
blocks: `s3-access-key-id`, `s3-secret-access-key`, `motherduck-token`.

```bash
uv run prefect cloud login
uv run prefect deploy --all                                  # after changing the flow
uv run prefect deployment run nyc-taxi-ingest/weekly         # run now
uv run prefect deployment run nyc-taxi-ingest/weekly \
  --param start_month=2023-01 --param end_month=2023-12      # backfill
```

Only the ingest flow is deployed. To check prod connections from your machine,
put the prod values in `.env.prod` (ignored by git) and run
`uv run --env-file .env.prod python -m nyc_taxi_elt.flows.smoke`.

## Settings

| Variable | dev default | prod |
|---|---|---|
| `ENV` | `dev` | `prod` |
| `S3_ENDPOINT_URL` | `http://127.0.0.1:9000` | B2 URL (empty = AWS S3) |
| `S3_REGION` | `us-east-1` | required |
| `S3_BUCKET` | `nyc-taxi` | required |
| `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | from `.env` | Secret blocks |
| `WAREHOUSE_DATABASE` | `./data/nyc_taxi.duckdb` | `md:nyc_taxi` |
| `WAREHOUSE_SCHEMA` | `nyc_taxi` | `zoomcamp` |
| `MOTHERDUCK_TOKEN` | not needed | Secret block (only for `md:`) |

In dev the bucket is created for you. In prod a missing bucket is an error.

## Tests

```bash
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
```
