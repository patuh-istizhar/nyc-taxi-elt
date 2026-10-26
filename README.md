# NYC Taxi ELT

[![CI](https://github.com/patuh-istizhar/nyc-taxi-prefect/actions/workflows/ci.yml/badge.svg)](https://github.com/patuh-istizhar/nyc-taxi-prefect/actions/workflows/ci.yml)

NYC TLC trip data is streamed to Backblaze B2 as raw parquet and loaded into
MotherDuck by a Prefect flow. It runs on Prefect Managed infrastructure, so there is
no worker to maintain.

```mermaid
flowchart LR
    TLC[NYC TLC] --> B2[B2 raw copy] --> MD[MotherDuck]
```

## How it works

- A weekly run picks the newest month that is still incomplete, loads yellow and green,
  then writes an audit row and refreshes the views.
- A month that already has an audit row is skipped; `force_download=true` reloads it.
- Reloading a file deletes its previous rows first, so runs are idempotent. A failed
  load rolls back and keeps the old data.
- Source columns outside the known schema are added automatically; known columns
  missing from a file are stored as `NULL`; missing required columns fail the run.
- Hard failures: row count mismatch, duplicate ids, null timestamps, more than 1% of
  pickups outside the month.
- Warnings only: out-of-month pickups, dropoff before pickup, negative totals, and
  timestamps more than a month outside the file's month.
- Every load writes to `pipeline_runs` and publishes a Prefect artifact
  (`nyc-taxi-<YYYY-MM>`).

## Warehouse

| Object | Contents |
|---|---|
| `zoomcamp.yellow_tripdata` | yellow trips, one row per unique trip |
| `zoomcamp.green_tripdata` | green trips |
| `zoomcamp.pipeline_runs` | audit log of every load (rows, size, sha256, warnings) |
| `zoomcamp.pipeline_health` | latest load per month/type, `days_since_load`, `months_behind` |
| `zoomcamp.trip_months` | rows, pickup range and `suspicious_rows` per file |

```sql
SELECT trip_month, taxi_type, rows_in_table, months_behind, last_run
FROM zoomcamp.pipeline_health
ORDER BY trip_month DESC;
```

Row id is `MD5(VendorID, pickup, dropoff, PULocationID, DOLocationID)`. B2 keeps only
the latest version of each key.

## Usage

```bash
# manual run; the deployment UI also exposes a date picker
uv run prefect deployment run 'nyc-taxi/nyc-taxi-weekly' --param trip_date=2026-08-01

# local run
set -a && . ./.env && set +a
uv run python -c "from flows.nyc_taxi import nyc_taxi; nyc_taxi(trip_date='2026-08-01')"

# checks
uv run pytest -q
uv run ruff check . && uv run ruff format --check .

# deploy
uv run prefect deploy --all
```

Credentials live in Prefect Secret blocks (`b2-key-id`, `b2-app-key`,
`motherduck-token`); everything else is in `.env` or `prefect.yaml`. Local flow runs
register in the active Prefect profile, so use a profile without `PREFECT_API_URL` for
ad-hoc scripts; tests already use an ephemeral server.

CI (`.github/workflows/ci.yml`) runs an import check, ruff and pytest on every push and
pull request.
