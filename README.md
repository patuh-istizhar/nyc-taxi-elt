# NYC Taxi ELT

[![CI](https://github.com/patuh-istizhar/nyc-taxi-prefect/actions/workflows/ci.yml/badge.svg)](https://github.com/patuh-istizhar/nyc-taxi-prefect/actions/workflows/ci.yml)

Monthly ELT for NYC TLC trip data, orchestrated with Prefect Cloud and running on
Prefect Managed infrastructure (serverless, no worker).

```
TLC parquet (HTTP)  ->  Backblaze B2  ->  MotherDuck (DuckDB)
                          (raw copy)        (deduped table)
```

## Stack

| Layer | Service | Free tier |
|---|---|---|
| Orchestration | Prefect Cloud Hobby | 500 managed minutes/month |
| Raw storage | Backblaze B2 | 10 GB |
| Warehouse | MotherDuck Lite | 10 GB, 10 compute h/month |

## Layout

```
flows/
  config.py      settings + Secret block fallback
  b2.py          S3-compatible B2 helpers (multipart streaming upload)
  warehouse.py   schema-aware MotherDuck loader + validation + audit
  nyc_taxi.py    main flow (plan -> B2 -> MotherDuck -> report)
  smoke.py       B2 + MotherDuck connectivity check
tests/           month resolution, loader and view tests
prefect.yaml     deployments (weekly + smoke)
```

## Flow behaviour

- Scheduled weekly (Monday 06:00 UTC). TLC publish dates slip, so each run loads the
  newest month in the last 6 that is still incomplete and exits immediately when there
  is nothing left to do.
- Manual runs accept `trip_date` (date picker; only year + month are used) and `taxi_type`
  (`all`, `yellow`, `green`).
- Files are stored as `b2://zoomcamp/<taxi_type>_tripdata_<YYYY-MM>.parquet`.
- A month/file that already has an audit row is skipped unless `force_download` is set.
- Reloading the same file deletes its previous rows first, so runs are idempotent.
- The delete + insert runs in one transaction: a failed reload keeps the previous rows.
- Source columns outside the known schema are added to the table automatically; known
  columns missing from a file are stored as `NULL`.
- After the insert the month is validated (row count, duplicate ids, null timestamps,
  pickup month) and the run fails if a hard invariant breaks. Timestamps more than a
  month outside the file's month are reported as warnings.
- Every load is recorded in `zoomcamp.pipeline_runs` (run id, file, sha256, rows, warnings)
  and summarised in a Prefect artifact keyed `nyc-taxi-<YYYY-MM>`.
- Scheduled flow runs are named `nyc-taxi-<taxi_type>` (the month is chosen at run time);
  manual runs use `nyc-taxi-<taxi_type>-<YYYY-MM>`. Deployment concurrency is 1.
- Row id = `MD5(VendorID, pickup, dropoff, PULocationID, DOLocationID)`.

B2 lifecycle on `zoomcamp` keeps only the latest version of each key
(`daysFromHidingToDeleting = 1`), so re-uploads do not pile up old versions.

## Warehouse tables

| Table | Contents |
|---|---|
| `zoomcamp.yellow_tripdata` | yellow trips (one row per unique trip) |
| `zoomcamp.green_tripdata` | green trips |
| `zoomcamp.pipeline_runs` | load audit log |

```sql
SELECT trip_month, taxi_type, filename, rows_loaded, loaded_at, warnings
FROM zoomcamp.pipeline_runs
ORDER BY loaded_at DESC
LIMIT 10;
```

## Observability

| Where | What |
|---|---|
| `zoomcamp.pipeline_runs` | one row per load: run name, rows, size, sha256, new columns, warnings |
| `zoomcamp.pipeline_health` | latest load per month/taxi type + `days_since_load`, `months_behind` |
| `zoomcamp.trip_months` | row counts, pickup range, `suspicious_rows` per loaded file |
| Prefect Artifacts | `nyc-taxi-<YYYY-MM>` summary of each run |
| Flow run names | `nyc-taxi-<taxi_type>[-<YYYY-MM>]` |

```sql
SELECT * FROM zoomcamp.pipeline_health ORDER BY trip_month DESC;
SELECT * FROM zoomcamp.trip_months ORDER BY filename;
```

The views are refreshed at the end of every flow run. A failed run is visible in
the Prefect run list and leaves `pipeline_health.days_since_load` growing; no
notification integration (email/Slack) is wired up on purpose.

## CI

`.github/workflows/ci.yml` runs on every push and pull request:

```bash
uv sync --locked
uv run python -c "import flows.nyc_taxi, flows.smoke"   # entrypoints import cleanly
ruff check . --output-format=github
ruff format --check .
pytest -q
```

Public repositories get GitHub-hosted runners for free.

## Configuration

Non-secret values live in `.env` (local) or `prefect.yaml` `job_variables.env` (cloud).
Secrets come from Prefect Secret blocks:

| Block | Value |
|---|---|
| `b2-key-id` | B2 application key ID |
| `b2-app-key` | B2 application key |
| `motherduck-token` | MotherDuck service account token |

## Commands

```bash
# local smoke test
set -a && . ./.env && set +a
uv run python -m flows.smoke

# load one month
uv run python -c "from flows.nyc_taxi import nyc_taxi; nyc_taxi(taxi_type='green', trip_date='2019-01-15')"

# tests
uv run pytest -q
uvx ruff check flows tests

# deploy
uv run prefect deploy --all
```

### Keeping the Cloud workspace clean

Every `@flow` executed locally is registered in whichever Prefect profile is
active, so ad-hoc scripts and tests can leave stray flows behind. Tests are
isolated through `tests/conftest.py` (temporary local server). For local script
runs, use a profile without a Cloud API URL:

```bash
prefect profile create local      # no PREFECT_API_URL -> ephemeral server
prefect profile use local
# ... run scripts ...
prefect profile use default       # switch back to Prefect Cloud
```
