# NYC Taxi ELT

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
tests/           month resolution + loader tests
prefect.yaml     deployments (monthly + smoke)
```

## Flow behaviour

- Scheduled on the 15th: processes the month that just ended.
- Manual runs accept `trip_date` (date picker; only year + month are used) and `taxi_type` (`all`, `yellow`, `green`).
- Files are stored as `b2://zoomcamp/<taxi_type>_tripdata_<YYYY-MM>.parquet`.
- Reloading the same file deletes its previous rows first, so runs are idempotent.
- The delete + insert runs in one transaction: a failed reload keeps the previous rows.
- Source columns outside the known schema are added to the table automatically; known
  columns missing from a file are stored as `NULL`.
- After the insert the month is validated (row count, duplicate ids, null timestamps,
  pickup month) and the run fails if a hard invariant breaks.
- Every load is recorded in `zoomcamp.pipeline_runs` (run id, file, sha256, rows, warnings)
  and summarised in a Prefect artifact keyed `nyc-taxi-<YYYY-MM>`.
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
