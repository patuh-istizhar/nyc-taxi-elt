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
  warehouse.py   MotherDuck attach + idempotent load
  nyc_taxi.py    main flow (plan -> B2 -> MotherDuck)
  smoke.py       B2 + MotherDuck connectivity check
tests/           month/parameter resolution tests
prefect.yaml     deployments (monthly + smoke)
```

## Flow behaviour

- Scheduled on the 15th: processes the month that just ended.
- Manual runs accept `trip_year` + `trip_month` (1-12) and `taxi_type` (`all`, `yellow`, `green`).
- Files are stored as `b2://zoomcamp/<taxi_type>_tripdata_<YYYY-MM>.parquet`.
- Reloading the same file deletes its previous rows first, so runs are idempotent.
- Row id = `MD5(VendorID, pickup, dropoff, PULocationID, DOLocationID)`.

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
uv run python -c "from flows.nyc_taxi import nyc_taxi; nyc_taxi(taxi_type='green', trip_year=2019, trip_month=1)"

# tests
uv run pytest -q
uvx ruff check flows tests

# deploy
uv run prefect deploy --all
```
