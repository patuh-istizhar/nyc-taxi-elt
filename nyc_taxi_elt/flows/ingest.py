"""Prefect flow: NYC TLC trip data  ->  S3  ->  warehouse.

Examples:
    python -m nyc_taxi_elt.flows.ingest --taxi-type green --month 2024-01
    python -m nyc_taxi_elt.flows.ingest --from 2023-01 --to 2023-12
    python -m nyc_taxi_elt.flows.ingest --month 2024-01 --force
    python -m nyc_taxi_elt.flows.ingest            # the last 3 finished months
"""

import argparse
import tempfile
from datetime import date
from pathlib import Path

from prefect import flow, get_run_logger, task

from nyc_taxi_elt import s3, warehouse
from nyc_taxi_elt.config import Settings, get_settings

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ["yellow", "green"]


# --- months -----------------------------------------------------------------


def parse_month(value: str) -> date:
    """'2024-01' -> date(2024, 1, 1)"""
    try:
        year, month = value.strip()[:7].split("-")
        return date(int(year), int(month), 1)
    except ValueError:
        raise ValueError(f"Bulan harus berformat YYYY-MM, dapat: {value!r}.") from None


def add_months(month: date, n: int) -> date:
    index = month.year * 12 + month.month - 1 + n
    return date(index // 12, index % 12 + 1, 1)


def month_range(start: date, end: date) -> list[date]:
    """All months from start to end, both included."""
    if end < start:
        raise ValueError("Bulan akhir tidak boleh sebelum bulan awal.")
    months = [start]
    while months[-1] < end:
        months.append(add_months(months[-1], 1))
    return months


def months_to_load(start_month: str | None, end_month: str | None) -> list[date]:
    this_month = date.today().replace(day=1)
    if start_month is None:
        # TLC publishes data weeks after a month ends, so check the last 3 finished months.
        return month_range(add_months(this_month, -3), add_months(this_month, -1))
    start = parse_month(start_month)
    end = parse_month(end_month) if end_month else start
    if end >= this_month:
        raise ValueError(f"{end:%Y-%m} belum selesai, datanya belum ada.")
    return month_range(start, end)


# --- tasks --------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=10)
def upload_to_s3(settings: Settings, filename: str, force: bool) -> bool:
    """Copy the file from the TLC website to S3. Returns False if TLC does not have it yet."""
    client = s3.get_client(settings)
    if not force and s3.object_exists(client, settings.s3_bucket, filename):
        get_run_logger().info("%s sudah ada di S3, unduh dilewati.", filename)
        return True
    found = s3.upload_from_url(client, f"{SOURCE_URL}/{filename}", settings.s3_bucket, filename)
    if found:
        get_run_logger().info("Uploaded s3://%s/%s", settings.s3_bucket, filename)
    return found


@task(retries=1, retry_delay_seconds=10)
def load_to_warehouse(settings: Settings, filename: str, taxi_type: str, month: date) -> int:
    """Download the file from S3 to a temp folder and load it into the warehouse."""
    client = s3.get_client(settings)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / filename
        s3.download(client, settings.s3_bucket, filename, path)
        with warehouse.connect(settings) as conn:
            rows = warehouse.load_parquet(conn, path, taxi_type, f"{month:%Y-%m}")
    get_run_logger().info("Loaded %s rows from %s into %s_tripdata", rows, filename, taxi_type)
    return rows


# --- flow ---------------------------------------------------------------------


@flow(name="nyc-taxi-ingest")
def ingest(
    taxi_type: str = "all",
    start_month: str | None = None,
    end_month: str | None = None,
    force: bool = False,
) -> None:
    """Load NYC taxi trips into the warehouse.

    Args:
        taxi_type: "yellow", "green" or "all".
        start_month: First month (YYYY-MM). Empty = the last 3 finished months.
        end_month: Last month (YYYY-MM) for a backfill. Empty = same as start_month.
        force: Load again even if the file was already loaded.
    """
    logger = get_run_logger()
    settings = get_settings()
    taxi_types = TAXI_TYPES if taxi_type == "all" else [taxi_type]
    months = months_to_load(start_month, end_month)
    logger.info("ENV=%s, months %s..%s", settings.env, f"{months[0]:%Y-%m}", f"{months[-1]:%Y-%m}")

    s3.ensure_bucket(s3.get_client(settings), settings.s3_bucket, create=settings.is_dev)

    for month in months:
        for taxi in taxi_types:
            filename = f"{taxi}_tripdata_{month:%Y-%m}.parquet"

            with warehouse.connect(settings) as conn:
                if not force and warehouse.is_loaded(conn, filename):
                    logger.info("%s sudah dimuat, dilewati.", filename)
                    continue

            if not upload_to_s3(settings, filename, force):
                logger.warning("%s belum tersedia di TLC, dilewati.", filename)
                continue

            # One file at a time: a local DuckDB file allows only one writer.
            load_to_warehouse(settings, filename, taxi, month)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load NYC taxi trips into the warehouse.")
    parser.add_argument("--taxi-type", choices=["all", *TAXI_TYPES], default="all")
    parser.add_argument("--month", help="one month, YYYY-MM")
    parser.add_argument("--from", dest="start_month", help="first month of a backfill, YYYY-MM")
    parser.add_argument("--to", dest="end_month", help="last month of a backfill, YYYY-MM")
    parser.add_argument("--force", action="store_true", help="load again even if already loaded")
    args = parser.parse_args()

    ingest(
        taxi_type=args.taxi_type,
        start_month=args.month or args.start_month,
        end_month=args.month or args.end_month,
        force=args.force,
    )


if __name__ == "__main__":
    main()
