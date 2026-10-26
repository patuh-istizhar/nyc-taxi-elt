"""NYC Taxi ELT: stream TLC parquet to B2, then load MotherDuck.

Scheduled runs fire on the 15th and process the month that just ended. Set
``trip_date`` to override the month for manual runs.
"""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal, TypedDict

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from flows.b2 import (
    bucket_exists,
    build_client,
    download_object,
    object_size,
    upload_from_http,
)
from flows.config import (
    DEFAULT_SOURCE_BASE,
    DEFAULT_TAXI_TYPE,
    TAXI_TYPES,
    get_settings,
)
from flows.warehouse import load_parquet


class TripFile(TypedDict):
    filename: str
    taxi_type: str
    year: str
    month: str
    force_download: bool


TaxiType = Literal["all", "yellow", "green"]


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _run_reference_date() -> date:
    scheduled = getattr(flow_run, "scheduled_start_time", None)
    if scheduled is not None:
        return scheduled.date()
    return datetime.now(UTC).date()


def _resolve_trip_month(trip_month: str | None) -> date:
    raw = str(trip_month or "").strip()

    if raw:
        try:
            month = datetime.strptime(raw, "%Y-%m").date()
        except ValueError as exc:
            raise ValueError(
                f"trip_month tidak valid: {raw!r}. Gunakan format YYYY-MM (contoh: 2019-01)."
            ) from exc
    else:
        month = _month_start(_month_start(_run_reference_date()) - timedelta(days=1))

    today = date.today()
    if (month.year, month.month) > (today.year, today.month):
        raise ValueError(f"Data untuk {month:%Y-%m} belum tersedia.")

    return month


def _resolve_taxi_types(taxi_type: str | None) -> list[str]:
    value = str(taxi_type or DEFAULT_TAXI_TYPE).strip().lower()
    if value == DEFAULT_TAXI_TYPE:
        return list(TAXI_TYPES)
    if value not in TAXI_TYPES:
        raise ValueError(
            f"taxi_type tidak valid: {value!r}. Pilih salah satu: all, {', '.join(TAXI_TYPES)}."
        )
    return [value]


@task(retries=2, retry_delay_seconds=30, timeout_seconds=600)
def plan(taxi_type: str | None, month: date, force_download: bool) -> list[TripFile]:
    logger = get_run_logger()
    taxi_types = _resolve_taxi_types(taxi_type)
    trips: list[TripFile] = [
        {
            "filename": f"{value}_tripdata_{month:%Y-%m}.parquet",
            "taxi_type": value,
            "year": f"{month.year:04d}",
            "month": f"{month.month:02d}",
            "force_download": bool(force_download),
        }
        for value in taxi_types
    ]
    logger.info(
        "Planned %s for %s: %s",
        ", ".join(taxi_types),
        month.strftime("%Y-%m"),
        ", ".join(trip["filename"] for trip in trips),
    )
    return trips


@task(retries=3, retry_delay_seconds=60, timeout_seconds=1800)
def stream_to_b2(trip: TripFile) -> TripFile:
    logger = get_run_logger()
    settings = get_settings()
    client = build_client(settings)

    if not bucket_exists(client, settings.b2_bucket):
        raise ValueError(
            f"Bucket B2 {settings.b2_bucket!r} tidak ditemukan. "
            "Buat bucket-nya manual: key yang di-scope ke bucket tidak boleh CreateBucket."
        )

    if not trip["force_download"]:
        existing = object_size(client, settings.b2_bucket, trip["filename"])
        if existing is not None and existing > 0:
            logger.info(
                "Skipping download: b2://%s/%s already exists (%.1f MB).",
                settings.b2_bucket,
                trip["filename"],
                existing / 1e6,
            )
            return trip

    source_base = os.environ.get("NYC_TLC_SOURCE_BASE", DEFAULT_SOURCE_BASE).rstrip("/")
    source_uri = f"{source_base}/{trip['filename']}"

    logger.info(
        "Streaming %s to b2://%s/%s",
        source_uri,
        settings.b2_bucket,
        trip["filename"],
    )

    size_bytes, sha256 = upload_from_http(
        source_uri,
        client,
        settings.b2_bucket,
        trip["filename"],
        metadata={
            "source-uri": source_uri,
            "taxi-type": trip["taxi_type"],
            "trip-month": f"{trip['year']}-{trip['month']}",
        },
    )
    logger.info(
        "Uploaded b2://%s/%s (%.1f MB, sha256=%s)",
        settings.b2_bucket,
        trip["filename"],
        size_bytes / 1e6,
        sha256,
    )
    return trip


@task(retries=2, retry_delay_seconds=60, timeout_seconds=1800)
def load_to_motherduck(trip: TripFile) -> None:
    logger = get_run_logger()
    settings = get_settings()
    client = build_client(settings)

    with tempfile.TemporaryDirectory(prefix="nyc_taxi_") as tmp_dir:
        parquet_path = Path(tmp_dir) / trip["filename"]
        logger.info(
            "Downloading b2://%s/%s for the MotherDuck load",
            settings.b2_bucket,
            trip["filename"],
        )
        download_object(client, settings.b2_bucket, trip["filename"], parquet_path)

        incoming, rows_before, rows_after = load_parquet(
            parquet_path,
            filename=trip["filename"],
            taxi_type=trip["taxi_type"],
            settings=settings,
        )

    logger.info(
        "Loaded %s rows from %s into %s.%s_tripdata (%s -> %s rows)",
        incoming,
        trip["filename"],
        settings.motherduck_schema,
        trip["taxi_type"],
        rows_before,
        rows_after,
    )


@flow(
    name="nyc-taxi",
    description="Monthly NYC TLC ELT: stream HTTP -> B2 -> MotherDuck.",
    log_prints=False,
)
def nyc_taxi(
    taxi_type: TaxiType = DEFAULT_TAXI_TYPE,
    trip_month: str | None = None,
    force_download: bool = False,
) -> None:
    """Monthly NYC TLC ELT: stream HTTP -> B2 -> MotherDuck.

    Args:
        taxi_type: all = yellow + green sekaligus, atau pilih salah satu jenis.
        trip_month: Bulan data, format YYYY-MM (contoh 2019-01). Kosongkan untuk bulan lalu.
        force_download: Unduh ulang dari TLC walaupun file sudah ada di B2.
    """
    month = _resolve_trip_month(trip_month)
    trips = plan(taxi_type, month, force_download)

    uploaded = stream_to_b2.map(trips)
    for trip in uploaded:
        load_to_motherduck(trip)


if __name__ == "__main__":
    nyc_taxi()
