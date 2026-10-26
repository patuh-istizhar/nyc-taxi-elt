"""NYC Taxi ELT: stream TLC parquet to B2, then load MotherDuck.

Scheduled runs fire on the 15th and process the month that just ended. Set
``trip_date`` to override the month for manual runs.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal, TypedDict

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
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
from flows.observability import refresh_observability_views
from flows.warehouse import LoadResult, load_parquet


class TripFile(TypedDict):
    filename: str
    taxi_type: str
    year: str
    month: str
    force_download: bool
    source_uri: str


TaxiType = Literal["all", "yellow", "green"]


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _run_reference_date() -> date:
    scheduled = getattr(flow_run, "scheduled_start_time", None)
    if scheduled is not None:
        return scheduled.date()
    return datetime.now(UTC).date()


def _resolve_trip_month(trip_date: date | str | None) -> date:
    if trip_date is None or (isinstance(trip_date, str) and not trip_date.strip()):
        return _month_start(_month_start(_run_reference_date()) - timedelta(days=1))

    if isinstance(trip_date, datetime):
        trip_date = trip_date.date()
    elif isinstance(trip_date, str):
        raw = trip_date.strip()
        try:
            trip_date = date.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"trip_date tidak valid: {raw!r}. Gunakan format YYYY-MM-DD.") from exc
    elif not isinstance(trip_date, date):
        raise ValueError(f"trip_date tidak valid: {trip_date!r}.")

    month = _month_start(trip_date)

    today = date.today()
    if (month.year, month.month) > (today.year, today.month):
        raise ValueError(f"Data untuk {month:%Y-%m} belum tersedia.")

    return month


def _flow_run_name() -> str:
    try:
        parameters = flow_run.parameters or {}
    except Exception:
        return "nyc-taxi"
    taxi_type = str(parameters.get("taxi_type") or DEFAULT_TAXI_TYPE)
    try:
        month = _resolve_trip_month(parameters.get("trip_date"))
    except ValueError:
        return f"nyc-taxi-{taxi_type}"
    return f"nyc-taxi-{taxi_type}-{month:%Y-%m}"


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
    source_base = os.environ.get("NYC_TLC_SOURCE_BASE", DEFAULT_SOURCE_BASE).rstrip("/")
    trips: list[TripFile] = [
        {
            "filename": f"{value}_tripdata_{month:%Y-%m}.parquet",
            "taxi_type": value,
            "year": f"{month.year:04d}",
            "month": f"{month.month:02d}",
            "force_download": bool(force_download),
            "source_uri": f"{source_base}/{value}_tripdata_{month:%Y-%m}.parquet",
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

    source_uri = trip["source_uri"]

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


def _file_digest(path: Path) -> tuple[int, str]:
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
            size += len(chunk)
    return size, hasher.hexdigest()


@task(retries=2, retry_delay_seconds=60, timeout_seconds=1800)
def load_to_motherduck(trip: TripFile) -> LoadResult:
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
        size_bytes, sha256 = _file_digest(parquet_path)

        result = load_parquet(
            parquet_path,
            filename=trip["filename"],
            taxi_type=trip["taxi_type"],
            trip_month=f"{trip['year']}-{trip['month']}",
            source_uri=trip["source_uri"],
            size_bytes=size_bytes,
            sha256=sha256,
            settings=settings,
            flow_run_id=str(flow_run.id),
            flow_run_name=flow_run.name,
        )

    logger.info(
        "Loaded %s rows from %s into %s.%s_tripdata (%s -> %s rows)",
        result["rows_in_file"],
        trip["filename"],
        settings.motherduck_schema,
        trip["taxi_type"],
        result["rows_before"],
        result["rows_after"],
    )
    if result["new_columns"]:
        logger.info("New source columns added: %s", ", ".join(result["new_columns"]))
    if result["missing_columns"]:
        logger.info(
            "Columns absent from this file (stored as NULL): %s",
            ", ".join(result["missing_columns"]),
        )
    for warning in result["warnings"]:
        logger.warning("%s: %s", trip["filename"], warning)
    return result


@task
def publish_report(month: date, results: list[LoadResult]) -> None:
    logger = get_run_logger()
    total = sum(result["rows_in_file"] for result in results)
    lines = [
        f"## NYC Taxi load - {month:%Y-%m}",
        "",
        "| Taxi | File | Rows | In table | Size | New columns | Warnings |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for result in results:
        lines.append(
            f"| {result['taxi_type']} | `{result['filename']}` "
            f"| {result['rows_in_file']:,} | {result['rows_after']:,} "
            f"| {result['size_bytes'] / 1e6:.1f} MB "
            f"| {', '.join(result['new_columns']) or '-'} "
            f"| {'; '.join(result['warnings']) or '-'} |"
        )
    lines.extend(
        [
            "",
            f"**Rows loaded:** {total:,}",
            f"**Finished:** {datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC",
            "",
            "sha256: "
            + ", ".join(f"`{result['sha256'][:16]}` ({result['filename']})" for result in results),
        ]
    )
    key = f"nyc-taxi-{month:%Y-%m}"
    create_markdown_artifact(
        key=key,
        markdown="\n".join(lines),
        description=f"Load summary for {month:%Y-%m}",
    )
    logger.info("Published artifact %s", key)


@task
def refresh_views() -> None:
    logger = get_run_logger()
    settings = get_settings()
    refresh_observability_views(settings)
    logger.info(
        "Refreshed %s.pipeline_health and %s.trip_months",
        settings.motherduck_schema,
        settings.motherduck_schema,
    )


@flow(
    name="nyc-taxi",
    description="Monthly NYC TLC ELT: stream HTTP -> B2 -> MotherDuck.",
    log_prints=False,
    flow_run_name=_flow_run_name,
)
def nyc_taxi(
    taxi_type: TaxiType = DEFAULT_TAXI_TYPE,
    trip_date: date | None = None,
    force_download: bool = False,
) -> None:
    """Monthly NYC TLC ELT: stream HTTP -> B2 -> MotherDuck.

    Args:
        taxi_type: all = yellow + green sekaligus, atau pilih salah satu jenis.
        trip_date: Tanggal di bulan data; hari diabaikan (yang dipakai tahun + bulan).
            Kosongkan untuk bulan lalu.
        force_download: Unduh ulang dari TLC walaupun file sudah ada di B2.
    """
    month = _resolve_trip_month(trip_date)
    trips = plan(taxi_type, month, force_download)

    uploaded = stream_to_b2.map(trips)
    results = [load_to_motherduck(trip) for trip in uploaded]
    publish_report(month, results)
    refresh_views()


if __name__ == "__main__":
    nyc_taxi()
