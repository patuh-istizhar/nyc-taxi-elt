"""Prefect ELT flow: NYC TLC parquet -> S3 -> warehouse.

* No month given: load every incomplete month in the lookback window
  (what the weekly deployment does).
* ``start_month`` [+ ``end_month``]: load that month, or every month in the range
  (backfill). Months that are already loaded are skipped unless ``force``.
"""

from __future__ import annotations

import argparse
import hashlib
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run

from nyc_taxi_elt.config import Settings, get_settings
from nyc_taxi_elt.months import add_months, month_range, month_start, months_between, parse_month
from nyc_taxi_elt.s3 import (
    SourceUnavailableError,
    build_client,
    download_object,
    ensure_bucket,
    object_size,
    upload_from_http,
)
from nyc_taxi_elt.tlc import (
    ALL_TAXI_TYPES,
    TAXI_TYPES,
    TaxiType,
    TripFile,
    file_name,
    resolve_taxi_types,
    trip_files,
)
from nyc_taxi_elt.warehouse.loader import LoadResult, load_parquet, loaded_files
from nyc_taxi_elt.warehouse.views import refresh_monitoring_views

LOOKBACK_MONTHS = 6
# A month is allowed to be this late before the run fails. TLC's observed lag is
# under three months, so a newer gap is "still coming", not an incident.
GRACE_MONTHS = 3


# ---------------------------------------------------------------------------
# Planning (pure functions)
# ---------------------------------------------------------------------------


def _today() -> date:
    return datetime.now(UTC).date()


def _run_reference_date() -> date:
    scheduled = getattr(flow_run, "scheduled_start_time", None)
    if scheduled is not None:
        return scheduled.date()
    return _today()


def resolve_months(
    start_month: date | str | None,
    end_month: date | str | None,
    *,
    today: date | None = None,
) -> list[date]:
    """Explicit months to load, oldest first; empty when none were requested."""
    if start_month is None:
        if end_month is not None:
            raise ValueError("end_month butuh start_month.")
        return []
    start = parse_month(start_month)
    end = parse_month(end_month) if end_month is not None else start
    months = month_range(start, end)
    if end > month_start(today or _today()):
        raise ValueError(f"Data untuk {end:%Y-%m} belum tersedia.")
    return months


def _expected_files(month: date, taxi_types: list[str]) -> set[str]:
    return {file_name(taxi_type, month) for taxi_type in taxi_types}


def _lookback_window(reference: date) -> list[date]:
    """The last LOOKBACK_MONTHS complete months, newest first."""
    newest = add_months(month_start(reference), -1)
    return [add_months(newest, -offset) for offset in range(LOOKBACK_MONTHS)]


def _incomplete_months(
    reference: date, loaded: dict[str, set[str]], taxi_types: list[str]
) -> list[date]:
    """Incomplete months in the lookback window, newest first."""
    return [
        month
        for month in _lookback_window(reference)
        if not _expected_files(month, taxi_types) <= loaded.get(f"{month:%Y-%m}", set())
    ]


def _stale_months(months: list[date], reference: date) -> list[date]:
    return [month for month in months if months_between(month, reference) > GRACE_MONTHS]


def _flow_run_name() -> str:
    try:
        parameters = flow_run.parameters or {}
    except Exception:
        return "nyc-taxi"
    name = f"nyc-taxi-{parameters.get('taxi_type') or ALL_TAXI_TYPES}"
    try:
        months = resolve_months(parameters.get("start_month"), parameters.get("end_month"))
    except ValueError:
        return name
    if not months:
        return name
    if len(months) == 1:
        return f"{name}-{months[0]:%Y-%m}"
    return f"{name}-{months[0]:%Y-%m}-to-{months[-1]:%Y-%m}"


def _warehouse_label(settings: Settings) -> str:
    return f"{settings.warehouse_database} (schema {settings.warehouse_schema})"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def select_months(settings: Settings, taxi_types: list[str]) -> list[date]:
    """Incomplete months in the lookback window, newest first."""
    logger = get_run_logger()
    reference = _run_reference_date()
    window = [f"{month:%Y-%m}" for month in _lookback_window(reference)]
    incomplete = _incomplete_months(reference, loaded_files(settings, window), taxi_types)
    if incomplete:
        logger.info("Bulan belum lengkap: %s", ", ".join(f"{month:%Y-%m}" for month in incomplete))
    return incomplete


@task
def pending_files(
    settings: Settings, month: date, trips: list[TripFile], force: bool
) -> list[TripFile]:
    if force:
        return trips
    loaded = loaded_files(settings, [f"{month:%Y-%m}"]).get(f"{month:%Y-%m}", set())
    skipped = [trip["filename"] for trip in trips if trip["filename"] in loaded]
    if skipped:
        get_run_logger().info("Sudah termuat, dilewati: %s", ", ".join(skipped))
    return [trip for trip in trips if trip["filename"] not in loaded]


@task(retries=2, retry_delay_seconds=10, timeout_seconds=120)
def ensure_s3_bucket(settings: Settings) -> None:
    """Fail if the bucket is missing; in dev, create it so a fresh local S3 just works."""
    ensure_bucket(build_client(settings), settings.s3_bucket, create=settings.is_dev)


def _is_unavailable(state: Any) -> bool:
    return isinstance(state.result(raise_on_failure=False), SourceUnavailableError)


def should_retry_upload(task: Any, task_run: Any, state: Any) -> bool:
    """Skip retries when TLC has not published the file yet."""
    return not _is_unavailable(state)


@task(
    retries=3,
    retry_delay_seconds=60,
    retry_condition_fn=should_retry_upload,
    timeout_seconds=1800,
)
def upload_to_s3(settings: Settings, trip: TripFile, force: bool) -> TripFile:
    logger = get_run_logger()
    client = build_client(settings)
    key = trip["filename"]

    if not force:
        existing = object_size(client, settings.s3_bucket, key)
        if existing:
            logger.info(
                "%s sudah ada (%.1f MB), unduh dilewati.", settings.s3_uri(key), existing / 1e6
            )
            return trip

    size_bytes, sha256 = upload_from_http(
        trip["source_uri"],
        client,
        settings.s3_bucket,
        key,
        metadata={
            "source-uri": trip["source_uri"],
            "taxi-type": trip["taxi_type"],
            "trip-month": trip["trip_month"],
        },
    )
    logger.info("Uploaded %s (%.1f MB, sha256=%s)", settings.s3_uri(key), size_bytes / 1e6, sha256)
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
def load_into_warehouse(settings: Settings, trip: TripFile) -> LoadResult:
    logger = get_run_logger()
    client = build_client(settings)

    with tempfile.TemporaryDirectory(prefix="nyc_taxi_") as tmp_dir:
        parquet_path = Path(tmp_dir) / trip["filename"]
        download_object(client, settings.s3_bucket, trip["filename"], parquet_path)
        size_bytes, sha256 = _file_digest(parquet_path)
        result = load_parquet(
            parquet_path,
            filename=trip["filename"],
            taxi_type=trip["taxi_type"],
            trip_month=trip["trip_month"],
            source_uri=trip["source_uri"],
            size_bytes=size_bytes,
            sha256=sha256,
            settings=settings,
            flow_run_id=str(flow_run.id),
            flow_run_name=flow_run.name,
        )

    logger.info(
        "Loaded %s rows from %s into %s_tripdata (%s -> %s rows) [%s]",
        result["rows_in_file"],
        trip["filename"],
        trip["taxi_type"],
        result["rows_before"],
        result["rows_after"],
        _warehouse_label(settings),
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
        key=key, markdown="\n".join(lines), description=f"Load summary for {month:%Y-%m}"
    )
    get_run_logger().info("Published artifact %s", key)


@task
def refresh_views(settings: Settings) -> None:
    refresh_monitoring_views(settings)
    get_run_logger().info("Refreshed pipeline_health and trip_months")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="nyc-taxi-ingest",
    description="NYC TLC trip data: HTTP -> S3 -> warehouse.",
    log_prints=False,
    flow_run_name=_flow_run_name,
)
def ingest(
    taxi_type: TaxiType = ALL_TAXI_TYPES,
    start_month: str | None = None,
    end_month: str | None = None,
    force: bool = False,
) -> None:
    """Load NYC TLC trip data into the warehouse.

    Args:
        taxi_type: all = yellow + green, atau pilih salah satu.
        start_month: Bulan pertama (YYYY-MM). Kosongkan agar flow memuat semua bulan
            yang belum lengkap dalam 6 bulan terakhir.
        end_month: Bulan terakhir untuk backfill (YYYY-MM); default = start_month.
        force: Unduh dan muat ulang walaupun sudah ada.
    """
    logger = get_run_logger()
    # Resolve once: tasks receive Settings so Secret blocks are not re-fetched.
    settings = get_settings()
    logger.info(
        "ENV=%s | S3 %s | warehouse %s",
        settings.env,
        settings.s3_uri(),
        _warehouse_label(settings),
    )

    taxi_types = resolve_taxi_types(taxi_type)
    explicit = start_month is not None or end_month is not None
    months = (
        resolve_months(start_month, end_month) if explicit else select_months(settings, taxi_types)
    )
    if not months:
        logger.info("Tidak ada bulan yang perlu dimuat.")
        return
    if len(months) > 1:
        logger.info(
            "Memproses %d bulan: %s .. %s", len(months), f"{months[0]:%Y-%m}", f"{months[-1]:%Y-%m}"
        )

    ensure_s3_bucket(settings)

    reference = _run_reference_date()
    missing_files: list[str] = []
    waiting_months: list[date] = []
    loaded_any = False
    for month in months:
        pending = pending_files(settings, month, trip_files(taxi_types, month), force)
        if not pending:
            continue

        states = upload_to_s3.map(settings, pending, force, return_state=True)
        failed = [
            (trip, state)
            for trip, state in zip(pending, states, strict=True)
            if not state.is_completed()
        ]
        errors = [
            f"{trip['filename']}: {state.result(raise_on_failure=False)}"
            for trip, state in failed
            if not _is_unavailable(state)
        ]
        if errors:
            raise ValueError("Upload ke S3 gagal untuk " + "; ".join(errors))

        unpublished = [trip["filename"] for trip, state in failed if _is_unavailable(state)]
        if unpublished:
            logger.warning("Belum tersedia di TLC, dilewati: %s", ", ".join(unpublished))
            if explicit:
                missing_files.extend(unpublished)
            else:
                waiting_months.append(month)

        uploaded = [state.result() for state in states if state.is_completed()]
        if uploaded:
            # Sequential: a local DuckDB file allows a single writer.
            results = [load_into_warehouse(settings, trip) for trip in uploaded]
            publish_report(month, results)
            loaded_any = True

    if loaded_any:
        refresh_views(settings)

    if missing_files:
        raise ValueError(
            f"{', '.join(missing_files)} belum tersedia di TLC. Jalankan ulang setelah file terbit."
        )
    stale = _stale_months(waiting_months, reference)
    if stale:
        raise ValueError(
            f"Bulan belum terbit melewati batas {GRACE_MONTHS} bulan: "
            + ", ".join(f"{month:%Y-%m}" for month in stale)
        )
    if waiting_months:
        logger.warning(
            "Menunggu terbit (masih dalam batas %d bulan): %s",
            GRACE_MONTHS,
            ", ".join(f"{month:%Y-%m}" for month in waiting_months),
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m nyc_taxi_elt.flows.ingest",
        description="Load NYC TLC trip data into the warehouse.",
    )
    parser.add_argument(
        "--taxi-type", choices=[ALL_TAXI_TYPES, *TAXI_TYPES], default=ALL_TAXI_TYPES
    )
    parser.add_argument("--month", help="Load one month (YYYY-MM).")
    parser.add_argument("--from", dest="start_month", help="First month of a backfill (YYYY-MM).")
    parser.add_argument("--to", dest="end_month", help="Last month of a backfill (YYYY-MM).")
    parser.add_argument("--force", action="store_true", help="Reload even if already loaded.")
    args = parser.parse_args(argv)

    if args.month and (args.start_month or args.end_month):
        parser.error("--month tidak bisa digabung dengan --from/--to.")
    if args.end_month and not args.start_month:
        parser.error("--to butuh --from.")
    start = args.month or args.start_month
    end = args.month or args.end_month
    for value in (start, end):
        if value is not None:
            try:
                parse_month(value)
            except ValueError as exc:
                parser.error(str(exc))

    ingest(taxi_type=args.taxi_type, start_month=start, end_month=end, force=args.force)


if __name__ == "__main__":
    main()
