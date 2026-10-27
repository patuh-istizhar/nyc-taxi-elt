"""Tests for the schema-aware warehouse loader (local DuckDB file)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from nyc_taxi_elt.config import Settings
from nyc_taxi_elt.warehouse import loader, views
from nyc_taxi_elt.warehouse.connection import connect_warehouse

TRIPS = [
    ("1", "2019-01-02 08:00:00", "2019-01-02 08:10:00", "100", "200", 12.5),
    ("2", "2019-01-03 09:00:00", "2019-01-03 09:05:00", "101", "201", 8.0),
    ("2", "2019-01-04 10:00:00", "2019-01-04 10:30:00", "102", "202", 21.0),
]

YELLOW_COLUMNS = [
    "VendorID VARCHAR",
    "tpep_pickup_datetime TIMESTAMP",
    "tpep_dropoff_datetime TIMESTAMP",
    "passenger_count BIGINT",
    "trip_distance DOUBLE",
    "RatecodeID VARCHAR",
    "store_and_fwd_flag VARCHAR",
    "PULocationID VARCHAR",
    "DOLocationID VARCHAR",
    "payment_type BIGINT",
    "fare_amount DOUBLE",
    "extra DOUBLE",
    "mta_tax DOUBLE",
    "tip_amount DOUBLE",
    "tolls_amount DOUBLE",
    "improvement_surcharge DOUBLE",
    "total_amount DOUBLE",
    "congestion_surcharge DOUBLE",
]


def write_yellow(
    path: Path,
    *,
    trips: list[tuple[str, str, str, str, str, float]] | None = None,
    omit: set[str] | None = None,
    extra: dict[str, str] | None = None,
) -> Path:
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TABLE t ({', '.join(YELLOW_COLUMNS)});")
        for vendor, pickup, dropoff, pu, do, total in trips or TRIPS:
            conn.execute(
                """
                INSERT INTO t VALUES (
                    ?, ?, ?, 1, 1.5, '1', 'N', ?, ?, 1,
                    10.0, 0.5, 0.5, 1.0, 0.0, 0.3, ?, 0.0
                );
                """,
                [vendor, pickup, dropoff, pu, do, total],
            )
        for name, data_type in (extra or {}).items():
            conn.execute(f'ALTER TABLE t ADD COLUMN "{name}" {data_type};')
            conn.execute(f'UPDATE t SET "{name}" = 1.75 WHERE "{name}" IS NULL;')

        select = "*"
        if omit:
            excluded = ", ".join(f'"{name}"' for name in sorted(omit))
            select = f"* EXCLUDE ({excluded})"
        conn.execute(f"COPY (SELECT {select} FROM t) TO '{path.as_posix()}' (FORMAT PARQUET);")
    finally:
        conn.close()
    return path


def load(
    settings: Settings,
    parquet_path: Path,
    *,
    filename: str = "yellow_tripdata_2019-01.parquet",
    trip_month: str = "2019-01",
) -> loader.LoadResult:
    return loader.load_parquet(
        parquet_path,
        filename=filename,
        taxi_type="yellow",
        trip_month=trip_month,
        source_uri=f"https://example.test/{filename}",
        size_bytes=parquet_path.stat().st_size,
        sha256="deadbeef",
        settings=settings,
        flow_run_id="run-1",
        flow_run_name="test-run",
    )


def table_rows(settings: Settings, filename: str) -> int:
    with connect_warehouse(settings) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM yellow_tripdata WHERE filename = ?",
            [filename],
        ).fetchone()[0]


def test_reload_is_idempotent_and_audited(tmp_path: Path, settings: Settings) -> None:
    parquet = write_yellow(tmp_path / "jan.parquet")

    first = load(settings, parquet)
    second = load(settings, parquet)

    assert first["rows_in_file"] == 3
    assert first["rows_before"] == 0
    assert first["rows_after"] == 3
    assert second["rows_before"] == 3
    assert second["rows_after"] == 3
    assert table_rows(settings, "yellow_tripdata_2019-01.parquet") == 3

    with connect_warehouse(settings) as conn:
        audited = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
        sha = conn.execute("SELECT DISTINCT sha256 FROM pipeline_runs").fetchone()[0]
    assert audited == 2
    assert sha == "deadbeef"


def test_new_source_column_is_added(tmp_path: Path, settings: Settings) -> None:
    base = write_yellow(tmp_path / "base.parquet")
    load(settings, base)

    extended = write_yellow(
        tmp_path / "extended.parquet",
        extra={"cbd_congestion_fee": "DOUBLE"},
    )
    result = load(settings, extended)

    assert result["new_columns"] == ["cbd_congestion_fee"]

    with connect_warehouse(settings) as conn:
        fees = conn.execute("SELECT SUM(cbd_congestion_fee) FROM yellow_tripdata").fetchone()[0]
        nulls = conn.execute(
            "SELECT COUNT(*) FROM yellow_tripdata WHERE cbd_congestion_fee IS NULL"
        ).fetchone()[0]
    assert fees == pytest.approx(3 * 1.75)
    assert nulls == 0


def test_missing_known_column_is_stored_as_null(tmp_path: Path, settings: Settings) -> None:
    parquet = write_yellow(
        tmp_path / "no_surcharge.parquet",
        omit={"congestion_surcharge"},
    )
    result = load(settings, parquet)

    assert "congestion_surcharge" in result["missing_columns"]

    with connect_warehouse(settings) as conn:
        nulls = conn.execute(
            "SELECT COUNT(*) FROM yellow_tripdata WHERE congestion_surcharge IS NULL"
        ).fetchone()[0]
    assert nulls == 3


def test_missing_required_column_fails(tmp_path: Path, settings: Settings) -> None:
    parquet = write_yellow(tmp_path / "no_vendor.parquet", omit={"VendorID"})

    with pytest.raises(ValueError, match="kolom wajib"):
        load(settings, parquet)

    assert table_rows(settings, "yellow_tripdata_2019-01.parquet") == 0


def test_failed_validation_rolls_back(tmp_path: Path, settings: Settings) -> None:
    good = write_yellow(tmp_path / "good.parquet")
    load(settings, good)

    bad = write_yellow(
        tmp_path / "bad_month.parquet",
        trips=[
            ("1", "2018-12-31 23:00:00", "2018-12-31 23:10:00", "100", "200", 5.0),
            ("1", "2018-12-30 23:00:00", "2018-12-30 23:10:00", "100", "200", 5.0),
            ("1", "2019-01-02 08:00:00", "2019-01-02 08:10:00", "101", "201", 7.0),
            ("1", "2019-01-03 08:00:00", "2019-01-03 08:10:00", "102", "202", 8.0),
            ("1", "2019-01-04 08:00:00", "2019-01-04 08:10:00", "103", "203", 9.0),
        ],
    )

    with pytest.raises(ValueError, match="di luar 2019-01"):
        load(settings, bad)

    # Previous rows for this filename must survive the failed reload.
    assert table_rows(settings, "yellow_tripdata_2019-01.parquet") == 3


def test_out_of_month_within_threshold_only_warns(tmp_path: Path, settings: Settings) -> None:
    many = [
        (
            "1",
            f"2019-01-{index % 28 + 1:02d} {index % 24:02d}:{index % 60:02d}:00",
            f"2019-01-{index % 28 + 1:02d} {index % 24:02d}:{index % 60:02d}:59",
            "100",
            str(1000 + index),
            5.0,
        )
        for index in range(199)
    ]
    parquet = write_yellow(
        tmp_path / "one_late.parquet",
        trips=many + [("1", "2018-12-31 23:00:00", "2018-12-31 23:10:00", "900", "901", 5.0)],
    )
    result = load(settings, parquet)

    assert result["rows_in_file"] == 200
    assert any("di luar 2019-01" in warning for warning in result["warnings"])


def test_monitoring_views(tmp_path: Path, settings: Settings) -> None:
    parquet = write_yellow(tmp_path / "obs.parquet")
    load(settings, parquet)

    views.refresh_monitoring_views(settings)

    with connect_warehouse(settings) as conn:
        health = conn.execute(
            """
            SELECT taxi_type, trip_month, rows_loaded, rows_in_table, load_count,
                   months_behind
            FROM pipeline_health
            """
        ).fetchall()
        months = conn.execute(
            "SELECT taxi_type, filename, rows, suspicious_rows FROM trip_months"
        ).fetchall()

    assert health[0][:5] == ("yellow", "2019-01", 3, 3, 1)
    assert health[0][5] >= 50
    assert months == [("yellow", "yellow_tripdata_2019-01.parquet", 3, 0)]


def test_loaded_files_reads_the_audit_table(tmp_path: Path, settings: Settings) -> None:
    assert loader.loaded_files(settings, ["2019-01"]) == {}

    parquet = write_yellow(tmp_path / "audit.parquet")
    load(settings, parquet)

    assert loader.loaded_files(settings, ["2019-01"]) == {
        "2019-01": {"yellow_tripdata_2019-01.parquet"}
    }
    assert loader.loaded_files(settings, ["2026-09"]) == {}
    assert loader.loaded_files(settings, []) == {}


def test_impossible_timestamp_only_warns(tmp_path: Path, settings: Settings) -> None:
    parquet = write_yellow(
        tmp_path / "future.parquet",
        trips=TRIPS + [("1", "2019-01-05 08:00:00", "2088-01-24 07:28:25", "500", "501", 9.0)],
    )
    result = load(settings, parquet)

    assert result["rows_in_file"] == 4
    assert any("timestamp jauh di luar" in warning for warning in result["warnings"])

    views.refresh_monitoring_views(settings)
    with connect_warehouse(settings) as conn:
        suspicious = conn.execute("SELECT suspicious_rows FROM trip_months").fetchone()[0]
    assert suspicious == 1
