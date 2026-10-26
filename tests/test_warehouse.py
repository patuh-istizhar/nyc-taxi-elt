"""Tests for the schema-aware MotherDuck loader.

MotherDuck is replaced by a local DuckDB file attached under the same ``md``
alias, so the SQL path (schema discovery, transaction, audit table) is exercised
without touching the cloud.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from flows import observability, warehouse
from flows.config import Settings

SETTINGS = Settings(
    b2_endpoint="",
    b2_region="",
    b2_key_id="",
    b2_app_key="",
    b2_bucket="",
    motherduck_database="nyc_taxi",
    motherduck_token="",
    motherduck_schema="zoomcamp",
)

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


@pytest.fixture()
def local_md(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "motherduck.duckdb"

    def attach(conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
        conn.execute(f"ATTACH '{db_path.as_posix()}' AS md;")

    monkeypatch.setattr(warehouse, "attach_motherduck", attach)
    monkeypatch.setattr(observability, "attach_motherduck", attach)
    return db_path


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
    parquet_path: Path,
    *,
    filename: str = "yellow_tripdata_2019-01.parquet",
    trip_month: str = "2019-01",
) -> warehouse.LoadResult:
    return warehouse.load_parquet(
        parquet_path,
        filename=filename,
        taxi_type="yellow",
        trip_month=trip_month,
        source_uri=f"https://example.test/{filename}",
        size_bytes=parquet_path.stat().st_size,
        sha256="deadbeef",
        settings=SETTINGS,
        flow_run_id="run-1",
        flow_run_name="test-run",
    )


def table_rows(db_path: Path, filename: str) -> int:
    conn = duckdb.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM zoomcamp.yellow_tripdata WHERE filename = ?",
            [filename],
        ).fetchone()[0]
    finally:
        conn.close()


def test_reload_is_idempotent_and_audited(tmp_path: Path, local_md: Path) -> None:
    parquet = write_yellow(tmp_path / "jan.parquet")

    first = load(parquet)
    second = load(parquet)

    assert first["rows_in_file"] == 3
    assert first["rows_before"] == 0
    assert first["rows_after"] == 3
    assert second["rows_before"] == 3
    assert second["rows_after"] == 3
    assert table_rows(local_md, "yellow_tripdata_2019-01.parquet") == 3

    conn = duckdb.connect(str(local_md))
    try:
        audited = conn.execute("SELECT COUNT(*) FROM zoomcamp.pipeline_runs").fetchone()[0]
        sha = conn.execute("SELECT DISTINCT sha256 FROM zoomcamp.pipeline_runs").fetchone()[0]
    finally:
        conn.close()
    assert audited == 2
    assert sha == "deadbeef"


def test_new_source_column_is_added(tmp_path: Path, local_md: Path) -> None:
    base = write_yellow(tmp_path / "base.parquet")
    load(base)

    extended = write_yellow(
        tmp_path / "extended.parquet",
        extra={"cbd_congestion_fee": "DOUBLE"},
    )
    result = load(extended)

    assert result["new_columns"] == ["cbd_congestion_fee"]

    conn = duckdb.connect(str(local_md))
    try:
        fees = conn.execute(
            "SELECT SUM(cbd_congestion_fee) FROM zoomcamp.yellow_tripdata"
        ).fetchone()[0]
        nulls = conn.execute(
            """
            SELECT COUNT(*) FROM zoomcamp.yellow_tripdata
            WHERE cbd_congestion_fee IS NULL
            """
        ).fetchone()[0]
    finally:
        conn.close()
    assert fees == pytest.approx(3 * 1.75)
    assert nulls == 0


def test_missing_known_column_is_stored_as_null(tmp_path: Path, local_md: Path) -> None:
    parquet = write_yellow(
        tmp_path / "no_surcharge.parquet",
        omit={"congestion_surcharge"},
    )
    result = load(parquet)

    assert "congestion_surcharge" in result["missing_columns"]

    conn = duckdb.connect(str(local_md))
    try:
        nulls = conn.execute(
            """
            SELECT COUNT(*) FROM zoomcamp.yellow_tripdata
            WHERE congestion_surcharge IS NULL
            """
        ).fetchone()[0]
    finally:
        conn.close()
    assert nulls == 3


def test_missing_required_column_fails(tmp_path: Path, local_md: Path) -> None:
    parquet = write_yellow(tmp_path / "no_vendor.parquet", omit={"VendorID"})

    with pytest.raises(ValueError, match="kolom wajib"):
        load(parquet)

    assert table_rows(local_md, "yellow_tripdata_2019-01.parquet") == 0


def test_failed_validation_rolls_back(tmp_path: Path, local_md: Path) -> None:
    good = write_yellow(tmp_path / "good.parquet")
    load(good)

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
        load(bad)

    # The previous rows for this filename must survive the failed reload.
    assert table_rows(local_md, "yellow_tripdata_2019-01.parquet") == 3


def test_out_of_month_within_threshold_only_warns(tmp_path: Path, local_md: Path) -> None:
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
    result = load(parquet)

    assert result["rows_in_file"] == 200
    assert any("di luar 2019-01" in warning for warning in result["warnings"])


def test_observability_views(tmp_path: Path, local_md: Path) -> None:
    parquet = write_yellow(tmp_path / "obs.parquet")
    load(parquet)

    observability.refresh_observability_views(SETTINGS)

    conn = duckdb.connect(str(local_md))
    try:
        health = conn.execute(
            """
            SELECT taxi_type, trip_month, rows_loaded, rows_in_table, load_count
            FROM zoomcamp.pipeline_health
            """
        ).fetchall()
        months = conn.execute(
            "SELECT taxi_type, filename, rows FROM zoomcamp.trip_months"
        ).fetchall()
    finally:
        conn.close()

    assert health == [("yellow", "2019-01", 3, 3, 1)]
    assert months == [("yellow", "yellow_tripdata_2019-01.parquet", 3)]


def test_loaded_files_reads_the_audit_table(tmp_path: Path, local_md: Path) -> None:
    # No audit table yet (fresh warehouse).
    assert warehouse.loaded_files(SETTINGS, ["2019-01"]) == {}

    parquet = write_yellow(tmp_path / "audit.parquet")
    load(parquet)

    assert warehouse.loaded_files(SETTINGS, ["2019-01"]) == {
        "2019-01": {"yellow_tripdata_2019-01.parquet"}
    }
    assert warehouse.loaded_files(SETTINGS, ["2026-09"]) == {}
    assert warehouse.loaded_files(SETTINGS, []) == {}
