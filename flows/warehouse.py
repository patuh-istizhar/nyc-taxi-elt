"""MotherDuck loading for the NYC Taxi flows.

The loader is schema aware: columns that appear in the source parquet but not in
``TRIP_TABLES`` are added to the table automatically, and known columns that are
absent from a file are stored as ``NULL``. Hard invariants (row counts, duplicate
ids, null timestamps) abort the transaction, so a failed reload never leaves the
month half deleted. Every successful load is recorded in ``pipeline_runs``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import TypedDict

import duckdb

from flows.config import Settings

# Files sometimes contain a few trips from the adjacent month; only treat the
# file as corrupt when the share is large.
MAX_OUT_OF_MONTH_RATIO = 0.01

INTERNAL_COLUMNS = {"unique_row_id", "filename"}


class TableConfig(TypedDict):
    pickup_column: str
    dropoff_column: str
    columns: list[tuple[str, str]]


class LoadResult(TypedDict):
    filename: str
    taxi_type: str
    trip_month: str
    source_uri: str
    size_bytes: int
    sha256: str
    rows_in_file: int
    rows_before: int
    rows_after: int
    new_columns: list[str]
    missing_columns: list[str]
    warnings: list[str]
    loaded_at: str


TRIP_TABLES: dict[str, TableConfig] = {
    "yellow": {
        "pickup_column": "tpep_pickup_datetime",
        "dropoff_column": "tpep_dropoff_datetime",
        "columns": [
            ("VendorID", "VARCHAR"),
            ("tpep_pickup_datetime", "TIMESTAMP"),
            ("tpep_dropoff_datetime", "TIMESTAMP"),
            ("passenger_count", "BIGINT"),
            ("trip_distance", "DOUBLE"),
            ("RatecodeID", "VARCHAR"),
            ("store_and_fwd_flag", "VARCHAR"),
            ("PULocationID", "VARCHAR"),
            ("DOLocationID", "VARCHAR"),
            ("payment_type", "BIGINT"),
            ("fare_amount", "DOUBLE"),
            ("extra", "DOUBLE"),
            ("mta_tax", "DOUBLE"),
            ("tip_amount", "DOUBLE"),
            ("tolls_amount", "DOUBLE"),
            ("improvement_surcharge", "DOUBLE"),
            ("total_amount", "DOUBLE"),
            ("congestion_surcharge", "DOUBLE"),
            ("airport_fee", "DOUBLE"),
        ],
    },
    "green": {
        "pickup_column": "lpep_pickup_datetime",
        "dropoff_column": "lpep_dropoff_datetime",
        "columns": [
            ("VendorID", "VARCHAR"),
            ("lpep_pickup_datetime", "TIMESTAMP"),
            ("lpep_dropoff_datetime", "TIMESTAMP"),
            ("store_and_fwd_flag", "VARCHAR"),
            ("RatecodeID", "VARCHAR"),
            ("PULocationID", "VARCHAR"),
            ("DOLocationID", "VARCHAR"),
            ("passenger_count", "BIGINT"),
            ("trip_distance", "DOUBLE"),
            ("fare_amount", "DOUBLE"),
            ("extra", "DOUBLE"),
            ("mta_tax", "DOUBLE"),
            ("tip_amount", "DOUBLE"),
            ("tolls_amount", "DOUBLE"),
            ("ehail_fee", "DOUBLE"),
            ("improvement_surcharge", "DOUBLE"),
            ("total_amount", "DOUBLE"),
            ("payment_type", "BIGINT"),
            ("trip_type", "BIGINT"),
            ("congestion_surcharge", "DOUBLE"),
        ],
    },
}

AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    flow_run_id VARCHAR,
    flow_run_name VARCHAR,
    taxi_type VARCHAR,
    trip_month VARCHAR,
    filename VARCHAR,
    source_uri VARCHAR,
    size_bytes BIGINT,
    sha256 VARCHAR,
    rows_loaded BIGINT,
    rows_in_table BIGINT,
    new_columns VARCHAR,
    missing_columns VARCHAR,
    warnings VARCHAR,
    loaded_at TIMESTAMP
);
"""


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _motherduck_uri(settings: Settings) -> str:
    return f"md:{settings.motherduck_database}"


def attach_motherduck(conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    conn.execute("INSTALL motherduck;")
    conn.execute("LOAD motherduck;")
    conn.execute(f"SET motherduck_token = {_sql_literal(settings.motherduck_token)};")
    conn.execute(f"ATTACH {_sql_literal(_motherduck_uri(settings))} AS md;")


def _shift_month(month: date, offset: int) -> date:
    index = month.year * 12 + month.month - 1 + offset
    return date(index // 12, index % 12 + 1, 1)


def month_bounds(trip_month: str) -> tuple[date, date]:
    year_text, month_text = trip_month.split("-")
    year, month = int(year_text), int(month_text)
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def _table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE database_name = 'md' AND schema_name = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return row is not None


def loaded_files(settings: Settings, trip_months: list[str]) -> dict[str, set[str]]:
    """Filenames with a successful load per month, according to the audit table."""
    if not trip_months:
        return {}

    schema = settings.motherduck_schema
    conn = duckdb.connect()
    try:
        attach_motherduck(conn, settings)
        if not _table_exists(conn, schema, "pipeline_runs"):
            return {}
        placeholders = ", ".join("?" for _ in trip_months)
        rows = conn.execute(
            f"""
            SELECT trip_month, filename
            FROM md.{schema}.pipeline_runs
            WHERE trip_month IN ({placeholders})
            GROUP BY trip_month, filename
            """,
            list(trip_months),
        ).fetchall()
    finally:
        conn.close()

    loaded: dict[str, set[str]] = {}
    for trip_month, filename in rows:
        loaded.setdefault(str(trip_month), set()).add(str(filename))
    return loaded


def _describe_parquet(conn: duckdb.DuckDBPyConnection, parquet_path: Path) -> list[tuple[str, str]]:
    rows = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_sql_literal(str(parquet_path))});"
    ).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    rows = conn.execute(f"DESCRIBE {table};").fetchall()
    return {str(row[0]).lower() for row in rows}


def _ensure_table_columns(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    table_config: TableConfig,
) -> None:
    existing = _table_columns(conn, table)
    for name, data_type in table_config["columns"]:
        if name.lower() not in existing:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {_sql_ident(name)} {data_type};"
            )


def _validate_load(
    conn: duckdb.DuckDBPyConnection,
    *,
    table: str,
    filename: str,
    table_config: TableConfig,
    trip_month: str,
    rows_in_file: int,
) -> list[str]:
    pickup = _sql_ident(table_config["pickup_column"])
    dropoff = _sql_ident(table_config["dropoff_column"])
    start, end = month_bounds(trip_month)
    # A few rows legitimately sit next to the month boundary; anything further
    # than one month out is treated as an impossible timestamp.
    lower = _shift_month(start, -1)
    upper = _shift_month(end, 1)

    (
        rows,
        distinct_rows,
        null_pickup,
        null_dropoff,
        bad_order,
        out_of_month,
        negative_total,
        impossible_time,
    ) = conn.execute(
        f"""
            SELECT
              COUNT(*),
              COUNT(DISTINCT unique_row_id),
              COUNT(*) FILTER (WHERE {pickup} IS NULL),
              COUNT(*) FILTER (WHERE {dropoff} IS NULL),
              COUNT(*) FILTER (WHERE {dropoff} < {pickup}),
              COUNT(*) FILTER (
                WHERE {pickup} < TIMESTAMP {_sql_literal(str(start))}
                   OR {pickup} >= TIMESTAMP {_sql_literal(str(end))}
              ),
              COUNT(*) FILTER (WHERE total_amount < 0),
              COUNT(*) FILTER (
                WHERE {pickup} < TIMESTAMP {_sql_literal(str(lower))}
                   OR {dropoff} >= TIMESTAMP {_sql_literal(str(upper))}
              )
            FROM {table}
            WHERE filename = {_sql_literal(filename)};
            """
    ).fetchone()

    if rows != rows_in_file:
        raise ValueError(
            f"Validasi gagal untuk {filename}: tabel berisi {rows} baris, "
            f"file menghasilkan {rows_in_file}."
        )
    if distinct_rows != rows:
        raise ValueError(
            f"Validasi gagal untuk {filename}: {rows - distinct_rows} baris duplikat "
            "setelah insert."
        )
    if null_pickup or null_dropoff:
        raise ValueError(
            f"Validasi gagal untuk {filename}: {null_pickup} pickup dan "
            f"{null_dropoff} dropoff bernilai NULL."
        )

    warnings: list[str] = []
    if out_of_month:
        ratio = out_of_month / rows if rows else 0.0
        message = f"{out_of_month} baris ({ratio:.2%}) pickup di luar {trip_month}"
        if ratio > MAX_OUT_OF_MONTH_RATIO:
            raise ValueError(f"Validasi gagal untuk {filename}: {message}.")
        warnings.append(message)
    if bad_order:
        warnings.append(f"{bad_order} baris dropoff sebelum pickup")
    if impossible_time:
        warnings.append(
            f"{impossible_time} baris timestamp jauh di luar {trip_month} "
            f"({lower:%Y-%m}..{upper:%Y-%m})"
        )
    if negative_total:
        warnings.append(f"{negative_total} baris total_amount negatif")
    return warnings


def _record_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    audit_table: str,
    result: LoadResult,
    flow_run_id: str,
    flow_run_name: str,
    loaded_at: datetime,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {audit_table} (
            flow_run_id, flow_run_name, taxi_type, trip_month, filename,
            source_uri, size_bytes, sha256, rows_loaded, rows_in_table,
            new_columns, missing_columns, warnings, loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            flow_run_id,
            flow_run_name,
            result["taxi_type"],
            result["trip_month"],
            result["filename"],
            result["source_uri"],
            result["size_bytes"],
            result["sha256"],
            result["rows_in_file"],
            result["rows_after"],
            ", ".join(result["new_columns"]),
            ", ".join(result["missing_columns"]),
            " | ".join(result["warnings"]),
            loaded_at,
        ],
    )


def load_parquet(
    parquet_path: Path,
    *,
    filename: str,
    taxi_type: str,
    trip_month: str,
    source_uri: str,
    size_bytes: int,
    sha256: str,
    settings: Settings,
    flow_run_id: str = "",
    flow_run_name: str = "",
) -> LoadResult:
    """Load one parquet file into MotherDuck.

    Rows for ``filename`` are replaced inside a transaction, so re-running a
    month is idempotent and a failed insert leaves the previous rows intact.
    Source columns outside ``TRIP_TABLES`` are added to the table on the fly.
    """
    table_config = TRIP_TABLES[taxi_type]
    schema = settings.motherduck_schema
    table = f"md.{schema}.{taxi_type}_tripdata"
    audit_table = f"md.{schema}.pipeline_runs"

    conn = duckdb.connect()
    try:
        attach_motherduck(conn, settings)

        conn.execute(f"CREATE SCHEMA IF NOT EXISTS md.{schema};")
        columns_ddl = ",\n                    ".join(
            f"{_sql_ident(name)} {data_type}" for name, data_type in table_config["columns"]
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                unique_row_id VARCHAR,
                filename VARCHAR,
                {columns_ddl}
            );
            """
        )
        _ensure_table_columns(conn, table, table_config)
        conn.execute(AUDIT_DDL.format(table=audit_table))

        source_columns = _describe_parquet(conn, parquet_path)
        if not source_columns:
            raise ValueError(f"File {filename} tidak punya kolom.")

        source_by_lower = {name.lower(): (name, data_type) for name, data_type in source_columns}
        known_by_lower = {name.lower(): data_type for name, data_type in table_config["columns"]}

        required = [
            "VendorID",
            "PULocationID",
            "DOLocationID",
            table_config["pickup_column"],
            table_config["dropoff_column"],
        ]
        missing_required = [name for name in required if name.lower() not in source_by_lower]
        if missing_required:
            raise ValueError(
                f"File {filename} tidak punya kolom wajib: {', '.join(missing_required)}."
            )

        extra_columns = [
            (name, data_type)
            for name, data_type in source_columns
            if name.lower() not in known_by_lower
        ]
        reserved = [name for name, _ in extra_columns if name.lower() in INTERNAL_COLUMNS]
        if reserved:
            raise ValueError(
                f"File {filename} punya kolom yang bertabrakan dengan kolom internal: "
                f"{', '.join(reserved)}."
            )
        for name, data_type in extra_columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {_sql_ident(name)} {data_type};"
            )

        missing_columns = [
            name for name, _ in table_config["columns"] if name.lower() not in source_by_lower
        ]

        select_exprs: list[str] = []
        for name, data_type in table_config["columns"]:
            source = source_by_lower.get(name.lower())
            if source is None:
                select_exprs.append(f"CAST(NULL AS {data_type}) AS {_sql_ident(name)}")
            else:
                select_exprs.append(
                    f"CAST({_sql_ident(source[0])} AS {data_type}) AS {_sql_ident(name)}"
                )
        for name, data_type in extra_columns:
            select_exprs.append(f"CAST({_sql_ident(name)} AS {data_type}) AS {_sql_ident(name)}")

        payload_columns = (
            ["unique_row_id", "filename"]
            + [name for name, _ in table_config["columns"]]
            + [name for name, _ in extra_columns]
        )
        column_sql = ", ".join(_sql_ident(name) for name in payload_columns)

        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE ext AS
            SELECT * FROM read_parquet({_sql_literal(str(parquet_path))});
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dedup AS
            SELECT
              MD5(CONCAT(
                COALESCE(CAST(VendorID AS VARCHAR), ''),
                COALESCE(CAST({_sql_ident(table_config["pickup_column"])} AS VARCHAR), ''),
                COALESCE(CAST({_sql_ident(table_config["dropoff_column"])} AS VARCHAR), ''),
                COALESCE(CAST(PULocationID AS VARCHAR), ''),
                COALESCE(CAST(DOLocationID AS VARCHAR), '')
              )) AS unique_row_id,
              {_sql_literal(filename)} AS filename,
              {", ".join(select_exprs)}
            FROM ext
            QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_row_id) = 1;
            """
        )

        rows_in_file = int(conn.execute("SELECT COUNT(*) FROM dedup;").fetchone()[0])
        if rows_in_file == 0:
            raise ValueError(f"File {filename} tidak berisi baris data.")

        rows_before = int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
        loaded_at = datetime.now(UTC)

        conn.execute("BEGIN TRANSACTION;")
        try:
            conn.execute(f"DELETE FROM {table} WHERE filename = {_sql_literal(filename)};")
            conn.execute(f"INSERT INTO {table} ({column_sql}) SELECT {column_sql} FROM dedup;")
            rows_after = int(conn.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
            warnings = _validate_load(
                conn,
                table=table,
                filename=filename,
                table_config=table_config,
                trip_month=trip_month,
                rows_in_file=rows_in_file,
            )
            result: LoadResult = {
                "filename": filename,
                "taxi_type": taxi_type,
                "trip_month": trip_month,
                "source_uri": source_uri,
                "size_bytes": size_bytes,
                "sha256": sha256,
                "rows_in_file": rows_in_file,
                "rows_before": rows_before,
                "rows_after": rows_after,
                "new_columns": [name for name, _ in extra_columns],
                "missing_columns": missing_columns,
                "warnings": warnings,
                "loaded_at": loaded_at.isoformat(),
            }
            _record_run(
                conn,
                audit_table=audit_table,
                result=result,
                flow_run_id=flow_run_id,
                flow_run_name=flow_run_name,
                loaded_at=loaded_at.replace(tzinfo=None),
            )
            conn.execute("COMMIT;")
        except BaseException:
            conn.execute("ROLLBACK;")
            raise
    finally:
        conn.close()

    return result
