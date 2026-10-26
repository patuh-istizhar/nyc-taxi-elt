"""MotherDuck loading for the NYC Taxi flows."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TypedDict

import duckdb

from flows.config import Settings

logger = logging.getLogger(__name__)


class TableConfig(TypedDict):
    pickup_column: str
    dropoff_column: str
    columns: list[tuple[str, str]]


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


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _motherduck_uri(settings: Settings) -> str:
    return f"md:{settings.motherduck_database}"


def attach_motherduck(conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    conn.execute("INSTALL motherduck;")
    conn.execute("LOAD motherduck;")
    conn.execute(f"SET motherduck_token = {_sql_literal(settings.motherduck_token)};")
    conn.execute(f"ATTACH {_sql_literal(_motherduck_uri(settings))} AS md;")


def load_parquet(
    parquet_path: Path,
    *,
    filename: str,
    taxi_type: str,
    settings: Settings,
) -> tuple[int, int, int]:
    """Load one parquet file into MotherDuck.

    Rows for ``filename`` are replaced before inserting, so re-running a month
    is idempotent. Returns ``(incoming_rows, rows_before, rows_after)``.
    """
    table_config = TRIP_TABLES[taxi_type]
    schema = settings.motherduck_schema
    table = f"md.{schema}.{taxi_type}_tripdata"

    select_columns = ",\n                  ".join(
        f"CAST({name} AS {data_type}) AS {name}"
        for name, data_type in table_config["columns"]
    )
    column_names = ["unique_row_id", "filename"] + [
        name for name, _ in table_config["columns"]
    ]
    column_sql = ", ".join(column_names)
    columns_ddl = ",\n                    ".join(
        f"{name} {data_type}" for name, data_type in table_config["columns"]
    )

    conn = duckdb.connect()
    try:
        attach_motherduck(conn, settings)

        conn.execute(f"CREATE SCHEMA IF NOT EXISTS md.{schema};")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                unique_row_id VARCHAR,
                filename VARCHAR,
                {columns_ddl}
            );
            """
        )

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
                COALESCE(CAST({table_config["pickup_column"]} AS VARCHAR), ''),
                COALESCE(CAST({table_config["dropoff_column"]} AS VARCHAR), ''),
                COALESCE(CAST(PULocationID AS VARCHAR), ''),
                COALESCE(CAST(DOLocationID AS VARCHAR), '')
              )) AS unique_row_id,
              {_sql_literal(filename)} AS filename,
              {select_columns}
            FROM ext
            QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_row_id) = 1;
            """
        )

        incoming = conn.execute("SELECT COUNT(*) FROM dedup").fetchone()[0]
        if incoming == 0:
            raise ValueError(f"File {filename} tidak berisi baris data.")

        rows_before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        conn.execute(f"DELETE FROM {table} WHERE filename = {_sql_literal(filename)};")
        conn.execute(
            f"""
            INSERT INTO {table} ({column_sql})
            SELECT {column_sql}
            FROM dedup;
            """
        )

        rows_after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()

    return int(incoming), int(rows_before), int(rows_after)
