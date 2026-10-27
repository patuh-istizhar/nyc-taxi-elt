"""The warehouse: a DuckDB database (a local file in dev, MotherDuck in prod)."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from nyc_taxi_elt.config import Settings

# Columns we keep for each taxi type. Columns in the file but not here are ignored;
# columns here but missing from the file are stored as NULL.
COLUMNS = {
    "yellow": {
        "VendorID": "VARCHAR",
        "tpep_pickup_datetime": "TIMESTAMP",
        "tpep_dropoff_datetime": "TIMESTAMP",
        "passenger_count": "BIGINT",
        "trip_distance": "DOUBLE",
        "RatecodeID": "VARCHAR",
        "store_and_fwd_flag": "VARCHAR",
        "PULocationID": "VARCHAR",
        "DOLocationID": "VARCHAR",
        "payment_type": "BIGINT",
        "fare_amount": "DOUBLE",
        "extra": "DOUBLE",
        "mta_tax": "DOUBLE",
        "tip_amount": "DOUBLE",
        "tolls_amount": "DOUBLE",
        "improvement_surcharge": "DOUBLE",
        "total_amount": "DOUBLE",
        "congestion_surcharge": "DOUBLE",
        "airport_fee": "DOUBLE",
    },
    "green": {
        "VendorID": "VARCHAR",
        "lpep_pickup_datetime": "TIMESTAMP",
        "lpep_dropoff_datetime": "TIMESTAMP",
        "store_and_fwd_flag": "VARCHAR",
        "RatecodeID": "VARCHAR",
        "PULocationID": "VARCHAR",
        "DOLocationID": "VARCHAR",
        "passenger_count": "BIGINT",
        "trip_distance": "DOUBLE",
        "fare_amount": "DOUBLE",
        "extra": "DOUBLE",
        "mta_tax": "DOUBLE",
        "tip_amount": "DOUBLE",
        "tolls_amount": "DOUBLE",
        "ehail_fee": "DOUBLE",
        "improvement_surcharge": "DOUBLE",
        "total_amount": "DOUBLE",
        "payment_type": "BIGINT",
        "trip_type": "BIGINT",
        "congestion_surcharge": "DOUBLE",
    },
}


@contextmanager
def connect(settings: Settings) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the warehouse and switch to WAREHOUSE_SCHEMA (created if needed)."""
    database = settings.warehouse_database
    if database.startswith("md:"):
        database += f"?motherduck_token={settings.motherduck_token}"
    else:
        Path(database).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(database)
    try:
        # "catalog.schema": the database file and the schema may have the same name.
        catalog = conn.execute("SELECT current_database()").fetchone()[0]
        schema = f'"{catalog}"."{settings.warehouse_schema}"'
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"USE {schema}")
        yield conn
    finally:
        conn.close()


def create_audit_table(conn: duckdb.DuckDBPyConnection) -> None:
    """pipeline_runs: one row per loaded file, so we know what is already done."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            taxi_type VARCHAR,
            trip_month VARCHAR,
            filename VARCHAR,
            rows_loaded BIGINT,
            loaded_at TIMESTAMP
        )
        """
    )


def is_loaded(conn: duckdb.DuckDBPyConnection, filename: str) -> bool:
    create_audit_table(conn)
    row = conn.execute("SELECT 1 FROM pipeline_runs WHERE filename = ?", [filename]).fetchone()
    return row is not None


def load_parquet(
    conn: duckdb.DuckDBPyConnection, path: Path, taxi_type: str, trip_month: str
) -> int:
    """Load one parquet file into <taxi_type>_tripdata and return the number of rows.

    Old rows of the same file are deleted first, so loading a file twice is safe.
    """
    table = f"{taxi_type}_tripdata"
    filename = path.name
    columns = COLUMNS[taxi_type]
    pickup, dropoff = [name for name in columns if name.endswith("_datetime")]

    column_ddl = ", ".join(f'"{name}" {type_}' for name, type_ in columns.items())
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"(unique_row_id VARCHAR, filename VARCHAR, {column_ddl})"
    )
    create_audit_table(conn)

    # Column names in TLC files are not always written the same way (Airport_fee vs
    # airport_fee), so match them case-insensitively.
    source = f"read_parquet('{path}')"
    in_file = {
        row[0].lower(): row[0] for row in conn.sql(f"DESCRIBE SELECT * FROM {source}").fetchall()
    }
    select = [
        f'CAST("{in_file[name.lower()]}" AS {type_}) AS "{name}"'
        if name.lower() in in_file
        else f'CAST(NULL AS {type_}) AS "{name}"'
        for name, type_ in columns.items()
    ]
    column_list = ", ".join(f'"{name}"' for name in columns)

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"DELETE FROM {table} WHERE filename = ?", [filename])
        conn.execute(
            f"""
            INSERT INTO {table} (unique_row_id, filename, {column_list})
            SELECT
                -- A trip is identified by vendor, times and locations.
                md5(concat(VendorID, "{pickup}", "{dropoff}", PULocationID, DOLocationID)),
                ?,
                {column_list}
            FROM (SELECT {", ".join(select)} FROM {source})
            -- Keep one row when the file contains the same trip twice.
            QUALIFY row_number() OVER (
                PARTITION BY VendorID, "{pickup}", "{dropoff}", PULocationID, DOLocationID
            ) = 1
            """,
            [filename],
        )
        rows = conn.execute(
            f"SELECT count(*) FROM {table} WHERE filename = ?", [filename]
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO pipeline_runs (taxi_type, trip_month, filename, rows_loaded, loaded_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [taxi_type, trip_month, filename, rows, datetime.now(UTC).replace(tzinfo=None)],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")  # keep the old rows if anything went wrong
        raise
    return rows
