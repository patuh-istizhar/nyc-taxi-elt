"""Read-only observability views over the loaded trip tables."""

from __future__ import annotations

import duckdb

from flows.config import Settings
from flows.warehouse import attach_motherduck

PIPELINE_HEALTH_SQL = """
CREATE OR REPLACE VIEW {schema}.pipeline_health AS
SELECT
    trip_month,
    taxi_type,
    max(loaded_at) AS last_loaded_at,
    arg_max(flow_run_name, loaded_at) AS last_run,
    arg_max(rows_loaded, loaded_at) AS rows_loaded,
    arg_max(rows_in_table, loaded_at) AS rows_in_table,
    arg_max(size_bytes, loaded_at) AS size_bytes,
    arg_max(warnings, loaded_at) AS warnings,
    count(*) AS load_count,
    date_diff('day', max(loaded_at), now()) AS days_since_load,
    date_diff(
        'month',
        CAST(trip_month || '-01' AS DATE),
        date_trunc('month', current_date)
    ) AS months_behind
FROM {schema}.pipeline_runs
GROUP BY trip_month, taxi_type;
"""

TRIP_MONTHS_TEMPLATE = """
SELECT
    '{taxi_type}' AS taxi_type,
    filename,
    count(*) AS rows,
    min({pickup}) AS first_pickup,
    max({dropoff}) AS last_dropoff,
    count(*) FILTER (
        WHERE {pickup} < month_start - INTERVAL 1 MONTH
           OR {dropoff} >= month_start + INTERVAL 2 MONTH
    ) AS suspicious_rows
FROM (
    SELECT
        *,
        strptime(
            regexp_extract(filename, '([0-9]{{4}}-[0-9]{{2}})', 1), '%Y-%m'
        ) AS month_start
    FROM {schema}.{taxi_type}_tripdata
)
GROUP BY filename
"""

TRIP_TIME_COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}


def _available_tables(conn: duckdb.DuckDBPyConnection, schema: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = 'md' AND schema_name = ?
        """,
        [schema],
    ).fetchall()
    return {str(row[0]) for row in rows}


def refresh_observability_views(settings: Settings) -> None:
    """Create or replace the views used to monitor the pipeline.

    Views are only created for tables that already exist, so a fresh database
    (or a run that loaded a single taxi type) does not break the refresh.
    """
    schema = settings.motherduck_schema
    conn = duckdb.connect()
    try:
        attach_motherduck(conn, settings)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS md.{schema};")
        # Views must not embed the catalog alias: MotherDuck share clones attach
        # the same database under a different name, and a stored "md." prefix
        # would not resolve there.
        conn.execute("USE md;")
        tables = _available_tables(conn, schema)

        if "pipeline_runs" in tables:
            conn.execute(PIPELINE_HEALTH_SQL.format(schema=schema))

        parts = [
            TRIP_MONTHS_TEMPLATE.format(
                schema=schema,
                taxi_type=taxi_type,
                pickup=pickup,
                dropoff=dropoff,
            )
            for taxi_type, (pickup, dropoff) in TRIP_TIME_COLUMNS.items()
            if f"{taxi_type}_tripdata" in tables
        ]
        if parts:
            conn.execute(
                f"CREATE OR REPLACE VIEW {schema}.trip_months AS " + " UNION ALL ".join(parts) + ";"
            )
    finally:
        conn.close()
