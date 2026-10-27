"""Monitoring views (pipeline_health, trip_months) over the trip tables."""

from __future__ import annotations

from nyc_taxi_elt.config import Settings
from nyc_taxi_elt.tlc import TRIP_TABLES, table_name
from nyc_taxi_elt.warehouse.connection import connect_warehouse

PIPELINE_HEALTH_SQL = """
CREATE OR REPLACE VIEW pipeline_health AS
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
FROM pipeline_runs
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
    FROM {taxi_type}_tripdata
)
GROUP BY filename
"""

TRIP_TIME_COLUMNS = {
    taxi_type: (spec["pickup_column"], spec["dropoff_column"])
    for taxi_type, spec in TRIP_TABLES.items()
}


def _available_tables(conn, schema: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE schema_name = ?
          AND database_name = current_database()
        """,
        [schema],
    ).fetchall()
    return {str(row[0]) for row in rows}


def refresh_monitoring_views(settings: Settings) -> None:
    """Create or replace the views used to monitor the pipeline.

    Views are only created for tables that already exist, so a fresh database
    (or a run that loaded a single taxi type) does not break the refresh.
    Unqualified names are intentional: after USE schema, views stay portable
    whatever catalog name the database is opened under.
    """
    schema = settings.warehouse_schema
    with connect_warehouse(settings) as conn:
        tables = _available_tables(conn, schema)

        if "pipeline_runs" in tables:
            conn.execute(PIPELINE_HEALTH_SQL)

        parts = [
            TRIP_MONTHS_TEMPLATE.format(
                taxi_type=taxi_type,
                pickup=pickup,
                dropoff=dropoff,
            )
            for taxi_type, (pickup, dropoff) in TRIP_TIME_COLUMNS.items()
            if table_name(taxi_type) in tables
        ]
        if parts:
            conn.execute("CREATE OR REPLACE VIEW trip_months AS " + " UNION ALL ".join(parts) + ";")
