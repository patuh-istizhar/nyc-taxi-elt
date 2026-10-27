"""Open the warehouse and quote SQL safely."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from nyc_taxi_elt.config import Settings


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@contextmanager
def connect_warehouse(settings: Settings) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open ``warehouse_database`` with ``warehouse_schema`` as the active schema.

    A local DuckDB file is single-writer, so the flow loads files one at a time.
    """
    database = settings.warehouse_database
    if settings.is_motherduck:
        if settings.motherduck_token:
            separator = "&" if "?" in database else "?"
            database = f"{database}{separator}motherduck_token={settings.motherduck_token}"
    elif database != ":memory:":
        Path(database).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(database)
    try:
        catalog = conn.execute("SELECT current_database()").fetchone()[0]
        # Qualify with the catalog: schema and database may share a name.
        schema = f"{sql_ident(catalog)}.{sql_ident(settings.warehouse_schema)}"
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn.execute(f"USE {schema};")
        yield conn
    finally:
        conn.close()


def table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    # Only look at the active database so other attached catalogs never match.
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE schema_name = ?
          AND table_name = ?
          AND database_name = current_database()
        """,
        [schema, table],
    ).fetchone()
    return row is not None
