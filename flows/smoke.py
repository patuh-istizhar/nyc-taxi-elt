"""Connectivity check for B2 and MotherDuck."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import duckdb
from prefect import flow, task

from flows.b2 import bucket_exists, build_client
from flows.config import get_settings
from flows.warehouse import attach_motherduck

logger = logging.getLogger(__name__)

SMOKE_KEY = "_smoke/healthcheck.txt"


@task(retries=1, retry_delay_seconds=10, timeout_seconds=300)
def check_b2() -> str:
    settings = get_settings()
    client = build_client(settings)

    if not bucket_exists(client, settings.b2_bucket):
        raise ValueError(f"Bucket B2 {settings.b2_bucket!r} tidak ditemukan.")

    payload = f"ok {datetime.now(UTC).isoformat()}".encode()
    client.put_object(
        Bucket=settings.b2_bucket,
        Key=SMOKE_KEY,
        Body=payload,
        ContentType="text/plain",
    )
    body = client.get_object(Bucket=settings.b2_bucket, Key=SMOKE_KEY)["Body"].read()
    client.delete_object(Bucket=settings.b2_bucket, Key=SMOKE_KEY)

    if body != payload:
        raise ValueError("Isi objek B2 tidak cocok saat dibaca ulang.")

    return f"B2 ok: {settings.b2_bucket}/{SMOKE_KEY} ({len(payload)} bytes)"


@task(retries=1, retry_delay_seconds=10, timeout_seconds=300)
def check_motherduck() -> str:
    settings = get_settings()

    schema = settings.motherduck_schema

    conn = duckdb.connect()
    try:
        attach_motherduck(conn, settings)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS md.{schema};")
        conn.execute(f"CREATE OR REPLACE TABLE md.{schema}._smoke (ok BOOLEAN);")
        conn.execute(f"INSERT INTO md.{schema}._smoke VALUES (true);")
        ok = conn.execute(f"SELECT ok FROM md.{schema}._smoke").fetchone()[0]
        conn.execute(f"DROP TABLE md.{schema}._smoke;")
        version = conn.execute("SELECT version()").fetchone()[0]
    finally:
        conn.close()

    if not ok:
        raise ValueError("Tulis/baca tabel uji di MotherDuck tidak konsisten.")

    return f"MotherDuck ok: {settings.motherduck_database}.{schema} (DuckDB {version})"


@flow(name="nyc-taxi-smoke", log_prints=False)
def smoke() -> None:
    logger.info(check_b2())
    logger.info(check_motherduck())


if __name__ == "__main__":
    smoke()
