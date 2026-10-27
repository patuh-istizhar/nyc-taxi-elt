"""Connectivity check: write/read/delete an S3 object and a warehouse table."""

from __future__ import annotations

from datetime import UTC, datetime

from prefect import flow, get_run_logger, task

from nyc_taxi_elt.config import Settings, get_settings
from nyc_taxi_elt.s3 import build_client, ensure_bucket
from nyc_taxi_elt.warehouse.connection import connect_warehouse

SMOKE_KEY = "_smoke/healthcheck.txt"


@task(retries=1, retry_delay_seconds=10, timeout_seconds=300)
def check_s3(settings: Settings) -> str:
    client = build_client(settings)
    bucket = settings.s3_bucket
    ensure_bucket(client, bucket, create=settings.is_dev)

    payload = f"ok {datetime.now(UTC).isoformat()}".encode()
    client.put_object(Bucket=bucket, Key=SMOKE_KEY, Body=payload, ContentType="text/plain")
    body = client.get_object(Bucket=bucket, Key=SMOKE_KEY)["Body"].read()
    client.delete_object(Bucket=bucket, Key=SMOKE_KEY)
    if body != payload:
        raise ValueError("Isi objek tidak cocok saat dibaca ulang.")

    endpoint = settings.s3_endpoint_url or "AWS S3"
    return f"S3 ok: {settings.s3_uri(SMOKE_KEY)} via {endpoint} ({len(payload)} bytes)"


@task(retries=1, retry_delay_seconds=10, timeout_seconds=300)
def check_warehouse(settings: Settings) -> str:
    with connect_warehouse(settings) as conn:
        conn.execute("CREATE OR REPLACE TABLE _smoke (ok BOOLEAN);")
        conn.execute("INSERT INTO _smoke VALUES (true);")
        ok = conn.execute("SELECT ok FROM _smoke").fetchone()[0]
        conn.execute("DROP TABLE _smoke;")
        version = conn.execute("SELECT version()").fetchone()[0]
    if not ok:
        raise ValueError("Tulis/baca tabel uji di warehouse tidak konsisten.")
    return (
        f"Warehouse ok: {settings.warehouse_database} "
        f"(schema {settings.warehouse_schema}, DuckDB {version})"
    )


@flow(name="nyc-taxi-smoke", log_prints=False)
def smoke() -> None:
    logger = get_run_logger()
    settings = get_settings()
    logger.info("Smoke check ENV=%s", settings.env)
    logger.info(check_s3(settings))
    logger.info(check_warehouse(settings))


if __name__ == "__main__":
    smoke()
