"""Connection check: can we write to S3 and query the warehouse?

python -m nyc_taxi_elt.flows.smoke
"""

from prefect import flow, get_run_logger

from nyc_taxi_elt import s3, warehouse
from nyc_taxi_elt.config import get_settings


@flow(name="nyc-taxi-smoke")
def smoke() -> None:
    logger = get_run_logger()
    settings = get_settings()

    client = s3.get_client(settings)
    s3.ensure_bucket(client, settings.s3_bucket, create=settings.is_dev)
    client.put_object(Bucket=settings.s3_bucket, Key="_smoke.txt", Body=b"ok")
    client.delete_object(Bucket=settings.s3_bucket, Key="_smoke.txt")
    logger.info("S3 ok: bucket %s", settings.s3_bucket)

    with warehouse.connect(settings) as conn:
        version = conn.execute("SELECT version()").fetchone()[0]
    logger.info(
        "Warehouse ok: %s (schema %s, DuckDB %s)",
        settings.warehouse_database,
        settings.warehouse_schema,
        version,
    )


if __name__ == "__main__":
    smoke()
