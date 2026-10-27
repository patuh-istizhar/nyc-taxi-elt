"""Small helpers for any S3-compatible storage (RustFS, Backblaze B2, R2, AWS S3, ...)."""

from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

from nyc_taxi_elt.config import Settings


def get_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
    )


def ensure_bucket(client, bucket: str, create: bool) -> None:
    """Make sure the bucket exists. In dev we create it; in prod a missing bucket is an error."""
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        if not create:
            raise ValueError(f"Bucket {bucket!r} tidak ditemukan. Buat dulu.") from None
        client.create_bucket(Bucket=bucket)


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def upload_from_url(client, url: str, bucket: str, key: str) -> bool:
    """Stream a file from a URL straight into S3.

    Returns False when the file does not exist (yet) at the source (HTTP 403/404).
    """
    with requests.get(url, stream=True, timeout=60) as response:
        if response.status_code in (403, 404):
            return False
        response.raise_for_status()
        # upload_fileobj reads the stream in chunks, so the file never sits fully in memory.
        client.upload_fileobj(response.raw, bucket, key)
    return True


def download(client, bucket: str, key: str, path: Path) -> None:
    client.download_file(bucket, key, str(path))
