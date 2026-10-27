"""Helpers for any S3-compatible store (AWS S3, RustFS, MinIO, Backblaze B2, R2, ...)."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from nyc_taxi_elt.config import Settings

logger = logging.getLogger(__name__)

DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024
PARQUET_MAGIC = b"PAR1"


def build_client(settings: Settings):
    """Build a boto3 S3 client pointed at the configured endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        config=Config(
            connect_timeout=10,
            read_timeout=300,
            retries={"max_attempts": 5, "mode": "standard"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"addressing_style": "path"},
        ),
    )


def bucket_exists(client, bucket: str) -> bool:
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchBucket", "NotFound"} or status == 404:
            return False
        if code in {"403", "AccessDenied"} or status == 403:
            # Bucket-scoped keys may lack ListBuckets while read/write works.
            return True
        raise


def ensure_bucket(client, bucket: str, *, create: bool) -> None:
    """Fail clearly when the bucket is missing, or create it when allowed (dev)."""
    if bucket_exists(client, bucket):
        return
    if not create:
        raise ValueError(f"Bucket {bucket!r} tidak ditemukan. Buat bucket-nya terlebih dahulu.")
    logger.info("Membuat bucket s3://%s", bucket)
    client.create_bucket(Bucket=bucket)


def object_size(client, bucket: str, key: str) -> int | None:
    try:
        return int(client.head_object(Bucket=bucket, Key=key)["ContentLength"])
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def _iter_parts(response: requests.Response, part_size: int) -> Iterator[bytes]:
    buffer = bytearray()
    for chunk in response.iter_content(chunk_size=part_size):
        if not chunk:
            continue
        buffer.extend(chunk)
        if len(buffer) >= part_size:
            yield bytes(buffer)
            buffer.clear()
    if buffer:
        yield bytes(buffer)


class SourceUnavailableError(ValueError):
    """Raised when TLC has not published the requested file yet."""


def upload_from_http(
    source_uri: str,
    client,
    bucket: str,
    key: str,
    *,
    metadata: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Stream an HTTP response into S3 and return (size_bytes, sha256_hex)."""
    logger.info("Streaming %s to s3://%s/%s", source_uri, bucket, key)

    with requests.get(source_uri, stream=True, timeout=(10, 300)) as response:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            if response.status_code in {403, 404}:
                raise SourceUnavailableError(
                    f"{source_uri} belum tersedia (HTTP {response.status_code}). "
                    "TLC biasanya menerbitkan file beberapa minggu setelah akhir bulan; "
                    "jalankan ulang deployment setelah file muncul."
                ) from exc
            raise ValueError(
                f"Gagal mengunduh {source_uri} (HTTP {response.status_code})."
            ) from exc

        content_encoding = (response.headers.get("Content-Encoding") or "identity").lower()
        raw_length = response.headers.get("Content-Length")
        expected_bytes = (
            int(raw_length) if raw_length and content_encoding in {"", "identity"} else None
        )

        upload = client.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType="application/vnd.apache.parquet",
            Metadata=metadata or {},
        )
        upload_id = upload["UploadId"]
        parts: list[dict[str, Any]] = []
        hasher = hashlib.sha256()
        total_bytes = 0
        last_magic = b""

        try:
            for part_number, part in enumerate(_iter_parts(response, DOWNLOAD_CHUNK_SIZE), start=1):
                if part_number == 1 and part[:4] != PARQUET_MAGIC:
                    raise ValueError(f"File dari {source_uri} bukan parquet (magic bytes salah).")

                hasher.update(part)
                total_bytes += len(part)
                last_magic = part[-4:]

                result = client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=part,
                )
                parts.append({"ETag": result["ETag"], "PartNumber": part_number})

            if not parts:
                raise ValueError(f"File dari {source_uri} kosong.")
            if last_magic != PARQUET_MAGIC:
                raise ValueError(f"File dari {source_uri} bukan parquet (footer rusak).")

            client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except BaseException:
            try:
                client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
                logger.warning(
                    "Gagal abort multipart upload %s untuk s3://%s/%s",
                    upload_id,
                    bucket,
                    key,
                    exc_info=True,
                )
            raise

    def _discard(reason: str) -> None:
        logger.error("%s; menghapus s3://%s/%s", reason, bucket, key)
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            logger.warning("Gagal menghapus objek rusak s3://%s/%s", bucket, key, exc_info=True)

    if expected_bytes is not None and total_bytes != expected_bytes:
        _discard(f"Ukuran unduhan tidak cocok: {total_bytes} != Content-Length {expected_bytes}")
        raise ValueError(
            f"Ukuran {key} tidak cocok: diterima {total_bytes} byte, "
            f"diharapkan {expected_bytes} byte."
        )

    head = client.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != total_bytes:
        _discard(f"Ukuran objek tidak cocok: {head['ContentLength']} != {total_bytes}")
        raise ValueError(
            f"Ukuran objek s3://{bucket}/{key} tidak cocok: "
            f"{head['ContentLength']} != {total_bytes} byte."
        )

    return total_bytes, hasher.hexdigest()


def download_object(client, bucket: str, key: str, destination: Path) -> int:
    client.download_file(bucket, key, str(destination))
    return destination.stat().st_size
