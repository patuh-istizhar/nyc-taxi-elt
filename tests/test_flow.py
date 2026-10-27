"""End-to-end flow tests: fake TLC source -> mocked S3 (moto) -> local DuckDB."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest
import requests
from moto import mock_aws

from nyc_taxi_elt import s3
from nyc_taxi_elt.flows.ingest import ingest
from nyc_taxi_elt.warehouse.connection import connect_warehouse
from tests.test_loader import TRIPS, write_yellow

BUCKET = "test-bucket"


class FakeResponse:
    def __init__(self, status_code: int, body: bytes = b"") -> None:
        self.status_code = status_code
        self.body = body
        self.headers = {"Content-Length": str(len(body))} if status_code == 200 else {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def iter_content(self, chunk_size: int):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start : start + chunk_size]

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


@pytest.fixture()
def source(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Fake TLC: serves files put in ``source.files``; anything else is a 404."""
    tlc = SimpleNamespace(files={}, requested=[])

    def fake_get(url: str, **kwargs: object) -> FakeResponse:
        tlc.requested.append(url)
        body = tlc.files.get(url.rsplit("/", 1)[-1])
        return FakeResponse(200, body) if body is not None else FakeResponse(404)

    monkeypatch.setattr(s3.requests, "get", fake_get)
    return tlc


def publish(source: SimpleNamespace, tmp_path: Path, month: str) -> str:
    trips = [
        (vendor, pickup.replace("2019-01", month), dropoff.replace("2019-01", month), pu, do, total)
        for vendor, pickup, dropoff, pu, do, total in TRIPS
    ]
    name = f"yellow_tripdata_{month}.parquet"
    source.files[name] = write_yellow(tmp_path / name, trips=trips).read_bytes()
    return name


@pytest.fixture()
def s3_client(monkeypatch: pytest.MonkeyPatch):
    # moto only intercepts AWS hostnames; the dev default would reach a real local S3.
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.us-east-1.amazonaws.com")
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def audit_rows(settings) -> list[tuple[str, int]]:
    with connect_warehouse(settings) as conn:
        return conn.execute(
            "SELECT filename, rows_loaded FROM pipeline_runs ORDER BY loaded_at"
        ).fetchall()


def test_loads_month_then_skips_it(source, s3_client, settings, tmp_path) -> None:
    name = publish(source, tmp_path, "2019-01")

    ingest(taxi_type="yellow", start_month="2019-01")

    # Dev creates the bucket on its own and keeps the raw copy there.
    assert s3_client.head_object(Bucket=BUCKET, Key=name)["ContentLength"] > 0
    assert audit_rows(settings) == [(name, 3)]
    with connect_warehouse(settings) as conn:
        assert conn.execute("SELECT COUNT(*) FROM yellow_tripdata").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM trip_months").fetchone()[0] == 1

    ingest(taxi_type="yellow", start_month="2019-01")
    assert len(source.requested) == 1
    assert audit_rows(settings) == [(name, 3)]

    ingest(taxi_type="yellow", start_month="2019-01", force=True)
    assert len(source.requested) == 2
    assert len(audit_rows(settings)) == 2


def test_backfill_loads_published_months_and_reports_missing(
    source, s3_client, settings, tmp_path
) -> None:
    jan = publish(source, tmp_path, "2019-01")
    feb = publish(source, tmp_path, "2019-02")

    with pytest.raises(ValueError, match="yellow_tripdata_2019-03.parquet belum tersedia"):
        ingest(taxi_type="yellow", start_month="2019-01", end_month="2019-03")

    assert audit_rows(settings) == [(jan, 3), (feb, 3)]

    # Re-running the same range only retries the missing month.
    source.requested.clear()
    with pytest.raises(ValueError, match="belum tersedia"):
        ingest(taxi_type="yellow", start_month="2019-01", end_month="2019-03")
    assert [url.rsplit("/", 1)[-1] for url in source.requested] == [
        "yellow_tripdata_2019-03.parquet"
    ]
