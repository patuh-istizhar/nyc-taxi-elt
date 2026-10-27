"""Run the whole flow: fake TLC website -> fake S3 (moto) -> local DuckDB file."""

import io

import boto3
import duckdb
import pytest
from moto import mock_aws

from nyc_taxi_elt import s3
from nyc_taxi_elt.flows.ingest import ingest

TRIPS = """
SELECT * FROM (VALUES
    (1, TIMESTAMP '2024-01-02 08:00', TIMESTAMP '2024-01-02 08:10', 100, 200, 12.5, 1.0),
    (2, TIMESTAMP '2024-01-03 09:00', TIMESTAMP '2024-01-03 09:05', 101, 201, 8.0, 2.0),
    (2, TIMESTAMP '2024-01-03 09:00', TIMESTAMP '2024-01-03 09:05', 101, 201, 8.0, 2.0)
) AS t(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
       PULocationID, DOLocationID, total_amount, Airport_fee)
"""


def make_parquet(tmp_path) -> bytes:
    path = tmp_path / "trips.parquet"
    duckdb.sql(f"COPY ({TRIPS}) TO '{path}' (FORMAT parquet)")
    return path.read_bytes()


class FakeResponse:
    def __init__(self, body: bytes | None):
        self.status_code = 200 if body is not None else 404
        self.raw = io.BytesIO(body or b"")

    def raise_for_status(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


@pytest.fixture()
def tlc(monkeypatch, tmp_path):
    """Fake TLC website. Put files in the dict; everything else is a 404."""
    files = {"yellow_tripdata_2024-01.parquet": make_parquet(tmp_path)}
    requested = []

    def fake_get(url, **kwargs):
        requested.append(url)
        return FakeResponse(files.get(url.rsplit("/", 1)[-1]))

    monkeypatch.setattr(s3.requests, "get", fake_get)
    return requested


@pytest.fixture()
def db(monkeypatch, tmp_path):
    path = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("WAREHOUSE_DATABASE", str(path))
    # moto fakes AWS S3 only, so point at an AWS URL instead of local RustFS.
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.us-east-1.amazonaws.com")
    with mock_aws():
        yield path


def query(path, sql):
    with duckdb.connect(str(path)) as conn:
        return conn.execute(sql).fetchall()


def test_load_skip_and_force(tlc, db):
    ingest(taxi_type="yellow", start_month="2024-01")
    assert boto3.client("s3", region_name="us-east-1").head_object(
        Bucket="nyc-taxi", Key="yellow_tripdata_2024-01.parquet"
    )
    # The duplicate trip is loaded once; Airport_fee lands in airport_fee.
    assert query(db, "SELECT count(*), sum(airport_fee) FROM nyc_taxi.yellow_tripdata") == [
        (2, 3.0)
    ]
    assert query(db, "SELECT filename, rows_loaded FROM nyc_taxi.pipeline_runs") == [
        ("yellow_tripdata_2024-01.parquet", 2)
    ]

    ingest(taxi_type="yellow", start_month="2024-01")  # already loaded -> skipped
    assert len(tlc) == 1

    ingest(taxi_type="yellow", start_month="2024-01", force=True)  # load again
    assert len(tlc) == 2
    assert query(db, "SELECT count(*) FROM nyc_taxi.yellow_tripdata") == [(2,)]
    assert query(db, "SELECT count(*) FROM nyc_taxi.pipeline_runs") == [(2,)]


def test_missing_file_is_skipped(tlc, db):
    ingest(taxi_type="yellow", start_month="2023-12", end_month="2024-01")
    assert query(db, "SELECT filename FROM nyc_taxi.pipeline_runs") == [
        ("yellow_tripdata_2024-01.parquet",)
    ]
