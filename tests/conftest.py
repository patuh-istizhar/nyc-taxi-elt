"""Shared pytest fixtures.

Flow tests run against a temporary local Prefect server (``prefect_test_harness``)
so test runs never register flows in the workspace of the active profile.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from prefect.testing.utilities import prefect_test_harness

from nyc_taxi_elt.config import Settings

# Tests must not pick up a developer's real .env / prod settings.
for _name in list(os.environ):
    if _name.startswith(("S3_", "WAREHOUSE_", "MOTHERDUCK_", "TLC_")):
        del os.environ[_name]
os.environ.update(
    ENV="dev",
    S3_ENDPOINT_URL="http://127.0.0.1:9000",
    S3_REGION="us-east-1",
    S3_ACCESS_KEY_ID="test",
    S3_SECRET_ACCESS_KEY="test",
    S3_BUCKET="test-bucket",
    WAREHOUSE_SCHEMA="nyc_taxi",
)


@pytest.fixture(scope="session", autouse=True)
def ephemeral_prefect_server():
    with prefect_test_harness():
        yield


def make_dev_settings(warehouse_database: Path | str) -> Settings:
    """Settings pointing at a local DuckDB file (no env needed)."""
    return Settings(
        env="dev",
        s3_endpoint_url="http://127.0.0.1:9000",
        s3_region="us-east-1",
        s3_access_key_id="test",
        s3_secret_access_key="test",
        s3_bucket="test-bucket",
        warehouse_database=str(warehouse_database),
        warehouse_schema="nyc_taxi",
    )


@pytest.fixture()
def warehouse_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("WAREHOUSE_DATABASE", str(path))
    return path


@pytest.fixture()
def settings(warehouse_path: Path) -> Settings:
    return make_dev_settings(warehouse_path)
