"""Tests for environment-aware settings."""

from __future__ import annotations

import pytest

from nyc_taxi_elt import config
from nyc_taxi_elt.config import get_settings


def fake_secret_blocks(monkeypatch: pytest.MonkeyPatch, blocks: dict[str, str]) -> None:
    class FakeSecret:
        def __init__(self, value: str) -> None:
            self.value = value

        def get(self) -> str:
            return self.value

        @classmethod
        def load(cls, name: str) -> FakeSecret:
            if name not in blocks:
                raise ValueError(f"block {name} not found")
            return cls(blocks[name])

    monkeypatch.setattr(config, "Secret", FakeSecret)


def use_prod(monkeypatch: pytest.MonkeyPatch, database: str) -> None:
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.example.com/")
    monkeypatch.setenv("S3_REGION", "auto")
    monkeypatch.setenv("S3_BUCKET", "prod-bucket")
    monkeypatch.setenv("WAREHOUSE_DATABASE", database)
    fake_secret_blocks(monkeypatch, {})


def test_dev_settings(warehouse_path):
    s = get_settings()
    assert s.is_dev
    assert not s.is_motherduck
    assert s.warehouse_database == str(warehouse_path)
    assert s.motherduck_token is None
    assert s.s3_bucket == "test-bucket"
    assert s.s3_uri("a.parquet") == "s3://test-bucket/a.parquet"


def test_dev_defaults(monkeypatch):
    for name in ("S3_ENDPOINT_URL", "S3_REGION", "S3_BUCKET", "WAREHOUSE_DATABASE"):
        monkeypatch.delenv(name, raising=False)
    s = get_settings()
    assert s.s3_endpoint_url == "http://127.0.0.1:9000"
    assert s.s3_bucket == "nyc-taxi"
    assert s.warehouse_database == "./data/nyc_taxi.duckdb"


def test_prod_with_motherduck(monkeypatch):
    use_prod(monkeypatch, "md:nyc_taxi")
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "md-token")
    s = get_settings()
    assert not s.is_dev
    assert s.is_motherduck
    assert s.motherduck_token == "md-token"
    assert s.s3_endpoint_url == "https://s3.example.com"


def test_prod_motherduck_requires_token(monkeypatch):
    use_prod(monkeypatch, "md:nyc_taxi")
    with pytest.raises(ValueError, match="MOTHERDUCK_TOKEN"):
        get_settings()


def test_prod_has_no_defaults(monkeypatch):
    use_prod(monkeypatch, "md:nyc_taxi")
    monkeypatch.delenv("WAREHOUSE_DATABASE")
    with pytest.raises(ValueError, match="WAREHOUSE_DATABASE"):
        get_settings()


def test_prod_endpoint_is_optional_for_aws(monkeypatch, tmp_path):
    use_prod(monkeypatch, str(tmp_path / "w.duckdb"))
    monkeypatch.delenv("S3_ENDPOINT_URL")
    assert get_settings().s3_endpoint_url is None


def test_keys_fall_back_to_secret_blocks(monkeypatch):
    monkeypatch.delenv("S3_ACCESS_KEY_ID")
    monkeypatch.delenv("S3_SECRET_ACCESS_KEY")
    fake_secret_blocks(monkeypatch, {"s3-access-key-id": "ak", "s3-secret-access-key": "sk"})
    s = get_settings()
    assert (s.s3_access_key_id, s.s3_secret_access_key) == ("ak", "sk")


def test_invalid_env(monkeypatch):
    monkeypatch.setenv("ENV", "staging")
    with pytest.raises(ValueError, match="ENV harus"):
        get_settings()


def test_invalid_schema(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_SCHEMA", "drop table;")
    with pytest.raises(ValueError, match="WAREHOUSE_SCHEMA"):
        get_settings()


def test_secrets_are_not_in_repr(monkeypatch):
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "super-secret")
    assert "super-secret" not in repr(get_settings())
