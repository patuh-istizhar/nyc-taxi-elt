"""Tests for S3 upload error handling."""

from __future__ import annotations

import pytest
import requests
from prefect.states import Failed

from nyc_taxi_elt import s3
from nyc_taxi_elt.flows import ingest as flow_module


class FakeResponse:
    status_code = 403
    headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        raise requests.HTTPError(response=self)

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False


def test_unpublished_file_raises_source_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(*args: object, **kwargs: object) -> FakeResponse:
        return FakeResponse()

    monkeypatch.setattr(s3.requests, "get", fake_get)

    with pytest.raises(s3.SourceUnavailableError, match="belum tersedia"):
        s3.upload_from_http(
            "https://example.test/green_tripdata_2026-09.parquet",
            client=None,
            bucket="test-bucket",
            key="green_tripdata_2026-09.parquet",
        )


def test_unpublished_file_is_not_retried() -> None:
    state = Failed(data=s3.SourceUnavailableError("belum tersedia"))
    assert flow_module.should_retry_upload(None, None, state) is False
    assert flow_module._is_unavailable(state) is True


def test_transient_error_is_retried() -> None:
    state = Failed(data=RuntimeError("connection reset"))
    assert flow_module.should_retry_upload(None, None, state) is True
    assert flow_module._is_unavailable(state) is False
