"""Tests for the NYC TLC source description."""

from __future__ import annotations

from datetime import date

import pytest

from nyc_taxi_elt.tlc import file_name, resolve_taxi_types, trip_files


def test_resolve_taxi_types():
    assert resolve_taxi_types("all") == ["yellow", "green"]
    assert resolve_taxi_types(None) == ["yellow", "green"]
    assert resolve_taxi_types(" Yellow ") == ["yellow"]
    with pytest.raises(ValueError):
        resolve_taxi_types("blue")


def test_file_name():
    assert file_name("green", date(2024, 1, 1)) == "green_tripdata_2024-01.parquet"


def test_trip_files_use_base_url(monkeypatch):
    monkeypatch.setenv("TLC_BASE_URL", "https://mirror.test/data/")
    assert trip_files(["yellow"], date(2024, 2, 1)) == [
        {
            "filename": "yellow_tripdata_2024-02.parquet",
            "taxi_type": "yellow",
            "trip_month": "2024-02",
            "source_uri": "https://mirror.test/data/yellow_tripdata_2024-02.parquet",
        }
    ]
