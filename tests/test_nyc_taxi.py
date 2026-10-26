"""Tests for month and parameter resolution."""

from __future__ import annotations

from datetime import date

import pytest

from flows import nyc_taxi as flow_module


def test_day_is_ignored_and_month_is_used() -> None:
    assert flow_module._resolve_trip_month(date(2019, 2, 17)) == date(2019, 2, 1)


def test_string_date_still_accepted() -> None:
    assert flow_module._resolve_trip_month("2019-02-01") == date(2019, 2, 1)


def test_month_offset_crosses_year_boundary() -> None:
    assert flow_module._month_offset(date(2026, 1, 1), -1) == date(2025, 12, 1)
    assert flow_module._month_offset(date(2026, 12, 1), 2) == date(2027, 2, 1)


def test_newest_incomplete_prefers_last_month() -> None:
    assert flow_module._newest_incomplete(date(2026, 10, 5), {}, ["yellow", "green"]) == date(
        2026, 9, 1
    )


def test_newest_incomplete_falls_back_to_late_month() -> None:
    loaded = {
        "2026-09": {
            "yellow_tripdata_2026-09.parquet",
            "green_tripdata_2026-09.parquet",
        }
    }
    assert flow_module._newest_incomplete(date(2026, 10, 5), loaded, ["yellow", "green"]) == date(
        2026, 8, 1
    )


def test_newest_incomplete_needs_every_requested_file() -> None:
    loaded = {"2026-09": {"yellow_tripdata_2026-09.parquet"}}
    assert flow_module._newest_incomplete(date(2026, 10, 5), loaded, ["yellow", "green"]) == date(
        2026, 9, 1
    )
    assert flow_module._newest_incomplete(date(2026, 10, 5), loaded, ["yellow"]) == date(2026, 8, 1)


def test_newest_incomplete_returns_none_when_caught_up() -> None:
    loaded: dict[str, set[str]] = {}
    for month in range(4, 10):
        loaded[f"2026-{month:02d}"] = {f"yellow_tripdata_2026-{month:02d}.parquet"}
    assert flow_module._newest_incomplete(date(2026, 10, 5), loaded, ["yellow"]) is None


def test_invalid_date_rejected() -> None:
    with pytest.raises(ValueError, match="trip_date tidak valid"):
        flow_module._resolve_trip_month("01-2019")


def test_future_month_rejected() -> None:
    with pytest.raises(ValueError, match="belum tersedia"):
        flow_module._resolve_trip_month(date(date.today().year + 1, 1, 1))


@pytest.mark.parametrize(
    ("value", "expected"),
    [("all", ["yellow", "green"]), ("GREEN", ["green"]), (None, ["yellow", "green"])],
)
def test_taxi_types(value: str | None, expected: list[str]) -> None:
    assert flow_module._resolve_taxi_types(value) == expected


def test_invalid_taxi_type() -> None:
    with pytest.raises(ValueError, match="taxi_type tidak valid"):
        flow_module._resolve_taxi_types("blue")


def test_plan_filenames() -> None:
    from prefect import flow

    @flow
    def run_plan() -> list[dict[str, object]]:
        return flow_module.plan.fn("all", date(2019, 1, 1), False)

    trips = run_plan()
    assert [trip["filename"] for trip in trips] == [
        "yellow_tripdata_2019-01.parquet",
        "green_tripdata_2019-01.parquet",
    ]
