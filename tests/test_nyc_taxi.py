"""Tests for month and parameter resolution."""

from __future__ import annotations

from datetime import date

import pytest

from flows import nyc_taxi as flow_module


def test_day_is_ignored_and_month_is_used() -> None:
    assert flow_module._resolve_trip_month(date(2019, 2, 17)) == date(2019, 2, 1)


def test_string_date_still_accepted() -> None:
    assert flow_module._resolve_trip_month("2019-02-01") == date(2019, 2, 1)


def test_scheduled_run_processes_previous_month(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(flow_module, "_run_reference_date", lambda: date(2026, 3, 15))
    assert flow_module._resolve_trip_month(None) == date(2026, 2, 1)


def test_manual_run_mid_month_processes_previous_month(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(flow_module, "_run_reference_date", lambda: date(2026, 3, 26))
    assert flow_module._resolve_trip_month(None) == date(2026, 2, 1)


def test_run_on_first_of_month_processes_previous_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(flow_module, "_run_reference_date", lambda: date(2026, 3, 1))
    assert flow_module._resolve_trip_month(None) == date(2026, 2, 1)


def test_empty_string_processes_previous_month(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(flow_module, "_run_reference_date", lambda: date(2026, 3, 1))
    assert flow_module._resolve_trip_month("  ") == date(2026, 2, 1)


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
