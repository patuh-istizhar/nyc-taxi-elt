"""Unit tests for the ingest flow's planning helpers and CLI (no I/O)."""

from __future__ import annotations

from datetime import date

import pytest

from nyc_taxi_elt.flows import ingest as flow_module
from nyc_taxi_elt.flows.ingest import (
    _expected_files,
    _incomplete_months,
    _stale_months,
    resolve_months,
)

TODAY = date(2024, 7, 15)


def test_resolve_months_single_and_range():
    assert resolve_months(None, None, today=TODAY) == []
    assert resolve_months("2024-03-15", None, today=TODAY) == [date(2024, 3, 1)]
    assert resolve_months("2024-01", "2024-03", today=TODAY) == [
        date(2024, 1, 1),
        date(2024, 2, 1),
        date(2024, 3, 1),
    ]


def test_resolve_months_rejects_bad_input():
    with pytest.raises(ValueError, match="butuh start_month"):
        resolve_months(None, "2024-03", today=TODAY)
    with pytest.raises(ValueError, match="belum tersedia"):
        resolve_months("2024-06", "2024-08", today=TODAY)
    with pytest.raises(ValueError, match="sebelum awal"):
        resolve_months("2024-03", "2024-01", today=TODAY)


def test_incomplete_months():
    loaded = {"2024-06": {"yellow_tripdata_2024-06.parquet"}}
    incomplete = _incomplete_months(TODAY, loaded, ["yellow", "green"])
    assert incomplete[0] == date(2024, 6, 1)
    assert len(incomplete) == 6
    assert _incomplete_months(TODAY, loaded, ["yellow"])[0] == date(2024, 5, 1)


def test_expected_files():
    assert _expected_files(date(2024, 1, 1), ["yellow", "green"]) == {
        "yellow_tripdata_2024-01.parquet",
        "green_tripdata_2024-01.parquet",
    }


def test_stale_months():
    assert _stale_months([date(2024, 6, 1), date(2024, 1, 1)], date(2024, 7, 1)) == [
        date(2024, 1, 1)
    ]


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        ([], ("all", None, None, False)),
        (["--month", "2024-01", "--taxi-type", "green"], ("green", "2024-01", "2024-01", False)),
        (["--from", "2023-01", "--to", "2023-12", "--force"], ("all", "2023-01", "2023-12", True)),
        (["--from", "2023-01"], ("all", "2023-01", None, False)),
    ],
)
def test_cli(monkeypatch, argv, expected):
    calls = []
    monkeypatch.setattr(
        flow_module,
        "ingest",
        lambda **kwargs: calls.append(
            (kwargs["taxi_type"], kwargs["start_month"], kwargs["end_month"], kwargs["force"])
        ),
    )
    flow_module.main(argv)
    assert calls == [expected]


@pytest.mark.parametrize(
    "argv", [["--to", "2024-01"], ["--month", "2024-01", "--from", "2024-01"], ["--month", "x"]]
)
def test_cli_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit):
        flow_module.main(argv)
