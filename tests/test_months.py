"""Tests for calendar-month helpers."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from nyc_taxi_elt.months import (
    add_months,
    month_bounds,
    month_range,
    months_between,
    parse_month,
)


def test_add_months():
    assert add_months(date(2024, 1, 1), -1) == date(2023, 12, 1)
    assert add_months(date(2024, 12, 1), 1) == date(2025, 1, 1)


def test_months_between():
    assert months_between(date(2023, 11, 1), date(2024, 2, 1)) == 3


@pytest.mark.parametrize(
    "value",
    ["2024-03", "2024-03-15", " 2024-03 ", date(2024, 3, 9), datetime(2024, 3, 9, 12)],
)
def test_parse_month(value):
    assert parse_month(value) == date(2024, 3, 1)


@pytest.mark.parametrize("value", ["not-a-month", "2024-13", "", 202403])
def test_parse_month_rejects_garbage(value):
    with pytest.raises(ValueError):
        parse_month(value)


def test_month_range():
    assert month_range(date(2023, 11, 1), date(2024, 1, 1)) == [
        date(2023, 11, 1),
        date(2023, 12, 1),
        date(2024, 1, 1),
    ]
    assert month_range(date(2024, 1, 1), date(2024, 1, 1)) == [date(2024, 1, 1)]
    with pytest.raises(ValueError, match="sebelum awal"):
        month_range(date(2024, 2, 1), date(2024, 1, 1))


def test_month_bounds():
    assert month_bounds("2024-12") == (date(2024, 12, 1), date(2025, 1, 1))
