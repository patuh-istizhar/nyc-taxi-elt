from datetime import date

import pytest

from nyc_taxi_elt.flows.ingest import add_months, month_range, months_to_load, parse_month


def test_parse_month():
    assert parse_month("2024-03") == date(2024, 3, 1)
    assert parse_month("2024-03-15") == date(2024, 3, 1)
    with pytest.raises(ValueError):
        parse_month("March")


def test_add_months_crosses_years():
    assert add_months(date(2024, 1, 1), -1) == date(2023, 12, 1)
    assert add_months(date(2024, 12, 1), 1) == date(2025, 1, 1)


def test_month_range_includes_both_ends():
    assert month_range(date(2023, 11, 1), date(2024, 1, 1)) == [
        date(2023, 11, 1),
        date(2023, 12, 1),
        date(2024, 1, 1),
    ]
    with pytest.raises(ValueError):
        month_range(date(2024, 2, 1), date(2024, 1, 1))


def test_months_to_load():
    assert months_to_load("2024-01", None) == [date(2024, 1, 1)]
    assert len(months_to_load("2023-01", "2023-12")) == 12
    assert len(months_to_load(None, None)) == 3
    with pytest.raises(ValueError, match="belum selesai"):
        months_to_load(f"{date.today():%Y-%m}", None)
