"""Calendar-month helpers. A month is represented by its first day."""

from __future__ import annotations

from datetime import date, datetime


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def add_months(month: date, offset: int) -> date:
    index = month.year * 12 + month.month - 1 + offset
    return date(index // 12, index % 12 + 1, 1)


def months_between(earlier: date, later: date) -> int:
    return (later.year - earlier.year) * 12 + (later.month - earlier.month)


def month_bounds(trip_month: str) -> tuple[date, date]:
    """``"2024-01"`` -> (2024-01-01, 2024-02-01)."""
    start = parse_month(trip_month)
    return start, add_months(start, 1)


def parse_month(value: date | str) -> date:
    """Accept ``YYYY-MM``, ``YYYY-MM-DD`` or a date; the day is ignored."""
    if isinstance(value, datetime):
        return month_start(value.date())
    if isinstance(value, date):
        return month_start(value)
    if isinstance(value, str):
        raw = value.strip()
        try:
            return month_start(date.fromisoformat(raw if len(raw) > 7 else f"{raw}-01"))
        except ValueError as exc:
            raise ValueError(f"Bulan tidak valid: {raw!r}. Gunakan YYYY-MM.") from exc
    raise ValueError(f"Bulan tidak valid: {value!r}.")


def month_range(start: date, end: date) -> list[date]:
    """Every month from ``start`` to ``end`` inclusive, oldest first."""
    if end < start:
        raise ValueError(f"Akhir rentang ({end:%Y-%m}) sebelum awal ({start:%Y-%m}).")
    return [add_months(start, offset) for offset in range(months_between(start, end) + 1)]
