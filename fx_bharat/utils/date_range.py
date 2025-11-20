"""Utility helpers for generating RBI-friendly date ranges."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator, Tuple


@dataclass(frozen=True)
class DateRange:
    """Container representing a closed date range."""

    start: date
    end: date

    def as_tuple(self) -> Tuple[date, date]:
        """Return the range as a tuple of ``(start, end)``."""
        return (self.start, self.end)


def parse_date(value: str | date) -> date:
    """Parse a date string in ISO format to :class:`date`."""

    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def month_ranges(start: str | date, end: str | date) -> Iterator[DateRange]:
    """Yield date ranges aligned by month within the inclusive window."""

    start_date = parse_date(start)
    end_date = parse_date(end)
    if start_date > end_date:
        raise ValueError("start date must not be after end date")

    current = start_date
    while current <= end_date:
        month_end = _end_of_month(current)
        chunk_end = min(month_end, end_date)
        yield DateRange(start=current, end=chunk_end)
        current = chunk_end + timedelta(days=1)


def split_ranges(start: str | date, end: str | date, window_days: int) -> Iterator[DateRange]:
    """Split the period into smaller windows of ``window_days`` length."""

    if window_days <= 0:
        raise ValueError("window_days must be positive")

    start_date = parse_date(start)
    end_date = parse_date(end)
    if start_date > end_date:
        raise ValueError("start date must not be after end date")

    current = start_date
    delta = timedelta(days=window_days - 1)
    while current <= end_date:
        chunk_end = min(current + delta, end_date)
        yield DateRange(start=current, end=chunk_end)
        current = chunk_end + timedelta(days=1)


def _end_of_month(day: date) -> date:
    """Return the last day of the month for ``day``."""

    if day.month == 12:
        return date(day.year, 12, 31)
    first_next_month = date(day.year, day.month + 1, 1)
    return first_next_month - timedelta(days=1)
