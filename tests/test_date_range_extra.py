from __future__ import annotations

from datetime import date

import pytest

from fx_bharat.utils.date_range import (
    DateRange,
    _end_of_month,
    month_ranges,
    parse_date,
    split_ranges,
)


def test_parse_date_accepts_date_instance() -> None:
    today = date(2024, 1, 1)
    assert parse_date(today) == today


def test_month_ranges_rejects_invalid_order() -> None:
    with pytest.raises(ValueError):
        list(month_ranges("2024-02-01", "2024-01-01"))


def test_split_ranges_rejects_non_positive_window() -> None:
    with pytest.raises(ValueError):
        list(split_ranges("2024-01-01", "2024-01-10", 0))


def test_date_range_as_tuple() -> None:
    range_item = DateRange(start=date(2024, 1, 1), end=date(2024, 1, 31))
    assert range_item.as_tuple() == (date(2024, 1, 1), date(2024, 1, 31))


def test_end_of_month_december() -> None:
    assert _end_of_month(date(2024, 12, 15)) == date(2024, 12, 31)
