from __future__ import annotations

from datetime import date

import pytest

from fx_bharat.utils.date_range import month_ranges, parse_date, split_ranges


def test_parse_date_accepts_date_instance() -> None:
    today = date(2024, 1, 1)
    assert parse_date(today) == today


def test_month_ranges_rejects_invalid_order() -> None:
    with pytest.raises(ValueError):
        list(month_ranges("2024-02-01", "2024-01-01"))


def test_split_ranges_rejects_non_positive_window() -> None:
    with pytest.raises(ValueError):
        list(split_ranges("2024-01-01", "2024-01-10", 0))
