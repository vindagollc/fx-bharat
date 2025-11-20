"""RBI-specific helpers and invariants used across the package."""

from __future__ import annotations

from datetime import date

RBI_MIN_AVAILABLE_DATE = date(2022, 4, 12)
RBI_MIN_DATE_MESSAGE = "RBI do not provide the data before 12/04/2022."


def enforce_rbi_min_date(*dates: date) -> None:
    """Ensure all provided dates are on/after :data:`RBI_MIN_AVAILABLE_DATE`."""

    if any(day < RBI_MIN_AVAILABLE_DATE for day in dates):
        raise ValueError(RBI_MIN_DATE_MESSAGE)


__all__ = ["RBI_MIN_AVAILABLE_DATE", "RBI_MIN_DATE_MESSAGE", "enforce_rbi_min_date"]
