"""Data models shared across ingestion modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class ForexRateRecord:
    """Representation of a single forex rate row extracted from RBI data."""

    rate_date: date
    currency: str
    rate: float
    source: str = "RBI"
