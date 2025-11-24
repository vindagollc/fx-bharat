"""Data models shared across ingestion modules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal


@dataclass(slots=True)
class ForexRateRecord:
    """Representation of a single forex rate row extracted from RBI data."""

    rate_date: date
    currency: str
    rate: float
    source: str = "RBI"
    tt_buy: float | None = None
    tt_sell: float | None = None
    bill_buy: float | None = None
    bill_sell: float | None = None
    travel_card_buy: float | None = None
    travel_card_sell: float | None = None
    cn_buy: float | None = None
    cn_sell: float | None = None


@dataclass(slots=True)
class LmeRateRecord:
    """Representation of a single LME cash seller row."""

    rate_date: date
    price: float | None
    price_3_month: float | None
    stock: int | None
    metal: Literal["ALUMINUM", "COPPER"] | str = "COPPER"


__all__ = ["ForexRateRecord", "LmeRateRecord"]
