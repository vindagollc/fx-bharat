"""Ingestion helpers for London Metal Exchange cash seller prices."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Literal

import pandas as pd
import requests

from fx_bharat.ingestion.models import LmeRateRecord
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

LME_URLS: dict[Literal["COPPER", "ALUMINUM"], str] = {
    "COPPER": "https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Cu_cash",
    "ALUMINUM": "https://www.westmetall.com/en/markdaten.php?action=table&field=LME_Al_cash",
}


@dataclass(slots=True)
class LmeTableParseResult:
    metal: Literal["COPPER", "ALUMINUM"]
    rows: list[LmeRateRecord]


def _normalise_metal(metal: str) -> Literal["COPPER", "ALUMINUM"]:
    upper = metal.upper()
    if upper in {"CU", "COPPER"}:
        return "COPPER"
    if upper in {"AL", "ALUMINUM", "ALUMINIUM"}:
        return "ALUMINUM"
    raise ValueError(f"Unsupported LME metal: {metal}")


def _parse_float(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    cleaned = re.sub(r"[^0-9,.-]", "", str(value))
    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def _coerce_date(value: object) -> date | None:
    try:
        parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _find_column(columns: Iterable[object], keywords: set[str]) -> object | None:
    for column in columns:
        lower = str(column).lower()
        if any(keyword in lower for keyword in keywords):
            return column
    return None


def parse_lme_table(html: str, metal: str) -> LmeTableParseResult:
    """Parse Westmetall HTML tables into structured records."""

    normalised = _normalise_metal(metal)
    tables = pd.read_html(html, flavor="bs4")
    if not tables:
        raise ValueError("No tables found in supplied HTML")
    frame = tables[0]
    frame.columns = [str(col).strip() for col in frame.columns]
    date_col = _find_column(frame.columns, {"date", "datum"}) or frame.columns[0]
    usd_col = _find_column(frame.columns, {"usd", "$"})
    eur_col = _find_column(frame.columns, {"eur", "€"})
    usd_change_col = _find_column(frame.columns, {"usd change", "usd +/-", "usd +/-"})
    eur_change_col = _find_column(frame.columns, {"eur change", "eur +/-", "eur +/-"})
    rows: list[LmeRateRecord] = []
    for _, row in frame.iterrows():
        rate_date = _coerce_date(row.get(date_col))
        if rate_date is None:
            continue
        record = LmeRateRecord(
            rate_date=rate_date,
            usd_price=_parse_float(row.get(usd_col)) if usd_col else None,
            eur_price=_parse_float(row.get(eur_col)) if eur_col else None,
            usd_change=_parse_float(row.get(usd_change_col)) if usd_change_col else None,
            eur_change=_parse_float(row.get(eur_change_col)) if eur_change_col else None,
            metal=normalised,
        )
        rows.append(record)
    return LmeTableParseResult(metal=normalised, rows=rows)


def fetch_lme_rates(metal: str, *, session: requests.Session | None = None) -> LmeTableParseResult:
    """Download LME cash seller table and return parsed rows."""

    normalised = _normalise_metal(metal)
    url = LME_URLS[normalised]
    sess = session or requests.Session()
    sess.headers.setdefault("User-Agent", "fx-bharat-lme-ingestor/1.0")
    response = sess.get(url, timeout=30)
    response.raise_for_status()
    LOGGER.info("Fetched %s data from %s", normalised, url)
    return parse_lme_table(response.text, normalised)


__all__ = ["fetch_lme_rates", "parse_lme_table", "LmeTableParseResult", "LME_URLS"]
