"""Ingestion helpers for London Metal Exchange cash seller prices."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Literal

import pandas as pd
import requests
from bs4 import BeautifulSoup

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


def _parse_int(value: object | None) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    try:
        return int(parsed)
    except (TypeError, ValueError):
        return None


def _coerce_date(value: object) -> date | None:
    if isinstance(value, str):
        value = re.sub(r"\s+", " ", value.strip())
        value = re.sub(r"([A-Za-z]{2,})\s+([A-Za-z]{2,})", r"\1\2", value)
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
    """Parse Westmetall HTML tables into structured records.

    The source pages render one table per year (anchored by ``#yYYYY``) and
    sometimes repeat header rows inside each table body. Rather than relying on
    :func:`pandas.read_html` to guess the header structure, we parse tables with
    BeautifulSoup to preserve every data row and then map the appropriate
    numeric columns (cash-settlement first, otherwise any numeric column).
    """

    def _cell_text(cell) -> str:
        return " ".join(cell.stripped_strings).strip()

    normalised = _normalise_metal(metal)
    soup = BeautifulSoup(html, "html.parser")

    tables: list[object] = []
    for anchor in soup.find_all("a", id=re.compile(r"^y\d{4}$")):
        table = anchor.find_next("table")
        if table:
            tables.append(table)
    if not tables:
        tables = soup.find_all("table")
    if not tables:
        raise ValueError("No tables found in supplied HTML")

    dataframes: list[pd.DataFrame] = []
    for table in tables:
        headers = [_cell_text(th) for th in table.find_all("th") if _cell_text(th)]
        rows: list[list[str]] = []
        for tr in table.find_all("tr"):
            cols = tr.find_all("td")
            if not cols:
                continue
            rows.append([_cell_text(col) for col in cols])
        if not rows:
            continue
        if headers and len(headers) == len(rows[0]):
            frame = pd.DataFrame(rows, columns=headers)
        else:
            frame = pd.DataFrame(rows)
        dataframes.append(frame)

    if not dataframes:
        raise ValueError("No data rows found in supplied HTML")

    def _numeric_columns(frame: pd.DataFrame, *, exclude: set[object]) -> list[object]:
        numerics: list[object] = []
        for column in frame.columns:
            if column in exclude:
                continue
            series = frame[column]
            if series.apply(_parse_float).notna().any():
                numerics.append(column)
        return numerics

    rows: list[LmeRateRecord] = []
    for frame in dataframes:
        frame.columns = [str(col).strip() for col in frame.columns]
        date_col = _find_column(frame.columns, {"date", "datum"}) or frame.columns[0]
        price_col = _find_column(frame.columns, {"cash", "settlement", "seller"})
        three_month_col = _find_column(frame.columns, {"3-month", "3 month", "3m"})
        stock_col = _find_column(frame.columns, {"stock", "stocks"})

        numeric_cols = _numeric_columns(frame, exclude={date_col})
        if price_col is None and numeric_cols:
            price_col = numeric_cols[0]
        if three_month_col is None and len(numeric_cols) > 1:
            fallback_index = 1 if numeric_cols[0] == price_col else 0
            three_month_candidates = [col for col in numeric_cols if col != price_col]
            if three_month_candidates:
                three_month_col = three_month_candidates[0]
            elif len(numeric_cols) > fallback_index:
                three_month_col = numeric_cols[fallback_index]
        if stock_col is None and numeric_cols:
            candidates = [col for col in numeric_cols if col not in {price_col, three_month_col}]
            if candidates:
                stock_col = candidates[-1]
        for _, row in frame.iterrows():
            rate_date = _coerce_date(row.get(date_col))
            if rate_date is None:
                continue
            record = LmeRateRecord(
                rate_date=rate_date,
                price=_parse_float(row.get(price_col)) if price_col else None,
                price_3_month=_parse_float(row.get(three_month_col)) if three_month_col else None,
                stock=_parse_int(row.get(stock_col)) if stock_col else None,
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
