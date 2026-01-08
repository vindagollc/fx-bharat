"""CLI helpers for seeding LME Copper and Aluminum prices."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Literal

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.lme import LME_URLS, LmeTableParseResult, fetch_lme_rates, parse_lme_table
from fx_bharat.ingestion.models import LmeRateRecord
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

LmeMetal = Literal["COPPER", "ALUMINUM"]


@dataclass(slots=True)
class SeedResult:
    metal: LmeMetal
    rows: PersistenceResult


def _filter_rows(
    rows: Iterable[LmeRateRecord], *, start: date | None = None, end: date | None = None
) -> list[LmeRateRecord]:
    filtered: list[LmeRateRecord] = []
    for row in rows:
        if start and row.rate_date < start:
            continue
        if end and row.rate_date > end:
            continue
        filtered.append(row)
    return filtered


def _normalise_metal(metal: str) -> LmeMetal:
    upper = metal.upper()
    if upper in {"CU", "COPPER"}:
        return "COPPER"
    if upper in {"AL", "ALUMINUM", "ALUMINIUM"}:
        return "ALUMINUM"
    raise ValueError(f"Unsupported LME metal: {metal}")


def seed_lme_prices(
    metal: str,
    *,
    db_path: str | Path = DEFAULT_SQLITE_DB_PATH,
    start: date | None = None,
    end: date | None = None,
    html: str | None = None,
    dry_run: bool = False,
) -> SeedResult:
    """Seed LME metal prices into SQLite."""

    normalised = _normalise_metal(metal)
    if dry_run:
        LOGGER.info("Dry-run enabled; skipping %s ingestion", normalised)
        return SeedResult(metal=normalised, rows=PersistenceResult())
    parse_result: LmeTableParseResult
    if html:
        parse_result = parse_lme_table(html, normalised)
    else:
        parse_result = fetch_lme_rates(normalised)
    filtered_rows = _filter_rows(parse_result.rows, start=start, end=end)
    with SQLiteManager(db_path) as manager:
        result = manager.insert_lme_rates(normalised, filtered_rows)
        if filtered_rows:
            latest_day = max(row.rate_date for row in filtered_rows)
            manager.update_ingestion_checkpoint(f"LME_{normalised}", latest_day)
    LOGGER.info(
        "Seeded %s LME rows (inserted=%s, updated=%s)",
        normalised,
        result.inserted,
        result.updated,
    )
    return SeedResult(metal=normalised, rows=result)


def seed_lme_copper(**kwargs) -> SeedResult:
    return seed_lme_prices("COPPER", **kwargs)


def seed_lme_aluminum(**kwargs) -> SeedResult:
    return seed_lme_prices("ALUMINUM", **kwargs)


__all__ = [
    "seed_lme_prices",
    "seed_lme_copper",
    "seed_lme_aluminum",
    "LME_URLS",
    "SeedResult",
]
