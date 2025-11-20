"""Populate forex rates from SBI PDF archives into SQLite."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.sbi_pdf import SBIPDFDownloader, SBIPDFParser
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

__all__ = ["seed_sbi_forex", "seed_sbi_historical", "seed_sbi_today", "parse_args", "main"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--resources",
        dest="resource_dir",
        default="resources",
        help="Directory containing historical SBI forex PDFs organised by date",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default=str(DEFAULT_SQLITE_DB_PATH),
        help="SQLite database path",
    )
    parser.add_argument(
        "--from",
        dest="start",
        help="Optional start date (YYYY-MM-DD) used to filter resource PDFs",
    )
    parser.add_argument(
        "--to",
        dest="end",
        help="Optional end date (YYYY-MM-DD) used to filter resource PDFs",
    )
    download_group = parser.add_mutually_exclusive_group()
    download_group.add_argument(
        "--download-latest",
        dest="download",
        action="store_true",
        help="Download and insert the latest SBI PDF (ignored when a date range is provided)",
    )
    download_group.add_argument(
        "--skip-download",
        dest="download",
        action="store_false",
        help="Do not fetch the latest PDF from SBI before inserting",
    )
    parser.set_defaults(download=False)
    return parser.parse_args()


def _iter_pdf_paths(resource_dir: Path, start: date | None, end: date | None) -> Iterable[Path]:
    if not resource_dir.exists():
        return
    for path in sorted(resource_dir.rglob("*.pdf")):
        try:
            rate_date = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if start and rate_date < start:
            continue
        if end and rate_date > end:
            continue
        yield path


def seed_sbi_historical(
    *,
    db_path: str | Path = DEFAULT_SQLITE_DB_PATH,
    resource_dir: str | Path = "resources",
    start: date | None = None,
    end: date | None = None,
    download: bool = True,
    incremental: bool = True,
    dry_run: bool = False,
) -> PersistenceResult:
    """Backfill SBI forex data from PDFs and optionally fetch the latest copy.

    ``end`` must be earlier than ``date.today()`` because this helper is intended for
    historical ingestion rather than same-day updates.
    """

    today = date.today()
    if end and end >= today:
        raise ValueError("Historical seeding only supports dates earlier than today")

    if dry_run:
        LOGGER.info("Dry-run enabled; skipping SBI ingestion for %s → %s", start, end)
        return PersistenceResult()
    parser = SBIPDFParser()
    downloader = SBIPDFDownloader()
    resources_root = Path(resource_dir)
    with SQLiteManager(db_path) as manager:
        effective_start = start
        if incremental and start is None:
            checkpoint = manager.ingestion_checkpoint("SBI") or manager.latest_rate_date("SBI")
            if checkpoint:
                effective_start = checkpoint + timedelta(days=1)
        pending: list[Path] = list(_iter_pdf_paths(resources_root, effective_start, end))
        if download and effective_start is None and end is None:
            pending.append(downloader.fetch_latest())

        total = PersistenceResult()
        for pdf_path in pending:
            parsed = parser.parse(pdf_path)
            result = manager.insert_rates(parsed.rates)
            LOGGER.info(
                "Inserted %s SBI rates for %s from %s", result.total, parsed.rate_date, pdf_path
            )
            total.inserted += result.inserted
            total.updated += result.updated
            if result.inserted:
                manager.update_ingestion_checkpoint("SBI", parsed.rate_date)
    return total


def seed_sbi_today(
    *,
    db_path: str | Path = DEFAULT_SQLITE_DB_PATH,
    resource_dir: str | Path = "resources",
    dry_run: bool = False,
) -> PersistenceResult:
    """Download today's SBI PDF, store it under ``resource_dir``, and insert rows."""

    if dry_run:
        LOGGER.info("Dry-run enabled; skipping SBI ingestion for today")
        return PersistenceResult()
    parser = SBIPDFParser()
    resources_root = Path(resource_dir)
    downloader = SBIPDFDownloader(download_dir=resources_root)
    pdf_path = downloader.fetch_latest()
    parsed = parser.parse(pdf_path)
    dated_dir = resources_root / str(parsed.rate_date.year) / str(parsed.rate_date.month)
    dated_dir.mkdir(parents=True, exist_ok=True)
    destination = dated_dir / f"{parsed.rate_date.isoformat()}.pdf"
    if pdf_path.resolve() != destination.resolve():
        destination.write_bytes(Path(pdf_path).read_bytes())

    total = PersistenceResult()
    with SQLiteManager(db_path) as manager:
        result = manager.insert_rates(parsed.rates)
        LOGGER.info(
            "Inserted %s SBI rates for %s from %s", result.total, parsed.rate_date, destination
        )
        total.inserted += result.inserted
        total.updated += result.updated
        if result.inserted:
            manager.update_ingestion_checkpoint("SBI", parsed.rate_date)
    return total


def seed_sbi_forex(*args, **kwargs) -> PersistenceResult:  # type: ignore[explicit-any]
    """Backward compatible alias for :func:`seed_sbi_historical`."""

    return seed_sbi_historical(*args, **kwargs)


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    seed_sbi_historical(
        db_path=args.db_path,
        resource_dir=args.resource_dir,
        start=start_date,
        end=end_date,
        download=args.download,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
