"""Populate forex rates from SBI PDF archives into SQLite."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Iterable

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.sbi_pdf import SBIPDFDownloader, SBIPDFParser
from fx_bharat.utils.logger import get_logger

LOGGER = get_logger(__name__)

__all__ = ["seed_sbi_forex", "parse_args", "main"]


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
    parser.add_argument(
        "--skip-download",
        dest="download",
        action="store_false",
        default=True,
        help="Do not fetch the latest PDF from SBI before inserting",
    )
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


def seed_sbi_forex(
    *,
    db_path: str | Path = DEFAULT_SQLITE_DB_PATH,
    resource_dir: str | Path = "resources",
    start: date | None = None,
    end: date | None = None,
    download: bool = True,
) -> PersistenceResult:
    """Backfill SBI forex data from PDFs and optionally fetch the latest copy."""

    parser = SBIPDFParser()
    downloader = SBIPDFDownloader()
    resources_root = Path(resource_dir)
    pending: list[Path] = list(_iter_pdf_paths(resources_root, start, end))
    if download:
        pending.append(downloader.fetch_latest())

    total = PersistenceResult()
    with SQLiteManager(db_path) as manager:
        for pdf_path in pending:
            parsed = parser.parse(pdf_path)
            result = manager.insert_rates(parsed.rates)
            LOGGER.info(
                "Inserted %s SBI rates for %s from %s", result.total, parsed.rate_date, pdf_path
            )
            total.inserted += result.inserted
            total.updated += result.updated
    return total


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    seed_sbi_forex(
        db_path=args.db_path,
        resource_dir=args.resource_dir,
        start=start_date,
        end=end_date,
        download=args.download,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
