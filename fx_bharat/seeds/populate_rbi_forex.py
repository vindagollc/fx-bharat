"""CLI + helpers for populating (seeding) the forex database from RBI archives."""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.rbi_csv import RBICSVParser
from fx_bharat.ingestion.rbi_selenium import RBISeleniumClient
from fx_bharat.ingestion.rbi_workbook import RBIWorkbookConverter
from fx_bharat.utils.date_range import month_ranges, parse_date
from fx_bharat.utils.logger import get_logger
from fx_bharat.utils.rbi import enforce_rbi_min_date

LOGGER = get_logger(__name__)

__all__ = ["seed_rbi_forex", "parse_args", "main"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--db",
        dest="db_path",
        default=str(DEFAULT_SQLITE_DB_PATH),
        help="SQLite database path",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        default=True,
        help="Disable headless Chrome mode",
    )
    parser.add_argument("--download-dir", dest="download_dir", help="Optional download directory")
    return parser.parse_args()


def _log_chunk_result(chunk_label: str, result: PersistenceResult) -> None:
    LOGGER.info(
        "%s → inserted %s rows, updated %s rows (total %s)",
        chunk_label,
        result.inserted,
        result.updated,
        result.total,
    )


def seed_rbi_forex(
    start: str,
    end: str,
    *,
    db_path: str | Path = DEFAULT_SQLITE_DB_PATH,
    headless: bool = True,
    download_dir: str | Path | None = None,
    incremental: bool = True,
    dry_run: bool = False,
) -> PersistenceResult:
    """Seed RBI forex data between ``start`` and ``end`` dates."""

    start_date = parse_date(start)
    end_date = parse_date(end)
    enforce_rbi_min_date(start_date, end_date)
    if dry_run:
        LOGGER.info("Dry-run enabled; skipping RBI ingestion for %s → %s", start, end)
        return PersistenceResult()
    converter = RBIWorkbookConverter()
    csv_parser = RBICSVParser()
    download_path = Path(download_dir) if download_dir else None
    total = PersistenceResult()
    with SQLiteManager(db_path) as manager:
        effective_start = start_date
        if incremental:
            checkpoint = manager.ingestion_checkpoint("RBI") or manager.latest_rate_date("RBI")
            if checkpoint and checkpoint >= start_date:
                effective_start = checkpoint + timedelta(days=1)
        if effective_start > end_date:
            LOGGER.info("RBI data already ingested up to %s; nothing to do", end_date)
            return total
        date_chunks = list(month_ranges(effective_start, end_date))
        with RBISeleniumClient(download_dir=download_path, headless=headless) as client:
            for chunk in date_chunks:
                LOGGER.info("Processing %s - %s", chunk.start, chunk.end)
                excel_path = client.fetch_excel(chunk.start, chunk.end)
                csv_path = converter.to_csv(
                    excel_path,
                    start_date=chunk.start,
                    end_date=chunk.end,
                    output_dir=client.download_dir,
                )
                csv_rows = csv_parser.parse(csv_path)
                result = manager.insert_rates(csv_rows)
                _log_chunk_result(f"Chunk {chunk.start} → {chunk.end}", result)
                total.inserted += result.inserted
                total.updated += result.updated
                if result.inserted:
                    latest_day = max(row.rate_date for row in csv_rows)
                    manager.update_ingestion_checkpoint("RBI", latest_day)
    LOGGER.info(
        "Seeding finished: inserted %s rows, updated %s rows (total %s)",
        total.inserted,
        total.updated,
        total.total,
    )
    return total


def main() -> None:
    args = parse_args()
    seed_rbi_forex(
        args.start,
        args.end,
        db_path=args.db_path,
        headless=args.headless,
        download_dir=args.download_dir,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
