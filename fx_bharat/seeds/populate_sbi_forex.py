"""Populate forex rates from SBI PDF archives into a configured database."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable
from urllib.request import urlretrieve

from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.sbi_pdf import SBI_ARCHIVE_BASE_URL, SBIPDFDownloader, SBIPDFParser
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
        "--db-url",
        dest="db_url",
        required=True,
        help="Database URL/DSN to write results to",
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


def _download_archive_pdf(
    rate_date: date, resources_root: Path, archive_base_url: str
) -> Path | None:
    """Fetch a historical SBI PDF from the public GitHub archive."""

    destination = resources_root / str(rate_date.year) / str(rate_date.month) / f"{rate_date}.pdf"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        LOGGER.info("Historical SBI PDF for %s already present at %s", rate_date, destination)
        return destination
    url = f"{archive_base_url.rstrip('/')}/{rate_date.year}/{rate_date.month}/{rate_date}.pdf"
    try:
        LOGGER.info("Downloading historical SBI PDF for %s from %s", rate_date, url)
        urlretrieve(url, destination)
        return destination
    except Exception as exc:  # pragma: no cover - network variability
        LOGGER.warning("Unable to download SBI historical PDF for %s (%s)", rate_date, exc)
        if destination.exists():
            destination.unlink(missing_ok=True)
        return None


def _download_archive_pdfs(
    rate_dates: Iterable[date],
    resources_root: Path,
    archive_base_url: str,
    *,
    max_workers: int = 8,
) -> list[Path]:
    """Download multiple SBI PDFs in parallel."""

    results: list[Path] = []
    tasks = list(rate_dates)
    if not tasks:
        return results
    # Short-circuit for dates that already exist on disk.
    existing: list[Path] = []
    pending_dates: list[date] = []
    for rate_date in tasks:
        dest = resources_root / str(rate_date.year) / str(rate_date.month) / f"{rate_date}.pdf"
        if dest.exists():
            existing.append(dest)
        else:
            pending_dates.append(rate_date)
    results.extend(existing)
    if not pending_dates:
        return results
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_date = {
            executor.submit(
                _download_archive_pdf, rate_date, resources_root, archive_base_url
            ): rate_date
            for rate_date in pending_dates
        }
        for future in as_completed(future_to_date):
            try:
                path = future.result()
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.warning("Parallel download failed: %s", exc)
                continue
            if path:
                results.append(path)
    return results


def seed_sbi_historical(
    *,
    backend: BackendStrategy,
    resource_dir: str | Path = "resources",
    start: date | None = None,
    end: date | None = None,
    download: bool = True,
    archive_base_url: str | None = SBI_ARCHIVE_BASE_URL,
    archive_workers: int = 8,
    incremental: bool = True,
    dry_run: bool = False,
) -> PersistenceResult:
    """Backfill SBI forex data from PDFs and optionally fetch the latest copy.

    ``end`` must be earlier than ``date.today()`` because this helper is intended for
    historical ingestion rather than same-day updates.
    """

    today = date.today()

    if end and end >= today:
        raise ValueError("End date must be earlier than today for historical SBI ingestion")

    if dry_run:
        LOGGER.info("Dry-run enabled; skipping SBI ingestion for %s → %s", start, end)
        return PersistenceResult()
    parser = SBIPDFParser()
    downloader = SBIPDFDownloader()
    resources_root = Path(resource_dir)
    effective_start = start
    if incremental and start is None:
        try:
            checkpoint = backend.ingestion_checkpoint("SBI")
        except NotImplementedError:
            checkpoint = None
        if checkpoint:
            effective_start = checkpoint + timedelta(days=1)
    pending: list[Path] = list(_iter_pdf_paths(resources_root, effective_start, end))
    seen_paths = {path.resolve() for path in pending}
    if download and archive_base_url and (effective_start is not None or end is not None):
        archive_start = effective_start or start or end
        archive_end = end or today - timedelta(days=1)
        if archive_start and archive_end and archive_start <= archive_end:
            archive_dates = [
                archive_start + timedelta(days=offset)
                for offset in range((archive_end - archive_start).days + 1)
            ]
            downloaded = _download_archive_pdfs(
                archive_dates,
                resources_root,
                archive_base_url,
                max_workers=archive_workers,
            )
            for dest in downloaded:
                if dest and dest.resolve() not in seen_paths:
                    pending.append(dest)
                    seen_paths.add(dest.resolve())
    if download and effective_start is None and end is None:
        pending.append(downloader.fetch_latest())

    total = PersistenceResult()
    for pdf_path in pending:
        parsed = parser.parse(pdf_path)
        result = backend.insert_rates(parsed.rates)
        LOGGER.info(
            "Inserted %s SBI rates for %s from %s", result.total, parsed.rate_date, pdf_path
        )
        total.inserted += result.inserted
        total.updated += result.updated
        if parsed.rates:
            latest_day = parsed.rate_date
            update_func = getattr(backend, "update_ingestion_checkpoint", None)
            if callable(update_func):
                update_func("SBI", latest_day)
    return total


def seed_sbi_today(
    *,
    backend: BackendStrategy,
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
    result = backend.insert_rates(parsed.rates)
    LOGGER.info("Inserted %s SBI rates for %s from %s", result.total, parsed.rate_date, destination)
    total.inserted += result.inserted
    total.updated += result.updated
    if parsed.rates:
        update_func = getattr(backend, "update_ingestion_checkpoint", None)
        if callable(update_func):
            update_func("SBI", parsed.rate_date)
    return total


def seed_sbi_forex(*args, **kwargs) -> PersistenceResult:  # type: ignore[explicit-any]
    """Backward compatible alias for :func:`seed_sbi_historical`."""

    return seed_sbi_historical(*args, **kwargs)


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None
    from fx_bharat import FxBharat

    client = FxBharat(db_config=args.db_url)
    backend = client._get_backend_strategy()
    seed_sbi_historical(
        backend=backend,
        resource_dir=args.resource_dir,
        start=start_date,
        end=end_date,
        download=args.download,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
