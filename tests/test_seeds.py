"""Tests covering the RBI seeding orchestration helpers."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import List

import pytest

from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.seeds import populate_rbi_forex as seeds_module


@dataclass
class _RecordedCall:
    workbook: Path
    start: date
    end: date
    output_dir: Path


def test_seed_rbi_forex_coordinates_pipeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recorded: List[_RecordedCall] = []
    inserted_rows: List[ForexRateRecord] = []

    class DummyConverter:
        def to_csv(
            self, workbook_path: Path, *, start_date: date, end_date: date, output_dir: Path
        ) -> Path:
            recorded.append(_RecordedCall(workbook_path, start_date, end_date, output_dir))
            csv_path = output_dir / "rates.csv"
            csv_path.write_text("csv")
            return csv_path

    class DummyParser:
        def parse(self, csv_path: Path) -> List[ForexRateRecord]:
            assert csv_path.exists()
            return [
                ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
                ForexRateRecord(rate_date=date(2024, 1, 1), currency="EUR", rate=89.1),
            ]

    class DummyManager:
        def __init__(self, db_path: Path) -> None:
            self.db_path = db_path

        def __enter__(self) -> "DummyManager":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def latest_rate_date(self, source: str):  # type: ignore[no-untyped-def]
            return None

        def insert_rates(self, rows: List[ForexRateRecord]):
            inserted_rows.extend(rows)
            from fx_bharat.db.sqlite_manager import PersistenceResult

            return PersistenceResult(inserted=len(rows), updated=0)

    class DummyClient:
        def __init__(self, *, download_dir: Path | None, headless: bool) -> None:
            self.download_dir = download_dir or tmp_path
            (self.download_dir).mkdir(parents=True, exist_ok=True)
            self.headless = headless

        def __enter__(self) -> "DummyClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def fetch_excel(self, start: date, end: date) -> Path:
            excel_path = Path(self.download_dir) / f"{start}_{end}.xls"
            excel_path.write_text("excel")
            return excel_path

    monkeypatch.setattr(seeds_module, "RBIWorkbookConverter", lambda: DummyConverter())
    monkeypatch.setattr(seeds_module, "RBICSVParser", lambda: DummyParser())
    monkeypatch.setattr(seeds_module, "RBISeleniumClient", DummyClient)
    monkeypatch.setattr(seeds_module, "SQLiteManager", lambda db_path: DummyManager(Path(db_path)))
    monkeypatch.setattr(
        seeds_module,
        "month_ranges",
        lambda start, end: [SimpleNamespace(start=date(2024, 1, 1), end=date(2024, 1, 31))],
    )

    result = seeds_module.seed_rbi_forex("2024-01-01", "2024-01-31", db_path=tmp_path / "fx.db")

    assert result.inserted == 2
    assert recorded
    assert inserted_rows


def test_seed_rbi_forex_rejects_dates_before_rbi_minimum(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="RBI do not provide the data before 12/04/2022"):
        seeds_module.seed_rbi_forex("2022-04-01", "2022-04-30", db_path=tmp_path / "fx.db")


def test_seed_rbi_forex_dry_run_skips_download(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class DummyManager:
        def __init__(self, db_path: Path) -> None:
            self.db_path = db_path

        def __enter__(self) -> "DummyManager":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def latest_rate_date(self, source: str):  # type: ignore[no-untyped-def]
            return None

        def insert_rates(self, rows):  # type: ignore[no-untyped-def]
            raise AssertionError("insert_rates should not be called in dry_run")

    def _no_client(**kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("RBISeleniumClient should not be instantiated during dry_run")

    monkeypatch.setattr(seeds_module, "SQLiteManager", lambda db_path: DummyManager(Path(db_path)))
    monkeypatch.setattr(seeds_module, "RBISeleniumClient", _no_client)

    result = seeds_module.seed_rbi_forex(
        "2024-01-01", "2024-01-31", db_path=tmp_path / "fx.db", dry_run=True
    )

    assert result.total == 0


def test_seeds_module_lazy_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()

    def _stub_seed(*args, **kwargs):  # type: ignore[no-untyped-def]
        return sentinel

    monkeypatch.setattr("fx_bharat.seeds.populate_rbi_forex.seed_rbi_forex", _stub_seed)

    import importlib

    import fx_bharat.seeds as seeds  # noqa: PLC0415

    importlib.reload(seeds)

    assert seeds.seed_rbi_forex() is sentinel


def test_scripts_module_reexports_main(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {}

    def _fake_main() -> None:
        called["ran"] = True

    monkeypatch.setattr("fx_bharat.seeds.populate_rbi_forex.main", _fake_main)

    import importlib

    import fx_bharat.scripts.populate_rbi_forex as script  # noqa: PLC0415

    importlib.reload(script)

    assert script.main is _fake_main


def test_parse_args_consumes_expected_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "seed_rbi_forex.py",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-31",
            "--db",
            "custom.db",
            "--no-headless",
            "--download-dir",
            "downloads",
        ],
    )

    args = seeds_module.parse_args()

    assert args.start == "2024-01-01"
    assert args.end == "2024-01-31"
    assert args.db_path == "custom.db"
    assert args.headless is False
    assert args.download_dir == "downloads"


def test_main_invokes_seed_with_parsed_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyNamespace:
        start = "2024-01-01"
        end = "2024-01-02"
        db_path = "path.db"
        headless = True
        download_dir = None
        dry_run = False

    monkeypatch.setattr(seeds_module, "parse_args", lambda: DummyNamespace)
    called: dict[str, tuple] = {}

    def _fake_seed(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["args"] = args
        called["kwargs"] = kwargs

    monkeypatch.setattr(seeds_module, "seed_rbi_forex", _fake_seed)

    seeds_module.main()

    assert called["args"] == ("2024-01-01", "2024-01-02")
    assert called["kwargs"]["db_path"] == "path.db"
