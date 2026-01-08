from __future__ import annotations

from datetime import date
from pathlib import Path

from fx_bharat.seeds import populate_rbi_forex
from fx_bharat.seeds.populate_rbi_forex import PersistenceResult, RBINoReferenceRateError


class _DummyManager:
    def __init__(self) -> None:
        self.insert_calls: list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def ingestion_checkpoint(self, _source: str):
        return None

    def latest_rate_date(self, _source: str):
        return None

    def insert_rates(self, rows):
        self.insert_calls.append(rows)
        return PersistenceResult(inserted=len(rows), updated=0)

    def update_ingestion_checkpoint(
        self, _source: str, _day: date
    ) -> None:  # pragma: no cover - unused
        return None


class _DummyCSVParser:
    def parse(self, _path: Path):  # pragma: no cover - should never be called when RBI data missing
        raise AssertionError("parse should not be called")


class _DummyConverter:
    def to_csv(
        self, *_args, **_kwargs
    ):  # pragma: no cover - should never be called when RBI data missing
        raise AssertionError("to_csv should not be called")


class _DummyClient:
    download_dir = Path("/tmp")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def fetch_excel(self, *_args, **_kwargs):
        raise RBINoReferenceRateError("not ready")


def test_seed_rbi_forex_stops_on_missing_reference_rate(monkeypatch):
    monkeypatch.setattr(
        populate_rbi_forex, "SQLiteManager", lambda *_args, **_kwargs: _DummyManager()
    )
    monkeypatch.setattr(populate_rbi_forex, "RBISeleniumClient", lambda **_kwargs: _DummyClient())
    monkeypatch.setattr(populate_rbi_forex, "RBICSVParser", lambda: _DummyCSVParser())
    monkeypatch.setattr(populate_rbi_forex, "RBIWorkbookConverter", lambda: _DummyConverter())

    result = populate_rbi_forex.seed_rbi_forex("2025-11-20", "2025-11-21")

    assert result == PersistenceResult()
