from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.ingestion.sbi_pdf import SBIPDFParseResult
from fx_bharat.seeds import populate_sbi_forex as sbi_module


def test_seed_sbi_historical_rejects_future_end() -> None:
    tomorrow = date.today().replace(day=min(date.today().day + 1, 28))
    with pytest.raises(ValueError):
        sbi_module.seed_sbi_historical(end=tomorrow)


def test_seed_sbi_historical_dry_run(tmp_path: Path) -> None:
    result = sbi_module.seed_sbi_historical(
        db_path=tmp_path / "sbi.db",
        resource_dir=tmp_path,
        dry_run=True,
    )
    assert result.total == 0


def test_seed_sbi_historical_uses_checkpoint(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "latest.pdf"
    pdf_path.write_bytes(b"dummy")

    class _DummyParser:
        def parse(self, path: str | Path):  # type: ignore[override]
            return SBIPDFParseResult(
                rate_date=date(2024, 1, 2),
                rates=[ForexRateRecord(rate_date=date(2024, 1, 2), currency="USD", rate=82.5)],
            )

    class _DummyDownloader:
        def fetch_latest(self) -> Path:  # pragma: no cover - should not be called
            return pdf_path

    class _DummyManager:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def ingestion_checkpoint(self, _source: str):
            return date(2024, 1, 1)

        def latest_rate_date(self, _source: str):
            return None

        def insert_rates(self, _rows):
            return sbi_module.PersistenceResult(inserted=1)

        def update_ingestion_checkpoint(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(sbi_module, "SBIPDFParser", _DummyParser)
    monkeypatch.setattr(sbi_module, "SBIPDFDownloader", lambda: _DummyDownloader())
    monkeypatch.setattr(sbi_module, "SQLiteManager", lambda *_args, **_kwargs: _DummyManager())
    monkeypatch.setattr(sbi_module, "_iter_pdf_paths", lambda *_args, **_kwargs: iter([]))

    result = sbi_module.seed_sbi_historical(
        db_path=tmp_path / "sbi.db",
        resource_dir=tmp_path,
        download=True,
    )

    assert result.total == 0


def test_seed_sbi_historical_downloads_latest(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "latest.pdf"
    pdf_path.write_bytes(b"dummy")

    class _DummyParser:
        def parse(self, path: str | Path):  # type: ignore[override]
            return SBIPDFParseResult(
                rate_date=date(2024, 1, 2),
                rates=[ForexRateRecord(rate_date=date(2024, 1, 2), currency="USD", rate=82.5)],
            )

    class _DummyDownloader:
        def fetch_latest(self) -> Path:
            return pdf_path

    class _DummyManager:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def ingestion_checkpoint(self, _source: str):
            return None

        def latest_rate_date(self, _source: str):
            return None

        def insert_rates(self, _rows):
            return sbi_module.PersistenceResult(inserted=1)

        def update_ingestion_checkpoint(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(sbi_module, "SBIPDFParser", _DummyParser)
    monkeypatch.setattr(sbi_module, "SBIPDFDownloader", lambda: _DummyDownloader())
    monkeypatch.setattr(sbi_module, "SQLiteManager", lambda *_args, **_kwargs: _DummyManager())
    monkeypatch.setattr(sbi_module, "_iter_pdf_paths", lambda *_args, **_kwargs: iter([]))

    result = sbi_module.seed_sbi_historical(
        db_path=tmp_path / "sbi.db",
        resource_dir=tmp_path,
        download=True,
    )

    assert result.total == 1


def test_seed_sbi_today_persists_pdf(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "latest.pdf"
    pdf_path.write_bytes(b"dummy")
    rates = [ForexRateRecord(rate_date=date(2024, 1, 2), currency="USD", rate=82.5, source="SBI")]

    class _DummyParser:
        def parse(self, path: str | Path):  # type: ignore[override]
            return SBIPDFParseResult(rate_date=date(2024, 1, 2), rates=rates)

    class _DummyDownloader:
        def __init__(self, download_dir: str | Path | None = None) -> None:
            self.download_dir = Path(download_dir) if download_dir else tmp_path

        def fetch_latest(self) -> Path:
            return pdf_path

    monkeypatch.setattr(sbi_module, "SBIPDFParser", _DummyParser)
    monkeypatch.setattr(sbi_module, "SBIPDFDownloader", _DummyDownloader)

    result = sbi_module.seed_sbi_today(db_path=tmp_path / "sbi.db", resource_dir=tmp_path)

    assert result.total == 1
    expected = tmp_path / "2024" / "1" / "2024-01-02.pdf"
    assert expected.exists()


def test_seed_sbi_forex_alias(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        sbi_module,
        "seed_sbi_historical",
        lambda **_kwargs: sbi_module.PersistenceResult(inserted=2),
    )
    result = sbi_module.seed_sbi_forex(db_path=tmp_path / "sbi.db")
    assert result.inserted == 2


def test_seed_sbi_main_invokes_historical(monkeypatch) -> None:
    class _Args:
        db_path = "db"
        resource_dir = "resources"
        start = "2024-01-01"
        end = "2024-01-31"
        download = False

    called = {"value": False}

    def _fake_seed(**_kwargs):
        called["value"] = True
        return sbi_module.PersistenceResult()

    monkeypatch.setattr(sbi_module, "parse_args", lambda: _Args())
    monkeypatch.setattr(sbi_module, "seed_sbi_historical", _fake_seed)

    sbi_module.main()

    assert called["value"] is True
