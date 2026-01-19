from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from fx_bharat.db.sqlite_backend import SQLiteBackend
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.ingestion.sbi_pdf import SBIPDFParseResult
from fx_bharat.seeds import populate_sbi_forex as sbi_module


def test_seed_sbi_historical_rejects_future_end() -> None:
    tomorrow = date.today().replace(day=min(date.today().day + 1, 28))
    with pytest.raises(ValueError):
        sbi_module.seed_sbi_historical(backend=SQLiteBackend(db_path=":memory:"), end=tomorrow)


def test_seed_sbi_historical_dry_run(tmp_path: Path) -> None:
    backend = SQLiteBackend(db_path=tmp_path / "sbi.db")
    result = sbi_module.seed_sbi_historical(
        backend=backend,
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

    monkeypatch.setattr(sbi_module, "SBIPDFParser", _DummyParser)
    monkeypatch.setattr(sbi_module, "SBIPDFDownloader", lambda: _DummyDownloader())
    monkeypatch.setattr(sbi_module, "_iter_pdf_paths", lambda *_args, **_kwargs: iter([]))

    backend = SQLiteBackend(db_path=tmp_path / "sbi.db")
    backend.update_ingestion_checkpoint("SBI", date(2024, 1, 1))
    result = sbi_module.seed_sbi_historical(
        backend=backend,
        resource_dir=tmp_path,
        download=False,
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

    monkeypatch.setattr(sbi_module, "SBIPDFParser", _DummyParser)
    monkeypatch.setattr(sbi_module, "SBIPDFDownloader", lambda: _DummyDownloader())
    monkeypatch.setattr(sbi_module, "_iter_pdf_paths", lambda *_args, **_kwargs: iter([]))

    backend = SQLiteBackend(db_path=tmp_path / "sbi.db")
    result = sbi_module.seed_sbi_historical(
        backend=backend,
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

    backend = SQLiteBackend(db_path=tmp_path / "sbi.db")
    result = sbi_module.seed_sbi_today(backend=backend, resource_dir=tmp_path)

    assert result.total == 1
    expected = tmp_path / "2024" / "1" / "2024-01-02.pdf"
    assert expected.exists()


def test_seed_sbi_forex_alias(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        sbi_module,
        "seed_sbi_historical",
        lambda **_kwargs: sbi_module.PersistenceResult(inserted=2),
    )
    backend = SQLiteBackend(db_path=tmp_path / "sbi.db")
    result = sbi_module.seed_sbi_forex(backend=backend)
    assert result.inserted == 2


def test_seed_sbi_historical_fetches_from_archive(tmp_path: Path, monkeypatch) -> None:
    downloads: list[date] = []

    def _fake_download_batch(
        rate_dates, resources_root: Path, archive_base_url: str, max_workers: int = 8
    ):
        dests: list[Path] = []
        for rate_date in rate_dates:
            downloads.append(rate_date)
            dest = resources_root / str(rate_date.year) / str(rate_date.month) / f"{rate_date}.pdf"
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                dest.write_text("pdf")
            dests.append(dest)
        return dests

    class _DummyParser:
        def parse(self, path: str | Path):  # type: ignore[override]
            rate_date = date.fromisoformat(Path(path).stem)
            return SBIPDFParseResult(
                rate_date=rate_date,
                rates=[
                    ForexRateRecord(rate_date=rate_date, currency="USD", rate=82.5, source="SBI")
                ],
            )

    monkeypatch.setattr(sbi_module, "_download_archive_pdfs", _fake_download_batch)
    monkeypatch.setattr(sbi_module, "SBIPDFParser", _DummyParser)
    monkeypatch.setattr(sbi_module, "_iter_pdf_paths", lambda *_args, **_kwargs: iter([]))

    start_date = date(2020, 1, 4)
    end_date = date(2020, 1, 5)
    backend = SQLiteBackend(db_path=tmp_path / "sbi.db")
    result = sbi_module.seed_sbi_historical(
        backend=backend,
        resource_dir=tmp_path,
        start=start_date,
        end=end_date,
        download=True,
    )

    assert result.inserted == 2
    assert downloads == [start_date, end_date]


def test_seed_sbi_main_invokes_historical(monkeypatch) -> None:
    class _Args:
        db_url = "sqlite:///db"
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

    class _DummyFx:
        def __init__(self, db_config):  # type: ignore[no-untyped-def]
            self.db_config = db_config

        def _get_backend_strategy(self):
            return object()

    monkeypatch.setattr("fx_bharat.FxBharat", _DummyFx)

    sbi_module.main()

    assert called["value"] is True
