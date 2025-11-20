from __future__ import annotations

from datetime import date
from pathlib import Path

from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.seeds.populate_sbi_forex import _iter_pdf_paths, seed_sbi_forex


def test_iter_pdf_paths_filters_by_range(tmp_path: Path) -> None:
    resources = tmp_path / "resources"
    resources.mkdir()
    in_range = resources / "2024-01-15.pdf"
    out_of_range = resources / "2024-02-01.pdf"
    invalid = resources / "not-a-date.pdf"
    in_range.write_text("USD 80 81 82 83 84 85 86 87")
    out_of_range.write_text("USD 1 2 3 4 5 6 7 8")
    invalid.write_text("USD 1 2 3 4 5 6 7 8")

    filtered = list(_iter_pdf_paths(resources, date(2024, 1, 1), date(2024, 1, 31)))

    assert filtered == [in_range]


def test_seed_sbi_forex_downloads_latest_when_enabled(monkeypatch, tmp_path: Path) -> None:
    downloaded = tmp_path / "2024-02-02.pdf"
    downloaded.write_text(
        """
        Date: 02/02/2024
        USD 84.0 85.0 83.9 85.1 83.8 85.2 83.7 85.3
        """
    )

    calls: dict[str, int] = {"fetch": 0}

    class DummyDownloader:
        def fetch_latest(self) -> Path:
            calls["fetch"] += 1
            return downloaded

    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.SBIPDFDownloader", DummyDownloader)

    db_path = tmp_path / "db.sqlite"
    result = seed_sbi_forex(db_path=db_path, resource_dir=tmp_path / "resources", download=True)

    assert calls["fetch"] == 1
    assert result.inserted == 1
    with SQLiteManager(db_path) as manager:
        rows = manager.fetch_range(source="SBI")
    assert len(rows) == 1
    usd = rows[0]
    assert usd.rate_date == date(2024, 2, 2)
    assert usd.tt_buy == 84.0
