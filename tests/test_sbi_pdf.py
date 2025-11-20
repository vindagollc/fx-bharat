from __future__ import annotations

from datetime import date
from pathlib import Path

from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.ingestion.sbi_pdf import SBIPDFParser
from fx_bharat.seeds.populate_sbi_forex import seed_sbi_forex


def test_sbi_parser_extracts_rates(tmp_path: Path) -> None:
    pdf_path = tmp_path / "2024-01-02.pdf"
    pdf_path.write_text(
        """
        Forex Card Rates
        Date: 02/01/2024
        USD 83.11
        EURO 90.22
        STERLING 101.33
        """
    )
    parser = SBIPDFParser()

    parsed = parser.parse(pdf_path)

    assert parsed.rate_date == date(2024, 1, 2)
    rates = {row.currency: row.rate for row in parsed.rates}
    assert rates == {"USD": 83.11, "EUR": 90.22, "GBP": 101.33}
    assert {row.source for row in parsed.rates} == {"SBI"}


def test_seed_sbi_forex_populates_sqlite(tmp_path: Path) -> None:
    resource_dir = tmp_path / "resources"
    resource_dir.mkdir()
    pdf_path = resource_dir / "2024-01-05.pdf"
    pdf_path.write_text(
        """
        05/01/2024
        USD 83.5
        AUD 55.0
        """
    )
    db_path = tmp_path / "forex.db"

    result = seed_sbi_forex(
        db_path=db_path,
        resource_dir=resource_dir,
        start=date(2024, 1, 1),
        end=date(2024, 1, 31),
        download=False,
    )

    assert result.total == 2

    with SQLiteManager(db_path) as manager:
        rows = manager.fetch_range(source="SBI")
    assert {(row.rate_date, row.currency, row.rate, row.source) for row in rows} == {
        (date(2024, 1, 5), "USD", 83.5, "SBI"),
        (date(2024, 1, 5), "AUD", 55.0, "SBI"),
    }
