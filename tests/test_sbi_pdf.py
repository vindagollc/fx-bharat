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
        USD 83.11 84.11 83.01 84.21 82.91 84.31
        EURO 90.22 91.22 90.12 91.32 90.02 91.42
        STERLING 101.33 102.33 101.23 102.43 101.13 102.53
        """
    )
    parser = SBIPDFParser()

    parsed = parser.parse(pdf_path)

    assert parsed.rate_date == date(2024, 1, 2)
    rates = {row.currency: row.tt_buy for row in parsed.rates}
    assert rates == {"USD": 83.11, "EUR": 90.22, "GBP": 101.33}
    usd = next(row for row in parsed.rates if row.currency == "USD")
    assert (
        usd.tt_buy,
        usd.tt_sell,
        usd.bill_buy,
        usd.bill_sell,
        usd.travel_card_buy,
        usd.travel_card_sell,
    ) == (83.11, 84.11, 83.01, 84.21, 82.91, 84.31)
    assert {row.source for row in parsed.rates} == {"SBI"}


def test_seed_sbi_forex_populates_sqlite(tmp_path: Path) -> None:
    resource_dir = tmp_path / "resources"
    resource_dir.mkdir()
    pdf_path = resource_dir / "2024-01-05.pdf"
    pdf_path.write_text(
        """
        05/01/2024
        USD 83.5 84.5 83.4 84.6 83.3 84.7
        AUD 55.0 56.0 54.9 56.1 54.8 56.2
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
    assert {
        (row.rate_date, row.currency, row.tt_buy, row.tt_sell, row.bill_buy, row.bill_sell)
        for row in rows
    } == {
        (date(2024, 1, 5), "USD", 83.5, 84.5, 83.4, 84.6),
        (date(2024, 1, 5), "AUD", 55.0, 56.0, 54.9, 56.1),
    }
