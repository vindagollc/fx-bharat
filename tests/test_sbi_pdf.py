from __future__ import annotations

from datetime import date
from pathlib import Path

from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.ingestion.sbi_pdf import SBIPDFParser
from fx_bharat.seeds.populate_sbi_forex import seed_sbi_historical


def test_sbi_parser_extracts_rates(tmp_path: Path) -> None:
    pdf_path = tmp_path / "2024-01-02.pdf"
    pdf_path.write_text(
        """
        Forex Card Rates
        Date: 02/01/2024
        USD 83.11 84.11 83.01 84.21 82.91 84.31 82.71 84.51
        EURO 90.22 91.22 90.12 91.32 90.02 91.42 89.92 91.52
        STERLING 101.33 102.33 101.23 102.43 101.13 102.53 101.03 102.63
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
        usd.cn_buy,
        usd.cn_sell,
    ) == (83.11, 84.11, 83.01, 84.21, 82.91, 84.31, 82.71, 84.51)
    assert {row.source for row in parsed.rates} == {"SBI"}


def test_seed_sbi_forex_populates_sqlite(tmp_path: Path) -> None:
    resource_dir = tmp_path / "resources"
    resource_dir.mkdir()
    pdf_path = resource_dir / "2024-01-05.pdf"
    pdf_path.write_text(
        """
        05/01/2024
        USD 83.5 84.5 83.4 84.6 83.3 84.7 83.2 84.8
        AUD 55.0 56.0 54.9 56.1 54.8 56.2 54.7 56.3
        """
    )
    db_path = tmp_path / "forex.db"

    result = seed_sbi_historical(
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
        (
            row.rate_date,
            row.currency,
            row.tt_buy,
            row.tt_sell,
            row.bill_buy,
            row.bill_sell,
            row.travel_card_buy,
            row.travel_card_sell,
            row.cn_buy,
            row.cn_sell,
        )
        for row in rows
    } == {
        (
            date(2024, 1, 5),
            "USD",
            83.5,
            84.5,
            83.4,
            84.6,
            83.3,
            84.7,
            83.2,
            84.8,
        ),
        (
            date(2024, 1, 5),
            "AUD",
            55.0,
            56.0,
            54.9,
            56.1,
            54.8,
            56.2,
            54.7,
            56.3,
        ),
    }


def test_parser_reads_decimal_spacing_and_cn_from_pdf() -> None:
    parser = SBIPDFParser()
    pdf_path = Path("resources/2025/1/2025-01-01.pdf")

    parsed = parser.parse(pdf_path)

    usd_rows = [row for row in parsed.rates if row.currency == "USD"]
    assert len(usd_rows) == 1
    usd = usd_rows[0]
    assert parsed.rate_date == date(2025, 1, 1)
    assert usd.tt_buy == 85.22
    assert usd.tt_sell == 86.07
    assert usd.bill_buy == 85.15
    assert usd.bill_sell == 86.24
    assert usd.travel_card_buy == 85.15
    assert usd.travel_card_sell == 86.24
    assert usd.cn_buy == 84.15
    assert usd.cn_sell == 86.55


def test_sbi_parser_infers_date_from_filename_when_missing_date(tmp_path: Path) -> None:
    pdf_path = tmp_path / "2024-12-31.pdf"
    pdf_path.write_text("USD 80 81 82 83 84 85 86 87")

    parsed = SBIPDFParser().parse(pdf_path)

    assert parsed.rate_date == date(2024, 12, 31)
    usd = parsed.rates[0]
    assert usd.currency == "USD"
    assert (
        usd.tt_buy,
        usd.tt_sell,
        usd.bill_buy,
        usd.bill_sell,
        usd.travel_card_buy,
        usd.travel_card_sell,
        usd.cn_buy,
        usd.cn_sell,
    ) == (80, 81, 82, 83, 84, 85, 86, 87)


def test_sbi_parser_deduplicates_currency_rows(tmp_path: Path) -> None:
    pdf_path = tmp_path / "2024-02-01.pdf"
    pdf_path.write_text(
        """
        Date: 01/02/2024
        USD 83.11 84.11 83.01 84.21 82.91 84.31 82.71 84.51
        USD 90.00 90.00 90.00 90.00 90.00 90.00 90.00 90.00
        """
    )

    parsed = SBIPDFParser().parse(pdf_path)

    usd_rows = [row for row in parsed.rates if row.currency == "USD"]
    assert len(usd_rows) == 1
    usd = usd_rows[0]
    assert usd.rate_date == date(2024, 2, 1)
    assert usd.tt_buy == 83.11


def test_sbi_parser_falls_back_to_filename_when_header_date_invalid(tmp_path: Path) -> None:
    pdf_path = tmp_path / "2020-10-12.pdf"
    pdf_path.write_text(
        """
        Date: 20/20/2020
        USD 80 81 82 83 84 85 86 87
        """
    )

    parsed = SBIPDFParser().parse(pdf_path)

    assert parsed.rate_date == date(2020, 10, 12)
    usd = parsed.rates[0]
    assert usd.currency == "USD"
    assert usd.tt_buy == 80
