from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from fx_bharat.ingestion.sbi_pdf import SBIPDFParser


def test_sbi_pdf_infer_date_patterns() -> None:
    parser = SBIPDFParser()

    parsed = parser._infer_date("DATE: 01/02/2024", "foo.pdf")
    assert parsed == date(2024, 2, 1)

    parsed = parser._infer_date("2024-03-04", "bar.pdf")
    assert parsed == date(2024, 3, 4)


def test_sbi_pdf_infer_date_from_filename() -> None:
    parser = SBIPDFParser()
    parsed = parser._infer_date("no date", Path("2024-04-05.pdf"))
    assert parsed == date(2024, 4, 5)


def test_sbi_pdf_infer_date_fallback_to_utc(monkeypatch) -> None:
    class _DummyDateTime(datetime):
        @classmethod
        def utcnow(cls) -> datetime:  # type: ignore[override]
            return cls(2024, 6, 1)

    monkeypatch.setattr("fx_bharat.ingestion.sbi_pdf.datetime", _DummyDateTime)
    parser = SBIPDFParser()
    parsed = parser._infer_date("no date", Path("invalid.pdf"))
    assert parsed == date(2024, 6, 1)


def test_sbi_pdf_extract_rates_alias_and_code() -> None:
    parser = SBIPDFParser()
    text = """
    UAE DIRHAM 1 2 3 4 5 6 7 8
    USD 9 10 11 12 13 14 15 16
    """
    rates = list(parser._extract_rates(text, date(2024, 1, 1)))

    assert rates[0].currency == "AED"
    assert rates[0].tt_buy == 1.0
    assert rates[1].currency == "USD"
    assert rates[1].tt_sell == 10.0
