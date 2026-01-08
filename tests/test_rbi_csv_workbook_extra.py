from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from fx_bharat.ingestion.rbi_csv import RBICSVExporter, RBICSVParser
from fx_bharat.ingestion.rbi_workbook import RBIWorkbookConverter
from fx_bharat.ingestion.models import ForexRateRecord


def test_rbi_csv_exporter_requires_records(tmp_path: Path) -> None:
    exporter = RBICSVExporter()
    with pytest.raises(ValueError):
        exporter.write([], start_date=date(2024, 1, 1), end_date=date(2024, 1, 31), output_dir=tmp_path)


def test_rbi_csv_parser_rejects_bad_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("Wrong,Header\n", encoding="utf-8")

    parser = RBICSVParser()
    with pytest.raises(ValueError):
        parser.parse(csv_path)


def test_rbi_csv_parser_handles_missing_and_invalid_values(tmp_path: Path) -> None:
    csv_path = tmp_path / "rates.csv"
    csv_path.write_text(
        "Date,USD,GBP,EURO,YEN\n01/01/2024,not-a-number,,,",
        encoding="utf-8",
    )

    parser = RBICSVParser()
    rows = parser.parse(csv_path)

    assert rows == []


def test_rbi_csv_parser_missing_file_raises() -> None:
    parser = RBICSVParser()
    with pytest.raises(FileNotFoundError):
        parser.parse(Path("missing.csv"))


def test_rbi_workbook_converter_html_fallback(tmp_path: Path, monkeypatch) -> None:
    html_path = tmp_path / "workbook.html"
    html_path.write_text(
        """
        <table>
            <tr><th>Date</th><th>USD</th><th>GBP</th><th>EURO</th><th>YEN</th></tr>
            <tr><td>01/01/2024</td><td>82.1</td><td>100.0</td><td>90.0</td><td></td></tr>
            <tr><td>invalid</td><td>bad</td><td></td><td></td><td></td></tr>
        </table>
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr("fx_bharat.ingestion.rbi_workbook._pd", None)
    converter = RBIWorkbookConverter(use_pandas=True, cleanup_source=True)

    csv_path = converter.to_csv(
        html_path,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        output_dir=tmp_path,
    )

    assert csv_path.exists()
    assert not html_path.exists()


def test_rbi_workbook_converter_helpers() -> None:
    converter = RBIWorkbookConverter()
    assert converter._stringify_rate(None) == ""
    assert converter._stringify_rate(" ") == ""
    assert converter._stringify_rate("1.25") == "1.25"
    assert converter._stringify_rate(2.5) == "2.5"
    with pytest.raises(ValueError):
        converter._parse_date("bad-date")


def test_rbi_csv_exporter_writes_rows(tmp_path: Path) -> None:
    exporter = RBICSVExporter()
    records = [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="EUR", rate=90.1),
    ]
    csv_path = exporter.write(
        records,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        output_dir=tmp_path,
    )
    assert csv_path.exists()


def test_rbi_workbook_converter_pandas_path(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    html_path = tmp_path / "workbook.html"
    html_path.write_text(
        """
        <table>
            <tr><th>Date</th><th>USD</th><th>GBP</th><th>EURO</th><th>YEN</th></tr>
            <tr><td>01/01/2024</td><td>82.1</td><td>100.0</td><td>90.0</td><td>1.0</td></tr>
        </table>
        """,
        encoding="utf-8",
    )
    converter = RBIWorkbookConverter(use_pandas=True, cleanup_source=False)

    csv_path = converter.to_csv(
        html_path,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        output_dir=tmp_path,
        remove_source=False,
    )

    assert csv_path.exists()
    assert html_path.exists()
