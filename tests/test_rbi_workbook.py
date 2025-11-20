from __future__ import annotations

import csv
import shutil
import tempfile
from datetime import date
from pathlib import Path
from unittest import TestCase

from fx_bharat.ingestion.rbi_csv import RBICSVParser
from fx_bharat.ingestion.rbi_workbook import RBIWorkbookConverter


class TestRBIWorkbookConverter(TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp())
        self.converter = RBIWorkbookConverter()
        self.parser = RBICSVParser()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_to_csv_converts_html_disguised_xls(self) -> None:
        workbook_path = self.tmpdir / "sample.xls"
        workbook_path.write_text(
            """
            <table>
                <tr><th>Date</th><th>USD</th><th>GBP</th><th>EURO</th><th>YEN</th></tr>
                <tr><td>Date</td><td>USD</td><td>GBP</td><td>EURO</td><td>YEN</td></tr>
                <tr><td>01/01/2024</td><td>82.1</td><td>104.5</td><td>89.2</td><td>0.55</td></tr>
                <tr><td>02/01/2024</td><td>82.2</td><td>104.6</td><td>89.3</td><td>0.56</td></tr>
                <tr><td>03/01/2024</td><td>82.3</td><td>104.7</td><td></td><td>0.57</td></tr>
            </table>
            """,
            encoding="utf-8",
        )

        csv_path = self.converter.to_csv(
            workbook_path,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            output_dir=self.tmpdir,
        )

        self.assertFalse(
            workbook_path.exists(),
            "Workbook should be removed after conversion to keep download directory clean",
        )

        with csv_path.open() as handle:
            reader = csv.reader(handle)
            rows = list(reader)

        self.assertEqual(rows[0], ["Date", "USD", "GBP", "EURO", "YEN"])
        self.assertEqual(rows[1][0], "01/01/2024")
        self.assertEqual(rows[2][0], "02/01/2024")
        self.assertEqual(rows[3][1:], ["82.3", "104.7", "", "0.57"])

        parsed = self.parser.parse(csv_path)
        self.assertEqual(len(parsed), 11)
        self.assertEqual(parsed[0].rate, 82.1)

    def test_missing_file_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.converter.to_csv(
                self.tmpdir / "missing.xls",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2),
            )

    def test_to_csv_can_keep_source_when_requested(self) -> None:
        workbook_path = self.tmpdir / "sample.xls"
        workbook_path.write_text(
            """
            <table>
                <tr><th>Date</th><th>USD</th><th>GBP</th><th>EURO</th><th>YEN</th></tr>
                <tr><td>01/01/2024</td><td>82.1</td><td>104.5</td><td>89.2</td><td>0.55</td></tr>
            </table>
            """,
            encoding="utf-8",
        )

        csv_path = self.converter.to_csv(
            workbook_path,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            output_dir=self.tmpdir,
            remove_source=False,
        )

        self.assertTrue(workbook_path.exists())
        self.assertTrue(csv_path.exists())

    def test_normalize_rows_filters_invalid_entries(self) -> None:
        rows = [
            [],
            ["Date", "USD"],
            ["bad", "ignored"],
            ["01-01-2024", " 82.3 ", "foo"],
            ["02/01/2024", "", ""],
        ]

        cleaned = self.converter._normalize_rows(rows)

        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0][0], "01/01/2024")
        self.assertEqual(cleaned[0][1], "82.3")

    def test_stringify_rate_handles_various_inputs(self) -> None:
        self.assertEqual(RBIWorkbookConverter._stringify_rate(82), "82.0")
        self.assertEqual(RBIWorkbookConverter._stringify_rate(" 83.5 "), "83.5")
        self.assertEqual(RBIWorkbookConverter._stringify_rate("invalid"), "")
