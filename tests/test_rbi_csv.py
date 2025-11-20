import tempfile
import unittest
from datetime import date
from pathlib import Path

from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.ingestion.rbi_csv import RBICSVExporter, RBICSVParser


class RBICSVTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.exporter = RBICSVExporter()
        self.parser = RBICSVParser()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_export_and_parse_round_trip(self) -> None:
        records = [
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="GBP", rate=103.5),
            ForexRateRecord(rate_date=date(2024, 1, 2), currency="EUR", rate=90.2),
            ForexRateRecord(rate_date=date(2024, 1, 2), currency="JPY", rate=0.55),
        ]
        csv_path = self.exporter.write(
            records,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            output_dir=Path(self.tmp_dir.name),
        )
        parsed = self.parser.parse(csv_path)

        def sort_key(row: ForexRateRecord) -> tuple[date, str]:
            return (row.rate_date, row.currency)

        self.assertEqual(sorted(parsed, key=sort_key), sorted(records, key=sort_key))

    def test_parser_requires_header(self) -> None:
        csv_path = Path(self.tmp_dir.name) / "bad.csv"
        csv_path.write_text("USD,GBP\n1,2\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.parser.parse(csv_path)

    def test_missing_file(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.parser.parse(Path(self.tmp_dir.name) / "missing.csv")


if __name__ == "__main__":  # pragma: no cover - manual debugging helper
    unittest.main()
