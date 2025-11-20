import tempfile
import unittest
from datetime import date
from pathlib import Path

from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.models import ForexRateRecord


class SQLiteManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        self.manager = SQLiteManager(self.db_path)

    def tearDown(self) -> None:
        self.manager.close()
        self.temp_dir.cleanup()

    def test_insert_and_upsert(self) -> None:
        rows = [
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.6),
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="EUR", rate=88.5),
        ]
        result = self.manager.insert_rates(rows)
        self.assertIsInstance(result, PersistenceResult)
        self.assertEqual(result.inserted, len(rows))
        self.assertEqual(result.updated, 0)

        updated_rows = [
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.7),
        ]
        update_result = self.manager.insert_rates(updated_rows)
        self.assertEqual(update_result.inserted, 0)
        self.assertEqual(update_result.updated, 1)

        all_rows = self.manager.fetch_all()
        self.assertEqual(len(all_rows), 2)
        self.assertTrue(all(isinstance(row, ForexRateRecord) for row in all_rows))
        usd_row = [row for row in all_rows if row.currency == "USD"][0]
        self.assertEqual(usd_row.rate, 82.7)

    def test_fetch_range_filters_dates(self) -> None:
        rows = [
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.6),
            ForexRateRecord(rate_date=date(2024, 2, 1), currency="USD", rate=83.1),
        ]
        self.manager.insert_rates(rows)

        jan_rows = self.manager.fetch_range(start=date(2024, 1, 1), end=date(2024, 1, 31))
        feb_rows = self.manager.fetch_range(start=date(2024, 2, 1))

        self.assertEqual(len(jan_rows), 1)
        self.assertEqual(jan_rows[0].rate_date, date(2024, 1, 1))
        self.assertEqual(len(feb_rows), 1)
        self.assertEqual(feb_rows[0].rate_date, date(2024, 2, 1))


class PersistenceResultTests(unittest.TestCase):
    def test_total_property_adds_inserted_and_updated(self) -> None:
        result = PersistenceResult(inserted=3, updated=2)

        self.assertEqual(result.total, 5)


class DatabaseModuleTests(unittest.TestCase):
    def test_bundled_sqlite_path_matches_default(self) -> None:
        from fx_bharat.db import DEFAULT_SQLITE_DB_PATH, bundled_sqlite_path

        self.assertEqual(bundled_sqlite_path(), DEFAULT_SQLITE_DB_PATH)


if __name__ == "__main__":
    unittest.main()
