import unittest
from datetime import date

from fx_bharat.utils.date_range import DateRange, month_ranges, split_ranges


class DateRangeTests(unittest.TestCase):
    def test_month_ranges(self) -> None:
        ranges = list(month_ranges("2024-01-15", "2024-03-05"))
        self.assertEqual(
            ranges,
            [
                DateRange(start=date(2024, 1, 15), end=date(2024, 1, 31)),
                DateRange(start=date(2024, 2, 1), end=date(2024, 2, 29)),
                DateRange(start=date(2024, 3, 1), end=date(2024, 3, 5)),
            ],
        )

    def test_split_ranges(self) -> None:
        ranges = list(split_ranges("2024-01-01", "2024-01-10", window_days=3))
        self.assertEqual(len(ranges), 4)
        self.assertEqual(ranges[0].start, date(2024, 1, 1))
        self.assertEqual(ranges[-1].end, date(2024, 1, 10))

    def test_invalid_input(self) -> None:
        with self.assertRaises(ValueError):
            list(month_ranges("2024-02-01", "2024-01-01"))
        with self.assertRaises(ValueError):
            list(split_ranges("2024-02-01", "2024-01-01", 5))
        with self.assertRaises(ValueError):
            list(split_ranges("2024-01-01", "2024-01-02", 0))


if __name__ == "__main__":
    unittest.main()
