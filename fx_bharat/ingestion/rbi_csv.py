"""CSV helpers for storing and re-reading RBI downloads."""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence, cast

from fx_bharat.ingestion.models import ForexRateRecord

EXPECTED_CURRENCIES = ("USD", "GBP", "EUR", "JPY")
CSV_HEADER = ("Date", "USD", "GBP", "EURO", "YEN")
ISO_TO_CSV = {
    "USD": "USD",
    "GBP": "GBP",
    "EUR": "EURO",
    "JPY": "YEN",
}
CSV_TO_ISO = {value: key for key, value in ISO_TO_CSV.items()}


class RBICSVExporter:
    """Export normalized forex rows into a RBI-style CSV file."""

    def __init__(self, *, date_format: str = "%d/%m/%Y") -> None:
        self.date_format = date_format

    def write(
        self,
        records: Sequence[ForexRateRecord],
        *,
        start_date: date,
        end_date: date,
        output_dir: Path | None = None,
    ) -> Path:
        if not records:
            raise ValueError("records collection is empty")

        directory = Path(output_dir) if output_dir else Path.cwd()
        directory.mkdir(parents=True, exist_ok=True)
        csv_name = self._build_filename(start_date, end_date)
        csv_path = directory / csv_name
        rows_by_date: dict[date, dict[str, float]] = defaultdict(dict)
        for record in records:
            rows_by_date[record.rate_date][record.currency] = record.rate

        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(CSV_HEADER)
            for rate_date in sorted(rows_by_date):
                date_str = rate_date.strftime(self.date_format)
                row = [date_str]
                for currency in EXPECTED_CURRENCIES:
                    value = rows_by_date[rate_date].get(currency)
                    row.append("" if value is None else f"{value}")
                writer.writerow(row)
        return csv_path

    def _build_filename(self, start_date: date, end_date: date) -> str:
        safe_from = start_date.strftime("%d-%m-%Y")
        safe_to = end_date.strftime("%d-%m-%Y")
        return f"RBI_Reference_Rates_{safe_from}_to_{safe_to}.csv"


class RBICSVParser:
    """Parse CSV files produced by :class:`RBICSVExporter`."""

    def __init__(self, *, date_format: str = "%d/%m/%Y") -> None:
        self.date_format = date_format

    def parse(self, csv_path: str | Path) -> list[ForexRateRecord]:
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(path)

        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            self._validate_header(reader.fieldnames)
            rows: list[ForexRateRecord] = []
            for row in reader:
                date_raw = row.get("Date")
                if not date_raw:
                    continue
                rate_date = datetime.strptime(date_raw.strip(), self.date_format).date()
                for csv_column, iso_code in CSV_TO_ISO.items():
                    value_raw = row.get(csv_column)
                    if value_raw in (None, ""):
                        continue
                    try:
                        rate = float(cast(str, value_raw))
                    except ValueError:
                        continue
                    rows.append(ForexRateRecord(rate_date=rate_date, currency=iso_code, rate=rate))
        return rows

    @staticmethod
    def _validate_header(fieldnames: Iterable[str] | None) -> None:
        if not fieldnames:
            raise ValueError("CSV file does not contain a header row")
        normalized = [field.strip() for field in fieldnames]
        if normalized[: len(CSV_HEADER)] != list(CSV_HEADER):
            raise ValueError("Unexpected CSV header format")
