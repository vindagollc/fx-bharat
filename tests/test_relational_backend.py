"""Relational backend integration tests using SQLite."""

from datetime import date, datetime
from pathlib import Path

from fx_bharat.db.relational_backend import RelationalBackend, _normalise_rate_date
from fx_bharat.ingestion.models import ForexRateRecord


def test_relational_backend_roundtrip(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'relational.db'}"
    backend = RelationalBackend(db_url)
    backend.ensure_schema()

    first_batch = [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
        ForexRateRecord(rate_date=date(2024, 1, 2), currency="EUR", rate=90.1),
    ]
    result = backend.insert_rates(first_batch)
    assert result.inserted == 2
    assert result.updated == 0

    second_batch = [ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=83.0)]
    update_result = backend.insert_rates(second_batch)
    assert update_result.inserted == 1
    assert backend.fetch_range(date(2024, 1, 1), date(2024, 1, 1))[0].rate == 83.0

    jan_rows = backend.fetch_range(date(2024, 1, 1), date(2024, 1, 31))
    assert len(jan_rows) == 2
    assert {row.currency for row in jan_rows} == {"USD", "EUR"}

    backend.close()


def test_normalise_rate_date_handles_multiple_input_types() -> None:
    assert _normalise_rate_date(date(2024, 5, 1)) == date(2024, 5, 1)
    assert _normalise_rate_date(datetime(2024, 5, 2, 15, 0)) == date(2024, 5, 2)
    assert _normalise_rate_date("2024-05-03") == date(2024, 5, 3)
