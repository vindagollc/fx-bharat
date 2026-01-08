from __future__ import annotations

from datetime import date

from fx_bharat.db.sqlite_backend import SQLiteBackend
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


def test_sqlite_backend_roundtrip(tmp_path) -> None:
    backend = SQLiteBackend(db_path=tmp_path / "sqlite_backend.db")
    assert backend.ensure_schema() is None

    rows = [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
        ForexRateRecord(rate_date=date(2024, 1, 2), currency="EUR", rate=90.1, source="SBI"),
    ]
    result = backend.insert_rates(rows)
    assert result.inserted == 2

    fetched = backend.fetch_range()
    assert len(fetched) == 2
    assert {row.source for row in fetched} == {"RBI", "SBI"}

    backend.close()


def test_sqlite_backend_lme_roundtrip(tmp_path) -> None:
    backend = SQLiteBackend(db_path=tmp_path / "sqlite_lme.db")

    rows = [
        LmeRateRecord(
            rate_date=date(2024, 2, 1),
            price=8500.0,
            price_3_month=8450.0,
            stock=100,
            metal="COPPER",
        )
    ]
    result = backend.insert_lme_rates("COPPER", rows)
    assert result.total == 1

    fetched = backend.fetch_lme_range("COPPER")
    assert len(fetched) == 1
    assert fetched[0].stock == 100

    backend.close()
