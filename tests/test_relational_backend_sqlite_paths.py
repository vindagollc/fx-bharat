from __future__ import annotations

from datetime import date

from fx_bharat.db.relational_backend import RelationalBackend
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


def test_insert_lme_rates_sqlite_upsert(tmp_path) -> None:
    backend = RelationalBackend(f"sqlite:///{tmp_path/'lme.db'}")
    backend.ensure_schema()

    rows = [
        LmeRateRecord(
            rate_date=date(2024, 1, 1), price=1.0, price_3_month=2.0, stock=3, metal="COPPER"
        ),
        LmeRateRecord(
            rate_date=date(2024, 1, 2), price=4.0, price_3_month=5.0, stock=6, metal="COPPER"
        ),
    ]
    result = backend.insert_lme_rates("COPPER", rows)
    assert result.inserted == 2

    # Upsert should update existing row, not duplicate it.
    updated = [
        LmeRateRecord(
            rate_date=date(2024, 1, 1), price=10.0, price_3_month=20.0, stock=30, metal="COPPER"
        )
    ]
    backend.insert_lme_rates("COPPER", updated)

    fetched = backend.fetch_lme_range("COPPER")
    assert {row.rate_date for row in fetched} == {date(2024, 1, 1), date(2024, 1, 2)}
    assert any(row.price == 10.0 for row in fetched if row.rate_date == date(2024, 1, 1))


def test_update_ingestion_checkpoint_sqlite(tmp_path) -> None:
    backend = RelationalBackend(f"sqlite:///{tmp_path/'checkpoint.db'}")
    backend.ensure_schema()

    backend.update_ingestion_checkpoint("RBI", date(2024, 1, 1))
    backend.update_ingestion_checkpoint("RBI", date(2024, 1, 3))
    backend.update_ingestion_checkpoint("RBI", date(2024, 1, 2))  # should not regress

    checkpoint = backend.ingestion_checkpoint("RBI")
    assert checkpoint == date(2024, 1, 3)


def test_fetch_range_filters_dates_and_source(tmp_path) -> None:
    backend = RelationalBackend(f"sqlite:///{tmp_path/'filters.db'}")
    backend.ensure_schema()
    rows = [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.0, source="RBI"),
        ForexRateRecord(rate_date=date(2024, 1, 5), currency="USD", rate=83.0, source="RBI"),
        ForexRateRecord(rate_date=date(2024, 1, 3), currency="USD", rate=81.5, source="SBI"),
    ]
    backend.insert_rates(rows)

    fetched = backend.fetch_range(date(2024, 1, 2), date(2024, 1, 5), source="RBI")
    assert {row.rate_date for row in fetched} == {date(2024, 1, 5)}
