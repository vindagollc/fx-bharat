from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from fx_bharat.db.relational_backend import RelationalBackend
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


def test_relational_backend_filters_by_source(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'relational_filters.db'}"
    backend = RelationalBackend(db_url)
    backend.ensure_schema()
    try:
        rows = [
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5, source="RBI"),
            ForexRateRecord(rate_date=date(2024, 1, 2), currency="EUR", rate=90.1, source="SBI"),
        ]
        backend.insert_rates(rows)
        fetched = backend.fetch_range(source="SBI")
        assert len(fetched) == 1
        assert fetched[0].currency == "EUR"
    finally:
        backend.close()


def test_relational_backend_lme_filters_and_invalid_metal(tmp_path: Path) -> None:
    db_url = f"sqlite:///{tmp_path / 'relational_lme.db'}"
    backend = RelationalBackend(db_url)
    backend.ensure_schema()
    try:
        rows = [
            LmeRateRecord(
                rate_date=date(2024, 2, 1),
                price=8500.0,
                price_3_month=8450.0,
                stock=100,
                metal="COPPER",
            ),
            LmeRateRecord(
                rate_date=date(2024, 2, 2),
                price=8600.0,
                price_3_month=8550.0,
                stock=110,
                metal="COPPER",
            ),
        ]
        backend.insert_lme_rates("COPPER", rows)
        fetched = backend.fetch_lme_range("COPPER", start=date(2024, 2, 2))
        assert len(fetched) == 1
        assert fetched[0].rate_date == date(2024, 2, 2)
        with pytest.raises(ValueError):
            backend.insert_lme_rates("GOLD", rows[:1])
    finally:
        backend.close()
