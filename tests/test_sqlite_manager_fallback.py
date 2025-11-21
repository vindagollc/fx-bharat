from datetime import date
from pathlib import Path

import pytest

from fx_bharat.db import sqlite_manager as sqlite_manager_module
from fx_bharat.ingestion.models import ForexRateRecord


def test_sqlite_manager_uses_fallback_when_sqlalchemy_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sqlite_manager_module, "SQLALCHEMY_AVAILABLE", False)

    manager = sqlite_manager_module.SQLiteManager(tmp_path / "fallback.db")
    assert isinstance(manager._backend, sqlite_manager_module._SQLiteFallbackBackend)

    rows = [
        ForexRateRecord(rate_date=date(2024, 3, 1), currency="USD", rate=83.2),
        ForexRateRecord(rate_date=date(2024, 3, 2), currency="EUR", rate=90.4),
    ]
    manager.insert_rates(rows)
    fetched = manager.fetch_range()

    assert len(fetched) == 2
    assert fetched[0].currency == "USD"

    manager.close()


def test_sqlite_fallback_updates_and_filters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sqlite_manager_module, "SQLALCHEMY_AVAILABLE", False)

    manager = sqlite_manager_module.SQLiteManager(tmp_path / "fallback_filters.db")

    first = ForexRateRecord(rate_date=date(2024, 4, 1), currency="USD", rate=80.0)
    updated = ForexRateRecord(rate_date=date(2024, 4, 1), currency="USD", rate=81.5)
    sbi_row = ForexRateRecord(
        rate_date=date(2024, 4, 2),
        currency="EUR",
        rate=90.0,
        source="SBI",
        tt_buy=1.0,
        tt_sell=2.0,
        bill_buy=3.0,
        bill_sell=4.0,
        travel_card_buy=5.0,
        travel_card_sell=6.0,
        cn_buy=7.0,
        cn_sell=8.0,
    )

    initial = manager.insert_rates([first, sbi_row])
    follow_up = manager.insert_rates([updated])

    assert initial.inserted == 2
    assert follow_up.updated == 1

    filtered = manager.fetch_range(start=date(2024, 4, 2), source="SBI")
    assert len(filtered) == 1
    assert filtered[0].currency == "EUR"
    assert filtered[0].tt_sell == 2.0

    manager.close()
