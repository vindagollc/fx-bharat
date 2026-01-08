from __future__ import annotations

from datetime import date

import pytest

from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


def test_sqlite_manager_lme_update_and_filter(tmp_path) -> None:
    manager = SQLiteManager(tmp_path / "lme.db")
    try:
        first = LmeRateRecord(
            rate_date=date(2024, 2, 1),
            price=8500.0,
            price_3_month=8450.0,
            stock=100,
            metal="COPPER",
        )
        second = LmeRateRecord(
            rate_date=date(2024, 2, 1),
            price=8600.0,
            price_3_month=8550.0,
            stock=200,
            metal="COPPER",
        )
        manager.insert_lme_rates("COPPER", [first])
        result = manager.insert_lme_rates("COPPER", [second])
        assert result.updated == 1

        fetched = manager.fetch_lme_range("COPPER", start=date(2024, 2, 1), end=date(2024, 2, 1))
        assert fetched[0].stock == 200
    finally:
        manager.close()


def test_sqlite_manager_latest_rate_date(tmp_path) -> None:
    manager = SQLiteManager(tmp_path / "latest.db")
    try:
        assert manager.latest_rate_date("RBI") is None
    finally:
        manager.close()


def test_sqlite_manager_fetches_sbi_rows(tmp_path) -> None:
    manager = SQLiteManager(tmp_path / "sbi.db")
    try:
        manager.insert_rates(
            [
                ForexRateRecord(
                    rate_date=date(2024, 1, 2),
                    currency="USD",
                    rate=82.5,
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
            ]
        )
        rows = manager.fetch_range(source="SBI")
        assert rows[0].tt_sell == 2.0
    finally:
        manager.close()


def test_sqlite_manager_invalid_lme_metal(tmp_path) -> None:
    manager = SQLiteManager(tmp_path / "invalid.db")
    try:
        with pytest.raises(ValueError):
            manager.insert_lme_rates("GOLD", [])
    finally:
        manager.close()
