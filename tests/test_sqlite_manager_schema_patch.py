from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from fx_bharat.db import sqlite_manager as sqlite_manager_module
from fx_bharat.db.sqlite_manager import SQLiteManager
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


def test_sqlalchemy_backend_patches_lme_schema(tmp_path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE lme_copper_rates (rate_date DATE PRIMARY KEY);
            CREATE TABLE lme_aluminum_rates (rate_date DATE PRIMARY KEY);
            """
        )

    manager = SQLiteManager(db_path)
    if not isinstance(manager._backend, sqlite_manager_module._SQLAlchemyBackend):
        manager.close()
        pytest.skip("SQLAlchemy backend not active")
    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(lme_aluminum_rates)").fetchall()
        }
    manager.close()

    assert {"price", "price_3_month", "stock", "created_at"}.issubset(columns)


def test_sqlalchemy_backend_updates_sbi_rows(tmp_path) -> None:
    manager = SQLiteManager(tmp_path / "updates.db")
    try:
        row = ForexRateRecord(
            rate_date=date(2024, 1, 1),
            currency="USD",
            rate=80.0,
            source="SBI",
            tt_buy=1.0,
            tt_sell=2.0,
        )
        manager.insert_rates([row])
        updated = ForexRateRecord(
            rate_date=date(2024, 1, 1),
            currency="USD",
            rate=81.0,
            source="SBI",
            tt_buy=2.0,
            tt_sell=3.0,
        )
        result = manager.insert_rates([updated])
    finally:
        manager.close()

    assert result.total == 1


def test_fallback_backend_lme_and_metadata(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(sqlite_manager_module, "SQLALCHEMY_AVAILABLE", False)
    manager = sqlite_manager_module.SQLiteManager(tmp_path / "fallback_lme.db")
    try:
        rows = [
            LmeRateRecord(
                rate_date=date(2024, 2, 1),
                price=100.0,
                price_3_month=101.0,
                stock=10,
                metal="COPPER",
            )
        ]
        manager.insert_lme_rates("COPPER", rows)
        updated = [
            LmeRateRecord(
                rate_date=date(2024, 2, 1),
                price=200.0,
                price_3_month=201.0,
                stock=20,
                metal="COPPER",
            )
        ]
        result = manager.insert_lme_rates("COPPER", updated)
        fetched = manager.fetch_lme_range("COPPER", start=date(2024, 2, 1), end=date(2024, 2, 1))
        latest = manager.latest_rate_date("SBI")
        manager.update_ingestion_checkpoint("LME_COPPER", date(2024, 2, 1))
        checkpoint = manager.ingestion_checkpoint("LME_COPPER")
    finally:
        manager.close()

    assert result.total == 1
    assert fetched[0].price == 200.0
    assert latest is None
    assert checkpoint == date(2024, 2, 1)
