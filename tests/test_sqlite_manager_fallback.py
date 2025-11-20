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
