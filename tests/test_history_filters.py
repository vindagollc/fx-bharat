from __future__ import annotations

from datetime import date

import pytest

from fx_bharat import DatabaseBackend, DatabaseConnectionInfo, FxBharat
from fx_bharat.ingestion.models import ForexRateRecord


@pytest.fixture()
def sqlite_fx(tmp_path):
    db_path = tmp_path / "fx.db"
    config = DatabaseConnectionInfo(
        backend=DatabaseBackend.SQLITE,
        url=f"sqlite:///{db_path}",
        name=str(db_path),
        username=None,
        password=None,
        host=None,
        port=None,
    )
    return FxBharat(db_config=config)


def _seed_sample_data(app: FxBharat) -> None:
    assert app.sqlite_manager is not None
    app.sqlite_manager.insert_rates(
        [
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.0, source="RBI"),
            ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=81.5, source="SBI"),
            ForexRateRecord(rate_date=date(2024, 1, 2), currency="USD", rate=82.2, source="RBI"),
        ]
    )


def test_history_source_filter(sqlite_fx: FxBharat) -> None:
    _seed_sample_data(sqlite_fx)

    snapshots = sqlite_fx.history(date(2024, 1, 1), date(2024, 1, 2), source_filter="sbi")

    assert all(snap["source"] == "SBI" for snap in snapshots)
    assert len(snapshots) == 1


def test_rate_sorts_and_blends(sqlite_fx: FxBharat) -> None:
    _seed_sample_data(sqlite_fx)

    snapshots = sqlite_fx.rate()

    assert snapshots[0]["source"] == "SBI"
    assert snapshots[1]["source"] == "RBI"
    assert snapshots[0]["rate_date"] <= snapshots[1]["rate_date"]
