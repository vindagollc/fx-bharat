from __future__ import annotations

from datetime import date

import pytest

import fx_bharat
from fx_bharat import DatabaseBackend, DatabaseConnectionInfo, FxBharat
from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord
from fx_bharat.seeds.populate_lme import SeedResult


def test_database_backend_resolve_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        DatabaseBackend.resolve_backend_and_scheme("oracle")


def test_seed_lme_wrappers_delegate(monkeypatch) -> None:
    called = {"prices": False, "copper": False, "aluminum": False}

    def _prices(*_args, **_kwargs):
        called["prices"] = True
        return "ok"

    def _copper(*_args, **_kwargs):
        called["copper"] = True
        return "ok"

    def _aluminum(*_args, **_kwargs):
        called["aluminum"] = True
        return "ok"

    monkeypatch.setattr("fx_bharat.seeds.populate_lme.seed_lme_prices", _prices)
    monkeypatch.setattr("fx_bharat.seeds.populate_lme.seed_lme_copper", _copper)
    monkeypatch.setattr("fx_bharat.seeds.populate_lme.seed_lme_aluminum", _aluminum)

    assert fx_bharat.seed_lme_prices("COPPER") == "ok"
    assert fx_bharat.seed_lme_copper() == "ok"
    assert fx_bharat.seed_lme_aluminum() == "ok"

    assert all(called.values())


def test_database_connection_info_normalises_database_name_param() -> None:
    url = "mongodb://localhost/?DATABASE_NAME=testdb"
    info = DatabaseConnectionInfo.from_url(url)
    assert info.name == "testdb"
    assert "DATABASE_NAME" not in info.url


def test_normalise_source_filter_invalid() -> None:
    with pytest.raises(ValueError):
        FxBharat._normalise_source_filter("bad")


def test_select_snapshot_dates_grouping() -> None:
    dates = [
        date(2024, 1, 1),
        date(2024, 1, 7),
        date(2024, 1, 8),
        date(2024, 2, 1),
        date(2025, 1, 1),
    ]
    assert FxBharat._select_snapshot_dates(dates, "monthly")[-1] == date(2025, 1, 1)
    assert FxBharat._select_snapshot_dates(dates, "yearly")[-1] == date(2025, 1, 1)


def test_connection_probe_with_missing_sqlalchemy(monkeypatch) -> None:
    monkeypatch.setattr(fx_bharat, "create_engine", None)
    monkeypatch.setattr(fx_bharat, "text", None)

    client = FxBharat()
    ok, message = client.connection()
    assert ok is False
    assert message is not None


def test_connection_probe_mongodb_missing_driver(monkeypatch) -> None:
    info = DatabaseConnectionInfo(
        backend=DatabaseBackend.MONGODB,
        url="mongodb://localhost/testdb",
        name="testdb",
        username=None,
        password=None,
        host=None,
        port=None,
    )
    monkeypatch.setattr(fx_bharat, "MongoClient", None)
    client = FxBharat(info)

    ok, message = client.connection()
    assert ok is False
    assert message is not None


def test_conection_alias_calls_connection() -> None:
    client = FxBharat()
    assert client.conection() == client.connection()


def test_seed_lme_mirrors_to_external(monkeypatch) -> None:
    info = DatabaseConnectionInfo(
        backend=DatabaseBackend.POSTGRES,
        url="postgresql://example.com/db",
        name="db",
        username=None,
        password=None,
        host=None,
        port=None,
    )
    client = FxBharat(info)

    class _DummyBackend:
        def __init__(self) -> None:
            self.inserted: list = []

        def ensure_schema(self) -> None:
            return None

        def insert_lme_rates(self, metal, rows):
            self.inserted.append((metal, rows))
            return PersistenceResult(inserted=len(rows))

    class _DummyManager:
        def __init__(self, _path):
            self.path = _path

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def fetch_lme_range(self, _metal, _start, _end):
            return []

    dummy_backend = _DummyBackend()
    monkeypatch.setattr("fx_bharat.FxBharat._get_backend_strategy", lambda self: dummy_backend)
    monkeypatch.setattr(
        fx_bharat,
        "seed_lme_prices",
        lambda *_args, **_kwargs: SeedResult(metal="COPPER", rows=PersistenceResult(inserted=0)),
    )
    monkeypatch.setattr(fx_bharat, "SQLiteManager", _DummyManager)

    result = client.seed_lme("COPPER")

    assert result.total == 0
    assert dummy_backend.inserted == [("COPPER", [])]


@pytest.fixture()
def sqlite_fx(tmp_path) -> FxBharat:
    db_path = tmp_path / "forex.db"
    info = DatabaseConnectionInfo(
        backend=DatabaseBackend.SQLITE,
        url=f"sqlite:///{db_path}",
        name=str(db_path),
        username=None,
        password=None,
        host=None,
        port=None,
    )
    return FxBharat(db_config=info)


def test_history_lme_validation(sqlite_fx: FxBharat) -> None:
    with pytest.raises(ValueError):
        sqlite_fx.history_lme(date(2024, 1, 2), date(2024, 1, 1))
    with pytest.raises(ValueError):
        sqlite_fx.history_lme(date(2024, 1, 1), date(2024, 1, 2), frequency="bad")


def test_rates_deprecated_warns(sqlite_fx: FxBharat) -> None:
    with pytest.warns(DeprecationWarning):
        sqlite_fx.rates(date(2024, 1, 1), date(2024, 1, 2))


def test_select_snapshot_dates_invalid_frequency() -> None:
    with pytest.raises(ValueError):
        FxBharat._select_snapshot_dates([date(2024, 1, 1)], "quarterly")


def test_latest_snapshot_returns_none_for_missing_date() -> None:
    rows = [ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=80.0)]
    assert FxBharat._latest_snapshot_from_rows(rows, date(2024, 1, 2), "RBI") is None
