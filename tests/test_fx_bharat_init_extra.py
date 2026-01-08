from __future__ import annotations

from datetime import date

import pytest

import fx_bharat
from fx_bharat import DatabaseBackend, DatabaseConnectionInfo, FxBharat


def test_database_backend_resolve_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        DatabaseBackend.resolve_backend_and_scheme("oracle")


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
