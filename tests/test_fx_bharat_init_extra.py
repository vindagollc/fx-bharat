"""Lightweight extra facade tests aligned to 0.4.x."""

from __future__ import annotations

from datetime import date

import pytest

from fx_bharat import DatabaseBackend, DatabaseConnectionInfo, FxBharat
from fx_bharat.utils.rbi import enforce_rbi_min_date


def test_database_backend_resolve_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        DatabaseBackend.resolve_backend_and_scheme("oracle")


def test_database_connection_info_normalises_database_name_param() -> None:
    url = "mongodb://localhost/?DATABASE_NAME=testdb"
    info = DatabaseConnectionInfo.from_url(url)
    assert info.name == "testdb"
    assert "DATABASE_NAME" not in info.url


def test_connection_probe_mongodb_missing_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    info = DatabaseConnectionInfo(
        backend=DatabaseBackend.MONGODB,
        url="mongodb://localhost/testdb",
        name="testdb",
        username=None,
        password=None,
        host=None,
        port=None,
    )
    monkeypatch.setattr("fx_bharat.MongoClient", None)
    monkeypatch.setattr("fx_bharat.MongoBackend", lambda url, database=None: object())
    client = FxBharat(info)

    ok, message = client.connection()
    assert ok is False
    assert "pymongo" in (message or "")


def test_conection_alias_calls_connection(tmp_path) -> None:
    client = FxBharat(db_config=f"sqlite:///{tmp_path/'fx.db'}")
    assert client.conection() == client.connection()


def test_enforce_rbi_min_date_raises_before_threshold() -> None:
    with pytest.raises(ValueError):
        enforce_rbi_min_date(date(2022, 3, 31))
