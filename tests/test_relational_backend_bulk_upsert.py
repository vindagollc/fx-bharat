from __future__ import annotations

import sys
import types
from datetime import date
from typing import Sequence

import pytest

from fx_bharat.db.relational_backend import RelationalBackend
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


class _DummyCursor:
    def __init__(self, calls: list) -> None:
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str) -> None:
        self.calls.append(sql)

    def executemany(self, sql: str, values) -> None:
        self.calls.append((sql, values))


class _DummyRaw:
    def __init__(self) -> None:
        self.calls: list = []

    def cursor(self):
        return _DummyCursor(self.calls)


class _DummyConnection:
    def __init__(self, dialect: str) -> None:
        self.engine = types.SimpleNamespace(dialect=types.SimpleNamespace(name=dialect))
        self.connection = _DummyRaw()
        self.executed: list = []

    def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        return types.SimpleNamespace(scalar=lambda: None, _mapping={})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyEngine:
    def __init__(self, dialect: str) -> None:
        self.dialect = types.SimpleNamespace(name=dialect)
        self._connection = _DummyConnection(dialect)

    def begin(self):
        return self._connection


def _forex_rows() -> Sequence[ForexRateRecord]:
    return [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.0, source="RBI"),
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=81.5, source="SBI"),
    ]


def test_postgres_bulk_upsert_uses_execute_values(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = RelationalBackend("postgresql://example.com/db")
    engine = _DummyEngine("postgresql")
    monkeypatch.setattr(backend, "_get_engine", lambda: engine)

    calls: list = []

    def _fake_execute_values(cursor, sql: str, values):  # noqa: ANN001
        calls.append((sql, values))

    extras = types.SimpleNamespace(execute_values=_fake_execute_values)
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(extras=extras))
    monkeypatch.setitem(sys.modules, "psycopg2.extras", extras)

    backend.insert_rates(_forex_rows())

    assert calls, "execute_values should be invoked for postgres fast path"
    assert "ON CONFLICT" in calls[0][0]


def test_mysql_bulk_upsert_uses_executemany(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = RelationalBackend("mysql://example.com/db")
    engine = _DummyEngine("mysql")
    monkeypatch.setattr(backend, "_get_engine", lambda: engine)

    backend.insert_rates(_forex_rows())

    raw_calls = engine._connection.connection.calls
    assert raw_calls, "executemany should be invoked for MySQL fast path"
    sql, values = raw_calls[0]
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert len(values) >= 1


def test_sqlite_lme_schema_recreates_table_without_unwanted_columns() -> None:
    backend = RelationalBackend("sqlite:///:memory:")

    class _DummyResult:
        def __init__(self, rows):
            self._rows = rows

        def __iter__(self):
            return iter(self._rows)

    class _SQLiteConnection(_DummyConnection):
        def __init__(self):
            super().__init__("sqlite")

        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "PRAGMA table_info" in sql:
                # Only unwanted column present to force recreation
                return _DummyResult([(0, "usd_price"), (1, "rate_date")])
            self.executed.append(sql)
            return _DummyResult([])

    connection = _SQLiteConnection()
    backend._ensure_lme_schema(connection)

    # Should create and swap temp tables for both LME tables
    assert any("CREATE TABLE lme_copper_rates_tmp" in sql for sql in connection.executed)
    assert any(
        "ALTER TABLE lme_copper_rates_tmp RENAME TO lme_copper_rates" in sql
        for sql in connection.executed
    )
    assert any("CREATE TABLE lme_aluminum_rates_tmp" in sql for sql in connection.executed)
    assert any(
        "ALTER TABLE lme_aluminum_rates_tmp RENAME TO lme_aluminum_rates" in sql
        for sql in connection.executed
    )


def test_mysql_upsert_sql_fallback_when_no_raw_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = RelationalBackend("mysql://example.com/db")

    class _NoRawConnection(_DummyConnection):
        def __init__(self):
            super().__init__("mysql")
            self.connection = None

    engine = _DummyEngine("mysql")
    engine._connection = _NoRawConnection()
    monkeypatch.setattr(backend, "_get_engine", lambda: engine)

    result = backend.insert_rates(_forex_rows())

    # Falls back to slower SQLAlchemy path; rows still counted
    assert result.inserted == 2


def test_postgres_fallback_when_psycopg2_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = RelationalBackend("postgresql://example.com/db")

    class _NoRawConnection(_DummyConnection):
        def __init__(self):
            super().__init__("postgresql")
            self.connection = None

    engine = _DummyEngine("postgresql")
    engine._connection = _NoRawConnection()
    monkeypatch.setattr(backend, "_get_engine", lambda: engine)

    # Ensure psycopg2 import fails inside _postgres_bulk_upsert
    for name in list(sys.modules):
        if name.startswith("psycopg2"):
            sys.modules.pop(name)

    result = backend.insert_rates(_forex_rows())

    assert result.inserted == 2
    # Fallback path should have executed SQLAlchemy text statement
    assert any("ON CONFLICT" in sql for sql, _ in engine._connection.executed)


def test_insert_rates_filters_outliers_sqlite(tmp_path) -> None:
    backend = RelationalBackend(f"sqlite:///{tmp_path/'outlier.db'}")
    backend.ensure_schema()

    rows = [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.0, source="RBI"),
        ForexRateRecord(rate_date=date(2024, 1, 2), currency="USD", rate=1e13, source="RBI"),
        ForexRateRecord(
            rate_date=date(2024, 1, 3), currency="USD", rate=float("nan"), source="RBI"
        ),
        ForexRateRecord(
            rate_date=date(2024, 1, 4), currency="USD", rate=90.0, source="SBI", tt_buy=1e20
        ),
    ]

    result = backend.insert_rates(rows)

    assert result.inserted == 1
    fetched = backend.fetch_range()
    assert len(fetched) == 1
    assert fetched[0].rate_date == date(2024, 1, 1)


def test_insert_lme_rates_postgres_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = RelationalBackend("postgresql://example.com/db")

    class _NoRawConnection(_DummyConnection):
        def __init__(self):
            super().__init__("postgresql")
            self.connection = None

    engine = _DummyEngine("postgresql")
    engine._connection = _NoRawConnection()
    monkeypatch.setattr(backend, "_get_engine", lambda: engine)
    for name in list(sys.modules):
        if name.startswith("psycopg2"):
            sys.modules.pop(name)

    rows = [
        LmeRateRecord(
            rate_date=date(2024, 1, 1), price=1.0, price_3_month=2.0, stock=3, metal="COPPER"
        )
    ]

    result = backend.insert_lme_rates("COPPER", rows)

    assert result.inserted == 1
    assert any("ON CONFLICT" in sql for sql, _ in engine._connection.executed)


def test_insert_lme_rates_mysql_fast_path(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = RelationalBackend("mysql://example.com/db")
    engine = _DummyEngine("mysql")
    monkeypatch.setattr(backend, "_get_engine", lambda: engine)

    rows = [
        LmeRateRecord(
            rate_date=date(2024, 1, 1), price=1.0, price_3_month=2.0, stock=3, metal="ALUMINUM"
        )
    ]

    result = backend.insert_lme_rates("ALUMINUM", rows)

    raw_calls = engine._connection.connection.calls
    assert result.inserted == 1
    assert raw_calls
    sql, values = raw_calls[0]
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert len(values) == 1
