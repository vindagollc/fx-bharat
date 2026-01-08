from __future__ import annotations

from fx_bharat.db.relational_backend import RelationalBackend


class _DummyResult:
    def __init__(self, rows=None, scalar_value=None) -> None:
        self._rows = rows or []
        self._scalar_value = scalar_value

    def __iter__(self):
        return iter(self._rows)

    def scalar(self):
        return self._scalar_value


class _DummyDialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _DummyEngine:
    def __init__(self, name: str) -> None:
        self.dialect = _DummyDialect(name)


class _DummyConnection:
    def __init__(self, dialect_name: str, existing: set[str]) -> None:
        self.engine = _DummyEngine(dialect_name)
        self.existing = existing
        self.executed: list[str] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append(sql)
        if "SELECT DATABASE()" in sql:
            return _DummyResult(scalar_value="testdb")
        if "information_schema.columns" in sql:
            return _DummyResult(rows=[(col,) for col in self.existing])
        return _DummyResult()


def test_relational_backend_schema_patch_postgres() -> None:
    backend = RelationalBackend("sqlite:///:memory:")
    connection = _DummyConnection("postgresql", {"rate_date", "usd_price"})

    backend._ensure_lme_schema(connection)

    assert any(
        "ALTER TABLE lme_copper_rates ADD COLUMN price" in sql for sql in connection.executed
    )
    assert any(
        "ALTER TABLE lme_copper_rates DROP COLUMN IF EXISTS usd_price" in sql
        for sql in connection.executed
    )


def test_relational_backend_schema_patch_mysql() -> None:
    backend = RelationalBackend("sqlite:///:memory:")
    connection = _DummyConnection("mysql", {"rate_date", "usd_price"})

    backend._ensure_lme_schema(connection)

    assert any(
        "ALTER TABLE lme_aluminum_rates ADD COLUMN price" in sql for sql in connection.executed
    )
    assert any(
        "ALTER TABLE lme_aluminum_rates DROP COLUMN usd_price" in sql for sql in connection.executed
    )
