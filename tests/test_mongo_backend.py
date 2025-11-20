"""Mongo backend tests that monkeypatch pymongo primitives."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pytest

from fx_bharat.db import mongo_backend as mongo_module
from fx_bharat.ingestion.models import ForexRateRecord


class _DummyCursor:
    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self._docs = docs

    def sort(self, field: str, direction: int) -> List[Dict[str, Any]]:
        reverse = direction == -1
        return sorted(self._docs, key=lambda doc: doc[field], reverse=reverse)


class _DummyCollection:
    def __init__(self) -> None:
        self.docs: Dict[tuple[str, str], Dict[str, Any]] = {}
        self.indexes: list[tuple[tuple[tuple[str, int], ...], bool]] = []

    def __bool__(self) -> bool:  # pragma: no cover - behavioural parity with pymongo
        raise NotImplementedError("Collection truthiness is undefined")

    def create_index(self, fields: list[tuple[str, int]], unique: bool) -> None:
        self.indexes.append((tuple(fields), unique))

    def bulk_write(self, operations: list["_DummyUpdateOne"], ordered: bool) -> "_DummyBulkResult":
        assert ordered is False
        for op in operations:
            assert isinstance(op, _DummyUpdateOne)
            key = (op.filter["rate_date"], op.filter["currency_code"])
            self.docs[key] = dict(op.update["$set"])
        return _DummyBulkResult()

    def find(self, query: Dict[str, Dict[str, str]]):  # type: ignore[override]
        docs = list(self.docs.values())
        if "rate_date" in query:
            range_query = query["rate_date"]
            if "$gte" in range_query:
                docs = [doc for doc in docs if doc["rate_date"] >= range_query["$gte"]]
            if "$lte" in range_query:
                docs = [doc for doc in docs if doc["rate_date"] <= range_query["$lte"]]
        return _DummyCursor(docs)


class _DummyBulkResult:
    inserted_count = 0
    upserted_count = 0
    modified_count = 0


class _DummyUpdateOne:
    def __init__(
        self, filter: Dict[str, str], update: Dict[str, Dict[str, Any]], *, upsert: bool
    ) -> None:
        assert upsert is True
        self.filter = filter
        self.update = update
        self.upsert = upsert


class _DummyDatabase(dict):
    def __getitem__(self, name: str) -> _DummyCollection:  # type: ignore[override]
        if name not in self:
            self[name] = _DummyCollection()
        return dict.__getitem__(self, name)


class _DummyClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.admin = self
        self.closed = False
        self.databases: Dict[str, _DummyDatabase] = {}

    def __getitem__(self, name: str) -> _DummyDatabase:
        return self.databases.setdefault(name, _DummyDatabase())

    def get_default_database(self) -> _DummyDatabase:
        return self.__getitem__("default")

    def command(self, name: str) -> None:
        assert name == "ping"

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def patch_mongo_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mongo_module, "MongoClient", _DummyClient)
    monkeypatch.setattr(mongo_module, "PyMongoError", RuntimeError)
    monkeypatch.setattr(mongo_module, "UpdateOne", _DummyUpdateOne)


def test_mongo_backend_roundtrip(tmp_path: Path) -> None:
    backend = mongo_module.MongoBackend("mongodb://example.com/", database="fx")
    backend.ensure_schema()

    rows = [
        ForexRateRecord(rate_date=date(2024, 1, 1), currency="USD", rate=82.5),
        ForexRateRecord(rate_date=date(2024, 1, 2), currency="EUR", rate=89.1),
    ]
    result = backend.insert_rates(rows)
    assert result.inserted == 2

    fetched = backend.fetch_range(date(2024, 1, 1), date(2024, 1, 2))
    assert len(fetched) == 2
    assert fetched[0].currency == "USD"
    assert fetched[1].currency == "EUR"

    backend.close()
