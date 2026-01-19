from __future__ import annotations

import pytest

from fx_bharat.db import mongo_backend as mongo_module


def test_mongo_backend_requires_client(monkeypatch) -> None:
    monkeypatch.setattr(mongo_module, "MongoClient", None)
    with pytest.raises(ModuleNotFoundError):
        mongo_module.MongoBackend("mongodb://example.com/", database="fx")


def test_mongo_backend_requires_database_name(monkeypatch) -> None:
    class _NoDefaultClient:
        def __init__(self, url: str) -> None:
            self.url = url

        def get_default_database(self):
            return None

    monkeypatch.setattr(mongo_module, "MongoClient", _NoDefaultClient)
    with pytest.raises(ValueError):
        mongo_module.MongoBackend("mongodb://example.com/")


def test_mongo_backend_rejects_unknown_metal() -> None:
    with pytest.raises(ValueError):
        mongo_module.MongoBackend._normalise_metal("gold")


def test_mongo_ingestion_checkpoint_roundtrip(monkeypatch) -> None:
    from datetime import date

    class _IngestionCollection(dict):
        def update_one(self, filter, update, upsert=False):  # noqa: ANN001
            key = filter["source"]
            current = self.get(key, {})
            # simulate $max on last_ingested_date
            new_date = update["$max"]["last_ingested_date"]
            current_date = current.get("last_ingested_date")
            if current_date is None or new_date > current_date:
                current["last_ingested_date"] = new_date
            current["updated_at"] = update["$set"]["updated_at"]
            current["source"] = key
            self[key] = current

        def find_one(self, filter):  # noqa: ANN001
            return self.get(filter["source"])

    class _DummyCollection(_IngestionCollection):
        def __init__(self):
            super().__init__()

        def bulk_write(self, *_args, **_kwargs):
            return None

        def create_index(self, *_args, **_kwargs):
            return None

        def find(self, *_args, **_kwargs):
            return []

        def sort(self, *_args, **_kwargs):
            return []

    class _DB(dict):
        def __getitem__(self, name: str):
            if name not in self:
                self[name] = _DummyCollection()
            return dict.__getitem__(self, name)

    class _Client:
        def __init__(self, url: str):
            self.url = url
            self.default = _DB()
            self.admin = self

        def get_default_database(self):
            return self.default

        def __getitem__(self, name: str):
            return self.default

        def command(self, name: str):
            assert name == "ping"

        def close(self):
            return None

    monkeypatch.setattr(mongo_module, "MongoClient", _Client)
    monkeypatch.setattr(mongo_module, "PyMongoError", RuntimeError)

    backend = mongo_module.MongoBackend("mongodb://example.com/")
    backend.update_ingestion_checkpoint("RBI", date(2024, 1, 2))
    backend.update_ingestion_checkpoint("RBI", date(2024, 1, 1))
    checkpoint = backend.ingestion_checkpoint("RBI")

    assert checkpoint == date(2024, 1, 2)
