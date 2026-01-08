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
