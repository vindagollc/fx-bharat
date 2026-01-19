from __future__ import annotations

from pathlib import Path

from fx_bharat.db.sqlite_backend import SQLiteBackend
from fx_bharat.seeds import populate_rbi_forex
from fx_bharat.seeds.populate_rbi_forex import PersistenceResult, RBINoReferenceRateError


class _DummyClient:
    download_dir = Path("/tmp")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def fetch_excel(self, *_args, **_kwargs):
        raise RBINoReferenceRateError("not ready")


def test_seed_rbi_forex_stops_on_missing_reference_rate(monkeypatch, tmp_path):
    monkeypatch.setattr(populate_rbi_forex, "RBISeleniumClient", lambda **_kwargs: _DummyClient())

    backend = SQLiteBackend(db_path=tmp_path / "rbi.db")
    result = populate_rbi_forex.seed_rbi_forex(
        "2025-11-20",
        "2025-11-21",
        backend=backend,
    )

    assert result == PersistenceResult()
