from __future__ import annotations

from datetime import date

import pytest

from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord


class _DummyBackend(BackendStrategy):
    def ensure_schema(self) -> None:
        return None

    def insert_rates(self, rows: list[ForexRateRecord]) -> PersistenceResult:
        return PersistenceResult(inserted=len(rows))

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        return []


def test_base_backend_lme_methods_raise_not_implemented() -> None:
    backend = _DummyBackend()

    with pytest.raises(NotImplementedError):
        backend.insert_lme_rates("COPPER", [])
    with pytest.raises(NotImplementedError):
        backend.fetch_lme_range("COPPER")
