"""Backend strategy interfaces for FxBharat."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Sequence

from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord


class BackendStrategy(ABC):
    """Common interface implemented by every database backend."""

    @abstractmethod
    def ensure_schema(self) -> None:
        """Create required tables/collections and verify connectivity."""

    @abstractmethod
    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        """Insert or update forex rates in bulk."""

    @abstractmethod
    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
    ) -> list[ForexRateRecord]:
        """Return forex rates constrained by the provided dates."""

    def close(self) -> None:  # pragma: no cover - optional cleanup hook
        """Backends may override to release connections/resources."""


__all__ = ["BackendStrategy"]
