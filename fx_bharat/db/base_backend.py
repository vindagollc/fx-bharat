"""Backend strategy interfaces for FxBharat."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Sequence

from fx_bharat.db.sqlite_manager import PersistenceResult
from fx_bharat.ingestion.models import ForexRateRecord, LmeRateRecord


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
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        """Return forex rates constrained by the provided dates."""

    def insert_lme_rates(self, metal: str, rows: Sequence[LmeRateRecord]) -> PersistenceResult:
        """Insert or update LME prices in bulk."""
        msg = f"LME inserts not implemented for backend {type(self).__name__}"
        raise NotImplementedError(msg)

    def fetch_lme_range(
        self, metal: str, start: date | None = None, end: date | None = None
    ) -> list[LmeRateRecord]:
        """Return LME prices for the selected metal."""
        msg = f"LME fetch not implemented for backend {type(self).__name__}"
        raise NotImplementedError(msg)

    def update_ingestion_checkpoint(self, source: str, rate_date: date) -> None:
        """Persist the latest ingested date for a source."""
        msg = f"Ingestion metadata not implemented for backend {type(self).__name__}"
        raise NotImplementedError(msg)

    def close(self) -> None:  # pragma: no cover - optional cleanup hook
        """Backends may override to release connections/resources."""


__all__ = ["BackendStrategy"]
