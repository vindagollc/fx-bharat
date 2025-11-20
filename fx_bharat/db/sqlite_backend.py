"""SQLite backend strategy implementation."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Sequence

from fx_bharat.db import DEFAULT_SQLITE_DB_PATH
from fx_bharat.db.base_backend import BackendStrategy
from fx_bharat.db.sqlite_manager import PersistenceResult, SQLiteManager
from fx_bharat.ingestion.models import ForexRateRecord


class SQLiteBackend(BackendStrategy):
    """Backend strategy that stores rates in the bundled SQLite database."""

    def __init__(
        self,
        db_path: str | Path = DEFAULT_SQLITE_DB_PATH,
        *,
        manager: SQLiteManager | None = None,
    ) -> None:
        self.manager = manager or SQLiteManager(db_path)
        self.db_path = Path(self.manager.db_path)

    def ensure_schema(self) -> None:
        # ``SQLiteManager`` creates the schema in its constructor, so nothing else
        # is required here.
        return None

    def insert_rates(self, rows: Sequence[ForexRateRecord]) -> PersistenceResult:
        return self.manager.insert_rates(rows)

    def fetch_range(
        self,
        start: date | None = None,
        end: date | None = None,
        *,
        source: str | None = None,
    ) -> list[ForexRateRecord]:
        return self.manager.fetch_range(start, end, source=source)

    def close(self) -> None:  # pragma: no cover - trivial delegator
        self.manager.close()


__all__ = ["SQLiteBackend"]
