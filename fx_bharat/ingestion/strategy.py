"""Abstractions for pluggable ingestion strategies."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Protocol


class IngestionStrategy(Protocol):
    """Contract for fetching raw forex artifacts.

    Concrete implementations are expected to retrieve data for a date range and
    return a local ``Path`` pointing at the downloaded artifact.
    """

    def fetch(self, start_date: date, end_date: date, *, destination: Path | None = None) -> Path:
        ...  # pragma: no cover - protocol definition


__all__ = ["IngestionStrategy"]
