"""Historical SQLite bundle support (deprecated/removed)."""

from __future__ import annotations

from pathlib import Path
from typing import Final

__all__ = ["DEFAULT_SQLITE_DB_PATH", "bundled_sqlite_path"]

# Kept for backward-compatibility; the file is no longer packaged.
DEFAULT_SQLITE_DB_PATH: Final[Path] = Path(__file__).resolve().with_name("forex.db")


def bundled_sqlite_path() -> Path:
    """Return the legacy SQLite path (file no longer packaged)."""

    return DEFAULT_SQLITE_DB_PATH
