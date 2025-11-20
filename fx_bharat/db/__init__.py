"""Helpers for working with the bundled SQLite database."""

from __future__ import annotations

from pathlib import Path
from typing import Final

__all__ = ["DEFAULT_SQLITE_DB_PATH", "bundled_sqlite_path"]

# ``Path(__file__)`` points at ``fx_bharat/db/__init__.py`` so replacing the
# filename gives us the location of ``forex.db`` irrespective of the working
# directory. Using ``resolve`` ensures callers always receive an absolute path
# which SQLite requires when the package is installed in site-packages.
DEFAULT_SQLITE_DB_PATH: Final[Path] = Path(__file__).resolve().with_name("forex.db")


def bundled_sqlite_path() -> Path:
    """Return the absolute path to the packaged ``forex.db`` file."""

    return DEFAULT_SQLITE_DB_PATH
