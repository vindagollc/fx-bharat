"""Database seeding utilities for :mod:`fx_bharat`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["seed_rbi_forex"]

if TYPE_CHECKING:  # pragma: no cover - import only for static analyzers
    from fx_bharat.seeds.populate_rbi_forex import seed_rbi_forex as seed_rbi_forex


def __getattr__(name: str) -> Any:
    """Lazily expose heavy seed helpers to avoid import-time side effects."""

    if name == "seed_rbi_forex":
        from fx_bharat.seeds.populate_rbi_forex import seed_rbi_forex as _seed

        return _seed
    raise AttributeError(f"module 'fx_bharat.seeds' has no attribute {name}")
