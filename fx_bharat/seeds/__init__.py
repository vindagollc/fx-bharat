"""Database seeding utilities for :mod:`fx_bharat`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = [
    "seed_rbi_forex",
    "seed_sbi_historical",
    "seed_sbi_today",
    "seed_sbi_forex",
    "seed_lme_prices",
    "seed_lme_copper",
    "seed_lme_aluminum",
]

if TYPE_CHECKING:  # pragma: no cover - import only for static analyzers
    from fx_bharat.seeds.populate_rbi_forex import seed_rbi_forex as seed_rbi_forex


def __getattr__(name: str) -> Any:
    """Lazily expose heavy seed helpers to avoid import-time side effects."""

    if name == "seed_rbi_forex":
        from fx_bharat.seeds.populate_rbi_forex import seed_rbi_forex as _seed

        return _seed
    if name in {"seed_lme_prices", "seed_lme_copper", "seed_lme_aluminum"}:
        from fx_bharat.seeds.populate_lme import seed_lme_aluminum as _seed_lme_aluminum
        from fx_bharat.seeds.populate_lme import seed_lme_copper as _seed_lme_copper
        from fx_bharat.seeds.populate_lme import seed_lme_prices as _seed_lme_prices

        return {
            "seed_lme_prices": _seed_lme_prices,
            "seed_lme_copper": _seed_lme_copper,
            "seed_lme_aluminum": _seed_lme_aluminum,
        }[name]
    if name in {"seed_sbi_historical", "seed_sbi_today", "seed_sbi_forex"}:
        from fx_bharat.seeds.populate_sbi_forex import seed_sbi_forex as _seed_sbi_forex
        from fx_bharat.seeds.populate_sbi_forex import seed_sbi_historical as _seed_sbi_historical
        from fx_bharat.seeds.populate_sbi_forex import seed_sbi_today as _seed_sbi_today

        return {
            "seed_sbi_historical": _seed_sbi_historical,
            "seed_sbi_forex": _seed_sbi_forex,
            "seed_sbi_today": _seed_sbi_today,
        }[name]
    raise AttributeError(f"module 'fx_bharat.seeds' has no attribute {name}")
