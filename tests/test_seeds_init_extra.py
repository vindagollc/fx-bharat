from __future__ import annotations

import pytest

import fx_bharat.seeds as seeds


def test_seeds_module_lazy_getattr_exports() -> None:
    assert callable(getattr(seeds, "seed_rbi_forex"))
    assert callable(getattr(seeds, "seed_lme_prices"))
    assert callable(getattr(seeds, "seed_sbi_today"))


def test_seeds_module_invalid_attribute() -> None:
    with pytest.raises(AttributeError):
        getattr(seeds, "unknown_seed")
