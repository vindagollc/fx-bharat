from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

import fx_bharat
from fx_bharat.seeds import __getattr__ as seeds_getattr


def test_package_seed_wrappers_delegate(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def _fake(name: str):
        def _inner(*_args, **_kwargs):
            calls.append(name)
            return name

        return _inner

    module = importlib.import_module("fx_bharat.seeds.populate_sbi_forex")
    monkeypatch.setattr(module, "seed_sbi_forex", _fake("forex"))
    monkeypatch.setattr(module, "seed_sbi_historical", _fake("historical"))
    monkeypatch.setattr(module, "seed_sbi_today", _fake("today"))

    assert fx_bharat.seed_sbi_forex() == "forex"
    assert fx_bharat.seed_sbi_historical() == "historical"
    assert fx_bharat.seed_sbi_today() == "today"

    assert calls == ["forex", "historical", "today"]


def test_seeds_dunder_getattr_lazy_import(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("fx_bharat.seeds.populate_sbi_forex")
    monkeypatch.setattr(module, "seed_sbi_forex", SimpleNamespace(name="sentinel_forex"))
    monkeypatch.setattr(module, "seed_sbi_historical", SimpleNamespace(name="sentinel_hist"))
    monkeypatch.setattr(module, "seed_sbi_today", SimpleNamespace(name="sentinel_today"))

    assert seeds_getattr("seed_sbi_forex").name == "sentinel_forex"
    assert seeds_getattr("seed_sbi_historical").name == "sentinel_hist"
    assert seeds_getattr("seed_sbi_today").name == "sentinel_today"

    with pytest.raises(AttributeError):
        seeds_getattr("nonexistent")
