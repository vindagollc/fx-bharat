from __future__ import annotations

from types import SimpleNamespace

import pytest

import fx_bharat
from fx_bharat import FxBharat


def test_getattr_lazy_seed_rbi(monkeypatch: pytest.MonkeyPatch) -> None:
    import fx_bharat.seeds.populate_rbi_forex as rbi_module

    sentinel = object()
    monkeypatch.setattr(rbi_module, "seed_rbi_forex", sentinel)

    assert fx_bharat.__getattr__("seed_rbi_forex") is sentinel
    with pytest.raises(AttributeError):
        fx_bharat.__getattr__("does_not_exist")


def test_build_connection_info_requires_config() -> None:
    with pytest.raises(ValueError):
        FxBharat(db_config=None)  # type: ignore[arg-type]


def test_build_external_backend_rejects_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    fx = FxBharat(db_config="sqlite:///tmp.db")
    fx.connection_info = SimpleNamespace(backend="NOPE", url="", name=None)
    with pytest.raises(ValueError):
        fx._build_external_backend()


def test_update_daily_delegates_seed(monkeypatch) -> None:
    fx = FxBharat(db_config="sqlite:///tmp.db")
    called: dict[str, object] = {}

    def _fake_seed(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        called.update(kwargs)

    monkeypatch.setattr(FxBharat, "seed", _fake_seed)

    fx.update_daily(source="RBI", include_lme=False, dry_run=True)

    assert called["source"] == "RBI"
    assert called["include_lme"] is False
    assert called["incremental"] is True
    assert called["dry_run"] is True
