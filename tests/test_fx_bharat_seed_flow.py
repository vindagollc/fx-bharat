from __future__ import annotations

from datetime import date, timedelta

import pytest

import fx_bharat
from fx_bharat import FxBharat
from fx_bharat.utils.rbi import RBI_MIN_AVAILABLE_DATE


def test_getattr_rbi_selenium(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Dummy:
        pass

    monkeypatch.setattr("fx_bharat.ingestion.rbi_selenium.RBISeleniumClient", _Dummy)
    assert fx_bharat.__getattr__("RBISeleniumClient") is _Dummy


def test_seed_invokes_all_sources_and_lme(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    fx = FxBharat(db_config=f"sqlite:///{tmp_path/'flow.db'}")
    calls: dict[str, object] = {}

    monkeypatch.setattr(fx._get_backend_strategy(), "ensure_schema", lambda: None)

    def _rbi(start, end, **kwargs):
        calls["rbi"] = (start, end, kwargs)

    def _sbi_hist(**kwargs):
        calls["sbi_hist"] = kwargs

    def _sbi_today(**kwargs):
        calls["sbi_today"] = kwargs

    def _lme(metal, **kwargs):
        calls.setdefault("lme", []).append((metal, kwargs))

    monkeypatch.setattr("fx_bharat.seeds.populate_rbi_forex.seed_rbi_forex", _rbi)
    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.seed_sbi_historical", _sbi_hist)
    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.seed_sbi_today", _sbi_today)
    monkeypatch.setattr("fx_bharat.seeds.populate_lme.seed_lme_prices", _lme)

    start = RBI_MIN_AVAILABLE_DATE
    end = start + timedelta(days=1)
    fx.seed(from_date=start, to_date=end, include_lme=True, incremental=True, dry_run=True)

    assert calls["rbi"][0] == start.isoformat()
    assert calls["sbi_hist"]["download"] is True
    assert len(calls["lme"]) == 2


def test_seed_includes_sbi_today_when_end_is_today(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    fx = FxBharat(db_config=f"sqlite:///{tmp_path/'flow_today.db'}")
    calls: dict[str, object] = {}

    monkeypatch.setattr(fx._get_backend_strategy(), "ensure_schema", lambda: None)

    class _CheckpointBackend:
        def __init__(self):
            self.calls = 0

        def ingestion_checkpoint(self, source: str):
            self.calls += 1
            return date(2024, 1, 1)

        def ensure_schema(self):
            return None

    backend = _CheckpointBackend()
    fx._backend_strategy = backend

    monkeypatch.setattr("fx_bharat.seeds.populate_rbi_forex.seed_rbi_forex", lambda *a, **k: None)

    def _sbi_hist(**kwargs):
        calls["hist"] = kwargs

    def _sbi_today(**kwargs):
        calls["today"] = kwargs

    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.seed_sbi_historical", _sbi_hist)
    monkeypatch.setattr("fx_bharat.seeds.populate_sbi_forex.seed_sbi_today", _sbi_today)

    today = date.today()
    fx.seed(from_date=today, to_date=today, include_lme=False, incremental=True, dry_run=True)

    # When start=end=today, historical ingest is skipped and only today's PDF is fetched.
    assert "hist" not in calls
    assert "today" in calls


def test_update_daily_uses_ingestion_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    fx = FxBharat(db_config="sqlite:///tmp.db")
    today = date.today()
    called: dict[str, object] = {}

    class _BackendStub:
        def __init__(self):
            self.checkpoints = {
                "RBI": today - timedelta(days=2),
                "SBI": today - timedelta(days=3),
                "LME_COPPER": today - timedelta(days=1),
                "LME_ALUMINUM": None,
            }

        def ingestion_checkpoint(self, source: str):
            return self.checkpoints.get(source)

        def ensure_schema(self):
            return None

    backend = _BackendStub()
    fx._backend_strategy = backend

    def _fake_seed(self, *, from_date, to_date, **kwargs):  # type: ignore[no-untyped-def]
        called["from_date"] = from_date
        called["to_date"] = to_date
        called["kwargs"] = kwargs

    monkeypatch.setattr(FxBharat, "seed", _fake_seed)

    fx.update_daily(source=None, include_lme=True, dry_run=True)

    assert called["from_date"] == RBI_MIN_AVAILABLE_DATE
    assert called["to_date"] == today
