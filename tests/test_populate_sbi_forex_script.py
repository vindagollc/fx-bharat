from __future__ import annotations

import runpy

from fx_bharat.seeds import populate_sbi_forex as seeds_module


def test_populate_sbi_forex_script_invokes_main(monkeypatch) -> None:
    called = {"value": False}

    def _fake_main() -> None:
        called["value"] = True

    monkeypatch.setattr(seeds_module, "main", _fake_main)

    runpy.run_module("fx_bharat.scripts.populate_sbi_forex", run_name="__main__")

    assert called["value"] is True
