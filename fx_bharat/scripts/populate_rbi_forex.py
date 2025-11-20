"""Compatibility shim that delegates to the seeds module."""

from __future__ import annotations

from fx_bharat.seeds.populate_rbi_forex import main

if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
