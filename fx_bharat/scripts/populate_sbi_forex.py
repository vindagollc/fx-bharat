"""CLI entry point for seeding SBI forex rates."""

from __future__ import annotations

from fx_bharat.seeds.populate_sbi_forex import main

if __name__ == "__main__":  # pragma: no cover - thin wrapper
    main()
