# Migration Guide

## Upgrading from 0.1.0 to 0.2.0
1. Install the new version: `pip install -U fx-bharat==0.2.1`.
2. Run `fx.seed(from_date=date(2022, 4, 12), to_date=date.today())` once to populate the new RBI/SBI split tables.
3. Update application code to use `history`/`historical` instead of `rates` (still available as a deprecated alias).
4. Review README examples for revised payload shapes that now include the `source` field.

## Upgrading from 0.2.0 to 0.2.1
1. Install the new version: `pip install -U fx-bharat==0.2.1`.
2. Allow the package to create the new `ingestion_metadata` table by running `fx.seed()`; this records the latest seeded date per source for incremental runs.
3. Replace any `seed_historical` usage with the unified `seed(from_date=..., to_date=..., source=...)` helper.
4. Schedule `fx.seed()` as a cron job to continue ingestion from the recorded checkpoint through the current day.
5. Re-run your test suite; coverage has moved to pytest-cov and Codecov workflows in CI.
6. The bundled RBI/SBI snapshots ship data through **21/11/2025**.
