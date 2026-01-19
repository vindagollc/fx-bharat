# Changelog

## [0.4.1] - 2026-01-20
### Added
- `update_daily` now backfills from the last ingestion checkpoints up to today to catch missed days automatically.
- RBI downloads reuse existing monthly CSV/Excel files; SBI archive downloads skip already-downloaded PDFs and support parallel batch fetching.
- Broader unit-test coverage for facades, relational backends (SQLite/MySQL/Postgres), Mongo ingestion checkpoints, and seed flows.
### Changed
- RBI workbook converter handles already-downloaded CSVs and gracefully falls back when `pandas.read_html` finds no tables.
- Ingestion checkpoints update `updated_at` on every write and guard against date regression across backends.
- SBI historical seeding enforces end date < today; SBI `seed` default end aligns with RBI start at 2022-04-01.
### Fixed
- Outlier filtering prevents overflow on extreme/NaN numeric values during relational inserts.
- SQLite LME schema patch rewrites tables to drop legacy `usd_*` columns.
- Example scripts now use explicit DB URLs, 2022-04-01 start, and updated version banners.

## [0.4.0] - 2026-01-19
### Breaking
- Removed bundled SQLite database/resources; a database URL is now required.
- Dropped the `migrate` helper and SQLite fallback APIs.
### Added
- Historical seeding now streams data directly into user databases, sourcing SBI archives from the public GitHub repo (`sahilgupta/sbi-fx-ratekeeper`) and RBI/LME data from live endpoints.
- New `update_daily()` helper to fetch the latest RBI/SBI/LME rows each day.
- Caching for RBI downloads: reuse existing monthly CSV/Excel files and skip Selenium fetch when present.
- SBI archive downloader now skips already-downloaded PDFs and supports parallel batch downloads.
- Broader test coverage across facades, relational backends (SQLite/MySQL/Postgres paths), Mongo ingestion checkpoints, and seed flows, lifting overall coverage above 90%.
- RBI workbook converter now handles already-downloaded CSVs and falls back gracefully when `pandas.read_html` finds no tables.
- Ingestion checkpoints now update `updated_at` on every write and guard against date regression across SQLite/MySQL/Postgres.
- Seed flow defaults clarified: historical SBI ingestion stops before today while `seed_sbi_today` handles same-day ingest.
- Example scripts updated to use explicit DB URLs and new seed/update paths.
### Notes
- SQLite remains supported only when explicitly configured via `db_config`; no database file ships with the package.
### Changed
- Historical seeding defaults to 2022-04-01 for RBI, SBI, and LME Copper/Aluminum.
- LME seeding and SBI/RBI ingestion update `ingestion_metadata` directly in external backends.

## [0.3.1] - 2025-11-23
### Changed
- `FxBharat.migrate()` now copies forex/LME rows in chunks and logs progress totals.
- `FxBharat.migrate()` accepts `from_date`/`to_date` to migrate a specific window.
- `FxBharat.migrate()` now updates `ingestion_metadata` in the target backend.
- LME tables drop unused `usd_*`/`eur_*` columns when ensuring schema.
- Relational backend migrations now use driver-level bulk upserts (psycopg2 execute_values / MySQL executemany).

## [0.3.0] - 2025-11-22
### Added
- LME Copper and Aluminum ingestion with resilient HTML parsing.
- New SQLite/relational/MongoDB tables for `lme_copper_rates` and `lme_aluminum_rates` plus migration support.
- `seed_lme`, `seed_lme_copper`, and `seed_lme_aluminum` helpers to populate the new datasets.
### Changed
- `FxBharat.migrate()` now copies LME data into external backends alongside forex rates.

## [0.2.1] - 2025-11-21
### Added
- Retry mechanism for ingestion
- Seed dry_run support
- Schema documentation
- Source filter for queries
- Yearly frequency cleanup
- Mermaid ER diagram and schema.sql
- Ingestion metadata checkpoints for cron-friendly seeding
- Migration guide for 0.1.0 → 0.2.x
- Bundled RBI reference data refreshed through **20/11/2025**
- Bundled SBI reference data refreshed through **21/11/2025**
### Fixed
- README inconsistencies
- Deprecated `.rates()` references
- Broken links & method typos
- RBI seeding now stops early when the archive reports "No Reference Rate Found."
### Changed
- Improved ingestion resilience
- Incremental seeding with unified `seed(from_date, to_date, source)` API (replaces `seed_historical`)
- CI coverage setup

## [0.2.0] - 2025-11-20
### Added
- Added SBI Forex Card PDF ingestion and seeding utilities, including CLI and seed helpers.
- Introduced source-aware `rate`/`history` queries and examples that surface the data source in responses.
- Split persistence into dedicated RBI and SBI tables/collections across SQLite, relational backends, and MongoDB.
- Updated public query helpers (`rate`, `history`/`historical`/`rates`) to return SBI and RBI snapshots with no `source` argument.
- Simplified daily seeding to populate both sources together and aligned migration/output examples with the new shape.
- Documented the dual-source return payloads and updated examples to show SBI + RBI ordering.
- Documented multi-database usage and SBI workflows in README and examples.
- Declared new PDF parsing dependency and version bump for the release.
### Changed
- Renamed the `rates` query helper to `history`/`historical` (with a deprecated `rates` alias for backward compatibility).

## [0.1.0] - 2025-11-19
### Added
- Added package structure
- Initial version of package.
- Documented multi-database usage and SBI workflows in README and examples.
