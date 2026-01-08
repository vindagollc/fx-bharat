# Changelog

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
