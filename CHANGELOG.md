# Changelog

## [0.2.1] - 2025-11-20
### Added
- Retry mechanism for ingestion  
- Seed dry_run support  
- Schema documentation  
- Source filter for queries  
- Yearly frequency cleanup  
- Mermaid ER diagram and schema.sql  
### Fixed
- README inconsistencies  
- Deprecated `.rates()` references  
- Broken links & method typos  
### Changed
- Improved ingestion resilience  
- Incremental seeding  
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