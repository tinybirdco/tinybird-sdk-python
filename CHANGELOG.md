# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `TinybirdApi.sample_datasource()` starts a sample import job for S3/GCS/DynamoDB connected data sources via `POST /v0/datasources/{name}/sample`. For blob storage connectors `max_files` bounds the number of imported files; for DynamoDB the sample is bounded by either `rows` or `max_bytes` (mutually exclusive), or `full_export` triggers a full PITR export of the whole table instead of a bounded scan. This lets cloud branches and local import bounded DynamoDB samples, avoiding slow exports and unnecessary egress costs on branches.

## [0.4.0] - 2026-06-29

### Added

- S3 and GCS import data sources accept an optional `import_format` (`csv`/`ndjson`/`parquet`), emitted as `IMPORT_FORMAT` in the generated `.datasource` and round-tripped by the datafile parser and migration emitter. Lets you ingest files whose extension does not imply the format (for example NDJSON delivered as `.log`), where the connector would otherwise fail with `Format not supported`.

## [0.1.11] - 2026-06-15

### Changed

- Relaxed bundled `tinybird` CLI dependency to `>=4.6.0,<4.7.0` to avoid resolution failures while keeping the SDK on the `4.6.x` line.
- Updated branch data config handling to use `branch_data_mode`; legacy `branch_data_on_create` now triggers an explicit migration error.
- `branch_data_mode` now only accepts `last_partition` as a user-facing value.
- In `dev_mode=local`, branch data mode warnings are now shown only when `branch_data_mode` is explicitly set in `tinybird.config.json`.
- `tinybird branch create` and `tinybird branch clear` now show a deprecation warning (instead of failing) when `--ignore-datasource` is passed, then continue by ignoring that flag.
