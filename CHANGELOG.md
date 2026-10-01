# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `define_gcs_connection` now accepts an HMAC key pair (`hmac_access_id`/`hmac_secret`) as an alternative to `service_account_credentials_json`, matching the Forward CLI's `gcloud_storage_hmac`/`gcloud_storage_sa` distinction. Emitted as `GCS_HMAC_ACCESS_ID`/`GCS_HMAC_SECRET` in the generated `.connection` file and round-tripped by the datafile parser and migration emitter.

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
