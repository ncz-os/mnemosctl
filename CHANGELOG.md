# Changelog

## [0.2.0] - 2026-05-05

### Added

- Added `--progress [N]` to `sync-from` and `import`.
- Added `--skip-bad` to `import`.

### Improved

- Stream JSONL imports line by line with an import ledger for rerun idempotency.
- Make `sync-from` report newly inserted rows while preserving paginated, idempotent sqlite upserts.
