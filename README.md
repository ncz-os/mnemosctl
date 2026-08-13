# mnemosctl

`mnemosctl` is a Rust desktop CLI for the MNEMOS memory system.

## Install

```sh
cargo install --git https://gitlab.com/mnemos-os/mnemosctl
```

## Config

Configure the REST API base URL and Bearer token with environment variables:

```sh
export MNEMOS_BASE=http://192.168.207.67:5002
export MNEMOS_API_KEY=your-api-key
```

You can also use `~/.mnemos/config.toml`:

```toml
base_url = "http://192.168.207.67:5002"
api_key = "your-api-key"
```

Environment variables take precedence over the config file.

## Commands

```sh
mnemosctl health
```

Calls `GET /health` and pretty-prints the JSON response.

```sh
mnemosctl search "project notes"
mnemosctl search "project notes" --limit 25 --namespace work --semantic
```

Posts a search request to `/v1/memories/search`.
The command uses MNEMOS's default `balanced` retrieval profile and accepts
limits from 1 through 100, matching that profile's server-side cap.

```sh
mnemosctl create --content "The launch checklist lives in Drive" --category facts
echo "Remember the staging host IP" | mnemosctl create --category facts
```

Creates a memory from `--content` or stdin.

```sh
mnemosctl get abc123
```

Fetches a single memory by ID.

```sh
mnemosctl sync-from http://remote-mnemos:5002
mnemosctl sync-from http://remote-mnemos:5002 --progress
mnemosctl sync-from http://remote-mnemos:5002 --progress 500
mnemosctl sync-from http://remote-mnemos:5002 --page-size 500
```

Pulls memories from a remote MNEMOS host in pages of 100 and upserts them into `~/.mnemos/mnemosctl.db`.
The credential must have the MNEMOS `root` role because synchronization requests
the unframed operational representation. The command completes only after two
consecutive scans contain the same complete set of memory IDs. Vault memories
are fetched individually. If structured response metadata says the server
redacted an incidental credential span, the command fails instead of storing
the redacted text as canonical content.

```sh
mnemosctl peers
```

Lists federation peer URL and last sync time.

```sh
mnemosctl import ./memories.jsonl
mnemosctl import ./memories.jsonl --skip-bad --progress
```

Imports newline-delimited MNEMOS memory objects and reports success and failure
counts. Ordinary create-shaped rows use the core `/v1/memories` route. Rows
with stable IDs, timestamps, or quality state use the authoritative
`/v1/import?preserve_owner=true` portability API, which requires CHARON and a
root bearer token. Identity and all supported archive metadata are preserved.
A row with fields the selected API cannot restore is rejected instead of being
silently imported with data loss.

```sh
mnemosctl config
```

Prints the resolved `base_url` and a masked API key.

All commands use finite HTTP deadlines. Override the defaults with global
`--connect-timeout-secs`, `--read-timeout-secs`, and
`--request-timeout-secs` options.

## Stress-tested behaviors

- `sync-from --progress [N]` emits `[progress] N/TOTAL rows processed` every N processed rows. The default interval is 100 when `--progress` is present.
- `sync-from --page-size N` adjusts the remote fetch and local transaction size from 1 through 500. The default remains 100.
- `import --progress [N]` uses the same progress format and default interval.
- `import --skip-bad` skips malformed JSON rows, locally invalid rows, and rows that receive server-side 4xx responses, logs each skipped row, and continues.
- `import` streams JSONL line by line and records successful imports in the local sqlite ledger by input memory ID or a path-independent line hash, so copied or moved input files remain idempotent.
- `sync-from` pulls remote memories with server-side pagination and upserts by memory ID, so reruns refresh existing rows and report zero newly inserted rows when nothing changed.

## What this replaces

`mnemosctl` replaces three Python sync helpers:

- `sync_from_pythia.py`
- federation probe helper
- bulk-import JSONL tool


## Build infrastructure & partners

Continuous integration and package distribution for this project are generously
supported by our open-source infrastructure partners:

- **[GitLab](https://gitlab.com/)** — canonical source hosting and CI pipelines
  (format / lint / test gates), via the
  [GitLab for Open Source](https://about.gitlab.com/solutions/open-source/) program.
- **[Buildkite](https://buildkite.com/)** — CI/CD orchestration with hosted macOS
  and Linux agents, and our APT package registry host
  (`packages.buildkite.com/ncz-os/ncz`), via the
  [Buildkite Open Source](https://buildkite.com/pricing) program.

Thank you to both for backing open-source software.
