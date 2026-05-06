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
```

Pulls memories from a remote MNEMOS host in pages of 100 and upserts them into `~/.mnemos/mnemosctl.db`.

```sh
mnemosctl peers
```

Lists federation peer URL and last sync time.

```sh
mnemosctl import ./memories.jsonl
mnemosctl import ./memories.jsonl --skip-bad --progress
```

Imports newline-delimited JSON, posting each line to `/v1/memories` and reporting success and failure counts.

```sh
mnemosctl config
```

Prints the resolved `base_url` and a masked API key.

## Stress-tested behaviors

- `sync-from --progress [N]` emits `[progress] N/TOTAL rows processed` every N processed rows. The default interval is 100 when `--progress` is present.
- `import --progress [N]` uses the same progress format and default interval.
- `import --skip-bad` skips malformed JSON rows, locally invalid rows, and rows that receive server-side 4xx responses, logs each skipped row, and continues.
- `import` streams JSONL line by line and records successful imports in the local sqlite ledger by memory ID or source line hash so reruns do not duplicate already imported rows.
- `sync-from` pulls remote memories with server-side pagination and upserts by memory ID, so reruns refresh existing rows and report zero newly inserted rows when nothing changed.

## What this replaces

`mnemosctl` replaces three Python sync helpers:

- `sync_from_pythia.py`
- federation probe helper
- bulk-import JSONL tool
