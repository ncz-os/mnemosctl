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
```

Pulls memories from a remote MNEMOS host in pages of 100 and upserts them into `~/.mnemos/mnemosctl.db`.

```sh
mnemosctl peers
```

Lists federation peer URL and last sync time.

```sh
mnemosctl import ./memories.jsonl
```

Imports newline-delimited JSON, posting each line to `/v1/memories` and reporting success and failure counts.

```sh
mnemosctl config
```

Prints the resolved `base_url` and a masked API key.

## What this replaces

`mnemosctl` replaces three Python sync helpers:

- `sync_from_pythia.py`
- federation probe helper
- bulk-import JSONL tool
