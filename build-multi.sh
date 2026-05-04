#!/usr/bin/env sh
set -eu

CARGO_BIN="${CARGO:-cargo}"
if ! command -v "$CARGO_BIN" >/dev/null 2>&1 && [ -x "$HOME/.cargo/bin/cargo" ]; then
  CARGO_BIN="$HOME/.cargo/bin/cargo"
fi

"$CARGO_BIN" build --release
printf '%s\n' "$(pwd)/target/release/mnemosctl"
