# mnemosctl audit findings v2 fix report

Date: 2026-09-09

Branch: `fix/audit-findings-v2`

## Findings confirmed

1. The reviewed implementation on `origin/fix/audit-findings` exposed
   `--allow-unlisted-host`, which allowed an arbitrary HTTPS endpoint to receive
   the local MNEMOS root Bearer token. A detached old-code countercheck against
   commit `6dfeb67ec6f8da498c6f54009085912c1a90aad1` reproduced successful sync to a
   mock that required `Authorization: Bearer root-sentinel` when the bypass flag
   was supplied.
2. The reviewed allowlist authorized hostname text without binding later socket
   connections to the addresses resolved during authorization, leaving a DNS
   rebinding window.
3. The reviewed rejection tests checked error text only and did not observe or
   count HTTP requests.

## Changes made

- `src/main.rs`
  - Added strict syntax parsing for the `sync-from` endpoint.
  - Provides the configured timeout values to the dedicated sync client.
  - Does not define or parse any allowlist-bypass flag.
- `src/lib.rs`
  - Added persistent allowlist loading from `MNEMOS_SYNC_HOSTS` or `sync_hosts`
    in `~/.mnemos/config.toml`; the configured `base_url` remains allowed.
  - Rejects an unlisted endpoint before DNS resolution, database access, request
    construction, or access to the API key.
  - Resolves an authorized hostname once and installs only those socket
    addresses with reqwest `resolve_to_addrs`, which is consulted whenever the
    dedicated client opens a connection. The URL hostname remains unchanged for
    HTTP Host and TLS certificate/SNI validation.
  - Disables proxies and redirects on the dedicated root-token sync client so
    those mechanisms cannot escape the pinned destination.
  - Extended the existing subprocess mock layer with a request log that records
    one non-secret line per observed HTTP request.
- `tests/integration_test.rs`
  - Added `sync_from_rejects_unlisted_host_without_requests_or_bypass_flag`,
    which asserts rejection, exactly zero observed HTTP requests, and a clap
    parse error for the removed bypass flag.
  - Added a positive recorder control that permits a mock endpoint, requires the
    sentinel Bearer header, completes sync, and observes exactly two requests.
  - Added persistent allowlist configuration to the existing successful sync
    tests.
- `tests/integration/stress_test.rs`
  - Updated the sync helper calls to use the secured configuration-based API.
- `README.md`
  - Documented the persistent allowlist, DNS pinning, and lack of a command-line
    bypass.

## Verification

Formatter:

```text
cargo fmt --all
```

Lint command and result:

```text
cargo clippy --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 8.33s
```

Exact final test command:

```text
cargo test
```

Actual final test output/counts:

```text
Running unittests src/lib.rs
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Running unittests src/main.rs
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Running tests/integration_test.rs
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Running tests/integration/stress_test.rs
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Doc-tests mnemosctl
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Total observed: **33 passed, 0 failed**.

Additional verification:

- `git diff --check` passed.
- `mnemosctl sync-from --help` contains no bypass flag.
- A tracked-string search found the removed flag name only in the negative
  regression test that proves clap rejects it; no definition, parser field,
  documentation, or help text contains it.
- The old vulnerable commit accepted the flag, sent the sentinel root token to
  the mock, and exited successfully, proving the new combined regression test
  fails against the vulnerable behavior.
- Zoder adversarial review was attempted three times. Its NVIDIA route failed
  at transport level each time, and a direct pinned MiniMax transport check also
  failed. No zoder reviewer completed, so no zoder approval is claimed. A
  secondary Codex source review found and removed test-only credential logging;
  all lint and test gates were rerun afterward.

## Implementation commit

Commit: `17ffcb893ace9ad3c0a7c27f3007fd34a96d82e9`

Message: `fix: pin allowed sync hosts and remove bypass`

Author: `Jason Perlow <jperlow@gmail.com>`

No remote push was performed.
