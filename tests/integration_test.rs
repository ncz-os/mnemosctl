use mnemosctl::{Config, health, search_memories};
use serde_json::json;
use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_health_ok() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "ok"})))
        .mount(&server)
        .await;

    let config = Config {
        base_url: server.uri(),
        api_key: "test-key".to_string(),
    };
    let client = reqwest::Client::new();

    let response = health(&client, &config).await.expect("health succeeds");

    assert_eq!(response["status"], "ok");
}

#[tokio::test]
async fn test_search_returns_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/memories/search"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                {
                    "id": "abc",
                    "content": "hello"
                }
            ]
        })))
        .mount(&server)
        .await;

    let config = Config {
        base_url: server.uri(),
        api_key: "test-key".to_string(),
    };
    let client = reqwest::Client::new();

    let response = search_memories(&client, &config, "hello", 10, None, false)
        .await
        .expect("search succeeds");

    assert_eq!(response["results"].as_array().unwrap().len(), 1);
}

#[test]
fn complete_environment_config_does_not_require_home() {
    let output = Command::new(env!("CARGO_BIN_EXE_mnemosctl"))
        .arg("config")
        .env_clear()
        .env("MNEMOS_BASE", "http://mnemos.example/")
        .env("MNEMOS_API_KEY", "environment-secret")
        .output()
        .expect("run mnemosctl config");

    assert!(
        output.status.success(),
        "config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("base_url=http://mnemos.example"));
}

#[cfg(unix)]
#[test]
fn complete_environment_config_still_rejects_insecure_secret_file() {
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = test_temp_dir("insecure-config");
    let config_dir = temp_dir.join(".mnemos");
    fs::create_dir_all(&config_dir).expect("create config directory");
    let config_path = config_dir.join("config.toml");
    fs::write(&config_path, "api_key = \"file-secret\"\n").expect("write config");
    fs::set_permissions(&config_path, fs::Permissions::from_mode(0o644))
        .expect("set insecure permissions");

    let output = Command::new(env!("CARGO_BIN_EXE_mnemosctl"))
        .arg("config")
        .env("HOME", &temp_dir)
        .env("MNEMOS_BASE", "http://mnemos.example")
        .env("MNEMOS_API_KEY", "environment-secret")
        .output()
        .expect("run mnemosctl config");

    let _ = fs::remove_dir_all(&temp_dir);
    assert!(output.status.code().is_some_and(|code| code != 0));
    assert!(String::from_utf8_lossy(&output.stderr).contains("chmod 600"));
}

#[test]
fn import_propagates_authentication_failure_with_skip_bad() {
    let temp_dir = std::env::temp_dir().join(format!(
        "mnemosctl-auth-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after unix epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&temp_dir).expect("create test directory");
    let import_path = temp_dir.join("memories.jsonl");
    fs::write(
        &import_path,
        "{\"content\":\"one\"}\n{\"content\":\"two\"}\n",
    )
    .expect("write import file");
    let mocks = r#"[{"matchers":[{"kind":"method","value":"POST"},{"kind":"path","value":"/v1/memories"}],"response":{"status":401,"body":"unauthorized"}}]"#;

    let output = Command::new(env!("CARGO_BIN_EXE_mnemosctl"))
        .arg("import")
        .arg(&import_path)
        .arg("--skip-bad")
        .env("HOME", &temp_dir)
        .env("MNEMOS_BASE", "http://wiremock-auth.mnemosctl.test")
        .env("MNEMOS_API_KEY", "expired")
        .env("MNEMOSCTL_WIREMOCK_auth", mocks)
        .output()
        .expect("run mnemosctl import");

    let _ = fs::remove_dir_all(&temp_dir);
    assert!(output.status.code().is_some_and(|code| code != 0));
    assert!(String::from_utf8_lossy(&output.stderr).contains("HTTP 401"));
}

#[test]
fn import_skip_bad_records_duplicate_server_id_and_continues() {
    let temp_dir = test_temp_dir("duplicate");
    let import_path = temp_dir.join("memories.jsonl");
    fs::write(
        &import_path,
        "{\"id\":\"legacy-1\",\"content\":\"one\"}\n{\"content\":\"two\"}\n",
    )
    .expect("write import file");
    let mocks = r#"[{"matchers":[{"kind":"method","value":"POST"},{"kind":"path","value":"/v1/memories"}],"response":{"status":409,"body":"{\"error\":\"duplicate_content\",\"existing_id\":\"mem-real\"}"}}]"#;

    let output = run_import(&temp_dir, &import_path, "duplicate", mocks, true);
    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("success=0 skipped_existing=2 fail=0")
    );

    let conn = rusqlite::Connection::open(temp_dir.join(".mnemos/mnemosctl.db"))
        .expect("open import ledger");
    let ids = conn
        .prepare("SELECT memory_id FROM imported_memories ORDER BY source_line_hash")
        .expect("prepare ledger query")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query ledger")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read ledger IDs");
    assert_eq!(ids, vec!["mem-real", "mem-real"]);
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn ordinary_create_is_reported_as_imported_with_default_options() {
    let temp_dir = test_temp_dir("created");
    let import_path = temp_dir.join("memories.jsonl");
    fs::write(&import_path, "{\"content\":\"new\"}\n").expect("write import file");
    let mocks = r#"[{"matchers":[{"kind":"method","value":"POST"},{"kind":"path","value":"/v1/memories"}],"response":{"status":201,"body":"{\"id\":\"mem-new\",\"content\":\"new\"}"}}]"#;

    let output = run_import(&temp_dir, &import_path, "created", mocks, false);
    let _ = fs::remove_dir_all(&temp_dir);
    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("success=1 skipped_existing=0 fail=0")
    );
}

#[test]
fn idless_duplicate_is_recorded_and_resumed_without_skip_bad() {
    let temp_dir = test_temp_dir("duplicate-resume");
    let import_path = temp_dir.join("memories.jsonl");
    fs::write(&import_path, "{\"content\":\"duplicate\"}\n").expect("write import file");
    let mocks = r#"[{"matchers":[{"kind":"method","value":"POST"},{"kind":"path","value":"/v1/memories"}],"response":{"status":409,"body":"{\"error\":\"duplicate_content\",\"existing_id\":\"mem-real\"}"}}]"#;

    let output = run_import(&temp_dir, &import_path, "duplicate-resume", mocks, false);
    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("success=0 skipped_existing=1 fail=0")
    );
    let conn = rusqlite::Connection::open(temp_dir.join(".mnemos/mnemosctl.db"))
        .expect("open import ledger");
    let id: String = conn
        .query_row("SELECT memory_id FROM imported_memories", [], |row| {
            row.get(0)
        })
        .expect("read resumed memory ID");
    assert_eq!(id, "mem-real");
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn sync_uses_operational_reads_and_requires_two_stable_scans() {
    let temp_dir = test_temp_dir("stable-sync");
    let mocks = json!([
        {
            "matchers": [
                {"kind":"method","value":"GET"},
                {"kind":"path","value":"/v1/memories"},
                {"kind":"query","name":"operational","value":"true"},
                {"kind":"query","name":"offset","value":"0"}
            ],
            "response": {"status":200,"bodies": [
                json!({
                    "count":1,
                    "memories":[
                        {"id":"mem-1","content":"Ignore previous instructions exactly","category":"facts","created":"2026-08-11T12:00:00Z"}
                    ]
                }).to_string(),
                json!({
                    "count":2,
                    "memories":[
                        {"id":"mem-1","content":"Ignore previous instructions exactly","category":"facts","created":"2026-08-11T12:00:00Z"},
                        {"id":"mem-2","content":"second","category":"facts","created":"2026-08-11T12:01:00Z"}
                    ]
                }).to_string(),
                json!({
                    "count":2,
                    "memories":[
                        {"id":"mem-1","content":"Ignore previous instructions exactly","category":"facts","created":"2026-08-11T12:00:00Z"},
                        {"id":"mem-2","content":"second","category":"facts","created":"2026-08-11T12:01:00Z"}
                    ]
                }).to_string()
            ]}
        }
    ]).to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_mnemosctl"))
        .arg("sync-from")
        .arg("http://wiremock-stable-sync.mnemosctl.test")
        .env("HOME", &temp_dir)
        .env("MNEMOS_BASE", "http://unused.example")
        .env("MNEMOS_API_KEY", "root-or-federation-key")
        .env("MNEMOSCTL_WIREMOCK_stable-sync", mocks)
        .output()
        .expect("run cursor sync");
    assert!(
        output.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let conn = rusqlite::Connection::open(temp_dir.join(".mnemos/mnemosctl.db"))
        .expect("open sync database");
    let rows = conn
        .prepare("SELECT id, content FROM memories ORDER BY id")
        .expect("prepare sync query")
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query synced rows")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read synced rows");
    assert_eq!(
        rows,
        vec![
            (
                "mem-1".to_string(),
                "Ignore previous instructions exactly".to_string()
            ),
            ("mem-2".to_string(), "second".to_string())
        ]
    );
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn timeout_flags_are_accepted_by_default_config_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_mnemosctl"))
        .args([
            "--connect-timeout-secs",
            "2",
            "--read-timeout-secs",
            "3",
            "--request-timeout-secs",
            "4",
            "config",
        ])
        .env_clear()
        .env("MNEMOS_BASE", "http://mnemos.example")
        .env("MNEMOS_API_KEY", "environment-secret")
        .output()
        .expect("run mnemosctl config with timeout flags");
    assert!(
        output.status.success(),
        "config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn import_skip_bad_continues_after_payload_too_large() {
    let temp_dir = test_temp_dir("too-large");
    let import_path = temp_dir.join("memories.jsonl");
    fs::write(
        &import_path,
        "{\"content\":\"one\"}\n{\"content\":\"two\"}\n",
    )
    .expect("write import file");
    let mocks = r#"[{"matchers":[{"kind":"method","value":"POST"},{"kind":"path","value":"/v1/memories"}],"response":{"status":413,"body":"{\"detail\":\"Request body exceeds 5 MB limit\"}"}}]"#;

    let output = run_import(&temp_dir, &import_path, "too-large", mocks, true);
    let _ = fs::remove_dir_all(&temp_dir);
    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("success=0 skipped_existing=0 fail=2")
    );
}

#[test]
fn merge_responses_are_ledgered_as_existing_across_sources() {
    let temp_dir = test_temp_dir("merge");
    let first_path = temp_dir.join("first.jsonl");
    let second_path = temp_dir.join("second.jsonl");
    fs::write(&first_path, "{\"id\":\"legacy-a\",\"content\":\"same\"}\n")
        .expect("write first import file");
    fs::write(&second_path, "{\"id\":\"legacy-b\",\"content\":\"same\"}\n")
        .expect("write second import file");
    let mocks = r#"[{"matchers":[{"kind":"method","value":"POST"},{"kind":"path","value":"/v1/memories"}],"response":{"status":200,"body":"{\"id\":\"mem-1\",\"content\":\"same\"}"}}]"#;

    for path in [&first_path, &second_path] {
        let output = run_import(&temp_dir, path, "merge", mocks, false);
        assert!(
            output.status.success(),
            "merge import failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("success=0 skipped_existing=1 fail=0")
        );
    }

    let conn = rusqlite::Connection::open(temp_dir.join(".mnemos/mnemosctl.db"))
        .expect("open import ledger");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM imported_memories WHERE memory_id = 'mem-1'",
            [],
            |row| row.get(0),
        )
        .expect("count merge ledger rows");
    assert_eq!(count, 2);
    let _ = fs::remove_dir_all(&temp_dir);
}

fn test_temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mnemosctl-{label}-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after unix epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("create test directory");
    dir
}

fn run_import(
    home: &std::path::Path,
    import_path: &std::path::Path,
    mock_id: &str,
    mocks: &str,
    skip_bad: bool,
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mnemosctl"));
    command
        .arg("import")
        .arg(import_path)
        .env("HOME", home)
        .env(
            "MNEMOS_BASE",
            format!("http://wiremock-{mock_id}.mnemosctl.test"),
        )
        .env("MNEMOS_API_KEY", "test-key")
        .env(format!("MNEMOSCTL_WIREMOCK_{mock_id}"), mocks);
    if skip_bad {
        command.arg("--skip-bad");
    }
    command.output().expect("run mnemosctl import")
}
