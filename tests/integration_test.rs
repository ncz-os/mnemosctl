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
