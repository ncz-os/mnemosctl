use mnemosctl::{Config, health, search_memories};
use serde_json::json;
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
