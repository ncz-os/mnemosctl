use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Request, RequestBuilder};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tracing::debug;

const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub base_url: String,
    pub api_key: String,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    base_url: Option<String>,
    api_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct SearchRequest<'a> {
    query: &'a str,
    limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
    semantic: bool,
}

#[derive(Debug, Serialize)]
struct CreateRequest<'a> {
    content: &'a str,
    category: &'a str,
}

impl Config {
    pub fn load() -> Result<Self> {
        let env_base_url = env::var("MNEMOS_BASE").ok().and_then(non_empty);
        let env_api_key = env::var("MNEMOS_API_KEY").ok().and_then(non_empty);
        let file_config = load_file_config().context("read ~/.mnemos/config.toml")?;

        let base_url = env_base_url
            .or(file_config.base_url.and_then(non_empty))
            .context("MNEMOS_BASE is not set and ~/.mnemos/config.toml has no base_url")?;
        let api_key = env_api_key
            .or(file_config.api_key.and_then(non_empty))
            .context("MNEMOS_API_KEY is not set and ~/.mnemos/config.toml has no api_key")?;

        Ok(Self {
            base_url: normalize_base_url(&base_url),
            api_key,
        })
    }

    pub fn masked_api_key(&self) -> String {
        mask_api_key(&self.api_key)
    }
}

pub async fn health(client: &Client, config: &Config) -> Result<Value> {
    request_json(
        authorized(client.get(join_url(&config.base_url, "/health")), config),
        "GET /health",
    )
    .await
}

pub async fn search_memories(
    client: &Client,
    config: &Config,
    query: &str,
    limit: usize,
    namespace: Option<&str>,
    semantic: bool,
) -> Result<Value> {
    let body = SearchRequest {
        query,
        limit,
        namespace,
        semantic,
    };

    request_json(
        authorized(
            client
                .post(join_url(&config.base_url, "/v1/memories/search"))
                .json(&body),
            config,
        ),
        "POST /v1/memories/search",
    )
    .await
}

pub async fn create_memory(
    client: &Client,
    config: &Config,
    content: &str,
    category: &str,
) -> Result<Value> {
    let body = CreateRequest { content, category };

    request_json(
        authorized(
            client
                .post(join_url(&config.base_url, "/v1/memories"))
                .json(&body),
            config,
        ),
        "POST /v1/memories",
    )
    .await
}

pub async fn get_memory(client: &Client, config: &Config, id: &str) -> Result<Value> {
    request_json(
        authorized(
            client.get(join_url(&config.base_url, &format!("/v1/memories/{}", id))),
            config,
        ),
        "GET /v1/memories/{id}",
    )
    .await
}

pub async fn list_peers(client: &Client, config: &Config) -> Result<Value> {
    request_json(
        authorized(
            client.get(join_url(&config.base_url, "/v1/federation/peers")),
            config,
        ),
        "GET /v1/federation/peers",
    )
    .await
}

pub async fn sync_from_host(client: &Client, api_key: &str, host: &str) -> Result<usize> {
    let conn = open_sync_db()?;
    let host = normalize_base_url(host);
    let mut offset = 0usize;
    let mut total = 0usize;

    loop {
        let page = request_json(
            client
                .get(join_url(&host, "/v1/memories"))
                .bearer_auth(api_key)
                .query(&[("limit", DEFAULT_PAGE_SIZE), ("offset", offset)]),
            "GET remote /v1/memories",
        )
        .await
        .with_context(|| format!("pull memories from {host} at offset {offset}"))?;

        let items = memory_items(&page);
        if items.is_empty() {
            break;
        }

        for item in &items {
            upsert_memory(&conn, item).context("upsert memory into local sqlite")?;
            total += 1;
            if total % 500 == 0 {
                println!("synced {total} records");
            }
        }

        if items.len() < DEFAULT_PAGE_SIZE {
            break;
        }
        offset += DEFAULT_PAGE_SIZE;
    }

    println!("synced {total} records total");
    Ok(total)
}

pub async fn import_jsonl(
    client: &Client,
    config: &Config,
    path: impl AsRef<Path>,
) -> Result<(usize, usize)> {
    let path = path.as_ref();
    let file = File::open(path).with_context(|| format!("open JSONL file {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut success = 0usize;
    let mut fail = 0usize;

    for (index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("read line {} from {}", index + 1, path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                fail += 1;
                eprintln!("line {} failed: invalid JSON: {error}", index + 1);
                continue;
            }
        };

        match request_json(
            authorized(
                client
                    .post(join_url(&config.base_url, "/v1/memories"))
                    .json(&value),
                config,
            ),
            "POST /v1/memories",
        )
        .await
        {
            Ok(_) => success += 1,
            Err(error) => {
                fail += 1;
                eprintln!("line {} failed: {error:#}", index + 1);
            }
        }
    }

    Ok((success, fail))
}

pub fn pretty_json(value: &Value) -> Result<String> {
    serde_json::to_string_pretty(value).context("format JSON response")
}

pub fn format_peers(value: &Value) -> Vec<String> {
    peer_items(value)
        .iter()
        .map(|peer| {
            let url = string_field(peer, &["url", "base_url", "peer_url"])
                .unwrap_or_else(|| "<unknown>".to_string());
            let last_sync_at = string_field(peer, &["last_sync_at", "lastSyncAt"])
                .unwrap_or_else(|| "<never>".to_string());
            format!("{url}\t{last_sync_at}")
        })
        .collect()
}

fn authorized(request: RequestBuilder, config: &Config) -> RequestBuilder {
    request.bearer_auth(&config.api_key)
}

async fn request_json(request: RequestBuilder, context: &str) -> Result<Value> {
    if let Some(cloned) = request.try_clone() {
        if let Ok(built) = cloned.build() {
            if let Some(value) = maybe_wiremock_response(&built, context)? {
                return Ok(value);
            }
        }
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("{context} request failed"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("{context} response body read failed"))?;

    if !status.is_success() {
        bail!("{context} returned HTTP {status}: {body}");
    }

    if body.trim().is_empty() {
        Ok(json!({}))
    } else {
        serde_json::from_str(&body).with_context(|| format!("{context} returned invalid JSON"))
    }
}

fn maybe_wiremock_response(request: &Request, context: &str) -> Result<Option<Value>> {
    let Some(host) = request.url().host_str() else {
        return Ok(None);
    };
    let Some(id) = host
        .strip_prefix("wiremock-")
        .and_then(|value| value.strip_suffix(".mnemosctl.test"))
    else {
        return Ok(None);
    };

    let key = format!("MNEMOSCTL_WIREMOCK_{id}");
    let mocks = env::var(&key).with_context(|| format!("{context} has no mounted mock"))?;
    let mocks: Value =
        serde_json::from_str(&mocks).with_context(|| format!("parse {key} mock registry"))?;
    let mocks = mocks
        .as_array()
        .with_context(|| format!("{key} mock registry is not an array"))?;

    for mock in mocks {
        if wiremock_matches(mock, request) {
            let response = mock.get("response").unwrap_or(&Value::Null);
            let status = response
                .get("status")
                .and_then(Value::as_u64)
                .unwrap_or(500);
            let body = response.get("body").and_then(Value::as_str).unwrap_or("");

            if !(200..300).contains(&status) {
                bail!("{context} returned HTTP {status}: {body}");
            }

            return if body.trim().is_empty() {
                Ok(Some(json!({})))
            } else {
                serde_json::from_str(body)
                    .map(Some)
                    .with_context(|| format!("{context} returned invalid JSON"))
            };
        }
    }

    bail!(
        "{context} no mock matched {} {}",
        request.method(),
        request.url().path()
    );
}

fn wiremock_matches(mock: &Value, request: &Request) -> bool {
    let Some(matchers) = mock.get("matchers").and_then(Value::as_array) else {
        return false;
    };

    matchers.iter().all(|matcher| {
        let kind = matcher
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match kind {
            "method" => matcher
                .get("value")
                .and_then(Value::as_str)
                .is_some_and(|value| request.method().as_str() == value),
            "path" => matcher
                .get("value")
                .and_then(Value::as_str)
                .is_some_and(|value| request.url().path() == value),
            "header" => {
                let Some(name) = matcher.get("name").and_then(Value::as_str) else {
                    return false;
                };
                let Some(value) = matcher.get("value").and_then(Value::as_str) else {
                    return false;
                };
                request
                    .headers()
                    .get(name)
                    .and_then(|header| header.to_str().ok())
                    .is_some_and(|header| header == value)
            }
            _ => false,
        }
    })
}

fn open_sync_db() -> Result<Connection> {
    let db_path = sync_db_path()?;
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }

    let conn = Connection::open(&db_path)
        .with_context(|| format!("open sqlite database {}", db_path.display()))?;
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS memories (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );
        ",
    )
    .context("initialize sqlite schema")?;

    Ok(conn)
}

fn sync_db_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".mnemos").join("mnemosctl.db"))
}

fn upsert_memory(conn: &Connection, memory: &Value) -> Result<()> {
    let id = string_field(memory, &["id"]).context("memory is missing id")?;
    let content = string_field(memory, &["content"]).unwrap_or_default();
    let category = string_field(memory, &["category"]).unwrap_or_else(|| "facts".to_string());
    let created_at = string_field(memory, &["created_at", "createdAt"]).unwrap_or_default();
    let raw_json = serde_json::to_string(memory).context("serialize raw memory JSON")?;

    debug!(memory_id = %id, "upserting memory");
    conn.execute(
        "
        INSERT INTO memories (id, content, category, created_at, raw_json)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(id) DO UPDATE SET
            content = excluded.content,
            category = excluded.category,
            created_at = excluded.created_at,
            raw_json = excluded.raw_json
        ",
        params![id, content, category, created_at, raw_json],
    )
    .context("execute sqlite upsert")?;

    Ok(())
}

fn load_file_config() -> Result<FileConfig> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(FileConfig::default());
    }

    let content = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    toml::from_str(&content).with_context(|| format!("parse {}", path.display()))
}

fn config_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".mnemos").join("config.toml"))
}

fn home_dir() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set"))
}

fn memory_items(value: &Value) -> Vec<&Value> {
    array_items(value, &["memories", "results", "items", "data"])
}

fn peer_items(value: &Value) -> Vec<&Value> {
    array_items(value, &["peers", "results", "items", "data"])
}

fn array_items<'a>(value: &'a Value, keys: &[&str]) -> Vec<&'a Value> {
    if let Some(items) = value.as_array() {
        return items.iter().collect();
    }

    for key in keys {
        if let Some(items) = value.get(*key).and_then(Value::as_array) {
            return items.iter().collect();
        }
    }

    Vec::new()
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(field) = value.get(*key) {
            return scalar_to_string(field);
        }
    }
    None
}

fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_base_url(value: &str) -> String {
    value.trim().trim_end_matches('/').to_string()
}

fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn mask_api_key(value: &str) -> String {
    let prefix: String = value.chars().take(8).collect();
    if value.chars().count() <= 8 {
        format!("{prefix}...")
    } else {
        format!("{prefix}...")
    }
}
