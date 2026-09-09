use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Request, RequestBuilder, Url, redirect};
use rusqlite::{Connection, OptionalExtension, Statement, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, SocketAddr};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use tracing::debug;

const DEFAULT_PAGE_SIZE: usize = 100;
pub const MAX_SYNC_PAGE_SIZE: usize = 500;
const BALANCED_SEARCH_LIMIT: usize = 100;
const MAX_SYNC_SCAN_ATTEMPTS: usize = 5;
const SYNC_HOSTS_ENV: &str = "MNEMOS_SYNC_HOSTS";
static MOCK_RESPONSE_HITS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub base_url: String,
    pub api_key: String,
}

#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    base_url: Option<String>,
    api_key: Option<String>,
    sync_hosts: Option<Vec<String>>,
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

#[derive(Clone, Copy, Debug)]
pub struct SyncOptions {
    pub progress_every: Option<usize>,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            progress_every: None,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SyncResult {
    pub processed: usize,
    pub inserted: usize,
    pub updated: usize,
    pub total: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ImportOptions {
    pub skip_bad: bool,
    pub progress_every: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ImportResult {
    pub processed: usize,
    pub imported: usize,
    pub skipped_existing: usize,
    pub failed: usize,
    pub total: usize,
}

impl Config {
    pub fn load() -> Result<Self> {
        let env_base_url = env::var("MNEMOS_BASE").ok().and_then(non_empty);
        let env_api_key = env::var("MNEMOS_API_KEY").ok().and_then(non_empty);

        let file_config = if env::var_os("HOME").is_some() {
            load_file_config().context("read ~/.mnemos/config.toml")?
        } else if env_base_url.is_some() && env_api_key.is_some() {
            FileConfig::default()
        } else {
            load_file_config().context("read ~/.mnemos/config.toml")?
        };

        let base_url = env_base_url
            .or(file_config.base_url.and_then(non_empty))
            .context("MNEMOS_BASE is not set and ~/.mnemos/config.toml has no base_url")?;
        let api_key = env_api_key
            .or(file_config.api_key.and_then(non_empty))
            .context("MNEMOS_API_KEY is not set and ~/.mnemos/config.toml has no api_key")?;

        let base_url = parse_endpoint_url(&base_url)
            .context("MNEMOS base URL is not usable")?
            .to_string();

        Ok(Self { base_url, api_key })
    }

    pub fn masked_api_key(&self) -> String {
        mask_api_key(&self.api_key)
    }
}

pub fn build_http_client(
    connect_timeout: Duration,
    read_timeout: Duration,
    total_timeout: Duration,
) -> Result<Client> {
    Client::builder()
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .timeout(total_timeout)
        .build()
        .context("build HTTP client")
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
    if !(1..=BALANCED_SEARCH_LIMIT).contains(&limit) {
        bail!(
            "search limit must be between 1 and {BALANCED_SEARCH_LIMIT}; MNEMOS uses the balanced retrieval profile for this command"
        );
    }
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

pub async fn sync_from_host(config: &Config, host: &str) -> Result<usize> {
    let result = sync_from_host_with_options(config, host, SyncOptions::default()).await?;
    Ok(result.inserted)
}

pub async fn sync_from_host_with_options(
    config: &Config,
    host: &str,
    options: SyncOptions,
) -> Result<SyncResult> {
    sync_from_host_with_page_size(config, host, options, DEFAULT_PAGE_SIZE).await
}

pub async fn sync_from_host_with_page_size(
    config: &Config,
    host: &str,
    options: SyncOptions,
    page_size: usize,
) -> Result<SyncResult> {
    if !(1..=MAX_SYNC_PAGE_SIZE).contains(&page_size) {
        bail!("sync page size must be between 1 and {MAX_SYNC_PAGE_SIZE}");
    }

    // Authorization must complete before DNS resolution, database access, or
    // request construction. The dedicated client pins the authorized
    // hostname to this resolution for every connection it opens.
    let endpoint = authorize_sync_host(host, &config.base_url, &sync_host_allowlist()?)?;
    let client = build_pinned_sync_client(&endpoint, options).await?;
    let host = endpoint.to_string();
    let api_key = config.api_key.as_str();
    let mut conn = open_sync_db()?;
    let mut result = SyncResult::default();
    let mut previous_ids: Option<BTreeSet<String>> = None;
    let mut stable_items = None;

    for _ in 0..MAX_SYNC_SCAN_ATTEMPTS {
        let (items, total) = scan_remote_memories(&client, api_key, &host, page_size).await?;
        let ids = items.iter().filter_map(memory_id).collect::<BTreeSet<_>>();
        if ids.len() != total {
            previous_ids = None;
            continue;
        }
        if previous_ids.as_ref() == Some(&ids) {
            result.total = Some(total);
            stable_items = Some(items);
            break;
        }
        previous_ids = Some(ids);
    }

    let items = stable_items.context(
        "remote memories changed during synchronization; no stable pagination boundary after 5 scans",
    )?;
    for page in items.chunks(page_size) {
        let mut page_items = Vec::with_capacity(page.len());
        for item in page {
            let mut item = item.clone();
            if item.get("vaulted").and_then(Value::as_bool) == Some(true) {
                let id = memory_id(&item).context("vaulted memory is missing id")?;
                item = request_json(
                    client
                        .get(join_url(&host, &format!("/v1/memories/{id}")))
                        .bearer_auth(api_key)
                        .query(&[("operational", true)]),
                    "GET remote vaulted memory",
                )
                .await
                .with_context(|| format!("pull vaulted memory {id} from {host}"))?;
            } else if response_has_redaction_metadata(&item) {
                let id = memory_id(&item).unwrap_or_else(|| "<unknown>".to_string());
                bail!(
                    "remote memory {id} contains redacted credential spans and cannot be stored as canonical content"
                );
            }
            page_items.push(item);
        }

        let transaction = conn
            .transaction()
            .context("begin sqlite sync page transaction")?;
        let first_processed = result.processed + 1;
        {
            let mut insert = transaction
                .prepare_cached(
                    "
                    INSERT INTO memories (id, content, category, created_at, raw_json)
                    VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(id) DO NOTHING
                    ",
                )
                .context("prepare sqlite memory insert")?;
            let mut update = transaction
                .prepare_cached(
                    "
                    UPDATE memories
                    SET content = ?2,
                        category = ?3,
                        created_at = ?4,
                        raw_json = ?5
                    WHERE id = ?1 AND raw_json IS NOT ?5
                    ",
                )
                .context("prepare sqlite memory update")?;

            for item in &page_items {
                if upsert_memory_with_statements(&mut insert, &mut update, item)
                    .context("upsert memory into local sqlite")?
                {
                    result.inserted += 1;
                } else {
                    result.updated += 1;
                }
                result.processed += 1;
            }
        }
        transaction
            .commit()
            .context("commit sqlite sync page transaction")?;
        for processed in first_processed..=result.processed {
            maybe_emit_progress(processed, result.total, options.progress_every);
        }
    }

    emit_final_progress(result.processed, result.total, options.progress_every);
    println!(
        "synced {} new records ({} rows processed)",
        result.inserted, result.processed
    );
    Ok(result)
}

async fn scan_remote_memories(
    client: &Client,
    api_key: &str,
    host: &str,
    page_size: usize,
) -> Result<(Vec<Value>, usize)> {
    let mut offset = 0usize;
    let mut expected_total = None;
    let mut items = Vec::new();

    loop {
        let page = request_json(
            client
                .get(join_url(host, "/v1/memories"))
                .bearer_auth(api_key)
                .query(&[
                    ("limit", page_size.to_string()),
                    ("offset", offset.to_string()),
                    ("operational", "true".to_string()),
                ]),
            "GET remote /v1/memories",
        )
        .await
        .with_context(|| format!("pull memories from {host} at offset {offset}"))?;
        let total = total_count(&page).context("remote memory list response is missing count")?;
        if expected_total.is_some_and(|expected| expected != total) {
            return Ok((Vec::new(), usize::MAX));
        }
        expected_total = Some(total);

        let page_items = memory_items(&page);
        let page_len = page_items.len();
        items.extend(page_items.into_iter().cloned());
        if items.len() >= total || page_len < page_size {
            break;
        }
        offset += page_len;
    }

    Ok((items, expected_total.unwrap_or(0)))
}

fn response_has_redaction_metadata(memory: &Value) -> bool {
    let Some(metadata) = memory.get("metadata").and_then(Value::as_object) else {
        return false;
    };
    metadata
        .get("secret_redact_fields")
        .and_then(|fields| fields.get("content"))
        .and_then(Value::as_array)
        .is_some_and(|spans| !spans.is_empty())
        || metadata
            .get("secret_redact_spans")
            .and_then(Value::as_array)
            .is_some_and(|spans| !spans.is_empty())
}

pub async fn import_jsonl(
    client: &Client,
    config: &Config,
    path: impl AsRef<Path>,
) -> Result<(usize, usize)> {
    let result = import_jsonl_with_options(client, config, path, ImportOptions::default()).await?;
    Ok((result.imported, result.failed))
}

pub async fn import_jsonl_with_options(
    client: &Client,
    config: &Config,
    path: impl AsRef<Path>,
    options: ImportOptions,
) -> Result<ImportResult> {
    let path = path.as_ref();
    let known_total = options
        .progress_every
        .filter(|value| *value > 0)
        .map(|_| count_jsonl_rows(path))
        .transpose()?;
    let file = File::open(path).with_context(|| format!("open JSONL file {}", path.display()))?;
    let reader = BufReader::new(file);
    let conn = open_sync_db()?;
    let source = import_source(path);
    let mut result = ImportResult {
        total: known_total.unwrap_or(0),
        ..ImportResult::default()
    };

    for (index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("read line {} from {}", index + 1, path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        result.processed += 1;
        let line_hash = stable_hash_hex(trimmed);

        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                handle_bad_import_row(
                    &mut result,
                    options,
                    index + 1,
                    format!("invalid JSON: {error}"),
                );
                maybe_emit_progress(result.processed, known_total, options.progress_every);
                if options.skip_bad {
                    continue;
                }
                bail!("import line {}: invalid JSON: {error}", index + 1);
            }
        };

        if let Err(error) = validate_import_row(&value) {
            handle_bad_import_row(&mut result, options, index + 1, error.to_string());
            maybe_emit_progress(result.processed, known_total, options.progress_every);
            if options.skip_bad {
                continue;
            }
            return Err(error).with_context(|| format!("import line {}", index + 1));
        }

        let input_id = memory_id(&value);
        if import_already_recorded(&conn, input_id.as_deref(), &line_hash, trimmed)? {
            result.skipped_existing += 1;
            maybe_emit_progress(result.processed, known_total, options.progress_every);
            continue;
        }

        let record_id = input_id
            .clone()
            .unwrap_or_else(|| format!("mnemosctl-{line_hash}"));
        let use_portability = requires_portability_import(&value);
        let request = if use_portability {
            let envelope = portability_import_envelope(&value, &record_id)?;
            authorized(
                client
                    .post(join_url(&config.base_url, "/v1/import"))
                    .query(&[("preserve_owner", true)])
                    .json(&envelope),
                config,
            )
        } else {
            authorized(
                client
                    .post(join_url(&config.base_url, "/v1/memories"))
                    .json(&value),
                config,
            )
        };
        let request_context = if use_portability {
            "POST /v1/import"
        } else {
            "POST /v1/memories"
        };

        match request_json_result(request, request_context).await {
            Ok(response) => {
                let (imported, skipped, failed, server_memory_id) = if use_portability {
                    let (imported, skipped, failed) = portability_import_counts(&response.value)?;
                    (imported, skipped, failed, Some(record_id.clone()))
                } else {
                    let skipped = usize::from(response.status == 200);
                    let imported = usize::from(response.status != 200);
                    let server_id = memory_id_from_create_response(&response.value);
                    (imported, skipped, 0, server_id)
                };
                if failed > 0 || imported + skipped != 1 {
                    let error = anyhow!(
                        "portability import reported imported={imported} skipped={skipped} failed={failed}: {}",
                        response.value
                    );
                    handle_bad_import_row(&mut result, options, index + 1, error.to_string());
                    maybe_emit_progress(result.processed, known_total, options.progress_every);
                    if options.skip_bad {
                        continue;
                    }
                    return Err(error).with_context(|| format!("import line {}", index + 1));
                }
                let recorded = record_import(
                    &conn,
                    &source,
                    &line_hash,
                    input_id.as_deref(),
                    server_memory_id.as_deref(),
                    trimmed,
                )?;
                if skipped == 1 {
                    result.skipped_existing += 1;
                } else if recorded {
                    result.imported += 1;
                } else {
                    result.skipped_existing += 1;
                }
            }
            Err(error) if error.duplicate_existing_id().is_some() => {
                let existing_id = error.duplicate_existing_id();
                record_import(
                    &conn,
                    &source,
                    &line_hash,
                    input_id.as_deref(),
                    existing_id.as_deref(),
                    trimmed,
                )?;
                result.skipped_existing += 1;
                eprintln!("line {} already exists: {error}", index + 1);
            }
            Err(error) if error.is_bad_import_row() => {
                handle_bad_import_row(&mut result, options, index + 1, error.to_string());
                maybe_emit_progress(result.processed, known_total, options.progress_every);
                if options.skip_bad {
                    continue;
                }
                return Err(error).with_context(|| format!("import line {}", index + 1));
            }
            Err(error) => {
                return Err(error).with_context(|| format!("import line {}", index + 1));
            }
        }

        maybe_emit_progress(result.processed, known_total, options.progress_every);
    }

    result.total = known_total.unwrap_or(result.processed);
    emit_final_progress(result.processed, known_total, options.progress_every);
    Ok(result)
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
    request_json_result(request, context)
        .await
        .map(|response| response.value)
        .map_err(anyhow::Error::from)
}

#[derive(Debug)]
struct JsonResponse {
    status: u16,
    value: Value,
}

async fn request_json_result(
    request: RequestBuilder,
    context: &str,
) -> std::result::Result<JsonResponse, JsonRequestError> {
    if let Some(cloned) = request.try_clone()
        && let Ok(built) = cloned.build()
        && let Some(value) = maybe_wiremock_response(&built, context)?
    {
        return Ok(value);
    }

    let response = request
        .send()
        .await
        .map_err(|error| JsonRequestError::other(anyhow!("{context} request failed: {error}")))?;
    let status = response.status();
    let body = response.text().await.map_err(|error| {
        JsonRequestError::other(anyhow!("{context} response body read failed: {error}"))
    })?;

    if !status.is_success() {
        return Err(JsonRequestError::http(context, status.as_u16(), body));
    }

    if body.trim().is_empty() {
        Ok(JsonResponse {
            status: status.as_u16(),
            value: json!({}),
        })
    } else {
        serde_json::from_str(&body)
            .map_err(|error| {
                JsonRequestError::other(anyhow!("{context} returned invalid JSON: {error}"))
            })
            .map(|value| JsonResponse {
                status: status.as_u16(),
                value,
            })
    }
}

#[derive(Debug)]
struct JsonRequestError {
    status: Option<u16>,
    body: Option<String>,
    error: anyhow::Error,
}

impl JsonRequestError {
    fn http(context: &str, status: u16, body: String) -> Self {
        Self {
            status: Some(status),
            body: Some(body.clone()),
            error: anyhow!("{context} returned HTTP {status}: {body}"),
        }
    }

    fn other(error: anyhow::Error) -> Self {
        Self {
            status: None,
            body: None,
            error,
        }
    }

    fn is_bad_import_row(&self) -> bool {
        matches!(self.status, Some(400 | 413 | 422))
    }

    fn duplicate_existing_id(&self) -> Option<String> {
        if self.status != Some(409) {
            return None;
        }
        let body: Value = serde_json::from_str(self.body.as_deref()?).ok()?;
        (body.get("error").and_then(Value::as_str) == Some("duplicate_content"))
            .then(|| string_field(&body, &["existing_id"]))
            .flatten()
    }
}

impl fmt::Display for JsonRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.error)
    }
}

impl Error for JsonRequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.error.as_ref())
    }
}

fn maybe_wiremock_response(
    request: &Request,
    context: &str,
) -> std::result::Result<Option<JsonResponse>, JsonRequestError> {
    let Some(host) = request.url().host_str() else {
        return Ok(None);
    };
    let Some(key) = mock_registry_key(host) else {
        return Ok(None);
    };

    record_wiremock_request(&key, request).map_err(JsonRequestError::other)?;
    let mocks = env::var(&key).map_err(|error| {
        JsonRequestError::other(anyhow!("{context} has no mounted mock: {error}"))
    })?;
    let mocks: Value = serde_json::from_str(&mocks)
        .map_err(|error| JsonRequestError::other(anyhow!("parse {key} mock registry: {error}")))?;
    let mocks = mocks
        .as_array()
        .ok_or_else(|| JsonRequestError::other(anyhow!("{key} mock registry is not an array")))?;

    for (mock_index, mock) in mocks.iter().enumerate() {
        if wiremock_matches(mock, request) {
            let response = mock.get("response").unwrap_or(&Value::Null);
            let status = response
                .get("status")
                .and_then(Value::as_u64)
                .unwrap_or(500) as u16;
            let body = if let Some(bodies) = response.get("bodies").and_then(Value::as_array) {
                let response_key = format!("{key}:{mock_index}");
                let mut hits = MOCK_RESPONSE_HITS.lock().map_err(|_| {
                    JsonRequestError::other(anyhow!("mock response counter lock poisoned"))
                })?;
                let hit = hits.entry(response_key).or_default();
                let index = (*hit).min(bodies.len().saturating_sub(1));
                *hit += 1;
                bodies
                    .get(index)
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            } else {
                response
                    .get("body")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            };

            if !(200..300).contains(&status) {
                return Err(JsonRequestError::http(context, status, body));
            }

            return if body.trim().is_empty() {
                Ok(Some(JsonResponse {
                    status,
                    value: json!({}),
                }))
            } else {
                serde_json::from_str(&body)
                    .map(|value| Some(JsonResponse { status, value }))
                    .map_err(|error| {
                        JsonRequestError::other(anyhow!("{context} returned invalid JSON: {error}"))
                    })
            };
        }
    }

    Err(JsonRequestError::other(anyhow!(
        "{context} no mock matched {} {}",
        request.method(),
        request.url().path()
    )))
}

fn record_wiremock_request(key: &str, request: &Request) -> Result<()> {
    let Some(path) = env::var_os(format!("{key}_REQUEST_LOG")) else {
        return Ok(());
    };
    writeln!(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open mock request log {}", PathBuf::from(&path).display()))?,
        "{} {}",
        request.method(),
        request.url()
    )
    .context("record mock HTTP request")
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
            "query" => {
                let Some(name) = matcher.get("name").and_then(Value::as_str) else {
                    return false;
                };
                let Some(value) = matcher.get("value").and_then(Value::as_str) else {
                    return false;
                };
                request
                    .url()
                    .query_pairs()
                    .any(|(key, candidate)| key == name && candidate == value)
            }
            _ => false,
        }
    })
}

fn open_sync_db() -> Result<Connection> {
    let db_path = sync_db_path()?;
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        ensure_private_dir(parent)?;
    }

    let conn = Connection::open(&db_path)
        .with_context(|| format!("open sqlite database {}", db_path.display()))?;
    ensure_private_file(&db_path)?;
    initialize_sync_schema(&conn)?;

    Ok(conn)
}

fn initialize_sync_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS memories (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS imported_memories (
            source TEXT NOT NULL,
            source_line_hash TEXT NOT NULL,
            memory_id TEXT,
            input_memory_id TEXT,
            server_memory_id TEXT,
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (source, source_line_hash)
        );

        ",
    )
    .context("initialize sqlite schema")?;

    add_import_ledger_column(conn, "input_memory_id", "TEXT")?;
    add_import_ledger_column(conn, "server_memory_id", "TEXT")?;
    conn.execute(
        "UPDATE imported_memories SET server_memory_id = memory_id WHERE server_memory_id IS NULL",
        [],
    )
    .context("migrate import ledger server memory IDs")?;
    migrate_import_input_ids(conn)?;

    let memory_id_index_is_unique: Option<bool> = conn
        .query_row(
            "
            SELECT [unique] != 0
            FROM pragma_index_list('imported_memories')
            WHERE name = 'imported_memories_memory_id_idx'
            ",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("inspect import ledger memory ID index")?;
    if memory_id_index_is_unique == Some(true) {
        conn.execute_batch("DROP INDEX IF EXISTS imported_memories_memory_id_idx;")
            .context("migrate import ledger memory ID index")?;
    }
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS imported_memories_memory_id_idx
            ON imported_memories(memory_id)
            WHERE memory_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS imported_memories_input_memory_id_idx
            ON imported_memories(input_memory_id)
            WHERE input_memory_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS imported_memories_server_memory_id_idx
            ON imported_memories(server_memory_id)
            WHERE server_memory_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS imported_memories_line_hash_idx
            ON imported_memories(source_line_hash);
        ",
    )
    .context("initialize import ledger memory ID index")?;

    Ok(())
}

fn add_import_ledger_column(conn: &Connection, name: &str, sql_type: &str) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('imported_memories') WHERE name = ?1",
            [name],
            |row| row.get(0),
        )
        .context("inspect import ledger columns")?;
    if !exists {
        conn.execute(
            &format!("ALTER TABLE imported_memories ADD COLUMN {name} {sql_type}"),
            [],
        )
        .with_context(|| format!("add import ledger column {name}"))?;
    }
    Ok(())
}

fn migrate_import_input_ids(conn: &Connection) -> Result<()> {
    let rows = {
        let mut statement = conn
            .prepare(
                "SELECT rowid, raw_json FROM imported_memories \
                 WHERE input_memory_id IS NULL ORDER BY imported_at, rowid",
            )
            .context("prepare import ledger input ID migration")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .context("query import ledger input IDs")?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("read import ledger input IDs")?
    };
    let mut seen_statement = conn
        .prepare("SELECT input_memory_id FROM imported_memories WHERE input_memory_id IS NOT NULL")
        .context("prepare existing import input IDs")?;
    let mut seen = seen_statement
        .query_map([], |row| row.get::<_, String>(0))
        .context("query existing import input IDs")?
        .collect::<rusqlite::Result<HashSet<_>>>()
        .context("read existing import input IDs")?;

    for (rowid, raw_json) in rows {
        let Ok(value) = serde_json::from_str::<Value>(&raw_json) else {
            continue;
        };
        let Some(input_id) = memory_id(&value) else {
            continue;
        };
        if seen.insert(input_id.clone()) {
            conn.execute(
                "UPDATE imported_memories SET input_memory_id = ?1 WHERE rowid = ?2",
                params![input_id, rowid],
            )
            .context("migrate import ledger input memory ID")?;
        }
    }
    Ok(())
}

fn sync_db_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".mnemos").join("mnemosctl.db"))
}

/// Prepare the insert/update statements and upsert a single memory.
///
/// Only the tests use this. The sync path prepares both statements once and
/// drives [`upsert_memory_with_statements`] in a loop, so building them per
/// call would be wasteful in production. Scoped to `cfg(test)` to keep it out
/// of the library target, where it is dead code and fails `-D warnings`.
#[cfg(test)]
fn upsert_memory(conn: &Connection, memory: &Value) -> Result<bool> {
    let mut insert = conn
        .prepare(
            "
            INSERT INTO memories (id, content, category, created_at, raw_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO NOTHING
            ",
        )
        .context("prepare sqlite insert")?;
    let mut update = conn
        .prepare(
            "
            UPDATE memories
            SET content = ?2,
                category = ?3,
                created_at = ?4,
                raw_json = ?5
            WHERE id = ?1 AND raw_json IS NOT ?5
            ",
        )
        .context("prepare sqlite update")?;
    upsert_memory_with_statements(&mut insert, &mut update, memory)
}

fn upsert_memory_with_statements(
    insert: &mut Statement<'_>,
    update: &mut Statement<'_>,
    memory: &Value,
) -> Result<bool> {
    let id = string_field(memory, &["id"]).context("memory is missing id")?;
    let content = string_field(memory, &["content"]).unwrap_or_default();
    let category = string_field(memory, &["category"]).unwrap_or_else(|| "facts".to_string());
    let created_at =
        string_field(memory, &["created", "created_at", "createdAt"]).unwrap_or_default();
    let raw_json = serde_json::to_string(memory).context("serialize raw memory JSON")?;

    debug!(memory_id = %id, "upserting memory");
    let inserted = insert
        .execute(params![id, content, category, created_at, raw_json])
        .context("execute sqlite insert")?
        == 1;

    if !inserted {
        update
            .execute(params![id, content, category, created_at, raw_json])
            .context("execute sqlite update")?;
    }

    Ok(inserted)
}

fn count_jsonl_rows(path: &Path) -> Result<usize> {
    let file = File::open(path).with_context(|| format!("open JSONL file {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut total = 0usize;

    for line in reader.lines() {
        if !line
            .with_context(|| format!("read JSONL file {}", path.display()))?
            .trim()
            .is_empty()
        {
            total += 1;
        }
    }

    Ok(total)
}

fn import_source(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn validate_import_row(value: &Value) -> Result<()> {
    let Some(object) = value.as_object() else {
        bail!("row must be a JSON object");
    };
    if string_field(value, &["content"]).is_none() {
        bail!("missing required field content");
    }
    const PORTABILITY_FIELDS: &[&str] = &[
        "id",
        "memory_id",
        "memoryId",
        "content",
        "category",
        "subcategory",
        "created",
        "updated",
        "owner_id",
        "namespace",
        "permission_mode",
        "quality_rating",
        "metadata",
        "source_model",
        "source_provider",
        "source_session",
        "source_agent",
        "source",
        "verbatim_content",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !PORTABILITY_FIELDS.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        bail!(
            "fields cannot be restored by the MNEMOS portability API: {}; refusing lossy import",
            unsupported.join(", ")
        );
    }
    Ok(())
}

fn requires_portability_import(value: &Value) -> bool {
    [
        "id",
        "memory_id",
        "memoryId",
        "created",
        "updated",
        "quality_rating",
    ]
    .iter()
    .any(|key| value.get(*key).is_some_and(|field| !field.is_null()))
}

fn portability_import_envelope(value: &Value, record_id: &str) -> Result<Value> {
    let object = value
        .as_object()
        .context("portability import row must be an object")?;
    if object.get("source").is_some_and(|field| !field.is_null()) {
        bail!(
            "field source cannot be restored together with identity/timestamps by the MNEMOS portability API; refusing lossy import"
        );
    }
    let mut payload = serde_json::Map::new();
    for key in [
        "content",
        "category",
        "subcategory",
        "created",
        "updated",
        "owner_id",
        "namespace",
        "permission_mode",
        "quality_rating",
        "metadata",
        "source_model",
        "source_provider",
        "source_session",
        "source_agent",
        "verbatim_content",
    ] {
        if let Some(field) = object.get(key)
            && !field.is_null()
        {
            payload.insert(key.to_string(), field.clone());
        }
    }

    Ok(json!({
        "mpf_version": "0.1.1",
        "source_system": "mnemosctl",
        "source_version": env!("CARGO_PKG_VERSION"),
        "record_count": 1,
        "records": [{
            "id": record_id,
            "kind": "memory",
            "payload_version": "mnemos-3.1",
            "payload": payload,
        }],
    }))
}

fn portability_import_counts(value: &Value) -> Result<(usize, usize, usize)> {
    let imported =
        usize_field(value, &["imported"]).context("import response is missing imported")?;
    let skipped = usize_field(value, &["skipped"]).context("import response is missing skipped")?;
    let mut failed =
        usize_field(value, &["failed"]).context("import response is missing failed")?;
    for key in ["sidecars_failed", "unsupported_kinds"] {
        if let Some(counts) = value.get(key).and_then(Value::as_object) {
            for count in counts.values() {
                failed += count
                    .as_u64()
                    .and_then(|count| usize::try_from(count).ok())
                    .with_context(|| format!("import response {key} count is not an integer"))?;
            }
        }
    }
    Ok((imported, skipped, failed))
}

fn handle_bad_import_row(
    result: &mut ImportResult,
    _options: ImportOptions,
    line_number: usize,
    error: String,
) {
    result.failed += 1;
    eprintln!("line {line_number} failed: {error}");
}

fn import_already_recorded(
    conn: &Connection,
    input_memory_id: Option<&str>,
    line_hash: &str,
    raw_json: &str,
) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "
            SELECT COUNT(*)
            FROM imported_memories
            WHERE (?1 IS NOT NULL AND input_memory_id = ?1)
               OR (source_line_hash = ?2 AND raw_json = ?3)
            ",
            params![input_memory_id, line_hash, raw_json],
            |row| row.get(0),
        )
        .context("query import ledger by input identity or content")?;

    Ok(count > 0)
}

fn record_import(
    conn: &Connection,
    source: &str,
    line_hash: &str,
    input_memory_id: Option<&str>,
    server_memory_id: Option<&str>,
    raw_json: &str,
) -> Result<bool> {
    let inserted = conn
        .execute(
            "
        INSERT INTO imported_memories (
            source,
            source_line_hash,
            memory_id,
            input_memory_id,
            server_memory_id,
            raw_json
        )
        VALUES (?1, ?2, ?4, ?3, ?4, ?5)
        ON CONFLICT(source, source_line_hash) DO NOTHING
        ",
            params![
                source,
                line_hash,
                input_memory_id,
                server_memory_id,
                raw_json
            ],
        )
        .context("record imported memory")?;

    Ok(inserted == 1)
}

fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn memory_id(value: &Value) -> Option<String> {
    string_field(value, &["id", "memory_id", "memoryId"])
}

fn memory_id_from_create_response(value: &Value) -> Option<String> {
    memory_id(value)
        .or_else(|| value.get("memory").and_then(memory_id))
        .or_else(|| value.get("data").and_then(memory_id))
}

fn maybe_emit_progress(processed: usize, total: Option<usize>, every: Option<usize>) {
    let Some(every) = every.filter(|value| *value > 0) else {
        return;
    };
    if processed > 0
        && (is_multiple_of(processed, every) || total.is_some_and(|total| processed == total))
    {
        emit_progress(processed, total);
    }
}

fn emit_final_progress(processed: usize, total: Option<usize>, every: Option<usize>) {
    let Some(every) = every.filter(|value| *value > 0) else {
        return;
    };
    if processed > 0 && !is_multiple_of(processed, every) {
        emit_progress(processed, total.or(Some(processed)));
    }
}

#[allow(clippy::manual_is_multiple_of)]
fn is_multiple_of(value: usize, divisor: usize) -> bool {
    value % divisor == 0
}

fn emit_progress(processed: usize, total: Option<usize>) {
    let total = total
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("[progress] {processed}/{total} rows processed");
}

fn total_count(value: &Value) -> Option<usize> {
    usize_field(
        value,
        &[
            "total",
            "total_count",
            "totalCount",
            "count",
            "total_results",
        ],
    )
}

fn usize_field(value: &Value, keys: &[&str]) -> Option<usize> {
    for key in keys {
        if let Some(field) = value.get(*key) {
            if let Some(number) = field.as_u64() {
                return usize::try_from(number).ok();
            }
            if let Some(text) = field.as_str()
                && let Ok(number) = text.parse::<usize>()
            {
                return Some(number);
            }
        }
    }
    None
}

fn load_file_config() -> Result<FileConfig> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(FileConfig::default());
    }

    let content = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: FileConfig =
        toml::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    if config.api_key.clone().and_then(non_empty).is_some() {
        validate_secret_file_permissions(&path)?;
    }
    Ok(config)
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

/// A validated MNEMOS endpoint. Equality is structural so harmless spelling
/// differences such as host casing or an explicit default port cannot evade
/// or accidentally miss the allowlist.
#[derive(Clone, Debug, Eq, PartialEq)]
struct EndpointUrl {
    scheme: String,
    host: String,
    port: u16,
    path: String,
}

impl fmt::Display for EndpointUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let default_port = match self.scheme.as_str() {
            "http" => 80,
            "https" => 443,
            _ => unreachable!("endpoint schemes are validated"),
        };
        if self.port == default_port {
            write!(formatter, "{}://{}{}", self.scheme, host, self.path)
        } else {
            write!(
                formatter,
                "{}://{}:{}{}",
                self.scheme, host, self.port, self.path
            )
        }
    }
}

fn parse_endpoint_url(value: &str) -> Result<EndpointUrl> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("host URL is empty");
    }
    if trimmed
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        bail!("host URL {trimmed:?} contains whitespace or control characters");
    }

    let url = Url::parse(trimmed).with_context(|| {
        format!("host URL {trimmed:?} is invalid; include an explicit http:// or https:// scheme")
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!(
            "host URL {trimmed:?} uses unsupported scheme {:?}; expected http or https",
            url.scheme()
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("host URL {trimmed:?} embeds userinfo credentials and will not be used");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("host URL {trimmed:?} must not carry a query string or fragment");
    }
    let host = url
        .host_str()
        .context("host URL has no hostname")?
        .to_ascii_lowercase();
    let port = url
        .port_or_known_default()
        .context("host URL has no usable port")?;
    let path = url.path().trim_end_matches('/').to_string();

    Ok(EndpointUrl {
        scheme: url.scheme().to_string(),
        host,
        port,
        path,
    })
}

/// clap value parser for `sync-from`, kept syntax-only so command parsing never
/// performs DNS or reads configuration.
pub fn parse_sync_host_arg(value: &str) -> std::result::Result<String, String> {
    parse_endpoint_url(value)
        .map(|endpoint| endpoint.to_string())
        .map_err(|error| format!("{error:#}"))
}

fn sync_host_allowlist() -> Result<Vec<String>> {
    if let Some(raw) = env::var(SYNC_HOSTS_ENV).ok().and_then(non_empty) {
        return Ok(raw
            .split([',', ' ', '\t', '\n'])
            .filter_map(|entry| non_empty(entry.to_string()))
            .collect());
    }
    if env::var_os("HOME").is_none() {
        return Ok(Vec::new());
    }
    Ok(load_file_config()
        .context("read ~/.mnemos/config.toml")?
        .sync_hosts
        .unwrap_or_default()
        .into_iter()
        .filter_map(non_empty)
        .collect())
}

fn authorize_sync_host(host: &str, base_url: &str, allowlist: &[String]) -> Result<EndpointUrl> {
    let target = parse_endpoint_url(host).context("reject sync-from host")?;
    let configured = parse_endpoint_url(base_url).context("validate configured MNEMOS host")?;
    if target == configured {
        return Ok(target);
    }

    for entry in allowlist {
        let allowed = parse_endpoint_url(entry)
            .with_context(|| format!("invalid {SYNC_HOSTS_ENV} entry {entry:?}"))?;
        if target == allowed {
            return Ok(target);
        }
    }

    bail!(
        "refusing to send the local MNEMOS root token to {target}: it is neither the configured \
         MNEMOS host ({base_url}) nor in the sync allowlist. Add it to {SYNC_HOSTS_ENV} or to \
         sync_hosts in ~/.mnemos/config.toml"
    )
}

async fn build_pinned_sync_client(endpoint: &EndpointUrl, options: SyncOptions) -> Result<Client> {
    let addrs = resolve_sync_endpoint(endpoint).await?;
    Client::builder()
        .connect_timeout(options.connect_timeout)
        .read_timeout(options.read_timeout)
        .timeout(options.request_timeout)
        // A proxy would connect somewhere other than the addresses authorized
        // below. Redirects would likewise change the credential destination.
        .no_proxy()
        .redirect(redirect::Policy::none())
        // reqwest consults this static override whenever it opens a connection,
        // so it cannot perform a second DNS lookup and rebind the hostname.
        .resolve_to_addrs(&endpoint.host, &addrs)
        .build()
        .context("build DNS-pinned sync HTTP client")
}

async fn resolve_sync_endpoint(endpoint: &EndpointUrl) -> Result<Vec<SocketAddr>> {
    if let Ok(ip) = endpoint.host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, endpoint.port)]);
    }

    // The repository's subprocess-safe mock layer recognizes these reserved
    // test hostnames before network I/O. Give the client a deterministic pin
    // so tests exercise the same pinned-client construction path without DNS.
    if mock_registry_key(&endpoint.host).is_some_and(|key| env::var_os(key).is_some()) {
        return Ok(vec![SocketAddr::new(
            IpAddr::from([127, 0, 0, 1]),
            endpoint.port,
        )]);
    }

    let mut addrs = tokio::net::lookup_host((endpoint.host.as_str(), endpoint.port))
        .await
        .with_context(|| format!("resolve authorized sync host {}", endpoint.host))?
        .collect::<Vec<_>>();
    addrs.sort_unstable();
    addrs.dedup();
    if addrs.is_empty() {
        bail!(
            "authorized sync host {} resolved to no addresses",
            endpoint.host
        );
    }
    Ok(addrs)
}

fn mock_registry_key(host: &str) -> Option<String> {
    host.strip_prefix("wiremock-")
        .and_then(|value| value.strip_suffix(".mnemosctl.test"))
        .map(|id| format!("MNEMOSCTL_WIREMOCK_{id}"))
}

fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn mask_api_key(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    match chars.len() {
        0..=8 => "********".to_string(),
        9..=16 => {
            let prefix: String = chars.iter().take(2).collect();
            let suffix: String = chars.iter().skip(chars.len() - 2).collect();
            format!("{prefix}...{suffix}")
        }
        _ => {
            let prefix: String = chars.iter().take(4).collect();
            let suffix: String = chars.iter().skip(chars.len() - 4).collect();
            format!("{prefix}...{suffix}")
        }
    }
}

fn validate_secret_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mode = fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            bail!(
                "{} contains api_key but is group/world accessible ({mode:o}); run: chmod 600 {}",
                path.display(),
                path.display()
            );
        }
    }
    Ok(())
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("chmod 700 {}", path.display()))?;
    }
    Ok(())
}

fn ensure_private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("chmod 600 {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn masks_short_api_keys_without_revealing_them() {
        assert_eq!(mask_api_key("short"), "********");
        assert_eq!(mask_api_key("12345678"), "********");
        assert_eq!(mask_api_key("1234567890"), "12...90");
        assert_eq!(mask_api_key("abcdefghijklmnopqrstuvwxyz"), "abcd...wxyz");
    }

    #[test]
    fn import_ledger_is_idempotent_across_file_paths() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        initialize_sync_schema(&conn)?;
        let raw = r#"{"id":"legacy-1","content":"same"}"#;
        record_import(
            &conn,
            "file-a",
            "hash-a",
            Some("legacy-1"),
            Some("legacy-1"),
            raw,
        )?;

        assert!(import_already_recorded(
            &conn,
            Some("legacy-1"),
            "different-hash",
            r#"{"id":"legacy-1","content":"changed"}"#,
        )?);
        assert!(import_already_recorded(&conn, None, "hash-a", raw)?);
        Ok(())
    }

    #[test]
    fn portability_envelope_preserves_identity_and_timestamps() -> Result<()> {
        let value = json!({
            "id": "mem-old",
            "content": "archive row",
            "created": "2024-01-01T00:00:00Z",
            "updated": "2024-02-01T00:00:00Z",
            "quality_rating": 91,
        });
        validate_import_row(&value)?;
        let envelope = portability_import_envelope(&value, "mem-old")?;
        assert_eq!(envelope["records"][0]["id"], "mem-old");
        assert_eq!(
            envelope["records"][0]["payload"]["created"],
            "2024-01-01T00:00:00Z"
        );
        assert_eq!(envelope["records"][0]["payload"]["quality_rating"], 91);
        Ok(())
    }

    #[test]
    fn lossy_archive_fields_are_rejected() {
        let error = validate_import_row(&json!({
            "id": "mem-old",
            "content": "archive row",
            "compressed_content": "summary",
            "archived_at": "2025-01-01T00:00:00Z",
        }))
        .expect_err("unsupported archive state must not be silently discarded")
        .to_string();
        assert!(error.contains("archived_at"));
        assert!(error.contains("compressed_content"));
    }

    #[test]
    fn only_row_validation_statuses_are_skippable() {
        for status in [400, 413, 422] {
            assert!(JsonRequestError::http("import", status, String::new()).is_bad_import_row());
        }
        for status in [401, 403, 408, 409, 429] {
            assert!(!JsonRequestError::http("import", status, String::new()).is_bad_import_row());
        }
    }

    #[test]
    fn duplicate_conflict_uses_server_existing_id() {
        let error = JsonRequestError::http(
            "import",
            409,
            json!({
                "error": "duplicate_content",
                "existing_id": "mem-real"
            })
            .to_string(),
        );

        assert_eq!(error.duplicate_existing_id().as_deref(), Some("mem-real"));
        assert!(
            JsonRequestError::http("import", 409, json!({"detail": "other"}).to_string())
                .duplicate_existing_id()
                .is_none()
        );
    }

    #[test]
    fn sync_uses_structured_redaction_metadata_not_a_content_sentinel() {
        assert!(response_has_redaction_metadata(&json!({
            "id": "mem-secret",
            "content": "token=[REDACTED]",
            "metadata": {"secret_redact_fields": {"content": [[6, 25]]}}
        })));
        assert!(!response_has_redaction_metadata(&json!({
            "id": "mem-literal",
            "content": "Use [REDACTED] in examples"
        })));
    }

    #[test]
    fn ledger_allows_multiple_source_rows_for_one_server_memory() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        initialize_sync_schema(&conn)?;

        assert!(record_import(
            &conn,
            "file-a",
            "hash-a",
            Some("legacy-a"),
            Some("mem-1"),
            "{}"
        )?);
        assert!(record_import(
            &conn,
            "file-b",
            "hash-b",
            Some("legacy-b"),
            Some("mem-1"),
            "{}"
        )?);
        assert!(!record_import(
            &conn,
            "file-b",
            "hash-b",
            Some("legacy-b"),
            Some("mem-1"),
            "{}"
        )?);

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM imported_memories WHERE server_memory_id = 'mem-1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    fn populated_ledger_unique_index_is_migrated_in_place() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            CREATE TABLE imported_memories (
                source TEXT NOT NULL,
                source_line_hash TEXT NOT NULL,
                memory_id TEXT,
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (source, source_line_hash)
            );
            CREATE UNIQUE INDEX imported_memories_memory_id_idx
                ON imported_memories(memory_id)
                WHERE memory_id IS NOT NULL;
            INSERT INTO imported_memories (source, source_line_hash, memory_id, raw_json)
                VALUES (
                    'old-file',
                    'old-hash',
                    'mem-1',
                    '{\"id\":\"legacy-1\",\"content\":\"old\"}'
                );
            ",
        )?;

        initialize_sync_schema(&conn)?;
        let migrated: (String, String) = conn.query_row(
            "SELECT input_memory_id, server_memory_id FROM imported_memories",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(migrated, ("legacy-1".to_string(), "mem-1".to_string()));
        assert!(import_already_recorded(
            &conn,
            Some("legacy-1"),
            "new-hash",
            r#"{"id":"legacy-1","content":"changed"}"#,
        )?);
        assert!(record_import(
            &conn,
            "new-file",
            "new-hash",
            Some("legacy-2"),
            Some("mem-1"),
            "{}"
        )?);
        Ok(())
    }

    #[test]
    fn sync_uses_created_wire_field() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            CREATE TABLE memories (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                category TEXT NOT NULL,
                created_at TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );
            ",
        )?;

        upsert_memory(
            &conn,
            &json!({
                "id": "mem-1",
                "content": "content",
                "category": "facts",
                "created": "2026-08-10T12:00:00Z"
            }),
        )?;

        let created_at: String = conn.query_row(
            "SELECT created_at FROM memories WHERE id = 'mem-1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(created_at, "2026-08-10T12:00:00Z");
        Ok(())
    }

    #[test]
    fn sync_does_not_rewrite_unchanged_rows() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            CREATE TABLE memories (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                category TEXT NOT NULL,
                created_at TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );
            ",
        )?;
        let memory = json!({
            "id": "mem-1",
            "content": "content",
            "category": "facts",
            "created": "2026-08-10T12:00:00Z"
        });

        assert!(upsert_memory(&conn, &memory)?);
        let changes_after_insert = conn.total_changes();
        assert!(!upsert_memory(&conn, &memory)?);
        assert_eq!(conn.total_changes(), changes_after_insert);

        let mut changed = memory;
        changed["content"] = Value::String("changed".to_string());
        assert!(!upsert_memory(&conn, &changed)?);
        assert_eq!(conn.total_changes(), changes_after_insert + 1);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_group_readable_config_with_api_key() -> Result<()> {
        let dir = temp_test_dir("mnemosctl-config-perms")?;
        let path = dir.join("config.toml");
        fs::write(
            &path,
            "base_url = \"http://localhost:5002\"\napi_key = \"secret\"\n",
        )?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;

        let error = validate_secret_file_permissions(&path)
            .expect_err("group-readable secret config should fail")
            .to_string();

        let _ = fs::remove_dir_all(&dir);
        assert!(error.contains("chmod 600"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn accepts_owner_only_config_with_api_key() -> Result<()> {
        let dir = temp_test_dir("mnemosctl-config-private")?;
        let path = dir.join("config.toml");
        fs::write(&path, "api_key = \"secret\"\n")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;

        validate_secret_file_permissions(&path)?;

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn configured_total_timeout_bounds_a_stalled_response() -> Result<()> {
        use tokio::net::TcpListener;

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept client");
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let client = build_http_client(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_millis(50),
        )?;

        let error = client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .expect_err("stalled response should time out");
        server.abort();
        assert!(error.is_timeout());
        Ok(())
    }

    #[cfg(unix)]
    fn temp_test_dir(prefix: &str) -> Result<PathBuf> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("clock before unix epoch")?
            .as_nanos();
        let dir = env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}
