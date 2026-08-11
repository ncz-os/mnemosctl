use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Request, RequestBuilder};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::env;
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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

#[derive(Clone, Copy, Debug, Default)]
pub struct SyncOptions {
    pub progress_every: Option<usize>,
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

        if let (Some(base_url), Some(api_key)) = (&env_base_url, &env_api_key) {
            return Ok(Self {
                base_url: normalize_base_url(base_url),
                api_key: api_key.clone(),
            });
        }

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
    let result = sync_from_host_with_options(client, api_key, host, SyncOptions::default()).await?;
    Ok(result.inserted)
}

pub async fn sync_from_host_with_options(
    client: &Client,
    api_key: &str,
    host: &str,
    options: SyncOptions,
) -> Result<SyncResult> {
    let conn = open_sync_db()?;
    let host = normalize_base_url(host);
    let mut offset = 0usize;
    let mut result = SyncResult::default();

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

        if result.total.is_none() {
            result.total = total_count(&page);
        }

        let items = memory_items(&page);
        if items.is_empty() {
            break;
        }

        for item in &items {
            if upsert_memory(&conn, item).context("upsert memory into local sqlite")? {
                result.inserted += 1;
            } else {
                result.updated += 1;
            }
            result.processed += 1;
            maybe_emit_progress(result.processed, result.total, options.progress_every);
        }

        if items.len() < DEFAULT_PAGE_SIZE {
            break;
        }
        offset += items.len();
    }

    emit_final_progress(result.processed, result.total, options.progress_every);
    println!(
        "synced {} new records ({} rows processed)",
        result.inserted, result.processed
    );
    Ok(result)
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
    let total = count_jsonl_rows(path)?;
    let file = File::open(path).with_context(|| format!("open JSONL file {}", path.display()))?;
    let reader = BufReader::new(file);
    let conn = open_sync_db()?;
    let source = import_source(path);
    let mut result = ImportResult {
        total,
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
                maybe_emit_progress(result.processed, Some(result.total), options.progress_every);
                if options.skip_bad {
                    continue;
                }
                bail!("import line {}: invalid JSON: {error}", index + 1);
            }
        };

        if let Err(error) = validate_import_row(&value) {
            handle_bad_import_row(&mut result, options, index + 1, error.to_string());
            maybe_emit_progress(result.processed, Some(result.total), options.progress_every);
            if options.skip_bad {
                continue;
            }
            return Err(error).with_context(|| format!("import line {}", index + 1));
        }

        let input_id = memory_id(&value);
        let has_input_id = input_id.is_some();
        if import_already_recorded(&conn, &source, &line_hash)? {
            result.skipped_existing += 1;
            maybe_emit_progress(result.processed, Some(result.total), options.progress_every);
            continue;
        }

        match request_json_result(
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
            Ok(response) => {
                let imported_id = imported_memory_id(input_id, &response.value);
                let recorded =
                    record_import(&conn, &source, &line_hash, imported_id.as_deref(), trimmed)?;
                if response.status == 200 {
                    result.skipped_existing += 1;
                } else if recorded {
                    result.imported += 1;
                } else {
                    result.skipped_existing += 1;
                }
            }
            Err(error)
                if (options.skip_bad || has_input_id)
                    && error.duplicate_existing_id().is_some() =>
            {
                let existing_id = error.duplicate_existing_id();
                record_import(&conn, &source, &line_hash, existing_id.as_deref(), trimmed)?;
                result.skipped_existing += 1;
                eprintln!("line {} already exists: {error}", index + 1);
            }
            Err(error) if error.is_bad_import_row() => {
                handle_bad_import_row(&mut result, options, index + 1, error.to_string());
                maybe_emit_progress(result.processed, Some(result.total), options.progress_every);
                if options.skip_bad {
                    continue;
                }
                return Err(error).with_context(|| format!("import line {}", index + 1));
            }
            Err(error) => {
                return Err(error).with_context(|| format!("import line {}", index + 1));
            }
        }

        maybe_emit_progress(result.processed, Some(result.total), options.progress_every);
    }

    emit_final_progress(result.processed, Some(result.total), options.progress_every);
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
    let Some(id) = host
        .strip_prefix("wiremock-")
        .and_then(|value| value.strip_suffix(".mnemosctl.test"))
    else {
        return Ok(None);
    };

    let key = format!("MNEMOSCTL_WIREMOCK_{id}");
    let mocks = env::var(&key).map_err(|error| {
        JsonRequestError::other(anyhow!("{context} has no mounted mock: {error}"))
    })?;
    let mocks: Value = serde_json::from_str(&mocks)
        .map_err(|error| JsonRequestError::other(anyhow!("parse {key} mock registry: {error}")))?;
    let mocks = mocks
        .as_array()
        .ok_or_else(|| JsonRequestError::other(anyhow!("{key} mock registry is not an array")))?;

    for mock in mocks {
        if wiremock_matches(mock, request) {
            let response = mock.get("response").unwrap_or(&Value::Null);
            let status = response
                .get("status")
                .and_then(Value::as_u64)
                .unwrap_or(500) as u16;
            let body = response.get("body").and_then(Value::as_str).unwrap_or("");

            if !(200..300).contains(&status) {
                return Err(JsonRequestError::http(context, status, body.to_string()));
            }

            return if body.trim().is_empty() {
                Ok(Some(JsonResponse {
                    status,
                    value: json!({}),
                }))
            } else {
                serde_json::from_str(body)
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
            imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (source, source_line_hash)
        );

        ",
    )
    .context("initialize sqlite schema")?;

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
        ",
    )
    .context("initialize import ledger memory ID index")?;

    Ok(())
}

fn sync_db_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".mnemos").join("mnemosctl.db"))
}

fn upsert_memory(conn: &Connection, memory: &Value) -> Result<bool> {
    let id = string_field(memory, &["id"]).context("memory is missing id")?;
    let content = string_field(memory, &["content"]).unwrap_or_default();
    let category = string_field(memory, &["category"]).unwrap_or_else(|| "facts".to_string());
    let created_at =
        string_field(memory, &["created", "created_at", "createdAt"]).unwrap_or_default();
    let raw_json = serde_json::to_string(memory).context("serialize raw memory JSON")?;

    debug!(memory_id = %id, "upserting memory");
    let inserted = conn
        .execute(
            "
            INSERT INTO memories (id, content, category, created_at, raw_json)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO NOTHING
            ",
            params![id, content, category, created_at, raw_json],
        )
        .context("execute sqlite insert")?
        == 1;

    if !inserted {
        conn.execute(
            "
            UPDATE memories
            SET content = ?2,
                category = ?3,
                created_at = ?4,
                raw_json = ?5
            WHERE id = ?1
            ",
            params![id, content, category, created_at, raw_json],
        )
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
    if !value.is_object() {
        bail!("row must be a JSON object");
    }
    if string_field(value, &["content"]).is_none() {
        bail!("missing required field content");
    }
    Ok(())
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

fn import_already_recorded(conn: &Connection, source: &str, line_hash: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "
            SELECT COUNT(*)
            FROM imported_memories
            WHERE source = ?1 AND source_line_hash = ?2
            ",
            params![source, line_hash],
            |row| row.get(0),
        )
        .context("query import ledger by source line")?;

    Ok(count > 0)
}

fn record_import(
    conn: &Connection,
    source: &str,
    line_hash: &str,
    memory_id: Option<&str>,
    raw_json: &str,
) -> Result<bool> {
    let inserted = conn
        .execute(
            "
        INSERT INTO imported_memories (
            source,
            source_line_hash,
            memory_id,
            raw_json
        )
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(source, source_line_hash) DO NOTHING
        ",
            params![source, line_hash, memory_id, raw_json],
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

fn imported_memory_id(input_id: Option<String>, response: &Value) -> Option<String> {
    memory_id_from_create_response(response).or(input_id)
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
    fn import_ledger_prefers_server_generated_id() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        initialize_sync_schema(&conn)?;
        let imported_id = imported_memory_id(
            Some("legacy-1".to_string()),
            &json!({"id": "mem_generated_a"}),
        );

        record_import(&conn, "file-a", "hash-a", imported_id.as_deref(), "{}")?;

        assert!(!import_already_recorded(&conn, "file-b", "hash-b")?);
        Ok(())
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
    fn ledger_allows_multiple_source_rows_for_one_server_memory() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        initialize_sync_schema(&conn)?;

        assert!(record_import(
            &conn,
            "file-a",
            "hash-a",
            Some("mem-1"),
            "{}"
        )?);
        assert!(record_import(
            &conn,
            "file-b",
            "hash-b",
            Some("mem-1"),
            "{}"
        )?);
        assert!(!record_import(
            &conn,
            "file-b",
            "hash-b",
            Some("mem-1"),
            "{}"
        )?);

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM imported_memories WHERE memory_id = 'mem-1'",
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
                VALUES ('old-file', 'old-hash', 'mem-1', '{}');
            ",
        )?;

        initialize_sync_schema(&conn)?;
        assert!(record_import(
            &conn,
            "new-file",
            "new-hash",
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
