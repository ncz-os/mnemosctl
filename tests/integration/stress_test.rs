use anyhow::{Context, Result, anyhow, bail};
use mnemosctl::{
    Config, ImportOptions, import_jsonl_with_options, search_memories, sync_from_host,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SYNC_STRESS_HOST: &str = "http://192.168.207.67:5002";

#[tokio::test]
async fn sync_from_host_is_fast_and_idempotent_at_corpus_scale() -> Result<()> {
    let Some(config) = stress_config() else {
        skip_stress();
        return Ok(());
    };
    let client = reqwest::Client::new();

    let started = Instant::now();
    sync_from_host(&client, &config.api_key, SYNC_STRESS_HOST).await?;
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "sync-from should complete in under 30s"
    );

    let local_count = local_memory_count()?;
    assert!(
        local_count >= 5000,
        "expected at least 5000 local sqlite rows, got {local_count}"
    );

    let inserted_on_rerun = sync_from_host(&client, &config.api_key, SYNC_STRESS_HOST).await?;
    assert_eq!(
        inserted_on_rerun, 0,
        "sync-from rerun should insert zero new rows"
    );

    Ok(())
}

#[tokio::test]
async fn bulk_import_streams_one_thousand_memories() -> Result<()> {
    let Some(config) = stress_config() else {
        skip_stress();
        return Ok(());
    };
    let client = reqwest::Client::new();
    let namespace = unique_namespace("bulk");
    let temp_dir = TempDir::new("mnemosctl-stress-bulk")?;
    let file_path = temp_dir.path().join("memories.jsonl");
    let expected_ids = write_jsonl(&file_path, &namespace, 1000, None)?;

    let import_result =
        import_jsonl_with_options(&client, &config, &file_path, ImportOptions::default()).await;
    let found_ids = memory_ids_for_namespace(&client, &config, &namespace, 1100)
        .await
        .unwrap_or_default();
    cleanup_memories(&client, &config, &found_ids, &expected_ids).await?;

    let import_result = import_result?;
    assert_eq!(import_result.imported, 1000);
    assert_eq!(import_result.failed, 0);
    assert_all_ids_present(&expected_ids, &found_ids)?;

    Ok(())
}

#[tokio::test]
async fn import_resumes_after_bad_row_with_skip_bad() -> Result<()> {
    let Some(config) = stress_config() else {
        skip_stress();
        return Ok(());
    };
    let client = reqwest::Client::new();
    let namespace = unique_namespace("resume");
    let temp_dir = TempDir::new("mnemosctl-stress-resume")?;
    let file_path = temp_dir.path().join("memories.jsonl");
    let expected_ids = write_jsonl(&file_path, &namespace, 100, Some(50))?;

    let first_result =
        import_jsonl_with_options(&client, &config, &file_path, ImportOptions::default()).await?;
    assert_eq!(first_result.imported, 49);
    assert_eq!(first_result.failed, 1);
    assert_eq!(first_result.processed, 50);

    let found_after_first = memory_ids_for_namespace(&client, &config, &namespace, 150).await?;
    assert_eq!(found_after_first.len(), 49);

    let second_result = import_jsonl_with_options(
        &client,
        &config,
        &file_path,
        ImportOptions {
            skip_bad: true,
            progress_every: None,
        },
    )
    .await;
    let found_after_second = memory_ids_for_namespace(&client, &config, &namespace, 150)
        .await
        .unwrap_or_default();
    cleanup_memories(&client, &config, &found_after_second, &expected_ids).await?;

    let second_result = second_result?;
    assert_eq!(second_result.imported, 50);
    assert_eq!(second_result.skipped_existing, 49);
    assert_eq!(second_result.failed, 1);
    assert_eq!(second_result.processed, 100);

    let expected_created = expected_ids
        .iter()
        .enumerate()
        .filter_map(|(index, id)| (index + 1 != 50).then_some(id.clone()))
        .collect::<Vec<_>>();
    assert_all_ids_present(&expected_created, &found_after_second)?;

    Ok(())
}

fn stress_config() -> Option<Config> {
    let base_url = env::var("MNEMOS_TEST_BASE").ok()?;
    let api_key = env::var("MNEMOS_API_KEY").ok()?;
    Some(Config {
        base_url: base_url.trim_end_matches('/').to_string(),
        api_key,
    })
}

fn skip_stress() {
    eprintln!("skipping stress tests; MNEMOS_TEST_BASE and MNEMOS_API_KEY must both be set");
}

fn local_memory_count() -> Result<i64> {
    let db_path = env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")?
        .join(".mnemos")
        .join("mnemosctl.db");
    let conn = Connection::open(&db_path)
        .with_context(|| format!("open sqlite database {}", db_path.display()))?;
    conn.query_row("SELECT COUNT(*) FROM memories", [], |row| row.get(0))
        .context("count local memories")
}

fn write_jsonl(
    path: &Path,
    namespace: &str,
    count: usize,
    bad_row: Option<usize>,
) -> Result<Vec<String>> {
    let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut ids = Vec::with_capacity(count);

    for index in 1..=count {
        let id = format!("{namespace}-{index:04}");
        let value = if bad_row == Some(index) {
            json!({
                "id": id,
                "category": "facts",
                "namespace": namespace,
            })
        } else {
            json!({
                "id": id,
                "content": format!("mnemosctl stress memory {index} in {namespace}"),
                "category": "facts",
                "namespace": namespace,
            })
        };
        writeln!(file, "{}", serde_json::to_string(&value)?)
            .with_context(|| format!("write {}", path.display()))?;
        ids.push(id);
    }

    Ok(ids)
}

async fn memory_ids_for_namespace(
    client: &reqwest::Client,
    config: &Config,
    namespace: &str,
    limit: usize,
) -> Result<Vec<String>> {
    let response =
        search_memories(client, config, namespace, limit, Some(namespace), false).await?;
    Ok(memory_items(&response)
        .into_iter()
        .filter_map(|value| string_field(value, &["id", "memory_id", "memoryId"]))
        .collect())
}

async fn cleanup_memories(
    client: &reqwest::Client,
    config: &Config,
    found_ids: &[String],
    fallback_ids: &[String],
) -> Result<()> {
    let ids = found_ids
        .iter()
        .chain(fallback_ids)
        .collect::<BTreeSet<_>>();

    for id in ids {
        let url = format!(
            "{}/v1/memories/{}",
            config.base_url.trim_end_matches('/'),
            id
        );
        let response = client
            .delete(url)
            .bearer_auth(&config.api_key)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() && status.as_u16() != 404 {
            let body = response.text().await.unwrap_or_default();
            bail!("DELETE /v1/memories/{id} returned HTTP {status}: {body}");
        }
    }

    Ok(())
}

fn assert_all_ids_present(expected: &[String], found: &[String]) -> Result<()> {
    let found = found.iter().collect::<BTreeSet<_>>();
    let missing = expected
        .iter()
        .filter(|id| !found.contains(id))
        .take(10)
        .cloned()
        .collect::<Vec<_>>();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("missing imported IDs: {}", missing.join(", ")))
    }
}

fn memory_items(value: &Value) -> Vec<&Value> {
    array_items(value, &["memories", "results", "items", "data"])
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
        if let Some(field) = value.get(*key)
            && let Some(value) = field.as_str()
        {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn unique_namespace(kind: &str) -> String {
    format!(
        "mnemosctl-stress-{kind}-{}-{}",
        std::process::id(),
        unix_millis()
    )
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis()
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Result<Self> {
        let path =
            env::temp_dir().join(format!("{prefix}-{}-{}", std::process::id(), unix_millis()));
        fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
