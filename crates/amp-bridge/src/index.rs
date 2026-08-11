use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use alleycat_bridge_core::Hydrator;
pub use alleycat_bridge_core::{
    IndexEntry as CoreIndexEntry, ListFilter, ListPage, ListSort, ThreadIndex as CoreThreadIndex,
};
use alleycat_codex_proto::{SessionSource, Thread, ThreadSourceKind, ThreadStatus};

pub const CLI_VERSION: &str = concat!("alleycat-amp-bridge/", env!("CARGO_PKG_VERSION"));
pub const INDEX_FILE_NAME: &str = "amp-threads.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AmpSessionRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amp_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amp_thread_path: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

pub type IndexEntry = CoreIndexEntry<AmpSessionRef>;

pub fn entry_to_thread(entry: &IndexEntry) -> Thread {
    Thread {
        id: entry.thread_id.clone(),
        session_id: entry
            .metadata
            .amp_thread_id
            .clone()
            .unwrap_or_else(|| entry.thread_id.clone()),
        forked_from_id: entry.forked_from_id.clone(),
        preview: entry.preview.clone(),
        ephemeral: false,
        model_provider: entry.model_provider.clone(),
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        status: ThreadStatus::NotLoaded,
        path: entry
            .metadata
            .amp_thread_path
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned()),
        cwd: entry.cwd.clone(),
        cli_version: CLI_VERSION.to_string(),
        source: source_kind_to_session_source(entry.source),
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        git_info: alleycat_bridge_core::git_info_for_cwd(&entry.cwd),
        name: entry.name.clone(),
        turns: Vec::new(),
    }
}

fn source_kind_to_session_source(kind: ThreadSourceKind) -> SessionSource {
    match kind {
        ThreadSourceKind::Cli => SessionSource::Cli,
        ThreadSourceKind::VsCode => SessionSource::VsCode,
        ThreadSourceKind::Exec => SessionSource::Exec,
        ThreadSourceKind::AppServer => SessionSource::AppServer,
        _ => SessionSource::AppServer,
    }
}

pub struct AmpHydrator {
    override_dir: Option<PathBuf>,
}

impl AmpHydrator {
    pub fn new() -> Self {
        Self { override_dir: None }
    }

    pub fn with_override_dir(dir: PathBuf) -> Self {
        Self {
            override_dir: Some(dir),
        }
    }
}

impl Default for AmpHydrator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Hydrator<AmpSessionRef> for AmpHydrator {
    async fn scan(&self) -> Result<Vec<IndexEntry>> {
        let Some(dir) = self.override_dir.clone().or_else(amp_threads_dir) else {
            return Ok(Vec::new());
        };
        Ok(scan_thread_dir(&dir).await)
    }
}

pub async fn open_and_hydrate(codex_home: &Path) -> Result<Arc<CoreThreadIndex<AmpSessionRef>>> {
    open_and_hydrate_with(codex_home, &AmpHydrator::new()).await
}

pub async fn open_and_hydrate_with<H>(
    codex_home: &Path,
    hydrator: &H,
) -> Result<Arc<CoreThreadIndex<AmpSessionRef>>>
where
    H: Hydrator<AmpSessionRef> + ?Sized,
{
    let index = CoreThreadIndex::open_at(codex_home.join(INDEX_FILE_NAME)).await?;
    hydrate_missing_amp_threads(&index, hydrator).await?;
    Ok(index)
}

async fn hydrate_missing_amp_threads<H>(
    index: &CoreThreadIndex<AmpSessionRef>,
    hydrator: &H,
) -> Result<usize>
where
    H: Hydrator<AmpSessionRef> + ?Sized,
{
    let scanned = hydrator.scan().await?;
    if scanned.is_empty() {
        return Ok(0);
    }

    let mut known_amp_sessions = HashSet::new();
    for entry in index.snapshot().await {
        known_amp_sessions.insert(entry.thread_id);
        if let Some(amp_thread_id) = entry.metadata.amp_thread_id {
            known_amp_sessions.insert(amp_thread_id);
        }
    }

    let mut added = 0;
    for entry in scanned {
        let amp_thread_id = entry.metadata.amp_thread_id.as_deref();
        if known_amp_sessions.contains(&entry.thread_id)
            || amp_thread_id.is_some_and(|id| known_amp_sessions.contains(id))
        {
            continue;
        }

        known_amp_sessions.insert(entry.thread_id.clone());
        if let Some(amp_thread_id) = &entry.metadata.amp_thread_id {
            known_amp_sessions.insert(amp_thread_id.clone());
        }
        index.insert(entry).await?;
        added += 1;
    }
    Ok(added)
}

pub fn amp_threads_dir() -> Option<PathBuf> {
    if let Some(env_dir) = std::env::var("AMP_THREADS_DIR")
        .ok()
        .filter(|value| !value.is_empty())
    {
        return Some(expand_tilde(&env_dir));
    }
    let home = directories::UserDirs::new()?.home_dir().to_path_buf();
    Some(
        home.join(".local")
            .join("share")
            .join("amp")
            .join("threads"),
    )
}

fn expand_tilde(input: &str) -> PathBuf {
    if input == "~"
        && let Some(home) = directories::UserDirs::new()
    {
        return home.home_dir().to_path_buf();
    }
    if let Some(rest) = input.strip_prefix("~/")
        && let Some(home) = directories::UserDirs::new()
    {
        return home.home_dir().join(rest);
    }
    PathBuf::from(input)
}

async fn scan_thread_dir(dir: &Path) -> Vec<IndexEntry> {
    let mut out = Vec::new();
    let mut read_dir = match tokio::fs::read_dir(dir).await {
        Ok(read_dir) => read_dir,
        Err(_) => return out,
    };
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        if let Some(row) = entry_from_thread_file(&path).await {
            out.push(row);
        }
    }
    out
}

async fn entry_from_thread_file(path: &Path) -> Option<IndexEntry> {
    let raw = tokio::fs::read_to_string(path).await.ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| path.file_stem().map(|s| s.to_string_lossy().into_owned()))?;
    let now = Utc::now().timestamp_millis();
    let created_at = value
        .get("created")
        .or_else(|| value.get("createdAt"))
        .and_then(value_to_millis)
        .unwrap_or(now);
    let updated_at = value
        .get("updatedAt")
        .or_else(|| value.get("updated"))
        .and_then(value_to_millis)
        .unwrap_or(created_at);
    let name = value
        .get("title")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    let preview = first_message_preview(&value)
        .or_else(|| name.clone())
        .unwrap_or_else(|| "(no messages)".to_string());
    let cwd = value
        .get("cwd")
        .or_else(|| value.get("workingDirectory"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(IndexEntry {
        thread_id: id.clone(),
        cwd,
        name,
        preview,
        created_at,
        updated_at,
        archived: false,
        forked_from_id: None,
        model_provider: "amp".to_string(),
        source: ThreadSourceKind::AppServer,
        metadata: AmpSessionRef {
            amp_thread_id: Some(id),
            amp_thread_path: Some(path.to_path_buf()),
            model: None,
            reasoning_effort: None,
        },
    })
}

fn value_to_millis(value: &Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(if n < 10_000_000_000 { n * 1000 } else { n });
    }
    let s = value.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn first_message_preview(value: &Value) -> Option<String> {
    let messages = value.get("messages")?.as_array()?;
    for message in messages {
        let content = message
            .get("content")
            .or_else(|| message.get("message").and_then(|m| m.get("content")))?;
        if let Some(text) = content.as_str() {
            let text = text.lines().next().unwrap_or("").trim();
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
        if let Some(blocks) = content.as_array() {
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("text")
                    && let Some(text) = block.get("text").and_then(Value::as_str)
                {
                    let text = text.lines().next().unwrap_or("").trim();
                    if !text.is_empty() {
                        return Some(text.to_string());
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn write_native_amp_thread(dir: &Path, id: &str, title: &str) -> PathBuf {
        let path = dir.join(format!("{id}.json"));
        tokio::fs::write(
            &path,
            json!({
                "id": id,
                "title": title,
                "created": "2026-05-27T09:00:00Z",
                "updatedAt": "2026-05-27T10:00:00Z",
                "cwd": "/tmp/project"
            })
            .to_string(),
        )
        .await
        .unwrap();
        path
    }

    #[tokio::test]
    async fn open_and_hydrate_imports_native_threads_into_amp_index() {
        let codex_home = tempfile::tempdir().unwrap();
        let amp_threads = tempfile::tempdir().unwrap();
        let native_path = write_native_amp_thread(
            amp_threads.path(),
            "native-amp-thread",
            "Fix mobile listing",
        )
        .await;

        let hydrator = AmpHydrator::with_override_dir(amp_threads.path().to_path_buf());
        let index = open_and_hydrate_with(codex_home.path(), &hydrator)
            .await
            .unwrap();

        let page = index
            .list(&ListFilter::default(), ListSort::default(), None, None)
            .await
            .unwrap();
        assert_eq!(page.data.len(), 1);
        let entry = &page.data[0];
        assert_eq!(entry.thread_id, "native-amp-thread");
        assert_eq!(entry.name.as_deref(), Some("Fix mobile listing"));
        assert_eq!(entry.cwd, "/tmp/project");
        assert_eq!(
            entry.metadata.amp_thread_id.as_deref(),
            Some("native-amp-thread")
        );
        assert_eq!(
            entry.metadata.amp_thread_path.as_deref(),
            Some(native_path.as_path())
        );
        assert!(codex_home.path().join(INDEX_FILE_NAME).exists());
        assert!(!codex_home.path().join("threads.json").exists());
    }

    #[tokio::test]
    async fn open_and_hydrate_skips_native_thread_when_amp_session_already_indexed() {
        let codex_home = tempfile::tempdir().unwrap();
        let amp_threads = tempfile::tempdir().unwrap();
        write_native_amp_thread(
            amp_threads.path(),
            "native-amp-thread",
            "Native duplicate title",
        )
        .await;

        let index =
            CoreThreadIndex::<AmpSessionRef>::open_at(codex_home.path().join(INDEX_FILE_NAME))
                .await
                .unwrap();
        index
            .insert(IndexEntry {
                thread_id: "app-thread".to_string(),
                cwd: "/tmp/project".to_string(),
                name: Some("Existing Alleycat title".to_string()),
                preview: "Existing Alleycat preview".to_string(),
                created_at: 1,
                updated_at: 2,
                archived: false,
                forked_from_id: None,
                model_provider: "amp".to_string(),
                source: ThreadSourceKind::AppServer,
                metadata: AmpSessionRef {
                    amp_thread_id: Some("native-amp-thread".to_string()),
                    amp_thread_path: Some(
                        codex_home.path().join("amp-transcripts/app-thread.jsonl"),
                    ),
                    model: None,
                    reasoning_effort: None,
                },
            })
            .await
            .unwrap();

        let hydrator = AmpHydrator::with_override_dir(amp_threads.path().to_path_buf());
        let hydrated = open_and_hydrate_with(codex_home.path(), &hydrator)
            .await
            .unwrap();

        let page = hydrated
            .list(&ListFilter::default(), ListSort::default(), None, None)
            .await
            .unwrap();
        assert_eq!(page.data.len(), 1);
        assert_eq!(page.data[0].thread_id, "app-thread");
        assert_eq!(
            page.data[0].name.as_deref(),
            Some("Existing Alleycat title")
        );
        assert_eq!(
            page.data[0].metadata.amp_thread_id.as_deref(),
            Some("native-amp-thread")
        );
    }
}
