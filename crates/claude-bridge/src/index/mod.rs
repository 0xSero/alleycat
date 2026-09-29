//! Bridge-side thread index.
//!
//! Wraps `bridge_core::ThreadIndex<ClaudeSessionRef>` and provides claude-
//! specific glue: hydration from `~/.claude/projects/<encoded-cwd>/<session_id>.jsonl`,
//! conversion of an [`IndexEntry`] into a wire `Thread`, and a [`ClaudeHydrator`]
//! used by the bridge to absorb pre-existing JSONL transcripts at startup.
//!
//! On-disk JSON layout is wire-compatible with the pre-refactor shape: each
//! row has the same `claudeSessionPath` / `claudeSessionId` fields at the top
//! level (via `#[serde(flatten)]` on [`ClaudeSessionRef`]).

pub mod claude_session_scan;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use claude_session_scan::{ClaudeSessionInfo, claude_projects_dir, list_all};

use alleycat_bridge_core::Hydrator;
pub use alleycat_bridge_core::{
    IndexEntry as CoreIndexEntry, ListFilter, ListPage, ListSort, ThreadIndex as CoreThreadIndex,
};
use alleycat_codex_proto::{Thread, ThreadSourceKind};

/// Bridge CLI version string baked into `Thread.cli_version`.
pub const CLI_VERSION: &str = concat!("alleycat-claude-bridge/", env!("CARGO_PKG_VERSION"));

/// Claude-specific metadata for an [`IndexEntry`]. Flattens into the row's top
/// level so the on-disk shape matches the pre-refactor `claudeSessionPath` /
/// `claudeSessionId` keys exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeSessionRef {
    /// Absolute path to the on-disk JSONL transcript (typically
    /// `~/.claude/projects/<encoded-cwd>/<thread_id>.jsonl`).
    pub claude_session_path: PathBuf,
    /// Claude session id (== `thread_id` in v1).
    pub claude_session_id: String,
}

/// Bridge-local alias so handler code reads `IndexEntry` instead of the
/// generic `bridge_core::IndexEntry<ClaudeSessionRef>`.
pub type IndexEntry = CoreIndexEntry<ClaudeSessionRef>;

/// Convert a [`ClaudeSessionInfo`] into a fresh index row.
pub fn entry_from_claude(info: &ClaudeSessionInfo) -> IndexEntry {
    IndexEntry {
        thread_id: info.session_id.clone(),
        cwd: info.cwd.clone(),
        created_at: info.created.timestamp_millis(),
        updated_at: info.modified.timestamp_millis(),
        archived: false,
        name: None,
        preview: info.first_message.clone(),
        forked_from_id: None,
        model_provider: "anthropic".to_string(),
        source: ThreadSourceKind::AppServer,
        metadata: ClaudeSessionRef {
            claude_session_path: info.path.clone(),
            claude_session_id: info.session_id.clone(),
        },
    }
}

/// Render an index row as a wire `Thread`.
pub fn entry_to_thread(entry: &IndexEntry) -> Thread {
    entry_to_thread_with_git_info(entry, alleycat_bridge_core::git_info_for_cwd(&entry.cwd))
}

pub fn entry_to_thread_with_git_info(
    entry: &IndexEntry,
    git_info: Option<alleycat_codex_proto::GitInfo>,
) -> Thread {
    entry.to_thread(
        entry.metadata.claude_session_id.clone(),
        Some(
            entry
                .metadata
                .claude_session_path
                .to_string_lossy()
                .into_owned(),
        ),
        CLI_VERSION,
        git_info,
    )
}

/// Hydrator that walks `~/.claude/projects/<encoded-cwd>/<session_id>.jsonl`
/// and produces fresh index rows for each session it finds.
pub struct ClaudeHydrator {
    /// Override directory; `None` uses [`claude_projects_dir`].
    pub override_dir: Option<PathBuf>,
}

impl ClaudeHydrator {
    pub fn new() -> Self {
        Self { override_dir: None }
    }

    pub fn with_override_dir(dir: PathBuf) -> Self {
        Self {
            override_dir: Some(dir),
        }
    }
}

impl Default for ClaudeHydrator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Hydrator<ClaudeSessionRef> for ClaudeHydrator {
    async fn scan(&self) -> Result<Vec<IndexEntry>> {
        let scanned = match self.override_dir.as_deref() {
            Some(dir) => claude_session_scan::list_sessions_from_dir(dir).await,
            None => list_all().await,
        };
        Ok(scanned.iter().map(entry_from_claude).collect())
    }
}

/// Convenience: open the index at `<codex_home>/threads.json` and hydrate from
/// `~/.claude/projects/`.
pub async fn open_and_hydrate(codex_home: &Path) -> Result<Arc<CoreThreadIndex<ClaudeSessionRef>>> {
    let path = codex_home.join("threads.json");
    let hydrator = ClaudeHydrator::new();
    CoreThreadIndex::open_and_hydrate(path, &hydrator).await
}

/// Compat shim. Today's daemon calls
/// `alleycat_claude_bridge::index::ThreadIndex::open_and_hydrate(&codex_home)`
/// and assigns the result to an `Arc<dyn ThreadIndexHandle<ClaudeSessionRef>>`.
/// The shim preserves the spelling so the daemon keeps compiling — it forwards
/// to [`CoreThreadIndex::open_and_hydrate`] with [`ClaudeHydrator`].
pub struct ThreadIndex;

impl ThreadIndex {
    pub async fn open_and_hydrate(
        codex_home: &Path,
    ) -> Result<Arc<CoreThreadIndex<ClaudeSessionRef>>> {
        open_and_hydrate(codex_home).await
    }

    pub async fn open(codex_home: &Path) -> Result<Arc<CoreThreadIndex<ClaudeSessionRef>>> {
        CoreThreadIndex::open_at(codex_home.join("threads.json")).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn entry(id: &str, cwd: &str, created: i64, updated: i64, archived: bool) -> IndexEntry {
        IndexEntry {
            thread_id: id.to_string(),
            cwd: cwd.to_string(),
            created_at: created,
            updated_at: updated,
            archived,
            name: None,
            preview: format!("preview {id}"),
            forked_from_id: None,
            model_provider: "anthropic".into(),
            source: ThreadSourceKind::AppServer,
            metadata: ClaudeSessionRef {
                claude_session_path: PathBuf::from(format!("/sessions/{id}.jsonl")),
                claude_session_id: id.to_string(),
            },
        }
    }

    #[tokio::test]
    async fn insert_then_lookup_roundtrips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("threads.json");
        let index = CoreThreadIndex::<ClaudeSessionRef>::open_at(path.clone())
            .await
            .unwrap();
        index
            .insert(entry("a", "/work", 100, 200, false))
            .await
            .unwrap();
        let row = index.lookup("a").await.unwrap();
        assert_eq!(row.cwd, "/work");

        drop(index);
        let reopened = CoreThreadIndex::<ClaudeSessionRef>::open_at(path)
            .await
            .unwrap();
        assert_eq!(
            reopened
                .lookup("a")
                .await
                .unwrap()
                .metadata
                .claude_session_id,
            "a"
        );
    }

    #[tokio::test]
    async fn on_disk_shape_uses_flat_camel_case_keys() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("threads.json");
        let index = CoreThreadIndex::<ClaudeSessionRef>::open_at(path.clone())
            .await
            .unwrap();
        index
            .insert(entry("abc", "/w", 100, 200, false))
            .await
            .unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        // Both legacy keys must appear at the row's top level.
        assert!(raw.contains("\"claudeSessionPath\""), "raw={raw}");
        assert!(raw.contains("\"claudeSessionId\""), "raw={raw}");
        assert!(raw.contains("\"threadId\""), "raw={raw}");
    }
}
