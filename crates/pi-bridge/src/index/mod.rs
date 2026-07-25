//! Bridge-side thread index.
//!
//! This module is now a thin layer over [`alleycat_bridge_core::ThreadIndex<PiSessionRef>`].
//! The on-disk wire format is preserved exactly: rows live at
//! `<codex_home>/threads.json` with the pi-specific `piSessionPath` /
//! `piSessionId` fields flattened at the row's top level (matching the
//! pre-A2 shape).

pub mod pi_session_scan;

#[cfg(any(test, feature = "test-helpers"))]
pub mod testing;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::codex_proto::{SessionSource, Thread, ThreadSourceKind, ThreadStatus};

pub use pi_session_scan::{
    PiSessionInfo, list_all, list_sessions_from_dir, list_sessions_modified_since, pi_sessions_dir,
};

pub use alleycat_bridge_core::{ListFilter, ListPage, ListSort};

/// Bridge CLI version string baked into `Thread.cli_version`.
pub const CLI_VERSION: &str = concat!("alleycat-pi-bridge/", env!("CARGO_PKG_VERSION"));

/// Pi-specific row metadata. Flattened into the on-disk JSON so the row
/// shape matches today's wire format byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PiSessionRef {
    /// Absolute path to the pi JSONL session file.
    pub pi_session_path: PathBuf,
    /// Pi session id (the UUID inside the JSONL header).
    pub pi_session_id: String,
}

/// Row in the index. Generic alias over `bridge_core::IndexEntry` with the
/// pi-specific metadata shape. Bridges, handlers, and the daemon all share
/// this exact shape.
pub type IndexEntry = alleycat_bridge_core::IndexEntry<PiSessionRef>;

/// Compatibility newtype around `bridge_core::ThreadIndex<PiSessionRef>`
/// that re-exposes the pre-A2 inherent constructors (`open`, `open_at`,
/// `open_and_hydrate`) the daemon's `agents.rs:54` callsite still uses.
/// Implementations of `ThreadIndexHandle<PiSessionRef>` go through `Deref`
/// so handlers / tests treating an `Arc<ThreadIndex>` as
/// `Arc<dyn ThreadIndexHandle>` keep working.
///
/// Removed in A5 along with the rest of the compat surface.
#[repr(transparent)]
pub struct ThreadIndex(alleycat_bridge_core::ThreadIndex<PiSessionRef>);

impl ThreadIndex {
    /// Open the index at `<codex_home>/threads.json`, creating its parent
    /// directory if needed. Does **not** hydrate from pi.
    pub async fn open(codex_home: &Path) -> Result<Arc<Self>> {
        Self::open_at(codex_home.join("threads.json")).await
    }

    /// Variant of `open` that takes the threads.json path directly.
    pub async fn open_at(path: PathBuf) -> Result<Arc<Self>> {
        let inner = alleycat_bridge_core::ThreadIndex::<PiSessionRef>::open_at(path).await?;
        Ok(unsafe {
            // Safety: `ThreadIndex` is `#[repr(transparent)]` over
            // `bridge_core::ThreadIndex<PiSessionRef>`, so the `Arc` layout
            // is identical and we can transmute the pointer type.
            let raw = Arc::into_raw(inner) as *const alleycat_bridge_core::ThreadIndex<PiSessionRef>
                as *const Self;
            Arc::from_raw(raw)
        })
    }

    /// Open + hydrate from on-disk pi sessions and resolve fork chains.
    pub async fn open_and_hydrate(codex_home: &Path) -> Result<Arc<Self>> {
        Self::open_and_hydrate_with(codex_home, &PiHydrator::new()).await
    }

    /// Variant of `open_and_hydrate` that accepts an explicit hydrator.
    pub async fn open_and_hydrate_with(
        codex_home: &Path,
        hydrator: &PiHydrator,
    ) -> Result<Arc<Self>> {
        let path = codex_home.join("threads.json");
        let inner = alleycat_bridge_core::ThreadIndex::<PiSessionRef>::open_at(path).await?;

        // Step 1: scan and insert any rows we haven't seen before.
        let scanned = hydrator.scan_sessions().await;
        if !scanned.is_empty() {
            let known_paths: std::collections::HashSet<PathBuf> = inner
                .snapshot()
                .await
                .into_iter()
                .map(|e| e.metadata.pi_session_path)
                .collect();
            for info in &scanned {
                if known_paths.contains(&info.path) {
                    continue;
                }
                inner.insert(entry_from_pi(info)).await.with_context(|| {
                    format!("inserting hydrated row for {}", info.path.display())
                })?;
            }
        }

        // Step 2: resolve fork chains.
        let snapshot = inner.snapshot().await;
        let path_to_thread: BTreeMap<PathBuf, String> = snapshot
            .iter()
            .map(|e| (e.metadata.pi_session_path.clone(), e.thread_id.clone()))
            .collect();
        let mut updates: Vec<(String, String)> = Vec::new();
        for entry in &snapshot {
            if entry.forked_from_id.is_some() {
                continue;
            }
            let Some(parent_path) = scanned
                .iter()
                .find(|s| s.path == entry.metadata.pi_session_path)
                .and_then(|s| s.parent_session_path.as_ref())
            else {
                continue;
            };
            if let Some(parent_thread) = path_to_thread.get(parent_path) {
                updates.push((entry.thread_id.clone(), parent_thread.clone()));
            }
        }
        for (child, parent) in updates {
            inner.set_forked_from_id(&child, Some(parent)).await?;
        }

        Ok(unsafe {
            let raw = Arc::into_raw(inner) as *const alleycat_bridge_core::ThreadIndex<PiSessionRef>
                as *const Self;
            Arc::from_raw(raw)
        })
    }

    /// Access the underlying `bridge_core::ThreadIndex<PiSessionRef>`.
    pub fn inner(&self) -> &alleycat_bridge_core::ThreadIndex<PiSessionRef> {
        &self.0
    }

    /// Pre-A2 hydration helper: walk a pi sessions directory (defaults to
    /// `pi_sessions_dir()`) and insert any sessions not already in the
    /// index. Returns the number of rows added. Tests in `v3_resume.rs` /
    /// `v6_thread_list_filter.rs` still call this method directly; the
    /// shim re-exposes the same surface against the new generic index.
    pub async fn hydrate_from_pi_dir(&self, override_dir: Option<&Path>) -> Result<usize> {
        let hydrator = match override_dir {
            Some(dir) => PiHydrator::with_override(dir.to_path_buf()),
            None => PiHydrator::new(),
        };
        let scanned = hydrator.scan_sessions().await;
        if scanned.is_empty() {
            return Ok(0);
        }
        let known_paths: std::collections::HashSet<PathBuf> = self
            .0
            .snapshot()
            .await
            .into_iter()
            .map(|e| e.metadata.pi_session_path)
            .collect();
        let mut added = 0usize;
        let mut path_to_thread: BTreeMap<PathBuf, String> = self
            .0
            .snapshot()
            .await
            .into_iter()
            .map(|e| (e.metadata.pi_session_path.clone(), e.thread_id.clone()))
            .collect();
        for info in &scanned {
            if known_paths.contains(&info.path) {
                continue;
            }
            let entry = entry_from_pi(info);
            path_to_thread.insert(info.path.clone(), entry.thread_id.clone());
            self.0.insert(entry).await?;
            added += 1;
        }

        // Resolve fork chains.
        let snapshot = self.0.snapshot().await;
        let mut updates: Vec<(String, String)> = Vec::new();
        for entry in &snapshot {
            if entry.forked_from_id.is_some() {
                continue;
            }
            let Some(parent_path) = scanned
                .iter()
                .find(|s| s.path == entry.metadata.pi_session_path)
                .and_then(|s| s.parent_session_path.as_ref())
            else {
                continue;
            };
            if let Some(parent_thread) = path_to_thread.get(parent_path) {
                updates.push((entry.thread_id.clone(), parent_thread.clone()));
            }
        }
        for (child, parent) in updates {
            self.0.set_forked_from_id(&child, Some(parent)).await?;
        }
        Ok(added)
    }

    pub async fn hydrate_modified_since(
        &self,
        sessions_root: &Path,
        modified_after: std::time::SystemTime,
    ) -> Result<usize> {
        let scanned = list_sessions_modified_since(sessions_root, modified_after).await;
        if scanned.is_empty() {
            return Ok(0);
        }
        let known_paths: std::collections::HashSet<PathBuf> = self
            .0
            .snapshot()
            .await
            .into_iter()
            .map(|entry| entry.metadata.pi_session_path)
            .collect();
        let mut added = 0;
        for info in scanned {
            if known_paths.contains(&info.path) {
                continue;
            }
            self.0.insert(entry_from_pi(&info)).await?;
            added += 1;
        }
        Ok(added)
    }
}

impl std::ops::Deref for ThreadIndex {
    type Target = alleycat_bridge_core::ThreadIndex<PiSessionRef>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait::async_trait]
impl alleycat_bridge_core::ThreadIndexHandle<PiSessionRef> for ThreadIndex {
    async fn lookup(&self, thread_id: &str) -> Option<IndexEntry> {
        self.0.lookup(thread_id).await
    }
    async fn insert(&self, entry: IndexEntry) -> Result<()> {
        self.0.insert(entry).await
    }
    async fn set_archived(&self, thread_id: &str, archived: bool) -> Result<bool> {
        self.0.set_archived(thread_id, archived).await
    }
    async fn set_name(&self, thread_id: &str, name: Option<String>) -> Result<bool> {
        self.0.set_name(thread_id, name).await
    }
    async fn update_preview_and_updated_at(
        &self,
        thread_id: &str,
        preview: String,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.0
            .update_preview_and_updated_at(thread_id, preview, updated_at)
            .await
    }
    async fn list(
        &self,
        filter: &ListFilter,
        sort: ListSort,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ListPage<PiSessionRef>> {
        self.0.list(filter, sort, cursor, limit).await
    }
    async fn loaded_thread_ids(&self) -> Vec<String> {
        self.0.loaded_thread_ids().await
    }
}

/// Convert a freshly-scanned pi session into an `IndexEntry`, minting a
/// new thread id. Used during hydration.
pub fn entry_from_pi(info: &PiSessionInfo) -> IndexEntry {
    IndexEntry {
        thread_id: Uuid::now_v7().to_string(),
        cwd: info.cwd.clone(),
        created_at: info.created.timestamp_millis(),
        updated_at: info.modified.timestamp_millis(),
        archived: false,
        name: info.name.clone(),
        preview: info.first_message.clone(),
        forked_from_id: None,
        model_provider: "pi".to_string(),
        source: ThreadSourceKind::AppServer,
        metadata: PiSessionRef {
            pi_session_path: info.path.clone(),
            pi_session_id: info.id.clone(),
        },
    }
}

/// Fold an `IndexEntry` into a codex `Thread` with no turns populated.
/// `thread/read` fills `turns` separately when `include_turns` is set.
pub fn thread_from_entry(entry: &IndexEntry) -> Thread {
    Thread {
        id: entry.thread_id.clone(),
        session_id: entry.metadata.pi_session_id.clone(),
        forked_from_id: entry.forked_from_id.clone(),
        preview: entry.preview.clone(),
        ephemeral: false,
        model_provider: entry.model_provider.clone(),
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        status: ThreadStatus::NotLoaded,
        path: Some(
            entry
                .metadata
                .pi_session_path
                .to_string_lossy()
                .into_owned(),
        ),
        cwd: entry.cwd.clone(),
        cli_version: CLI_VERSION.to_string(),
        source: match entry.source {
            ThreadSourceKind::Cli => SessionSource::Cli,
            ThreadSourceKind::VsCode => SessionSource::VsCode,
            ThreadSourceKind::Exec => SessionSource::Exec,
            ThreadSourceKind::AppServer => SessionSource::AppServer,
            _ => SessionSource::AppServer,
        },
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        git_info: alleycat_bridge_core::git_info_for_cwd(&entry.cwd),
        name: entry.name.clone(),
        turns: Vec::new(),
    }
}

/// Pi-specific hydrator: walks `~/.pi/agent/sessions/` (or its env-var
/// override), inserts any sessions not already in the index, then resolves
/// fork chains in a second pass.
pub struct PiHydrator {
    /// When `Some`, the hydrator scans this directory instead of the
    /// default `pi_sessions_dir()`. Useful for tests with tempdir-backed
    /// session stores.
    pub override_dir: Option<PathBuf>,
    /// Already-scanned sessions. Used by remote launchers that need to obtain
    /// pi JSONL bytes over their transport but still want the bridge-owned
    /// session-list semantics and fork-chain repair.
    pub sessions: Option<Vec<PiSessionInfo>>,
}

impl PiHydrator {
    pub fn new() -> Self {
        Self {
            override_dir: None,
            sessions: None,
        }
    }

    pub fn with_override(dir: PathBuf) -> Self {
        Self {
            override_dir: Some(dir),
            sessions: None,
        }
    }

    pub fn with_sessions(sessions: Vec<PiSessionInfo>) -> Self {
        Self {
            override_dir: None,
            sessions: Some(sessions),
        }
    }

    /// Scan pi sessions directory and produce raw `PiSessionInfo` records.
    pub async fn scan_sessions(&self) -> Vec<PiSessionInfo> {
        if let Some(sessions) = &self.sessions {
            return sessions.clone();
        }
        match &self.override_dir {
            Some(dir) => {
                let mut all = Vec::new();
                let mut read_dir = match tokio::fs::read_dir(dir).await {
                    Ok(rd) => rd,
                    Err(_) => return all,
                };
                while let Ok(Some(entry)) = read_dir.next_entry().await {
                    let path = entry.path();
                    if entry
                        .file_type()
                        .await
                        .map(|ft| ft.is_dir())
                        .unwrap_or(false)
                    {
                        all.extend(list_sessions_from_dir(&path).await);
                    }
                }
                all
            }
            None => list_all().await,
        }
    }
}

impl Default for PiHydrator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl alleycat_bridge_core::Hydrator<PiSessionRef> for PiHydrator {
    async fn scan(&self) -> Result<Vec<IndexEntry>> {
        let scanned = self.scan_sessions().await;
        Ok(scanned.iter().map(entry_from_pi).collect())
    }
}

/// Free-function form of [`ThreadIndex::open_and_hydrate`]. Kept so newer
/// callers don't have to depend on the compatibility newtype.
pub async fn open_and_hydrate(codex_home: &Path) -> Result<Arc<ThreadIndex>> {
    ThreadIndex::open_and_hydrate(codex_home).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
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
            model_provider: "pi".to_string(),
            source: ThreadSourceKind::AppServer,
            metadata: PiSessionRef {
                pi_session_path: PathBuf::from(format!("/sessions/{id}.jsonl")),
                pi_session_id: format!("pi-{id}"),
            },
        }
    }

    #[tokio::test]
    async fn open_creates_parent_directory_and_starts_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nested/codex/threads.json");
        let index = ThreadIndex::open_at(path.clone()).await.unwrap();
        assert!(index.snapshot().await.is_empty());
        assert!(path.parent().unwrap().is_dir());
    }

    #[tokio::test]
    async fn insert_then_lookup_roundtrips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("threads.json");
        let index = ThreadIndex::open_at(path.clone()).await.unwrap();

        index
            .insert(entry("a", "/work", 100, 200, false))
            .await
            .unwrap();
        let row = index.lookup("a").await.unwrap();
        assert_eq!(row.cwd, "/work");
        assert!(path.exists());

        // Re-open from disk, the row survives.
        drop(index);
        let reopened = ThreadIndex::open_at(path).await.unwrap();
        assert_eq!(
            reopened.lookup("a").await.unwrap().metadata.pi_session_id,
            "pi-a"
        );
    }

    #[tokio::test]
    async fn hydrate_inserts_new_sessions_and_resolves_forks() {
        let dir = TempDir::new().unwrap();
        let pi_root = dir.path().join("sessions");
        let cwd_dir = pi_root.join("encoded");
        std::fs::create_dir_all(&cwd_dir).unwrap();

        let parent_path = cwd_dir.join("parent.jsonl");
        std::fs::write(
            &parent_path,
            r#"{"type":"session","version":3,"id":"parent","timestamp":"2026-04-27T10:00:00Z","cwd":"/p"}
"#,
        )
        .unwrap();
        let parent_path_str = parent_path.to_string_lossy().into_owned();
        let child_header = format!(
            r#"{{"type":"session","version":3,"id":"child","timestamp":"2026-04-27T11:00:00Z","cwd":"/p","parentSession":"{parent_path_str}"}}
"#
        );
        let child_path = cwd_dir.join("child.jsonl");
        let mut f = std::fs::File::create(&child_path).unwrap();
        f.write_all(child_header.as_bytes()).unwrap();
        drop(f);

        let codex_home = dir.path().to_path_buf();
        let hydrator = PiHydrator::with_override(pi_root.clone());
        let index = ThreadIndex::open_and_hydrate_with(&codex_home, &hydrator)
            .await
            .unwrap();
        let rows = index.snapshot().await;
        assert_eq!(rows.len(), 2);

        let parent_row = rows
            .iter()
            .find(|r| r.metadata.pi_session_id == "parent")
            .expect("parent");
        let child_row = rows
            .iter()
            .find(|r| r.metadata.pi_session_id == "child")
            .expect("child");
        assert_eq!(
            child_row.forked_from_id.as_deref(),
            Some(parent_row.thread_id.as_str())
        );

        // Re-running hydrate is idempotent.
        let reopened = ThreadIndex::open_and_hydrate_with(&codex_home, &hydrator)
            .await
            .unwrap();
        assert_eq!(reopened.snapshot().await.len(), 2);
    }

    #[tokio::test]
    async fn incremental_hydration_discovers_external_session_once() {
        let dir = TempDir::new().unwrap();
        let pi_root = dir.path().join("sessions");
        let cwd_dir = pi_root.join("encoded");
        std::fs::create_dir_all(&cwd_dir).unwrap();
        let index = ThreadIndex::open_at(dir.path().join("threads.json"))
            .await
            .unwrap();

        std::fs::write(
            cwd_dir.join("external.jsonl"),
            r#"{"type":"session","version":3,"id":"external","timestamp":"2026-07-24T23:59:00Z","cwd":"/shared"}
"#,
        )
        .unwrap();

        assert_eq!(
            index
                .hydrate_modified_since(&pi_root, std::time::SystemTime::UNIX_EPOCH)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            index
                .hydrate_modified_since(&pi_root, std::time::SystemTime::UNIX_EPOCH)
                .await
                .unwrap(),
            0
        );
        let rows = index.snapshot().await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].metadata.pi_session_id, "external");
    }

    #[tokio::test]
    async fn wire_format_round_trips_today_shape() {
        // Wire-compat regression: the on-disk JSON shape must be flat
        // (piSessionPath / piSessionId at the row's top level), not nested
        // under a `metadata` key.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("threads.json");
        let index = ThreadIndex::open_at(path.clone()).await.unwrap();
        index
            .insert(entry("a", "/work", 100, 200, false))
            .await
            .unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(
            raw.contains("\"piSessionPath\""),
            "expected flat camelCase key in {raw}"
        );
        assert!(
            raw.contains("\"piSessionId\""),
            "expected flat camelCase key in {raw}"
        );
        assert!(
            !raw.contains("\"metadata\""),
            "metadata must be flattened, not nested: {raw}"
        );
    }
}
