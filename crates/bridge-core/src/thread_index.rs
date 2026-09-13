//! Generic thread index, lifted from `pi-bridge`/`claude-bridge`.
//!
//! Both pi and claude bridges keep a JSON-backed catalogue of every codex
//! `Thread` they've seen. The storage layout, atomic write-tmp-rename
//! persistence, and CRUD surface were byte-identical between the two crates;
//! the only divergence was a pair of bridge-specific fields naming the
//! underlying source-of-truth (pi session JSONL path + id, or claude session
//! JSONL path + id). This module hoists everything except those bridge-
//! specific fields into a generic `IndexEntry<M>` and `ThreadIndex<M>`,
//! parameterized over a `Metadata` type that carries the bridge-shaped extras
//! and serializes flat into the on-disk row.
//!
//! `Metadata` is required to be `Serialize + DeserializeOwned + Send + Sync +
//! Clone + 'static`. Bridges pick a metadata type once
//! (`PiSessionRef`/`ClaudeSessionRef`) and never see a runtime type erasure
//! cost — `ThreadIndex<M>` is monomorphized.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_codex_proto::{SortDirection, ThreadSortKey, ThreadSourceKind};
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

/// One row in the index. Wire-compatible with the existing pi/claude
/// `IndexEntry` shapes when `M` flattens its fields with `#[serde(flatten)]`.
///
/// `cwd` is a `String` (not `PathBuf`) to match the existing on-disk JSON
/// shape — pi/claude both already serialize it that way and re-shaping here
/// would force a schema migration.
///
/// Timestamps are epoch-millis i64s for the same reason; converting to
/// `DateTime<Utc>` is the caller's job at the boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexEntry<M> {
    pub thread_id: String,
    pub cwd: String,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default)]
    pub archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub preview: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from_id: Option<String>,
    pub model_provider: String,
    pub source: ThreadSourceKind,
    /// Bridge-specific row data — pi uses `PiSessionRef { pi_session_path,
    /// pi_session_id }`, claude uses `ClaudeSessionRef { claude_session_path,
    /// claude_session_id }`. Implementations apply `#[serde(flatten)]` so the
    /// extras land at the row's top level and the on-disk JSON stays
    /// compatible with the pre-refactor shape.
    #[serde(flatten)]
    pub metadata: M,
}

/// Filter knobs accepted by `ThreadIndex::list`. Mirrors the codex
/// `ThreadListParams` subset the bridges actually filter on.
#[derive(Debug, Default, Clone)]
pub struct ListFilter {
    pub archived: Option<bool>,
    pub cwds: Option<Vec<String>>,
    pub search_term: Option<String>,
    pub model_providers: Option<Vec<String>>,
    pub source_kinds: Option<Vec<ThreadSourceKind>>,
}

/// Sort knobs. Defaults to `updated_at desc`, matching codex defaults.
#[derive(Debug, Clone, Copy)]
pub struct ListSort {
    pub key: ThreadSortKey,
    pub direction: SortDirection,
}

impl Default for ListSort {
    fn default() -> Self {
        Self {
            key: ThreadSortKey::UpdatedAt,
            direction: SortDirection::Desc,
        }
    }
}

/// One paginated page of entries — the generic peer of pi/claude's `ListPage`.
/// Bridges convert each `IndexEntry<M>` to a codex `Thread` themselves (the
/// `path` field is metadata-shaped and only the bridge knows how to spell it).
#[derive(Debug, Clone)]
pub struct ListPage<M> {
    pub data: Vec<IndexEntry<M>>,
    pub next_cursor: Option<String>,
}

/// Codex-rs `THREAD_LIST_DEFAULT_LIMIT`. Used when `ThreadListParams.limit`
/// is omitted.
pub const DEFAULT_LIST_LIMIT: u32 = 25;

/// Codex-rs `THREAD_LIST_MAX_LIMIT`. The handler clamps user-supplied
/// `limit` to this ceiling.
pub const MAX_LIST_LIMIT: u32 = 100;

/// Resolve `ThreadListParams.limit` to the same effective page size codex-rs
/// applies: 25 when omitted, clamped to [1, 100].
pub fn resolve_list_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT)
}

/// Encode a `backwards_cursor` for a list page anchor. Pass the first entry
/// of the current page; the returned string, when supplied as `cursor` on a
/// subsequent call with the opposite `SortDirection`, navigates back toward
/// (but past) this anchor.
pub fn encode_backwards_cursor<M>(entry: &IndexEntry<M>, sort: ListSort) -> String {
    encode_cursor(entry, sort)
}

#[derive(Debug, Serialize, Deserialize)]
struct OnDisk<M> {
    #[serde(default = "Vec::new", bound(deserialize = "M: DeserializeOwned"))]
    threads: Vec<IndexEntry<M>>,
}

impl<M> Default for OnDisk<M> {
    fn default() -> Self {
        Self {
            threads: Vec::new(),
        }
    }
}

/// Thread-safe, JSON-backed catalogue of `IndexEntry<M>` rows.
pub struct ThreadIndex<M> {
    storage_path: PathBuf,
    inner: RwLock<BTreeMap<String, IndexEntry<M>>>,
    /// Serializes `persist()` callers. Two concurrent inserts would otherwise
    /// race on the shared `<index>.tmp` filename; the mutex turns those races
    /// into FIFO writes. Readers (`list`, `lookup`, etc.) never take it.
    persist_lock: Mutex<()>,
}

impl<M> ThreadIndex<M>
where
    M: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Open the index at `path`. Creates the parent directory if missing and
    /// loads any existing rows. Does **not** hydrate from a bridge-specific
    /// scanner — call `open_and_hydrate` for that.
    pub async fn open_at(path: PathBuf) -> Result<Arc<Self>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating index dir {}", parent.display()))?;
        }
        let entries = match fs::read_to_string(&path).await {
            Ok(text) if !text.trim().is_empty() => {
                let parsed: OnDisk<M> = serde_json::from_str(&text)
                    .with_context(|| format!("parsing {}", path.display()))?;
                parsed
                    .threads
                    .into_iter()
                    .map(|e| (e.thread_id.clone(), e))
                    .collect::<BTreeMap<_, _>>()
            }
            _ => BTreeMap::new(),
        };
        Ok(Arc::new(Self {
            storage_path: path,
            inner: RwLock::new(entries),
            persist_lock: Mutex::new(()),
        }))
    }

    /// Convenience: open + hydrate from a bridge-supplied scanner.
    ///
    /// The hydrator returns the full set of rows it observed on disk; this
    /// helper inserts only the rows whose `thread_id` is not already present.
    /// Returns the opened index regardless of how many rows were added.
    pub async fn open_and_hydrate<H>(path: PathBuf, hydrator: &H) -> Result<Arc<Self>>
    where
        H: Hydrator<M> + ?Sized,
    {
        let index = Self::open_at(path).await?;
        index.hydrate_from(hydrator).await?;
        Ok(index)
    }

    /// Run a hydrator and absorb any rows whose `thread_id` we don't already
    /// know about. Returns the number of rows inserted.
    pub async fn hydrate_from<H>(&self, hydrator: &H) -> Result<usize>
    where
        H: Hydrator<M> + ?Sized,
    {
        let scanned = hydrator.scan().await?;
        if scanned.is_empty() {
            return Ok(0);
        }

        let mut to_insert = Vec::new();
        {
            let guard = self.inner.read().await;
            for entry in scanned {
                if !guard.contains_key(&entry.thread_id) {
                    to_insert.push(entry);
                }
            }
        }
        if to_insert.is_empty() {
            return Ok(0);
        }

        let added = to_insert.len();
        {
            let mut guard = self.inner.write().await;
            for entry in to_insert {
                guard.insert(entry.thread_id.clone(), entry);
            }
        }
        self.persist().await?;
        Ok(added)
    }

    /// Insert (or replace) an entry.
    pub async fn insert(&self, entry: IndexEntry<M>) -> Result<()> {
        {
            let mut guard = self.inner.write().await;
            guard.insert(entry.thread_id.clone(), entry);
        }
        self.persist().await
    }

    /// Update preview + `updated_at`. Silently no-ops on missing rows so
    /// callers can fire-and-forget from event handlers.
    pub async fn update_preview_and_updated_at(
        &self,
        thread_id: &str,
        preview: String,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        let changed = {
            let mut guard = self.inner.write().await;
            if let Some(row) = guard.get_mut(thread_id) {
                row.preview = preview;
                row.updated_at = updated_at.timestamp_millis();
                true
            } else {
                false
            }
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    /// Toggle the archive flag. Returns `true` if the row existed.
    pub async fn set_archived(&self, thread_id: &str, archived: bool) -> Result<bool> {
        let changed = {
            let mut guard = self.inner.write().await;
            match guard.get_mut(thread_id) {
                Some(row) => {
                    row.archived = archived;
                    true
                }
                None => false,
            }
        };
        if changed {
            self.persist().await?;
        }
        Ok(changed)
    }

    /// Set the user-defined name. Pass `None` (or a whitespace-only string)
    /// to clear it. Returns `true` if the row existed.
    pub async fn set_name(&self, thread_id: &str, name: Option<String>) -> Result<bool> {
        let changed = {
            let mut guard = self.inner.write().await;
            match guard.get_mut(thread_id) {
                Some(row) => {
                    row.name = name.and_then(|n| {
                        let trimmed = n.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    });
                    true
                }
                None => false,
            }
        };
        if changed {
            self.persist().await?;
        }
        Ok(changed)
    }

    /// Set `forked_from_id`. Used by hydrators that resolve fork chains
    /// lazily (pi-bridge does this after both parent and child rows land).
    /// Returns `true` if the row existed.
    pub async fn set_forked_from_id(
        &self,
        thread_id: &str,
        forked_from_id: Option<String>,
    ) -> Result<bool> {
        let changed = {
            let mut guard = self.inner.write().await;
            match guard.get_mut(thread_id) {
                Some(row) => {
                    row.forked_from_id = forked_from_id;
                    true
                }
                None => false,
            }
        };
        if changed {
            self.persist().await?;
        }
        Ok(changed)
    }

    /// Rewrite the provider label for every indexed row. Embedders that
    /// expose a scoped Pi runtime (for example Local Studio) use this after
    /// hydration so historical sessions keep the embedding runtime's
    /// identity instead of appearing as standalone Pi.
    pub async fn set_all_model_providers(&self, model_provider: &str) -> Result<()> {
        let changed = {
            let mut guard = self.inner.write().await;
            let mut changed = false;
            for row in guard.values_mut() {
                if row.model_provider != model_provider {
                    row.model_provider = model_provider.to_string();
                    changed = true;
                }
            }
            changed
        };
        if changed {
            self.persist().await?;
        }
        Ok(())
    }

    /// Look a row up by thread id.
    pub async fn lookup(&self, thread_id: &str) -> Option<IndexEntry<M>> {
        self.inner.read().await.get(thread_id).cloned()
    }

    /// Every thread id in the index, in undefined order.
    pub async fn loaded_thread_ids(&self) -> Vec<String> {
        self.inner.read().await.keys().cloned().collect()
    }

    /// Snapshot the entire index. Intended for hydrators that need the
    /// current state (e.g. fork-chain post-processing).
    pub async fn snapshot(&self) -> Vec<IndexEntry<M>> {
        self.inner.read().await.values().cloned().collect()
    }

    /// Paginated, filtered, sorted listing. Returns `IndexEntry<M>` rows;
    /// the bridge converts each to a codex `Thread` (its metadata holds the
    /// bridge-specific path that codex' `Thread.path` reflects).
    pub async fn list(
        &self,
        filter: &ListFilter,
        sort: ListSort,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ListPage<M>> {
        let snapshot: Vec<IndexEntry<M>> = self.inner.read().await.values().cloned().collect();
        let mut filtered: Vec<IndexEntry<M>> = snapshot
            .into_iter()
            .filter(|e| matches_filter(e, filter))
            .collect();

        sort_entries(&mut filtered, sort);

        let after = cursor
            .map(decode_cursor)
            .transpose()
            .context("invalid cursor")?;
        let starting = match after {
            Some(c) => filtered
                .iter()
                .position(|e| cursor_after(e, &c, sort))
                .unwrap_or(filtered.len()),
            None => 0,
        };

        let limit = limit.map(|l| l as usize).unwrap_or(filtered.len());
        let end = (starting + limit).min(filtered.len());
        let page = &filtered[starting..end];
        let next_cursor = if end < filtered.len() {
            Some(encode_cursor(&page[page.len() - 1], sort))
        } else {
            None
        };

        Ok(ListPage {
            data: page.to_vec(),
            next_cursor,
        })
    }

    async fn persist(&self) -> Result<()> {
        // FIFO writers — see `persist_lock` doc on the struct.
        let _guard = self.persist_lock.lock().await;
        let snapshot: Vec<IndexEntry<M>> = self.inner.read().await.values().cloned().collect();
        let payload = OnDisk { threads: snapshot };
        let serialized = serde_json::to_vec_pretty(&payload).context("serializing thread index")?;
        let tmp = self.storage_path.with_extension("json.tmp");
        fs::write(&tmp, &serialized)
            .await
            .with_context(|| format!("writing {}", tmp.display()))?;
        fs::rename(&tmp, &self.storage_path)
            .await
            .with_context(|| {
                format!(
                    "renaming {} -> {}",
                    tmp.display(),
                    self.storage_path.display()
                )
            })?;
        Ok(())
    }
}

/// Async handle the bridge handlers program against. Pi/claude both already
/// expose this shape (with their own `IndexEntry`); the generic version lets
/// handler crates take an `Arc<dyn ThreadIndexHandle<M>>` and stay
/// implementation-agnostic for tests.
#[async_trait::async_trait]
pub trait ThreadIndexHandle<M>: Send + Sync + 'static
where
    M: Send + Sync + 'static,
{
    async fn lookup(&self, thread_id: &str) -> Option<IndexEntry<M>>;
    async fn insert(&self, entry: IndexEntry<M>) -> Result<()>;
    async fn set_archived(&self, thread_id: &str, archived: bool) -> Result<bool>;
    async fn set_name(&self, thread_id: &str, name: Option<String>) -> Result<bool>;
    async fn update_preview_and_updated_at(
        &self,
        thread_id: &str,
        preview: String,
        updated_at: DateTime<Utc>,
    ) -> Result<()>;
    async fn list(
        &self,
        filter: &ListFilter,
        sort: ListSort,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ListPage<M>>;
    async fn loaded_thread_ids(&self) -> Vec<String>;
}

#[async_trait::async_trait]
impl<M> ThreadIndexHandle<M> for ThreadIndex<M>
where
    M: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    async fn lookup(&self, thread_id: &str) -> Option<IndexEntry<M>> {
        ThreadIndex::lookup(self, thread_id).await
    }
    async fn insert(&self, entry: IndexEntry<M>) -> Result<()> {
        ThreadIndex::insert(self, entry).await
    }
    async fn set_archived(&self, thread_id: &str, archived: bool) -> Result<bool> {
        ThreadIndex::set_archived(self, thread_id, archived).await
    }
    async fn set_name(&self, thread_id: &str, name: Option<String>) -> Result<bool> {
        ThreadIndex::set_name(self, thread_id, name).await
    }
    async fn update_preview_and_updated_at(
        &self,
        thread_id: &str,
        preview: String,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        ThreadIndex::update_preview_and_updated_at(self, thread_id, preview, updated_at).await
    }
    async fn list(
        &self,
        filter: &ListFilter,
        sort: ListSort,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ListPage<M>> {
        ThreadIndex::list(self, filter, sort, cursor, limit).await
    }
    async fn loaded_thread_ids(&self) -> Vec<String> {
        ThreadIndex::loaded_thread_ids(self).await
    }
}

/// Bridge-specific scanner that produces fresh `IndexEntry<M>` rows from
/// whatever filesystem layout the underlying agent uses (pi
/// `~/.pi/agent/sessions/`, claude `~/.claude/projects/`, etc.).
#[async_trait::async_trait]
pub trait Hydrator<M>: Send + Sync {
    async fn scan(&self) -> Result<Vec<IndexEntry<M>>>;
}

fn matches_filter<M>(entry: &IndexEntry<M>, filter: &ListFilter) -> bool {
    if filter.archived.is_some_and(|want| entry.archived != want) {
        return false;
    }
    if filter
        .cwds
        .as_ref()
        .is_some_and(|cwds| !cwds.iter().any(|c| c == &entry.cwd))
    {
        return false;
    }
    if filter
        .model_providers
        .as_ref()
        .is_some_and(|providers| !providers.iter().any(|p| p == &entry.model_provider))
    {
        return false;
    }
    if filter
        .source_kinds
        .as_ref()
        .is_some_and(|sources| !sources.contains(&entry.source))
    {
        return false;
    }
    if let Some(term) = filter.search_term.as_deref().filter(|t| !t.is_empty()) {
        let needle = term.to_lowercase();
        let in_name = entry
            .name
            .as_deref()
            .map(|n| n.to_lowercase().contains(&needle))
            .unwrap_or(false);
        let in_preview = entry.preview.to_lowercase().contains(&needle);
        if !in_name && !in_preview {
            return false;
        }
    }
    true
}

fn sort_entries<M>(entries: &mut [IndexEntry<M>], sort: ListSort) {
    entries.sort_by(|a, b| {
        let (ak, bk) = match sort.key {
            ThreadSortKey::CreatedAt => (a.created_at, b.created_at),
            ThreadSortKey::UpdatedAt => (a.updated_at, b.updated_at),
        };
        let primary = ak.cmp(&bk);
        let primary = if matches!(sort.direction, SortDirection::Desc) {
            primary.reverse()
        } else {
            primary
        };
        // Tiebreaker on thread_id keeps pagination deterministic across
        // entries whose timestamps collide.
        primary.then_with(|| a.thread_id.cmp(&b.thread_id))
    });
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorPayload {
    /// `created_at` or `updated_at` depending on the sort key.
    ts: i64,
    id: String,
}

fn encode_cursor<M>(entry: &IndexEntry<M>, sort: ListSort) -> String {
    let ts = match sort.key {
        ThreadSortKey::CreatedAt => entry.created_at,
        ThreadSortKey::UpdatedAt => entry.updated_at,
    };
    let payload = CursorPayload {
        ts,
        id: entry.thread_id.clone(),
    };
    let json = serde_json::to_vec(&payload).expect("CursorPayload always serializes");
    URL_SAFE_NO_PAD.encode(json)
}

fn decode_cursor(raw: &str) -> Result<CursorPayload> {
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .context("base64-decoding cursor")?;
    let payload: CursorPayload =
        serde_json::from_slice(&bytes).context("parsing cursor payload")?;
    Ok(payload)
}

fn cursor_after<M>(entry: &IndexEntry<M>, cursor: &CursorPayload, sort: ListSort) -> bool {
    let entry_ts = match sort.key {
        ThreadSortKey::CreatedAt => entry.created_at,
        ThreadSortKey::UpdatedAt => entry.updated_at,
    };
    let primary = entry_ts.cmp(&cursor.ts);
    let primary = if matches!(sort.direction, SortDirection::Desc) {
        primary.reverse()
    } else {
        primary
    };
    match primary {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => entry.thread_id.cmp(&cursor.id) == std::cmp::Ordering::Greater,
    }
}

/// `Path` convenience: open the index at `<dir>/threads.json`.
pub async fn open_in_dir<M>(dir: &Path) -> Result<Arc<ThreadIndex<M>>>
where
    M: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    ThreadIndex::open_at(dir.join("threads.json")).await
}
