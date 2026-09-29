//! Thread index — maps codex thread IDs to Hermes session IDs.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// A single binding: codex thread ↔ Hermes session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HermesBinding {
    pub thread_id: String,
    pub hermes_session_id: String,
    /// The native CLI assigns its own ID after the first one-shot turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_session_id: Option<String>,
    pub model: Option<String>,
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
    #[serde(default)]
    pub preview: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub forked_from_id: Option<String>,
    #[serde(default)]
    pub archived: bool,
}

/// Persisted index structure.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedIndex {
    bindings: Vec<HermesBinding>,
}

/// Thread-safe in-memory (optionally persisted) thread index.
pub struct ThreadIndex {
    path: Option<PathBuf>,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    by_thread: HashMap<String, HermesBinding>,
    by_session: HashMap<String, String>,
}

impl ThreadIndex {
    /// Create an in-memory index (no persistence).
    pub fn new_in_memory() -> Self {
        Self {
            path: None,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Open or create the index at `path`.
    #[allow(dead_code)]
    pub async fn open(path: PathBuf) -> anyhow::Result<Self> {
        Self::open_sync(path)
    }

    /// Synchronous open used by bridge construction.
    pub fn open_sync(path: PathBuf) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let persisted = match std::fs::read_to_string(&path) {
            Ok(text) if !text.trim().is_empty() => serde_json::from_str::<PersistedIndex>(&text)?,
            _ => PersistedIndex::default(),
        };
        let mut by_thread = HashMap::new();
        let mut by_session = HashMap::new();
        for mut binding in persisted.bindings {
            normalize_binding(&mut binding);
            let sid = binding.hermes_session_id.clone();
            let tid = binding.thread_id.clone();
            by_session.insert(sid, tid.clone());
            by_thread.insert(tid, binding);
        }
        Ok(Self {
            path: Some(path),
            inner: Mutex::new(Inner {
                by_thread,
                by_session,
            }),
        })
    }

    /// Insert or update a binding.
    pub fn upsert(&self, mut binding: HermesBinding) {
        normalize_binding(&mut binding);
        let mut inner = self.inner.lock().unwrap();
        if let Some(old) = inner.by_thread.remove(&binding.thread_id) {
            inner.by_session.remove(&old.hermes_session_id);
        }
        let sid = binding.hermes_session_id.clone();
        let tid = binding.thread_id.clone();
        inner.by_session.insert(sid, tid.clone());
        inner.by_thread.insert(tid, binding);
    }

    /// Look up a binding by codex thread id.
    pub fn get_by_thread(&self, thread_id: &str) -> Option<HermesBinding> {
        self.inner.lock().unwrap().by_thread.get(thread_id).cloned()
    }

    /// Look up a codex thread id by Hermes session id.
    #[allow(dead_code)]
    pub fn get_thread_id_by_session(&self, session_id: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .by_session
            .get(session_id)
            .cloned()
    }

    /// Remove a binding and return it.
    #[allow(dead_code)]
    pub fn remove(&self, thread_id: &str) -> Option<HermesBinding> {
        let mut inner = self.inner.lock().unwrap();
        let binding = inner.by_thread.remove(thread_id);
        if let Some(ref b) = binding {
            inner.by_session.remove(&b.hermes_session_id);
        }
        binding
    }

    pub fn all(&self) -> Vec<HermesBinding> {
        let mut bindings: Vec<_> = self
            .inner
            .lock()
            .unwrap()
            .by_thread
            .values()
            .cloned()
            .collect();
        bindings.sort_by_key(|binding| std::cmp::Reverse(binding.updated_at));
        bindings
    }

    /// All non-archived thread ids in the index.
    pub fn thread_ids(&self) -> Vec<String> {
        self.all()
            .into_iter()
            .filter(|binding| !binding.archived)
            .map(|binding| binding.thread_id)
            .collect()
    }

    pub fn set_archived(
        &self,
        thread_id: &str,
        archived: bool,
        updated_at: i64,
    ) -> Option<HermesBinding> {
        let mut inner = self.inner.lock().unwrap();
        let binding = inner.by_thread.get_mut(thread_id)?;
        binding.archived = archived;
        binding.updated_at = updated_at;
        Some(binding.clone())
    }

    pub fn set_name(
        &self,
        thread_id: &str,
        name: Option<String>,
        updated_at: i64,
    ) -> Option<HermesBinding> {
        let mut inner = self.inner.lock().unwrap();
        let binding = inner.by_thread.get_mut(thread_id)?;
        binding.name = name;
        binding.updated_at = updated_at;
        Some(binding.clone())
    }

    pub fn update_after_turn(
        &self,
        thread_id: &str,
        hermes_session_id: Option<String>,
        model: Option<String>,
        preview: Option<String>,
        cwd: Option<String>,
        updated_at: i64,
    ) -> Option<HermesBinding> {
        let mut inner = self.inner.lock().unwrap();
        let mut old_session_id = None;
        let mut new_session_id = None;
        let updated = {
            let binding = inner.by_thread.get_mut(thread_id)?;
            if let Some(session_id) = hermes_session_id {
                old_session_id = Some(std::mem::replace(
                    &mut binding.hermes_session_id,
                    session_id.clone(),
                ));
                new_session_id = Some(session_id);
            }
            if model.is_some() {
                binding.model = model;
            }
            if preview.as_deref().is_some_and(|value| !value.is_empty()) {
                binding.preview = preview;
            }
            if cwd.is_some() {
                binding.cwd = cwd;
            }
            binding.updated_at = updated_at;
            binding.clone()
        };
        if let Some(old) = old_session_id {
            inner.by_session.remove(&old);
        }
        if let Some(new) = new_session_id {
            inner.by_session.insert(new, thread_id.to_string());
        }
        Some(updated)
    }

    /// Persist to disk. In-memory indexes intentionally no-op.
    pub fn persist(&self) -> anyhow::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let inner = self.inner.lock().unwrap();
        let mut bindings: Vec<_> = inner.by_thread.values().cloned().collect();
        bindings.sort_by(|a, b| a.thread_id.cmp(&b.thread_id));
        let data = PersistedIndex { bindings };
        let json = serde_json::to_string_pretty(&data)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

fn normalize_binding(binding: &mut HermesBinding) {
    if binding.updated_at == 0 {
        binding.updated_at = binding.created_at;
    }
}
