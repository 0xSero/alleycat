use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpencodeBinding {
    pub thread_id: String,
    pub session_id: String,
    pub directory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub preview: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedIndex {
    bindings: Vec<OpencodeBinding>,
}

pub struct ThreadIndex {
    path: PathBuf,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    by_thread: HashMap<String, OpencodeBinding>,
    by_session: HashMap<String, String>,
}

impl ThreadIndex {
    pub async fn open(path: PathBuf) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let persisted = match tokio::fs::read_to_string(&path).await {
            Ok(raw) => serde_json::from_str::<PersistedIndex>(&raw).unwrap_or_default(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => PersistedIndex::default(),
            Err(error) => return Err(error.into()),
        };
        let mut inner = Inner::default();
        for binding in persisted.bindings {
            inner
                .by_session
                .insert(binding.session_id.clone(), binding.thread_id.clone());
            inner.by_thread.insert(binding.thread_id.clone(), binding);
        }
        Ok(Self {
            path,
            inner: Mutex::new(inner),
        })
    }

    pub async fn bind_session(
        &self,
        session: &serde_json::Value,
    ) -> anyhow::Result<OpencodeBinding> {
        let session_id = string_field(session, "id").unwrap_or_else(|| "unknown".to_string());
        let existing_thread_id = {
            self.inner
                .lock()
                .unwrap()
                .by_session
                .get(&session_id)
                .cloned()
        };
        if let Some(thread_id) = existing_thread_id
            && let Some(binding) = self
                .inner
                .lock()
                .unwrap()
                .by_thread
                .get(&thread_id)
                .cloned()
        {
            return Ok(binding);
        }
        let now = now_secs();
        let binding = OpencodeBinding {
            thread_id: codex_thread_id(&session_id),
            session_id,
            directory: string_field(session, "directory").unwrap_or_else(|| {
                std::env::current_dir()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            }),
            workspace_id: string_field(session, "workspaceID"),
            archived: session
                .pointer("/time/archived")
                .is_some_and(|value| !value.is_null()),
            name: string_field(session, "title"),
            created_at: session
                .pointer("/time/created")
                .and_then(|v| v.as_i64())
                .map(|v| v / 1000)
                .unwrap_or(now),
            updated_at: session
                .pointer("/time/updated")
                .and_then(|v| v.as_i64())
                .map(|v| v / 1000)
                .unwrap_or(now),
            preview: session
                .pointer("/summary/body")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        };
        self.insert(binding.clone()).await?;
        Ok(binding)
    }

    pub async fn insert(&self, binding: OpencodeBinding) -> anyhow::Result<()> {
        let persisted = {
            let mut inner = self.inner.lock().unwrap();
            inner
                .by_session
                .insert(binding.session_id.clone(), binding.thread_id.clone());
            inner.by_thread.insert(binding.thread_id.clone(), binding);
            PersistedIndex {
                bindings: inner.by_thread.values().cloned().collect(),
            }
        };
        tokio::fs::write(&self.path, serde_json::to_vec_pretty(&persisted)?).await?;
        Ok(())
    }

    pub fn by_thread(&self, thread_id: &str) -> Option<OpencodeBinding> {
        self.inner.lock().unwrap().by_thread.get(thread_id).cloned()
    }

    pub fn thread_for_session(&self, session_id: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .by_session
            .get(session_id)
            .cloned()
    }
}

fn string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
}

fn codex_thread_id(session_id: &str) -> String {
    let digest = Sha256::digest(session_id.as_bytes());
    let hex = hex::encode(digest);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
