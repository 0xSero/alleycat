use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alleycat_bridge_core::ProcessLauncher;
use alleycat_bridge_core::session::Session;
use alleycat_codex_proto::{
    ApprovalsReviewer, AskForApproval, InitializeCapabilities, JsonRpcMessage, SandboxMode,
    ThreadItem, Turn, TurnError, TurnStatus,
};
use serde_json::Value;

use crate::index::AmpSessionRef;

pub trait ThreadIndexHandle: alleycat_bridge_core::ThreadIndexHandle<AmpSessionRef> {}

impl<T> ThreadIndexHandle for T where
    T: alleycat_bridge_core::ThreadIndexHandle<AmpSessionRef> + ?Sized
{
}

pub use crate::index::{IndexEntry, ListFilter, ListPage, ListSort};
pub use alleycat_bridge_core::state::Capabilities;

pub struct ConnectionState {
    defaults: Mutex<ThreadDefaults>,
    session: Arc<Session>,
    thread_index: Arc<dyn ThreadIndexHandle>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    caches: Mutex<AmpCaches>,
    thread_logs: Mutex<HashMap<String, Vec<RecordedTurn>>>,
}

#[derive(Debug, Clone)]
pub struct RecordedTurn {
    pub turn_id: String,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub status: TurnStatus,
    pub error: Option<TurnError>,
    pub items: Vec<ThreadItem>,
}

#[derive(Debug, Clone, Default)]
pub struct ThreadDefaults {
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub approval_policy: Option<AskForApproval>,
    pub approvals_reviewer: Option<ApprovalsReviewer>,
    pub sandbox: Option<SandboxMode>,
    pub service_name: Option<String>,
    pub system_prompt: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AmpCaches {
    pub last_session_id: Option<String>,
    pub mcp_servers: Vec<(String, String)>,
    pub tools: Vec<String>,
}

impl ConnectionState {
    pub fn new(
        session: Arc<Session>,
        thread_index: Arc<dyn ThreadIndexHandle>,
        defaults: ThreadDefaults,
        launcher: Option<Arc<dyn ProcessLauncher>>,
    ) -> Self {
        Self {
            defaults: Mutex::new(defaults),
            session,
            thread_index,
            launcher,
            caches: Mutex::new(AmpCaches::default()),
            thread_logs: Mutex::new(HashMap::new()),
        }
    }

    pub fn launcher(&self) -> Option<&Arc<dyn ProcessLauncher>> {
        self.launcher.as_ref()
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub fn set_capabilities(
        &self,
        client_name: Option<String>,
        client_title: Option<String>,
        client_version: Option<String>,
        caps: Option<&InitializeCapabilities>,
    ) {
        let opt_out = caps
            .and_then(|c| c.opt_out_notification_methods.as_ref())
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();
        self.session.set_capabilities(Capabilities {
            experimental_api: caps.is_some_and(|c| c.experimental_api),
            opt_out_notification_methods: opt_out,
            client_name,
            client_title,
            client_version,
        });
    }

    pub fn should_emit(&self, method: &str) -> bool {
        self.session.should_emit(method)
    }

    pub fn defaults(&self) -> ThreadDefaults {
        self.defaults.lock().unwrap().clone()
    }

    pub fn update_defaults(&self, f: impl FnOnce(&mut ThreadDefaults)) {
        let mut slot = self.defaults.lock().unwrap();
        f(&mut slot);
    }

    pub fn send(&self, msg: JsonRpcMessage) -> Result<(), SendError> {
        match serde_json::to_value(&msg) {
            Ok(value) => {
                self.session.enqueue(value);
                Ok(())
            }
            Err(_) => Err(SendError::ConnectionClosed),
        }
    }

    pub fn thread_index(&self) -> &Arc<dyn ThreadIndexHandle> {
        &self.thread_index
    }

    pub fn caches(&self) -> AmpCaches {
        self.caches.lock().unwrap().clone()
    }

    pub fn refresh_init_cache(&self, value: &Value) {
        let mut slot = self.caches.lock().unwrap();
        slot.last_session_id = value
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(slot.last_session_id.take());
        slot.tools = value
            .get("tools")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        slot.mcp_servers = value
            .get("mcp_servers")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        Some((
                            item.get("name")?.as_str()?.to_string(),
                            item.get("status")
                                .and_then(Value::as_str)
                                .unwrap_or("connected")
                                .to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
    }

    pub fn record_turn_started(&self, thread_id: &str, turn_id: String, started_at: i64) {
        let mut logs = self.thread_logs.lock().unwrap();
        let list = logs.entry(thread_id.to_string()).or_default();
        list.push(RecordedTurn {
            turn_id,
            started_at,
            completed_at: None,
            status: TurnStatus::InProgress,
            error: None,
            items: Vec::new(),
        });
    }

    pub fn record_item(&self, thread_id: &str, turn_id: &str, item: ThreadItem) {
        let mut logs = self.thread_logs.lock().unwrap();
        let Some(list) = logs.get_mut(thread_id) else {
            return;
        };
        let Some(turn) = list.iter_mut().rev().find(|t| t.turn_id == turn_id) else {
            return;
        };
        let new_id = item.id().to_string();
        if let Some(idx) = turn
            .items
            .iter()
            .position(|existing| existing.id() == new_id)
        {
            turn.items[idx] = item;
        } else {
            turn.items.push(item);
        }
    }

    pub fn record_turn_completed(
        &self,
        thread_id: &str,
        turn_id: &str,
        completed_at: i64,
        status: TurnStatus,
        error: Option<TurnError>,
    ) {
        let mut logs = self.thread_logs.lock().unwrap();
        let Some(list) = logs.get_mut(thread_id) else {
            return;
        };
        if let Some(turn) = list.iter_mut().rev().find(|t| t.turn_id == turn_id) {
            turn.completed_at = Some(completed_at);
            turn.status = status;
            turn.error = error;
        }
    }

    pub fn thread_log(&self, thread_id: &str) -> Vec<Turn> {
        let logs = self.thread_logs.lock().unwrap();
        let Some(list) = logs.get(thread_id) else {
            return Vec::new();
        };
        list.iter()
            .map(|t| {
                let started_at = t.started_at;
                let completed_at = t.completed_at;
                let duration_ms = completed_at.map(|end| ((end - started_at) * 1000).max(0));
                Turn {
                    id: t.turn_id.clone(),
                    items: t.items.clone(),
                    items_view: alleycat_codex_proto::default_items_view(),
                    status: t.status,
                    error: t.error.clone(),
                    started_at: Some(started_at),
                    completed_at,
                    duration_ms,
                }
            })
            .collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("connection writer is closed")]
    ConnectionClosed,
}
