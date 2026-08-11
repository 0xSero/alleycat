//! `AcpBridge` — the unified `Bridge` impl for ACP-compliant agents.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::server::{Bridge, Conn};
use alleycat_bridge_core::{JsonRpcError, LocalLauncher, ProcessLauncher, error_codes};
use alleycat_codex_proto as p;
use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, info, instrument, warn};

use crate::handlers;
use crate::persistence::SessionPersistence;
use crate::pool::{AcpPool, PoolPolicy};

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, JsonRpcError> {
    serde_json::from_value(value).map_err(|err| invalid_params(err.to_string()))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|err| internal(err.to_string()))
}

fn invalid_params(msg: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: msg.into(),
        data: None,
    }
}

fn internal(msg: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: msg.into(),
        data: None,
    }
}

/// One completed turn worth of state, stored in memory (and optionally
/// persisted) so `thread/read` and `thread/resume` can return the full
/// history without re-asking the upstream agent.
///
/// The previous flat-`(role, content)` representation chunked entries
/// pairwise into turns, which silently broke the moment any tool call,
/// reasoning item, or plan landed between the user message and the
/// agent's reply. Storing each turn as a whole (id + ordered ThreadItem
/// JSON) keeps tool calls, reasoning, plans, and multi-segment agent
/// messages all in the same record without the bridge having to invent
/// a strict alternation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTurn {
    pub id: String,
    /// codex `ThreadItem` JSON values — exactly what we want to round-trip
    /// back through `thread/read` and `thread/resume`. Keeping it as
    /// `Value` (not a typed struct) means the bridge doesn't have to
    /// reimplement every variant of `ThreadItem`; the translator
    /// (`session_update_items`) produces these directly.
    pub items: Vec<Value>,
    /// codex `TurnStatus` string: "completed" | "interrupted" | "failed" | "inProgress".
    pub status: String,
    /// Unix millis when this turn started (`session/prompt` sent or
    /// session/load segment began).
    pub started_at_ms: i64,
    /// Unix millis when this turn completed. None for in-flight turns.
    pub completed_at_ms: Option<i64>,
    /// codex `TurnError` payload when status == "failed".
    #[serde(default)]
    pub error: Option<Value>,
}

/// Session status.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionStatus {
    Idle,
    Active,
}

/// Snapshot of an ACP session's mode list + currently-selected mode.
/// Each mode is the raw ACP `{id, name, ...}` object so the bridge
/// doesn't have to mirror every future ACP mode field.
#[derive(Debug, Clone, Default)]
pub struct ModesSnapshot {
    pub current: Option<String>,
    pub available: Vec<Value>,
}

/// Unified ACP bridge facade.
pub struct AcpBridge {
    pool: Arc<AcpPool>,
    /// All completed turns we've observed, keyed by codex thread/session
    /// id. The list is ordered oldest→newest so `thread/read` can emit
    /// turns in chronological order without re-sorting.
    turns: DashMap<String, Vec<StoredTurn>>,
    /// Session status per thread ID
    session_status: DashMap<String, SessionStatus>,
    /// Latest ACP `available_commands_update` payload, keyed by ACP
    /// session id. Served back via `skills/list`. ACP commands tend to
    /// be agent-wide rather than truly per-session, but the spec
    /// scopes them per-session so we honor that scoping.
    available_commands: DashMap<String, Vec<Value>>,
    /// Latest model list extracted from ACP `session/new` configOptions
    /// (devin advertises every model the agent can switch to). Keyed by
    /// session id; aggregated across sessions for `model/list`.
    models: DashMap<String, Vec<Value>>,
    /// Latest mode list + current mode id per session. Captured from
    /// `session/new` (`modes.availableModes`/`modes.currentModeId`) plus
    /// any `current_mode_update` notifications during turns.
    modes: DashMap<String, ModesSnapshot>,
    /// Client-renamed session titles. ACP has no server-side rename
    /// method (devin advertises `cognition.ai/sessionRename` only as a
    /// capability signal — there's no matching JSON-RPC call), so
    /// thread names are tracked locally and echoed back via
    /// `thread/name/updated` so iOS's display stays consistent.
    thread_titles: DashMap<String, String>,
    /// Optional session persistence manager
    persistence: Option<SessionPersistence>,
    /// JoinHandle for the background pool-eviction task so we can abort
    /// it on `shutdown()` instead of relying on tokio runtime drop. Held
    /// in a `Mutex<Option<…>>` so `shutdown` can take it.
    eviction_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for AcpBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpBridge")
            .field("threads_with_turns", &self.turns.len())
            .field("sessions", &self.session_status.len())
            .field("persistence_enabled", &self.persistence.is_some())
            .finish_non_exhaustive()
    }
}

impl AcpBridge {
    pub fn builder() -> AcpBridgeBuilder {
        AcpBridgeBuilder::default()
    }

    /// Ensure an ACP client exists for the given session, creating one if needed.
    ///
    /// Delegates straight to the pool — the previous implementation kept a
    /// separate `per_conn` `Arc` cache, but pool eviction (5-min idle TTL,
    /// or daemon shutdown) only removes the pool entry, not the cached
    /// `Arc`. After eviction the cache still handed out the *dead* client
    /// and subsequent `send_request`s tore down on broken-pipe, causing the
    /// iOS stream to close the moment it tried to `initialize`. The pool
    /// already dedupes by `session_key` and lazily respawns on miss, so
    /// the cache wasn't buying anything but a footgun.
    #[instrument(skip(self), fields(session_key = %session_key))]
    pub async fn ensure_client(
        &self,
        session_key: &str,
    ) -> Result<Arc<crate::acp_client::AcpClient>> {
        self.pool.get_client(session_key).await
    }

    /// Append a fully-assembled turn to a thread's history. The bridge
    /// expects callers to construct the entire `StoredTurn` before
    /// handing it in — translation from ACP `session/update` →
    /// `ThreadItem` lives in `handlers::session_update_items`, not here.
    #[instrument(skip(self, turn), fields(session_id = %session_id, turn_id = %turn.id, items = turn.items.len()))]
    pub fn append_turn(&self, session_id: &str, turn: StoredTurn) {
        self.turns
            .entry(session_id.to_string())
            .or_default()
            .push(turn);
        debug!(
            "Appended turn (total turns: {})",
            self.turns.get(session_id).map(|v| v.len()).unwrap_or(0)
        );
    }

    /// Read the full turn history for a thread, falling back to disk
    /// persistence if memory is empty.
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub fn get_turns(&self, session_id: &str) -> Vec<StoredTurn> {
        if let Some(turns) = self.turns.get(session_id) {
            debug!("Retrieved turns from memory ({} turns)", turns.len());
            return turns.clone();
        }

        if let Some(ref persistence) = self.persistence
            && let Ok(Some(disk_turns)) = persistence.load_session(session_id)
        {
            debug!("Retrieved turns from disk ({} turns)", disk_turns.len());
            self.turns
                .insert(session_id.to_string(), disk_turns.clone());
            return disk_turns;
        }

        debug!("No turn history found for session");
        Vec::new()
    }

    /// Replace the entire turn history for a thread (used when
    /// `session/load` streams a fresh history from the agent).
    #[instrument(skip(self, turns), fields(session_id = %session_id, count = turns.len()))]
    pub fn set_turns(&self, session_id: &str, turns: Vec<StoredTurn>) {
        debug!("Replacing turn history with {} turns", turns.len());
        self.turns.insert(session_id.to_string(), turns);
    }

    /// Clear all stored turns for a thread.
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub fn clear_turns(&self, session_id: &str) {
        self.turns.remove(session_id);

        if let Some(ref persistence) = self.persistence {
            let _ = persistence.delete_session(session_id);
        }

        info!("Cleared turn history for session");
    }

    /// Persist current turn history to disk if persistence is enabled.
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub fn save_turns(&self, session_id: &str) {
        if let Some(ref persistence) = self.persistence
            && let Some(turns) = self.turns.get(session_id)
        {
            let status = match self.get_session_status(session_id) {
                SessionStatus::Idle => "Idle",
                SessionStatus::Active => "Active",
            };
            let _ = persistence.save_session(session_id, &turns, status);
        }
    }

    /// Cache the most-recent `available_commands` list for a session.
    /// Each ACP `available_commands_update` notification overwrites the
    /// previous snapshot — the list is small and the agent sends it in
    /// full each time.
    #[instrument(skip(self, commands), fields(session_id = %session_id, count = commands.len()))]
    pub fn set_available_commands(&self, session_id: &str, commands: Vec<Value>) {
        debug!("Caching {} available commands", commands.len());
        self.available_commands
            .insert(session_id.to_string(), commands);
    }

    /// Return the union of cached `available_commands` across all
    /// sessions, deduped by command name. `skills/list` calls this:
    /// codex's request shape is keyed by `cwds[]`, not session id, so
    /// we can't scope the response — return everything we have.
    pub fn all_available_commands(&self) -> Vec<Value> {
        let mut seen = std::collections::HashSet::<String>::new();
        let mut out = Vec::new();
        for entry in self.available_commands.iter() {
            for cmd in entry.value().iter() {
                let name = cmd
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if name.is_empty() || seen.insert(name) {
                    out.push(cmd.clone());
                }
            }
        }
        out
    }

    /// Cache the most-recent model catalog seen for a session.
    #[instrument(skip(self, models), fields(session_id = %session_id, count = models.len()))]
    pub fn set_models(&self, session_id: &str, models: Vec<Value>) {
        debug!("Caching {} models", models.len());
        self.models.insert(session_id.to_string(), models);
    }

    /// Union of cached models across all sessions, deduped by `value`
    /// (ACP) / `id` (codex). codex `model/list` has no per-session
    /// scoping, so we hand back everything we know about.
    pub fn all_models(&self) -> Vec<Value> {
        let mut seen = std::collections::HashSet::<String>::new();
        let mut out = Vec::new();
        for entry in self.models.iter() {
            for m in entry.value().iter() {
                let key = m
                    .get("value")
                    .or_else(|| m.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if key.is_empty() || seen.insert(key) {
                    out.push(m.clone());
                }
            }
        }
        out
    }

    /// Cache the mode snapshot for a session (from session/new or a
    /// `current_mode_update`).
    #[instrument(skip(self, snapshot), fields(session_id = %session_id, current = ?snapshot.current, count = snapshot.available.len()))]
    pub fn set_modes(&self, session_id: &str, snapshot: ModesSnapshot) {
        debug!(
            "Caching modes (current={:?}, available={})",
            snapshot.current,
            snapshot.available.len()
        );
        self.modes.insert(session_id.to_string(), snapshot);
    }

    /// Update only the `current` mode id for a session, leaving the
    /// available list untouched. Used by ACP `current_mode_update`
    /// notifications which carry only the new mode id.
    pub fn set_current_mode(&self, session_id: &str, current: String) {
        let mut entry = self.modes.entry(session_id.to_string()).or_default();
        entry.current = Some(current);
    }

    /// Record a client-set title for a thread.
    pub fn set_thread_title(&self, thread_id: &str, title: &str) {
        self.thread_titles
            .insert(thread_id.to_string(), title.to_string());
    }

    /// Fetch the client-set title for a thread, if any.
    pub fn get_thread_title(&self, thread_id: &str) -> Option<String> {
        self.thread_titles.get(thread_id).map(|t| t.clone())
    }

    /// Union of cached available modes across sessions, deduped by id.
    pub fn all_modes(&self) -> Vec<Value> {
        let mut seen = std::collections::HashSet::<String>::new();
        let mut out = Vec::new();
        for entry in self.modes.iter() {
            for m in entry.value().available.iter() {
                let id = m
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if id.is_empty() || seen.insert(id) {
                    out.push(m.clone());
                }
            }
        }
        out
    }

    /// Emit a thread/name/updated notification.
    pub fn emit_thread_name_updated(&self, ctx: &Conn, thread_id: &str, name: &str) {
        let notification = json!({
            "threadId": thread_id,
            "threadName": name,
        });
        let _ = ctx
            .notifier()
            .send_notification("thread/name/updated", notification);
    }

    /// Set session status and emit thread/status/changed notification if it changed.
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id, new_status = ?new_status))]
    pub fn set_session_status(&self, ctx: &Conn, thread_id: &str, new_status: SessionStatus) {
        let old_status = self
            .session_status
            .entry(thread_id.to_string())
            .or_insert(SessionStatus::Idle)
            .clone();

        if old_status != new_status {
            self.session_status
                .insert(thread_id.to_string(), new_status.clone());

            let status = match new_status {
                SessionStatus::Idle => json!({
                    "type": "idle"
                }),
                SessionStatus::Active => json!({
                    "type": "active",
                    "activeFlags": []
                }),
            };

            info!(
                "Session status changed: {:?} -> {:?}",
                old_status, new_status
            );

            let notification = json!({
                "threadId": thread_id,
                "status": status,
            });
            let _ = ctx
                .notifier()
                .send_notification("thread/status/changed", notification);
        }
    }

    /// Get session status for a thread.
    #[instrument(skip(self), fields(thread_id = %thread_id))]
    pub fn get_session_status(&self, thread_id: &str) -> SessionStatus {
        let status = self
            .session_status
            .get(thread_id)
            .map(|v| v.clone())
            .unwrap_or(SessionStatus::Idle);
        debug!("Session status: {:?}", status);
        status
    }

    /// Emit a warning notification.
    #[instrument(skip(self, ctx), fields(message = %message))]
    pub fn emit_warning(&self, ctx: &Conn, message: &str) {
        warn!("Emitting warning: {}", message);
        let notification = json!({
            "threadId": null,
            "message": message,
        });
        let _ = ctx.notifier().send_notification("warning", notification);
    }

    /// Emit a warning notification for a specific thread.
    #[instrument(skip(self, ctx), fields(thread_id = %thread_id, message = %message))]
    pub fn emit_thread_warning(&self, ctx: &Conn, thread_id: &str, message: &str) {
        warn!("Emitting warning for thread {}: {}", thread_id, message);
        let notification = json!({
            "threadId": thread_id,
            "message": message,
        });
        let _ = ctx.notifier().send_notification("warning", notification);
    }
}

/// Builder for AcpBridge.
#[derive(Default)]
pub struct AcpBridgeBuilder {
    agent_bin: Option<PathBuf>,
    agent_args: Option<Vec<String>>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    pool_capacity: Option<usize>,
    idle_ttl: Option<Duration>,
    request_timeout: Option<Duration>,
    max_retries: Option<usize>,
    retry_backoff: Option<Duration>,
    state_dir: Option<PathBuf>,
    enable_persistence: bool,
}

impl AcpBridgeBuilder {
    pub fn agent_bin(mut self, bin: impl Into<PathBuf>) -> Self {
        self.agent_bin = Some(bin.into());
        self
    }

    pub fn agent_args(mut self, args: Vec<String>) -> Self {
        self.agent_args = Some(args);
        self
    }

    pub fn launcher(mut self, launcher: Arc<dyn ProcessLauncher>) -> Self {
        self.launcher = Some(launcher);
        self
    }

    pub fn pool_capacity(mut self, n: usize) -> Self {
        self.pool_capacity = Some(n);
        self
    }

    pub fn idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = Some(ttl);
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    pub fn max_retries(mut self, max: usize) -> Self {
        self.max_retries = Some(max);
        self
    }

    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = Some(backoff);
        self
    }

    pub fn state_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.state_dir = Some(dir.into());
        self
    }

    pub fn enable_persistence(mut self, enabled: bool) -> Self {
        self.enable_persistence = enabled;
        self
    }

    /// Populate fields from environment variables. Reads:
    /// - `ACP_BRIDGE_AGENT_BIN` for the agent binary path
    /// - `ACP_BRIDGE_AGENT_ARGS` for the agent arguments (space-separated)
    /// - `ACP_BRIDGE_STATE_DIR` for the state directory
    /// - `ACP_BRIDGE_POOL_CAPACITY` for the pool capacity
    /// - `ACP_BRIDGE_IDLE_TTL_SECS` for the idle TTL
    /// - `ACP_BRIDGE_REQUEST_TIMEOUT_SECS` for request timeout
    /// - `ACP_BRIDGE_MAX_RETRIES` for max retries
    /// - `ACP_BRIDGE_RETRY_BACKOFF_MS` for retry backoff
    ///
    /// Builder-set values stay; env vars only fill in fields the caller
    /// hasn't already set explicitly.
    pub fn from_env(mut self) -> Self {
        if self.agent_bin.is_none()
            && let Some(bin) = std::env::var_os("ACP_BRIDGE_AGENT_BIN")
        {
            self.agent_bin = Some(PathBuf::from(bin));
        }
        if self.agent_args.is_none()
            && let Ok(args_str) = std::env::var("ACP_BRIDGE_AGENT_ARGS")
        {
            self.agent_args = Some(args_str.split_whitespace().map(|s| s.to_string()).collect());
        }
        if self.state_dir.is_none()
            && let Some(state_dir) = std::env::var_os("ACP_BRIDGE_STATE_DIR")
        {
            self.state_dir = Some(PathBuf::from(state_dir));
        }
        if self.pool_capacity.is_none()
            && let Ok(cap) = std::env::var("ACP_BRIDGE_POOL_CAPACITY")
            && let Ok(capacity) = cap.parse::<usize>()
        {
            self.pool_capacity = Some(capacity);
        }
        if self.idle_ttl.is_none()
            && let Ok(ttl) = std::env::var("ACP_BRIDGE_IDLE_TTL_SECS")
            && let Ok(secs) = ttl.parse::<u64>()
        {
            self.idle_ttl = Some(Duration::from_secs(secs));
        }
        if self.request_timeout.is_none()
            && let Ok(timeout) = std::env::var("ACP_BRIDGE_REQUEST_TIMEOUT_SECS")
            && let Ok(secs) = timeout.parse::<u64>()
        {
            self.request_timeout = Some(Duration::from_secs(secs));
        }
        if self.max_retries.is_none()
            && let Ok(retries) = std::env::var("ACP_BRIDGE_MAX_RETRIES")
            && let Ok(max) = retries.parse::<usize>()
        {
            self.max_retries = Some(max);
        }
        if self.retry_backoff.is_none()
            && let Ok(backoff) = std::env::var("ACP_BRIDGE_RETRY_BACKOFF_MS")
            && let Ok(ms) = backoff.parse::<u64>()
        {
            self.retry_backoff = Some(Duration::from_millis(ms));
        }
        self
    }

    pub async fn build(self) -> Result<Arc<AcpBridge>> {
        let agent_bin = self.agent_bin.unwrap_or_else(|| PathBuf::from("devin"));
        let agent_args = self.agent_args.unwrap_or_else(|| vec!["acp".to_string()]);
        let launcher: Arc<dyn ProcessLauncher> = self
            .launcher
            .unwrap_or_else(|| Arc::new(LocalLauncher) as Arc<dyn ProcessLauncher>);

        let config = crate::config::AcpBridgeConfig {
            agent_bin,
            agent_args,
            state_dir: self.state_dir.clone(),
            pool_capacity: self.pool_capacity,
            idle_ttl_secs: self.idle_ttl.map(|d| d.as_secs()),
            request_timeout_secs: self.request_timeout.map(|d| d.as_secs()),
            max_retries: self.max_retries,
            retry_backoff_ms: self.retry_backoff.map(|d| d.as_millis() as u64),
        }
        .from_env();

        // Extract state_dir before moving config
        let state_dir_for_persistence = config.state_dir.clone();

        let policy = PoolPolicy {
            max_processes: config
                .pool_capacity
                .unwrap_or(crate::pool::DEFAULT_MAX_PROCESSES),
            idle_ttl: config
                .idle_ttl_secs
                .map(Duration::from_secs)
                .unwrap_or(crate::pool::DEFAULT_IDLE_TTL),
        };

        let pool = Arc::new(AcpPool::new(config, launcher, policy));

        // Initialize persistence if enabled
        let persistence = if self.enable_persistence {
            let state_dir = state_dir_for_persistence.unwrap_or_else(|| {
                std::env::var("HOME")
                    .map(|home| PathBuf::from(home).join(".alleycat-acp-bridge"))
                    .unwrap_or_else(|_| PathBuf::from("/tmp/alleycat-acp-bridge"))
            });

            match SessionPersistence::new(state_dir) {
                Ok(p) => {
                    info!("Session persistence enabled");
                    Some(p)
                }
                Err(e) => {
                    warn!("Failed to initialize session persistence: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Start background eviction task. Retain the JoinHandle so
        // `AcpBridge::shutdown()` can abort it; previously the handle was
        // dropped immediately and the task ran forever (which doesn't
        // matter much, but it also kept an `Arc<AcpPool>` clone alive and
        // delayed pool drop on shutdown).
        let pool_clone = Arc::clone(&pool);
        let eviction_handle = pool_clone.start_eviction_task();

        Ok(Arc::new(AcpBridge {
            pool,
            turns: DashMap::new(),
            session_status: DashMap::new(),
            available_commands: DashMap::new(),
            models: DashMap::new(),
            modes: DashMap::new(),
            thread_titles: DashMap::new(),
            persistence,
            eviction_handle: std::sync::Mutex::new(Some(eviction_handle)),
        }))
    }
}

impl AcpBridge {
    /// Tear down background work and kill every pooled ACP child. Called
    /// during daemon shutdown so we don't leak `devin acp` (or other
    /// agent) child processes between restarts.
    pub async fn shutdown(&self) {
        if let Some(handle) = self.eviction_handle.lock().unwrap().take() {
            handle.abort();
        }
        self.pool.shutdown().await;
    }
}

#[async_trait]
impl Bridge for AcpBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let session = ctx.session();
        let session_key = format!("{}:{}", session.agent, session.node_id);
        let client = self
            .ensure_client(&session_key)
            .await
            .map_err(|e| JsonRpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("Failed to create ACP client: {}", e),
                data: None,
            })?;

        handlers::handle_initialize(&client, params).await
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        debug!("Dispatching method: {}", method);

        let session = ctx.session();
        let session_key = format!("{}:{}", session.agent, session.node_id);
        let client = self
            .ensure_client(&session_key)
            .await
            .map_err(|e| JsonRpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("Failed to get ACP client: {}", e),
                data: None,
            })?;

        match method {
            // Read-only operations
            "account/read" => {
                let typed: p::GetAccountParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                to_value(handlers::handle_account_read(typed))
            }
            "account/rateLimits/read" => to_value(handlers::handle_account_rate_limits_read()),
            "config/read" => {
                let typed: p::ConfigReadParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                to_value(handlers::handle_config_read(typed))
            }
            "configRequirements/read" => to_value(handlers::handle_config_requirements_read()),
            "model/list" => {
                let typed: p::ModelListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                to_value(handlers::handle_model_list(
                    self,
                    ctx.session().agent,
                    typed,
                ))
            }
            "experimentalFeature/list" => to_value(handlers::handle_experimental_feature_list()),
            "collaborationMode/list" => to_value(handlers::handle_collaboration_mode_list(self)),
            "mcpServerStatus/list" => {
                let typed: p::ListMcpServerStatusParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                to_value(handlers::handle_mcp_server_status_list(typed))
            }
            "skills/list" => {
                let typed: p::SkillsListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                to_value(handlers::handle_skills_list(self, typed))
            }
            // Thread operations
            "thread/list" => {
                let typed: p::ThreadListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                handlers::handle_thread_list(&client, typed).await
            }
            "thread/start" => handlers::handle_thread_start(ctx, self, &client, params).await,
            "thread/resume" => handlers::handle_thread_resume(ctx, self, &client, params).await,
            "thread/read" => handlers::handle_thread_read(ctx, self, &client, params).await,
            "thread/name/set" => {
                let typed: p::ThreadSetNameParams = decode(params)?;
                to_value(handlers::handle_thread_name_set(ctx, self, typed))
            }
            "thread/fork" => handlers::handle_thread_fork(ctx, &client, params).await,
            "thread/rollback" => {
                let typed: p::ThreadRollbackParams = decode(params)?;
                handlers::handle_thread_rollback(typed)
            }
            "thread/archive" => {
                let typed: p::ThreadArchiveParams = decode(params)?;
                handlers::handle_thread_archive(typed)
            }
            "thread/unarchive" => {
                let typed: p::ThreadUnarchiveParams = decode(params)?;
                handlers::handle_thread_unarchive(typed)
            }
            // Turn operations
            "turn/start" => handlers::handle_turn_start(ctx, self, &client, params).await,
            "turn/steer" => {
                let typed: p::TurnSteerParams = decode(params)?;
                handlers::handle_turn_steer(&client, typed)
                    .await
                    .and_then(to_value)
            }
            "turn/interrupt" => {
                let typed: p::TurnInterruptParams = decode(params)?;
                handlers::handle_turn_interrupt(&client, typed)
                    .await
                    .and_then(to_value)
            }
            // Review operations
            "review/start" => {
                let typed: p::ReviewStartParams = decode(params)?;
                handlers::handle_review_start(typed)
            }
            // Command operations
            "command/exec" => handlers::handle_command_exec(ctx, self, &client, params).await,
            "command/exec/terminate" => {
                let typed: p::CommandExecTerminateParams = decode(params)?;
                handlers::handle_command_exec_terminate(&client, typed)
                    .await
                    .and_then(to_value)
            }
            "command/exec/write" => {
                let typed: p::CommandExecWriteParams = decode(params)?;
                handlers::handle_command_exec_write(typed)
            }
            "command/exec/resize" => {
                let typed: p::CommandExecResizeParams = decode(params)?;
                handlers::handle_command_exec_resize(typed)
            }
            _ => Err(JsonRpcError {
                code: error_codes::METHOD_NOT_FOUND,
                message: format!("Method `{}` is not implemented", method),
                data: None,
            }),
        }
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        match method {
            "thread/name/set" => {
                // ACP has no server-side rename method. Devin advertises
                // `cognition.ai/sessionRename` as a capability hint but
                // probing the binary shows no matching JSON-RPC method —
                // it's a client-side display label. Cache the title
                // locally and echo `thread/name/updated` so iOS's UI
                // stays consistent across reconnects.
                let thread_id = params
                    .get("threadId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if thread_id.is_empty() {
                    return;
                }
                self.set_thread_title(thread_id, name);
                self.emit_thread_name_updated(ctx, thread_id, name);
            }
            _ => {
                debug!(method, "Unhandled notification from client");
            }
        }
    }

    async fn shutdown(&self) {
        AcpBridge::shutdown(self).await;
    }
}
