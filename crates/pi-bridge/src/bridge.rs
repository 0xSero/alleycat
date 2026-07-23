//! `PiBridge` — the public unified type implementing
//! `bridge_core::Bridge`.
//!
//! Construction goes through [`PiBridge::builder`]. The bridge owns the pi
//! process pool, the `bridge_core::ThreadIndex<PiSessionRef>`, the codex_home
//! path, and a `ProcessLauncher` (the daemon plugs `LocalLauncher`; Litter
//! plugs `SshLauncher`). Per-connection `ThreadDefaults` lives in a
//! `DashMap` keyed by `session_id` so the same `(client_node_id, agent)`
//! session keeps its config across iroh disconnects.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use alleycat_bridge_core::{
    Bridge, Conn, JsonRpcError, LocalLauncher, ProcessLauncher, error_codes,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde_json::Value;

use crate::codex_proto as p;
use crate::handlers;
use crate::index::{PiHydrator, PiSessionInfo, ThreadIndex};
use crate::pool::{self as pi, PiPool};
use crate::state::{ConnectionState, ThreadDefaults, ThreadIndexHandle};

/// Build the per-session map key from the session's `(node_id, agent)`
/// identity. Matches the registry's keying so the same daemon-managed
/// session gets the same `ThreadDefaults` slot across reattaches.
fn session_key(session: &alleycat_bridge_core::session::Session) -> String {
    format!("{}:{}", session.node_id, session.agent)
}

/// Default pi binary name. Honored by `from_env()` via `PI_BRIDGE_PI_BIN`.
const DEFAULT_PI_BIN: &str = "pi";

/// Public unified type. Constructed via [`PiBridge::builder`].
pub struct PiBridge {
    pool: Arc<PiPool>,
    thread_index: Arc<dyn ThreadIndexHandle>,
    codex_home: PathBuf,
    launcher: Arc<dyn ProcessLauncher>,
    /// Per-connection `ThreadDefaults`, keyed by `session_id`. Entries are
    /// populated lazily on first use within a dispatch and live for the
    /// session's lifetime. The session reaper drops entries by id when the
    /// underlying registry session expires.
    per_conn: DashMap<String, Arc<Mutex<ThreadDefaults>>>,
    trust_persisted_cwd: bool,
    model_provider_prefixes: Arc<Vec<String>>,
}

impl PiBridge {
    pub fn builder() -> PiBridgeBuilder {
        PiBridgeBuilder::default()
    }

    pub fn pool(&self) -> &Arc<PiPool> {
        &self.pool
    }

    pub fn thread_index(&self) -> &Arc<dyn ThreadIndexHandle> {
        &self.thread_index
    }

    pub fn codex_home(&self) -> &std::path::Path {
        &self.codex_home
    }

    pub fn launcher(&self) -> &Arc<dyn ProcessLauncher> {
        &self.launcher
    }

    /// Get or create the per-conn defaults slot for `session_id`.
    pub fn defaults_for(&self, session_id: &str) -> Arc<Mutex<ThreadDefaults>> {
        self.per_conn
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(ThreadDefaults::default())))
            .clone()
    }

    /// Drop the per-conn defaults for `session_id`. Called by the session
    /// reaper when a registry session expires; safe to call when no entry
    /// exists.
    pub fn drop_session(&self, session_id: &str) {
        self.per_conn.remove(session_id);
    }

    /// Build a per-request `ConnectionState` facade from `&self` plus the
    /// connection's session. The handlers continue to program against
    /// `Arc<ConnectionState>` (no signature churn during A2); the bridge
    /// constructs a fresh facade for each dispatch call.
    fn connection_state(&self, ctx: &Conn) -> Arc<ConnectionState> {
        let session = Arc::clone(ctx.session());
        let key = session_key(&session);
        let defaults = self.defaults_for(&key);
        Arc::new(ConnectionState::new(
            session,
            Arc::clone(&self.pool),
            Arc::clone(&self.thread_index),
            defaults,
            Arc::clone(&self.launcher),
            self.trust_persisted_cwd,
            Arc::clone(&self.model_provider_prefixes),
        ))
    }
}

/// Builder for [`PiBridge`].
#[derive(Default)]
pub struct PiBridgeBuilder {
    agent_bin: Option<PathBuf>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    codex_home: Option<PathBuf>,
    pool_capacity: Option<usize>,
    idle_ttl: Option<Duration>,
    trust_persisted_cwd: bool,
    hydrator: Option<PiHydrator>,
    rpc_session_listing_only: bool,
    model_provider_prefixes: Vec<String>,
}

impl PiBridgeBuilder {
    pub fn agent_bin(mut self, bin: impl Into<PathBuf>) -> Self {
        self.agent_bin = Some(bin.into());
        self
    }

    pub fn launcher(mut self, launcher: Arc<dyn ProcessLauncher>) -> Self {
        self.launcher = Some(launcher);
        self
    }

    pub fn codex_home(mut self, home: impl Into<PathBuf>) -> Self {
        self.codex_home = Some(home.into());
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

    pub fn trust_persisted_cwd(mut self, trust: bool) -> Self {
        self.trust_persisted_cwd = trust;
        self
    }

    pub fn hydrator(mut self, hydrator: PiHydrator) -> Self {
        self.hydrator = Some(hydrator);
        self
    }

    /// When enabled, startup session hydration uses only Pi's RPC
    /// `list_sessions` command. If that command fails, the bridge starts with
    /// an empty index instead of falling back to a local `~/.pi` scan. Remote
    /// launchers use this so session discovery stays on the remote machine.
    pub fn rpc_session_listing_only(mut self, enabled: bool) -> Self {
        self.rpc_session_listing_only = enabled;
        self
    }

    /// Restrict `model/list` to exact provider ids or their `prefix-*`
    /// controller variants. Empty means the runtime's complete catalog.
    pub fn model_provider_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.model_provider_prefixes.push(prefix.into());
        self
    }

    /// Apply env-var overrides on top of any explicit settings. Reads:
    /// `PI_BRIDGE_PI_BIN`, `CODEX_HOME`. Builder-set values win when both
    /// are present.
    pub fn from_env(mut self) -> Self {
        if self.agent_bin.is_none() {
            if let Some(bin) = std::env::var_os("PI_BRIDGE_PI_BIN") {
                self.agent_bin = Some(PathBuf::from(bin));
            }
        }
        if self.codex_home.is_none() {
            if let Some(home) = std::env::var_os("CODEX_HOME").filter(|v| !v.is_empty()) {
                self.codex_home = Some(PathBuf::from(home));
            }
        }
        self
    }

    pub async fn build(self) -> Result<Arc<PiBridge>> {
        let agent_bin = self
            .agent_bin
            .unwrap_or_else(|| PathBuf::from(DEFAULT_PI_BIN));
        let launcher = self.launcher.unwrap_or_else(|| Arc::new(LocalLauncher));
        let codex_home = self
            .codex_home
            .unwrap_or_else(handlers::lifecycle::default_codex_home);
        if let Err(err) = std::fs::create_dir_all(&codex_home) {
            tracing::warn!(?codex_home, %err, "failed to create codex_home; continuing");
        }

        let max_processes = self
            .pool_capacity
            .unwrap_or(crate::pool::DEFAULT_MAX_PROCESSES);
        let idle_ttl = self.idle_ttl.unwrap_or(crate::pool::DEFAULT_IDLE_TTL);
        let pool = Arc::new(PiPool::with_launcher_and_limits(
            agent_bin,
            Arc::clone(&launcher),
            max_processes,
            idle_ttl,
        ));

        let hydrator = match self.hydrator {
            Some(hydrator) => hydrator,
            None => match list_sessions_via_rpc(&pool).await {
                Ok(sessions) => PiHydrator::with_sessions(sessions),
                Err(error) => {
                    tracing::warn!(%error, "pi RPC list_sessions failed during bridge startup");
                    if self.rpc_session_listing_only {
                        PiHydrator::with_sessions(Vec::new())
                    } else {
                        PiHydrator::new()
                    }
                }
            },
        };
        let thread_index: Arc<ThreadIndex> =
            ThreadIndex::open_and_hydrate_with(&codex_home, &hydrator).await?;
        let thread_index_handle: Arc<dyn ThreadIndexHandle> = thread_index;

        Ok(Arc::new(PiBridge {
            pool,
            thread_index: thread_index_handle,
            codex_home,
            launcher,
            per_conn: DashMap::new(),
            trust_persisted_cwd: self.trust_persisted_cwd,
            model_provider_prefixes: Arc::new(self.model_provider_prefixes),
        }))
    }
}

async fn list_sessions_via_rpc(pool: &PiPool) -> Result<Vec<PiSessionInfo>> {
    let handle = pool.acquire_utility(None).await?;
    let response = handle
        .send_request(pi::RpcCommand::ListSessions(pi::ListSessionsCmd {
            id: None,
            all_projects: true,
        }))
        .await?;
    if !response.success {
        bail!(
            "pi list_sessions returned error: {}",
            response
                .error
                .unwrap_or_else(|| "unknown error".to_string())
        );
    }
    let data: pi::ListSessionsData = serde_json::from_value(
        response
            .data
            .context("pi list_sessions response was missing data")?,
    )
    .context("parsing pi list_sessions response")?;
    Ok(data.sessions.into_iter().map(pi_session_from_rpc).collect())
}

fn pi_session_from_rpc(session: pi::SessionInfoData) -> PiSessionInfo {
    PiSessionInfo {
        path: session.path.into(),
        id: session.id,
        cwd: session.cwd,
        name: session.name,
        parent_session_path: session.parent_session_path.map(Into::into),
        created: parse_pi_datetime(&session.created),
        modified: parse_pi_datetime(&session.modified),
        message_count: session.message_count,
        first_message: if session.first_message.trim().is_empty() {
            "(no messages)".to_string()
        } else {
            session.first_message
        },
        all_messages_text: session.all_messages_text,
    }
}

fn parse_pi_datetime(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

#[async_trait]
impl Bridge for PiBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let state = self.connection_state(ctx);
        let typed: p::InitializeParams = decode(&params)?;
        Ok(serde_json::to_value(handlers::lifecycle::handle_initialize(
            &state,
            typed,
            &self.codex_home,
        ))
        .map_err(serde_err)?)
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let state = self.connection_state(ctx);
        dispatch(&state, &self.codex_home, method, params).await
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        let state = self.connection_state(ctx);
        match method {
            "initialized" => handlers::lifecycle::handle_initialized(&state),
            other => {
                tracing::debug!(method = %other, params = %params, "ignoring unknown client notification")
            }
        }
    }
}

async fn dispatch(
    state: &Arc<ConnectionState>,
    codex_home: &std::path::Path,
    method: &str,
    params: Value,
) -> Result<Value, JsonRpcError> {
    match method {
        "account/read" => {
            let typed: p::GetAccountParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            ok(handlers::lifecycle::handle_account_read(state, typed))
        }
        "account/rateLimits/read" => {
            ok(handlers::lifecycle::handle_account_rate_limits_read(state))
        }
        "account/login/start" => {
            let typed: p::LoginAccountParams = decode(&params)?;
            handlers::lifecycle::handle_account_login_start(state, typed)
                .map_err(internal_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "account/login/cancel" => {
            let typed: p::CancelLoginAccountParams = decode(&params)?;
            ok(handlers::lifecycle::handle_account_login_cancel(
                state, typed,
            ))
        }
        "account/logout" => ok(handlers::lifecycle::handle_account_logout(state)),
        "feedback/upload" => {
            let typed: p::FeedbackUploadParams = decode(&params)?;
            ok(handlers::lifecycle::handle_feedback_upload(state, typed))
        }
        "config/read" => {
            let typed: p::ConfigReadParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            handlers::config::handle_config_read(state, codex_home, typed)
                .map_err(internal_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "config/value/write" => {
            let typed: p::ConfigValueWriteParams = decode(&params)?;
            handlers::config::handle_config_value_write(state, codex_home, typed)
                .map_err(internal_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "config/batchWrite" => {
            let typed: p::ConfigBatchWriteParams = decode(&params)?;
            handlers::config::handle_config_batch_write(state, codex_home, typed)
                .map_err(internal_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "configRequirements/read" => ok(handlers::config::handle_config_requirements_read(state)),
        "mcpServerStatus/list" => {
            let typed: p::ListMcpServerStatusParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            ok(handlers::mcp::handle_mcp_server_status_list(state, typed))
        }
        "config/mcpServer/reload" => ok(handlers::mcp::handle_mcp_server_refresh(state)),
        "mcpServer/oauth/login" => {
            let typed: p::McpServerOauthLoginParams = decode(&params)?;
            ok(handlers::mcp::handle_mcp_server_oauth_login(state, typed))
        }
        "mock/experimentalMethod" => {
            let typed: p::MockExperimentalMethodParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            ok(p::MockExperimentalMethodResponse {
                echoed: typed.value,
            })
        }
        "experimentalFeature/list" => ok(p::ExperimentalFeatureListResponse {
            data: Vec::new(),
            next_cursor: None,
        }),
        "collaborationMode/list" => ok(p::CollaborationModeListResponse { data: Vec::new() }),
        "model/list" => {
            let typed: p::ModelListParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            ok(handlers::model::handle_model_list(state, typed).await)
        }
        "skills/list" => {
            let typed: p::SkillsListParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            ok(handlers::skills::handle_skills_list(state, typed).await)
        }
        "skills/remote/list" => Ok(handlers::skills::handle_skills_remote_list(state).await),
        "skills/remote/export" => {
            Ok(handlers::skills::handle_skills_remote_export(state, params).await)
        }
        "skills/config/write" => {
            let typed: p::SkillsConfigWriteParams = decode(&params)?;
            ok(handlers::skills::handle_skills_config_write(state, typed).await)
        }
        "thread/start" => {
            let typed: p::ThreadStartParams = decode(&params)?;
            handlers::thread::handle_thread_start(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/resume" => {
            let typed: p::ThreadResumeParams = decode(&params)?;
            handlers::thread::handle_thread_resume(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/fork" => {
            let typed: p::ThreadForkParams = decode(&params)?;
            handlers::thread::handle_thread_fork(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/archive" => {
            let typed: p::ThreadArchiveParams = decode(&params)?;
            handlers::thread::handle_thread_archive(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/unarchive" => {
            let typed: p::ThreadUnarchiveParams = decode(&params)?;
            handlers::thread::handle_thread_unarchive(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/name/set" => {
            let typed: p::ThreadSetNameParams = decode(&params)?;
            handlers::thread::handle_thread_set_name(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/compact/start" => {
            let typed: p::ThreadCompactStartParams = decode(&params)?;
            handlers::thread::handle_thread_compact_start(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/rollback" => {
            let typed: p::ThreadRollbackParams = decode(&params)?;
            handlers::thread::handle_thread_rollback(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/list" => {
            let typed: p::ThreadListParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            handlers::thread::handle_thread_list(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/loaded/list" => {
            let typed: p::ThreadLoadedListParams = if params.is_null() {
                Default::default()
            } else {
                decode(&params)?
            };
            ok(handlers::thread::handle_thread_loaded_list(state, typed).await)
        }
        "thread/read" => {
            let typed: p::ThreadReadParams = decode(&params)?;
            handlers::thread::handle_thread_read(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "thread/backgroundTerminals/clean" => {
            let typed: p::ThreadBackgroundTerminalsCleanParams = decode(&params)?;
            ok(handlers::thread::handle_thread_background_terminals_clean(state, typed).await)
        }
        "thread/turns/list" => {
            let typed: p::ThreadTurnsListParams = decode(&params)?;
            handlers::thread::handle_thread_turns_list(state, typed)
                .await
                .map_err(thread_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "turn/start" => {
            let typed: p::TurnStartParams = decode(&params)?;
            handlers::turn::handle_turn_start(state, typed)
                .await
                .map_err(turn_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "turn/steer" => {
            let typed: p::TurnSteerParams = decode(&params)?;
            handlers::turn::handle_turn_steer(state, typed)
                .await
                .map_err(turn_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "turn/interrupt" => {
            let typed: p::TurnInterruptParams = decode(&params)?;
            handlers::turn::handle_turn_interrupt(state, typed)
                .await
                .map_err(turn_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "review/start" => {
            let typed: p::ReviewStartParams = decode(&params)?;
            handlers::turn::handle_review_start(state, typed)
                .await
                .map_err(turn_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "command/exec" => {
            let typed: p::CommandExecParams = decode(&params)?;
            handlers::command_exec::handle_command_exec(state, typed)
                .await
                .map_err(exec_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "command/exec/terminate" => {
            let typed: p::CommandExecTerminateParams = decode(&params)?;
            ok(handlers::command_exec::handle_command_exec_terminate(state, typed).await)
        }
        "command/exec/write" => {
            let typed: p::CommandExecWriteParams = decode(&params)?;
            handlers::command_exec::handle_command_exec_write(state, typed)
                .await
                .map_err(exec_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        "command/exec/resize" => {
            let typed: p::CommandExecResizeParams = decode(&params)?;
            handlers::command_exec::handle_command_exec_resize(state, typed)
                .await
                .map_err(exec_err)
                .and_then(|v| serde_json::to_value(v).map_err(serde_err))
        }
        other => Err(JsonRpcError {
            code: error_codes::METHOD_NOT_FOUND,
            message: format!("method `{other}` is not implemented"),
            data: None,
        }),
    }
}

fn ok<T: serde::Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(serde_err)
}

fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, JsonRpcError> {
    serde_json::from_value(value.clone()).map_err(|err| JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!("invalid params: {err}"),
        data: None,
    })
}

fn serde_err(err: serde_json::Error) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: format!("serialization error: {err}"),
        data: None,
    }
}

fn internal_err<E: std::fmt::Display>(err: E) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: format!("internal error: {err}"),
        data: None,
    }
}

fn exec_err(err: handlers::command_exec::ExecError) -> JsonRpcError {
    JsonRpcError {
        code: err.rpc_code(),
        message: err.to_string(),
        data: None,
    }
}

fn thread_err(err: handlers::thread::ThreadError) -> JsonRpcError {
    JsonRpcError {
        code: err.rpc_code(),
        message: err.to_string(),
        data: None,
    }
}

fn turn_err(err: handlers::turn::TurnError) -> JsonRpcError {
    JsonRpcError {
        code: err.rpc_code(),
        message: err.to_string(),
        data: None,
    }
}
