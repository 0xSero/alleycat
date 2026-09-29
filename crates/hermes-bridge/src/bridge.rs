//! Hermes Bridge — Bridge trait implementation.
//!
//! Translates between the codex app-server JSON-RPC surface and the
//! Hermes Agent backend (API or CLI mode). Core chat/thread methods are
//! implemented; unsupported Codex-only features return explicit -32601 errors.

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::process::Command;

use alleycat_bridge_core::{Bridge, Conn, JsonRpcError};
use alleycat_codex_proto::account::{
    Account, CancelLoginAccountResponse, CancelLoginAccountStatus, GetAccountRateLimitsResponse,
    GetAccountResponse, LoginAccountResponse, LogoutAccountResponse,
};
use alleycat_codex_proto::command_exec::{
    CommandExecParams, CommandExecResizeResponse, CommandExecResponse,
    CommandExecTerminateResponse, CommandExecWriteResponse,
};
use alleycat_codex_proto::common::{
    ApprovalsReviewer, AskForApproval, ReasoningEffort, SandboxMode, SessionSource, ThreadStatus,
    TurnStatus,
};
use alleycat_codex_proto::config::ConfigRequirementsReadResponse;
use alleycat_codex_proto::items::{ThreadItem, UserInput};
use alleycat_codex_proto::lifecycle::InitializeResponse;
use alleycat_codex_proto::mcp::{
    ListMcpServerStatusResponse, McpServerOauthLoginResponse, McpServerRefreshResponse,
};
use alleycat_codex_proto::model::ModelListResponse;
use alleycat_codex_proto::notifications::{
    AgentMessageDeltaNotification, ItemCompletedNotification, ItemStartedNotification,
    ThreadIdOnly, ThreadNameUpdatedNotification, ThreadStartedNotification,
    TurnCompletedNotification, TurnStartedNotification,
};
use alleycat_codex_proto::skills::{SkillsConfigWriteResponse, SkillsListResponse};
use alleycat_codex_proto::thread::{
    Thread, ThreadArchiveParams, ThreadArchiveResponse, ThreadBackgroundTerminalsCleanResponse,
    ThreadForkParams, ThreadForkResponse, ThreadListParams, ThreadListResponse,
    ThreadLoadedListResponse, ThreadReadParams, ThreadReadResponse, ThreadResumeParams,
    ThreadResumeResponse, ThreadRollbackParams, ThreadSetNameParams, ThreadSetNameResponse,
    ThreadStartParams, ThreadStartResponse, ThreadTurnsListParams, ThreadTurnsListResponse, Turn,
};
use alleycat_codex_proto::turn::{TurnInterruptResponse, TurnStartParams, TurnStartResponse};

use crate::api_client::{CreateRunRequest, DEFAULT_API_KEY_ENV, HermesApiClient};
use crate::config::HermesBridgeConfig;
use crate::index::{HermesBinding, ThreadIndex};
use crate::state::{ActiveTurn, TurnState};

pub(crate) fn gateway_model_ids(catalog: &Value) -> anyhow::Result<Vec<String>> {
    let mut seen = std::collections::HashSet::new();
    let mut ids = Vec::new();
    if let Some(providers) = catalog["providers"].as_array() {
        for provider in providers {
            if provider["authenticated"] == false {
                continue;
            }
            let Some(slug) = provider["slug"].as_str().filter(|slug| !slug.is_empty()) else {
                continue;
            };
            for model in provider["models"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if model.is_empty() {
                    continue;
                }
                let id = format!("hermes/{slug}/{model}");
                if seen.insert(id.clone()) {
                    ids.push(id);
                }
            }
        }
        if let (Some(provider), Some(model)) =
            (catalog["provider"].as_str(), catalog["model"].as_str())
        {
            let current = format!("hermes/{provider}/{model}");
            if let Some(index) = ids.iter().position(|id| id == &current) {
                ids.swap(0, index);
            }
        }
    } else {
        let rows = catalog["data"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Hermes catalog is missing data"))?;
        for id in rows
            .iter()
            .filter_map(|row| row["id"].as_str())
            .filter(|id| !id.trim().is_empty())
        {
            if seen.insert(id.to_string()) {
                ids.push(id.to_string());
            }
        }
    }
    anyhow::ensure!(
        !ids.is_empty(),
        "Hermes returned no selectable model routes"
    );
    Ok(ids)
}

/// Our explicit namespace disambiguates provider routing from ordinary model
/// ids such as anthropic/claude on OpenRouter. Never split unqualified ids.
pub(crate) fn hermes_model_selection(value: Option<&str>) -> (Option<String>, Option<String>) {
    let Some(value) = value.filter(|value| !value.is_empty() && *value != "hermes-agent") else {
        return (None, None);
    };
    if let Some((provider, model)) = value
        .strip_prefix("hermes/")
        .and_then(|value| value.split_once('/'))
    {
        if !provider.is_empty() && !model.is_empty() {
            return (Some(model.into()), Some(provider.into()));
        }
    }
    (Some(value.into()), None)
}

fn random_hex(len: usize) -> String {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    std::iter::repeat_with(|| rng.next_u32() as u8)
        .take(len)
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn rpc_error(code: i64, message: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code,
        message: message.into(),
        data: None,
    }
}

fn error_response(code: i64, message: &str) -> Result<Value, JsonRpcError> {
    Err(rpc_error(code, message))
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|e| rpc_error(-32603, format!("serialize response: {e}")))
}

fn default_cwd() -> String {
    std::env::current_dir()
        .ok()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "/tmp".to_string())
}

fn user_text(input: &[UserInput]) -> String {
    input
        .iter()
        .filter_map(|inp| match inp {
            UserInput::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<&str>>()
        .join("\n")
}

fn preview_for(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(120).collect())
    }
}

fn truncate_string(value: &mut String, cap: usize) {
    if value.len() <= cap {
        return;
    }
    let mut end = cap;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push_str("\n...[truncated]");
}

fn in_progress_turn(turn_id: &str) -> Turn {
    Turn {
        id: turn_id.to_string(),
        items: vec![],
        items_view: "full".to_string(),
        status: TurnStatus::InProgress,
        error: None,
        started_at: Some(epoch_ms()),
        completed_at: None,
        duration_ms: None,
    }
}

fn completed_turn(turn_id: &str) -> Turn {
    let mut turn = in_progress_turn(turn_id);
    turn.status = TurnStatus::Completed;
    turn.completed_at = Some(epoch_ms());
    turn
}

#[derive(Clone)]
pub struct HermesBridge {
    config: HermesBridgeConfig,
    index: Arc<ThreadIndex>,
    state: Arc<TurnState>,
    turns: Arc<Mutex<HashMap<String, Vec<Turn>>>>,
    api_client: Arc<HermesApiClient>,
}

impl HermesBridge {
    pub fn new(config: HermesBridgeConfig) -> Self {
        let api_base = match &config.mode {
            crate::config::HermesMode::Api { api_base } => api_base.clone(),
            crate::config::HermesMode::Auto { api_base, .. } => api_base.clone(),
            crate::config::HermesMode::Cli { .. } => {
                crate::api_client::DEFAULT_API_BASE.to_string()
            }
        };
        let api_key = std::env::var(DEFAULT_API_KEY_ENV)
            .or_else(|_| std::env::var("API_SERVER_KEY"))
            .ok();
        let index = config
            .state_dir
            .as_ref()
            .and_then(|dir| ThreadIndex::open_sync(PathBuf::from(dir).join("threads.json")).ok())
            .unwrap_or_else(ThreadIndex::new_in_memory);
        Self {
            config,
            index: Arc::new(index),
            state: Arc::new(TurnState::new()),
            turns: Arc::new(Mutex::new(HashMap::new())),
            api_client: Arc::new(HermesApiClient::new(&api_base, api_key)),
        }
    }

    fn binding_to_thread(binding: &HermesBinding) -> Thread {
        let now = epoch_ms();
        Thread {
            id: binding.thread_id.clone(),
            session_id: binding.hermes_session_id.clone(),
            forked_from_id: binding.forked_from_id.clone(),
            preview: binding.preview.clone().unwrap_or_default(),
            ephemeral: false,
            model_provider: "hermes-agent".to_string(),
            created_at: binding.created_at,
            updated_at: if binding.updated_at == 0 {
                now
            } else {
                binding.updated_at
            },
            status: ThreadStatus::Idle,
            path: None,
            cwd: binding.cwd.clone().unwrap_or_else(default_cwd),
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            source: SessionSource::AppServer,
            thread_source: None,
            agent_nickname: Some("hermes".to_string()),
            agent_role: Some("assistant".to_string()),
            git_info: None,
            name: binding.name.clone(),
            turns: vec![],
        }
    }

    fn start_like_response(thread: Thread, model: Option<String>) -> ThreadResumeResponse {
        let cwd = thread.cwd.clone();
        let model = model.unwrap_or_else(|| "hermes-agent".to_string());
        ThreadResumeResponse {
            thread,
            model,
            model_provider: "hermes-agent".to_string(),
            service_tier: None,
            cwd,
            instruction_sources: vec![],
            approval_policy: AskForApproval::Never,
            approvals_reviewer: ApprovalsReviewer::User,
            sandbox: json!({"type": "dangerFullAccess"}),
            permission_profile: None,
            active_permission_profile: None,
            reasoning_effort: Some(ReasoningEffort::Medium),
        }
    }

    fn persist_index(&self) -> Result<(), JsonRpcError> {
        self.index
            .persist()
            .map_err(|e| rpc_error(-32603, format!("persist thread index: {e}")))
    }

    fn start_logged_turn(&self, thread_id: &str, turn_id: &str) {
        let turn = Turn {
            id: turn_id.to_string(),
            items: vec![],
            items_view: "full".to_string(),
            status: TurnStatus::InProgress,
            error: None,
            started_at: Some(epoch_ms()),
            completed_at: None,
            duration_ms: None,
        };
        self.turns
            .lock()
            .unwrap()
            .entry(thread_id.to_string())
            .or_default()
            .push(turn);
    }

    fn push_logged_item(&self, thread_id: &str, turn_id: &str, item: ThreadItem) {
        if let Some(turn) = self
            .turns
            .lock()
            .unwrap()
            .get_mut(thread_id)
            .and_then(|turns| turns.iter_mut().rev().find(|turn| turn.id == turn_id))
        {
            turn.items.push(item);
        }
    }

    fn complete_logged_turn(&self, thread_id: &str, turn_id: &str, error: Option<String>) {
        if let Some(turn) = self
            .turns
            .lock()
            .unwrap()
            .get_mut(thread_id)
            .and_then(|turns| turns.iter_mut().rev().find(|turn| turn.id == turn_id))
        {
            turn.status = if error.is_some() {
                TurnStatus::Failed
            } else {
                TurnStatus::Completed
            };
            turn.error = error.map(|message| alleycat_codex_proto::common::TurnError {
                message,
                codex_error_info: None,
                additional_details: None,
            });
            turn.completed_at = Some(epoch_ms());
        }
    }

    fn logged_turns(&self, thread_id: &str) -> Vec<Turn> {
        self.turns
            .lock()
            .unwrap()
            .get(thread_id)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl Bridge for HermesBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        ctx.set_initialize_capabilities(&params);
        let home = directories::ProjectDirs::from("", "", "alleycat")
            .map(|d| d.data_dir().to_string_lossy().to_string())
            .unwrap_or_else(|| "/tmp/alleycat".to_string());
        to_value(InitializeResponse {
            user_agent: format!("hermes-bridge/{}", env!("CARGO_PKG_VERSION")),
            codex_home: home,
            platform_family: "linux".to_string(),
            platform_os: std::env::consts::OS.to_string(),
        })
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "thread/start" => self.handle_thread_start(ctx, params).await,
            "thread/resume" => self.handle_thread_resume(ctx, params).await,
            "thread/fork" => self.handle_thread_fork(ctx, params).await,
            "thread/archive" => self.handle_thread_archive(ctx, params).await,
            "thread/unarchive" => self.handle_thread_unarchive(ctx, params).await,
            "thread/name/set" => self.handle_thread_name_set(ctx, params).await,
            "thread/compact/start" => self.handle_thread_compact_start(ctx, params).await,
            "thread/rollback" => self.handle_thread_rollback(ctx, params).await,
            "thread/list" => self.handle_thread_list(ctx, params).await,
            "thread/loaded/list" => self.handle_thread_loaded_list(ctx, params).await,
            "thread/read" => self.handle_thread_read(ctx, params).await,
            "thread/turns/list" => self.handle_thread_turns_list(ctx, params).await,
            "thread/backgroundTerminals/clean" => {
                self.handle_thread_background_terminals_clean(ctx, params)
                    .await
            }
            "turn/start" => self.handle_turn_start(ctx, params).await,
            "turn/steer" => self.handle_turn_steer(ctx, params).await,
            "turn/interrupt" => self.handle_turn_interrupt(ctx, params).await,
            "review/start" => self.handle_review_start(ctx, params).await,
            "model/list" => self.handle_model_list(ctx, params).await,
            "account/read" => self.handle_account_read(ctx, params).await,
            "account/rateLimits/read" => self.handle_account_rate_limits_read(ctx, params).await,
            "account/login/start" => self.handle_account_login_start(ctx, params).await,
            "account/login/cancel" => self.handle_account_login_cancel(ctx, params).await,
            "account/logout" => self.handle_account_logout(ctx, params).await,
            "config/read" => self.handle_config_read(ctx, params).await,
            "config/value/write" => self.handle_config_value_write(ctx, params).await,
            "config/batchWrite" => self.handle_config_batch_write(ctx, params).await,
            "configRequirements/read" => self.handle_config_requirements_read(ctx, params).await,
            "mcpServerStatus/list" => self.handle_mcp_server_status_list(ctx, params).await,
            "config/mcpServer/reload" => self.handle_config_mcp_server_reload(ctx, params).await,
            "mcpServer/oauth/login" => self.handle_mcp_server_oauth_login(ctx, params).await,
            "skills/list" => self.handle_skills_list(ctx, params).await,
            "skills/remote/list" => self.handle_skills_remote_list(ctx, params).await,
            "skills/remote/export" => self.handle_skills_remote_export(ctx, params).await,
            "skills/config/write" => self.handle_skills_config_write(ctx, params).await,
            "command/exec" => self.handle_command_exec(ctx, params).await,
            "command/exec/write" => self.handle_command_exec_write(ctx, params).await,
            "command/exec/terminate" => self.handle_command_exec_terminate(ctx, params).await,
            "command/exec/resize" => self.handle_command_exec_resize(ctx, params).await,
            "mock/experimentalMethod" => self.handle_mock_experimental_method(ctx, params).await,
            "experimentalFeature/list" => self.handle_experimental_feature_list(ctx, params).await,
            "collaborationMode/list" => self.handle_collaboration_mode_list(ctx, params).await,
            "feedback/upload" => self.handle_feedback_upload(ctx, params).await,
            _ => error_response(-32601, &format!("Method not found: {method}")),
        }
    }

    async fn notification(&self, _ctx: &Conn, _method: &str, _params: Value) {
        // No client-originated notifications currently require bridge-side state changes.
    }
}

impl HermesBridge {
    async fn handle_thread_start(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: ThreadStartParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        let now = epoch_ms();
        let thread_id = format!("thread_{}", random_hex(12));
        let session_id = format!("ses_{}", random_hex(12));
        let cwd = p.cwd.clone().unwrap_or_else(default_cwd);
        let binding = HermesBinding {
            thread_id: thread_id.clone(),
            hermes_session_id: session_id,
            cli_session_id: None,
            model: p.model.clone(),
            created_at: now,
            updated_at: now,
            preview: None,
            cwd: Some(cwd.clone()),
            name: None,
            forked_from_id: None,
            archived: false,
        };
        self.index.upsert(binding.clone());
        self.persist_index()?;
        let thread = Self::binding_to_thread(&binding);
        let _ = ctx.notifier().send_notification(
            "thread/started",
            ThreadStartedNotification {
                thread: thread.clone(),
            },
        );
        let base = Self::start_like_response(thread, p.model.clone());
        to_value(ThreadStartResponse {
            thread: base.thread,
            model: base.model,
            model_provider: base.model_provider,
            service_tier: base.service_tier,
            cwd,
            instruction_sources: base.instruction_sources,
            approval_policy: p.approval_policy.unwrap_or(AskForApproval::Never),
            approvals_reviewer: p.approvals_reviewer.unwrap_or(ApprovalsReviewer::User),
            sandbox: p
                .sandbox
                .map(|mode| match mode {
                    SandboxMode::ReadOnly => json!({"type": "readOnly"}),
                    SandboxMode::WorkspaceWrite => json!({"type": "workspaceWrite"}),
                    SandboxMode::DangerFullAccess => json!({"type": "dangerFullAccess"}),
                })
                .unwrap_or_else(|| json!({"type": "dangerFullAccess"})),
            permission_profile: p.permission_profile,
            active_permission_profile: None,
            reasoning_effort: Some(ReasoningEffort::Medium),
        })
    }

    async fn handle_thread_resume(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: ThreadResumeParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        let Some(mut binding) = self.index.get_by_thread(&p.thread_id) else {
            return error_response(-32602, "thread not found");
        };
        if let Some(cwd) = p.cwd.clone() {
            binding.cwd = Some(cwd);
            binding.updated_at = epoch_ms();
            self.index.upsert(binding.clone());
            self.persist_index()?;
        }
        let mut thread = Self::binding_to_thread(&binding);
        if !p.exclude_turns {
            thread.turns = self.logged_turns(&p.thread_id);
        }
        let _ = ctx.notifier().send_notification(
            "thread/started",
            ThreadStartedNotification {
                thread: thread.clone(),
            },
        );
        to_value(Self::start_like_response(thread, p.model.or(binding.model)))
    }

    async fn handle_thread_fork(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: ThreadForkParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        let Some(parent) = self.index.get_by_thread(&p.thread_id) else {
            return error_response(-32602, "thread not found");
        };
        let now = epoch_ms();
        let thread_id = format!("thread_{}", random_hex(12));
        let binding = HermesBinding {
            thread_id: thread_id.clone(),
            hermes_session_id: parent.hermes_session_id.clone(),
            cli_session_id: parent.cli_session_id.clone(),
            model: p.model.clone().or(parent.model.clone()),
            created_at: now,
            updated_at: now,
            preview: parent.preview.clone(),
            cwd: p.cwd.clone().or(parent.cwd.clone()),
            name: parent.name.clone().map(|name| format!("{name} (fork)")),
            forked_from_id: Some(parent.thread_id.clone()),
            archived: false,
        };
        self.index.upsert(binding.clone());
        self.persist_index()?;
        let thread = Self::binding_to_thread(&binding);
        let _ = ctx.notifier().send_notification(
            "thread/started",
            ThreadStartedNotification {
                thread: thread.clone(),
            },
        );
        to_value(ThreadForkResponse {
            ..Self::start_like_response(thread, binding.model)
        })
    }

    async fn handle_thread_archive(
        &self,
        ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let p: ThreadArchiveParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        if self
            .index
            .set_archived(&p.thread_id, true, epoch_ms())
            .is_none()
        {
            return error_response(-32602, "thread not found");
        }
        self.persist_index()?;
        let _ = ctx.notifier().send_notification(
            "thread/archived",
            ThreadIdOnly {
                thread_id: p.thread_id,
            },
        );
        to_value(ThreadArchiveResponse::default())
    }

    async fn handle_thread_unarchive(
        &self,
        ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let p: alleycat_codex_proto::thread::ThreadUnarchiveParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        let Some(binding) = self.index.set_archived(&p.thread_id, false, epoch_ms()) else {
            return error_response(-32602, "thread not found");
        };
        self.persist_index()?;
        let _ = ctx.notifier().send_notification(
            "thread/unarchived",
            ThreadIdOnly {
                thread_id: p.thread_id.clone(),
            },
        );
        to_value(alleycat_codex_proto::thread::ThreadUnarchiveResponse {
            thread: Self::binding_to_thread(&binding),
        })
    }

    async fn handle_thread_name_set(
        &self,
        ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let p: ThreadSetNameParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        if self
            .index
            .set_name(&p.thread_id, Some(p.name.clone()), epoch_ms())
            .is_none()
        {
            return error_response(-32602, "thread not found");
        }
        self.persist_index()?;
        let _ = ctx.notifier().send_notification(
            "thread/name/updated",
            ThreadNameUpdatedNotification {
                thread_id: p.thread_id,
                thread_name: Some(p.name),
            },
        );
        to_value(ThreadSetNameResponse::default())
    }

    async fn handle_thread_compact_start(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        error_response(
            -32601,
            "thread/compact/start is not supported by Hermes bridge",
        )
    }

    async fn handle_thread_rollback(
        &self,
        _ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let _p: ThreadRollbackParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        error_response(-32601, "thread/rollback is not supported by Hermes bridge")
    }

    async fn handle_thread_list(&self, _ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: ThreadListParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        let mut bindings = self.index.all();
        if let Some(archived) = p.archived {
            bindings.retain(|binding| binding.archived == archived);
        } else {
            bindings.retain(|binding| !binding.archived);
        }
        if let Some(term) = p.search_term.as_deref().filter(|s| !s.is_empty()) {
            let needle = term.to_lowercase();
            bindings.retain(|binding| {
                binding
                    .preview
                    .as_deref()
                    .unwrap_or_default()
                    .to_lowercase()
                    .contains(&needle)
                    || binding
                        .name
                        .as_deref()
                        .unwrap_or_default()
                        .to_lowercase()
                        .contains(&needle)
            });
        }
        let threads = bindings.iter().map(Self::binding_to_thread).collect();
        to_value(ThreadListResponse {
            data: threads,
            next_cursor: None,
            backwards_cursor: None,
        })
    }

    async fn handle_thread_loaded_list(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(ThreadLoadedListResponse {
            data: self.index.thread_ids(),
            next_cursor: None,
        })
    }

    async fn handle_thread_read(&self, _ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: ThreadReadParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        match self.index.get_by_thread(&p.thread_id) {
            Some(binding) => {
                let mut thread = Self::binding_to_thread(&binding);
                if p.include_turns {
                    thread.turns = self.logged_turns(&p.thread_id);
                }
                to_value(ThreadReadResponse { thread })
            }
            None => error_response(-32602, "thread not found"),
        }
    }

    async fn handle_thread_turns_list(
        &self,
        _ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let p: ThreadTurnsListParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        if self.index.get_by_thread(&p.thread_id).is_none() {
            return error_response(-32602, "thread not found");
        }
        to_value(ThreadTurnsListResponse {
            data: self.logged_turns(&p.thread_id),
            next_cursor: None,
            backwards_cursor: None,
        })
    }

    async fn handle_thread_background_terminals_clean(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(ThreadBackgroundTerminalsCleanResponse::default())
    }
}

impl HermesBridge {
    async fn handle_turn_start(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: TurnStartParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        let thread_id = p.thread_id.clone();
        let turn_id = format!("turn_{}", random_hex(12));
        let binding = self
            .index
            .get_by_thread(&thread_id)
            .ok_or_else(|| rpc_error(-32602, "thread not found"))?;
        let session_id = binding.hermes_session_id.clone();
        self.start_logged_turn(&thread_id, &turn_id);
        self.state.insert(
            thread_id.clone(),
            ActiveTurn {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
                hermes_session_id: session_id.clone(),
                run_id: None,
            },
        );
        let turn = Turn {
            id: turn_id.clone(),
            items: vec![],
            items_view: "full".to_string(),
            status: TurnStatus::InProgress,
            error: None,
            started_at: Some(epoch_ms()),
            completed_at: None,
            duration_ms: None,
        };
        let _ = ctx.notifier().send_notification(
            "turn/started",
            TurnStartedNotification {
                thread_id: thread_id.clone(),
                turn,
            },
        );
        self.emit_user_message(ctx, &thread_id, &turn_id, &p.input)
            .await;
        let text = user_text(&p.input);
        let cwd = p.cwd.as_ref().map(|p| p.to_string_lossy().to_string());
        self.index.update_after_turn(
            &thread_id,
            None,
            p.model.clone(),
            preview_for(&text),
            cwd,
            epoch_ms(),
        );
        self.persist_index()?;
        match &self.config.mode {
            crate::config::HermesMode::Api { .. } => {
                self.dispatch_turn_api(ctx, &thread_id, &turn_id, &session_id, &p)
                    .await
            }
            crate::config::HermesMode::Auto { .. } => {
                if self
                    .api_client
                    .health()
                    .await
                    .map(|h| h.status == "ok")
                    .unwrap_or(false)
                {
                    self.dispatch_turn_api(ctx, &thread_id, &turn_id, &session_id, &p)
                        .await
                } else {
                    self.dispatch_turn_cli(ctx, &thread_id, &turn_id, &p).await
                }
            }
            crate::config::HermesMode::Cli { .. } => {
                self.dispatch_turn_cli(ctx, &thread_id, &turn_id, &p).await
            }
        }
    }

    async fn handle_turn_steer(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
        Ok(json!({}))
    }

    async fn handle_turn_interrupt(
        &self,
        _ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let thread_id = params.get("threadId").and_then(Value::as_str).unwrap_or("");
        if let Some(active) = self
            .state
            .remove(thread_id)
            .and_then(|active| active.run_id)
        {
            let _ = self.api_client.stop_run(&active).await;
        }
        to_value(TurnInterruptResponse::default())
    }

    async fn handle_review_start(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        error_response(
            -32601,
            "review/start is not supported by the Hermes backend",
        )
    }
}

impl HermesBridge {
    async fn handle_model_list(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
        let catalog = match &self.config.mode {
            crate::config::HermesMode::Cli { bin } => crate::catalog::discover(bin.as_deref()).await,
            crate::config::HermesMode::Api { .. } => self.api_client.models().await,
            crate::config::HermesMode::Auto { bin, .. } => {
                // Match turn routing: a healthy gateway owns its catalog; a
                // missing gateway uses the installed CLI's native inventory.
                let gateway = tokio::time::timeout(Duration::from_secs(3), self.api_client.health())
                    .await.ok().and_then(Result::ok).is_some_and(|h| h.status == "ok");
                if gateway {
                    self.api_client.models().await
                } else {
                    crate::catalog::discover(bin.as_deref()).await
                }
            }
        }.map_err(|error| rpc_error(-32603, error.to_string()))?;
        let ids = gateway_model_ids(&catalog).map_err(|error| rpc_error(-32603, error.to_string()))?;
        to_value(ModelListResponse {
            data: ids
                .into_iter()
                .enumerate()
                .map(|(index, id)| alleycat_codex_proto::model::Model {
                    id: id.clone(),
                    model: id.clone(),
                    upgrade: None,
                    upgrade_info: None,
                    availability_nux: None,
                    display_name: id.strip_prefix("hermes/").unwrap_or(&id).to_string(),
                    description: "Configured Hermes model route".to_string(),
                    hidden: false,
                    supported_reasoning_efforts: vec![],
                    default_reasoning_effort: ReasoningEffort::None,
                    input_modalities: vec![json!("text")],
                    supports_personality: false,
                    additional_speed_tiers: vec![],
                    service_tiers: vec![],
                    is_default: index == 0,
                })
                .collect(),
            next_cursor: None,
        })
    }

    async fn handle_account_read(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(GetAccountResponse {
            account: Some(Account::ApiKey {}),
            requires_openai_auth: false,
        })
    }

    async fn handle_account_rate_limits_read(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(GetAccountRateLimitsResponse::default())
    }

    async fn handle_account_login_start(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(LoginAccountResponse::ApiKey {})
    }

    async fn handle_account_login_cancel(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(CancelLoginAccountResponse {
            status: CancelLoginAccountStatus::NotFound,
        })
    }

    async fn handle_account_logout(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(LogoutAccountResponse::default())
    }

    async fn active_native_settings_path(
        &self,
    ) -> Result<Option<std::path::PathBuf>, JsonRpcError> {
        let gateway = match &self.config.mode {
            crate::config::HermesMode::Api { .. } => true,
            crate::config::HermesMode::Cli { .. } => false,
            crate::config::HermesMode::Auto { .. } => {
                tokio::time::timeout(std::time::Duration::from_secs(3), self.api_client.health())
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some_and(|h| h.status == "ok")
            }
        };
        if gateway {
            Ok(None)
        } else {
            hermes_settings_path().map(Some).map_err(settings_error)
        }
    }
    async fn native_settings_catalog(
        &self,
    ) -> Result<(Value, bool, std::path::PathBuf), JsonRpcError> {
        let bin = match &self.config.mode {
            crate::config::HermesMode::Cli { bin }
            | crate::config::HermesMode::Auto { bin, .. } => bin.as_deref().unwrap_or("hermes"),
            _ => "hermes",
        };
        let installed = which::which(bin)
            .and_then(|p| {
                p.canonicalize()
                    .map_err(|_| which::Error::CannotCanonicalize)
            })
            .map_err(|_| {
                settings_error(anyhow::anyhow!(
                    "Cannot resolve installed Hermes interpreter for settings discovery"
                ))
            })?;
        let interpreter = installed
            .parent()
            .ok_or_else(|| settings_error(anyhow::anyhow!("Invalid Hermes installation path")))?
            .join("python");
        let output=tokio::time::timeout(std::time::Duration::from_secs(10),tokio::process::Command::new(interpreter).args(["-c","import json; from hermes_cli.config import load_config_readonly, get_config_path, is_managed; print(json.dumps({'config':load_config_readonly(),'source':str(get_config_path()),'managed':is_managed()}))"]).stdin(std::process::Stdio::null()).stderr(std::process::Stdio::null()).kill_on_drop(true).output()).await.map_err(|_|settings_error(anyhow::anyhow!("Hermes settings discovery timed out")))?.map_err(|_|settings_error(anyhow::anyhow!("Hermes settings interpreter could not start")))?;
        if !output.status.success() {
            return Err(settings_error(anyhow::anyhow!(
                "Installed Hermes could not expose its native settings defaults"
            )));
        }
        let response: Value = serde_json::from_slice(&output.stdout)
            .map_err(|_| settings_error(anyhow::anyhow!("Invalid Hermes settings catalog")))?;
        let config = response
            .get("config")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| {
                settings_error(anyhow::anyhow!("Hermes configuration is not an object"))
            })?;
        let source = response["source"]
            .as_str()
            .ok_or_else(|| settings_error(anyhow::anyhow!("Hermes settings source missing")))?;
        Ok((
            config,
            response["managed"].as_bool().unwrap_or(true),
            source.into(),
        ))
    }
    async fn handle_config_read(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
        match self.active_native_settings_path().await? {
            Some(_) => {
                let (effective, managed, path) = self.native_settings_catalog().await?;
                let reason =
                    managed.then_some("Hermes configuration is managed by administrator policy");
                let mut response = alleycat_bridge_core::settings::response(
                    alleycat_bridge_core::settings::read(&path).map_err(settings_error)?,
                    &path.to_string_lossy(),
                    !managed,
                    reason,
                );
                let defaults = alleycat_bridge_core::settings::response(
                    effective,
                    "Installed Hermes defaults and user overrides",
                    !managed,
                    reason,
                );
                let descriptors = response.config["_litterSettings"].as_array_mut().unwrap();
                for descriptor in defaults.config["_litterSettings"].as_array().unwrap() {
                    if !descriptors
                        .iter()
                        .any(|row| row["key"] == descriptor["key"])
                    {
                        descriptors.push(descriptor.clone());
                    }
                }
                to_value(response)
            }
            None => to_value(alleycat_bridge_core::settings::response(
                json!({}),
                "Hermes gateway",
                false,
                Some(
                    "The connected Hermes gateway does not expose a settings API; configure settings on its host",
                ),
            )),
        }
    }
    async fn handle_config_value_write(
        &self,
        _ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let params = serde_json::from_value(params).map_err(|e| settings_error(e.into()))?;
        let _path = self.active_native_settings_path().await?.ok_or_else(|| {
            settings_error(anyhow::anyhow!(
                "Hermes gateway does not expose a settings writer"
            ))
        })?;
        let (_, managed, path) = self.native_settings_catalog().await?;
        if managed {
            return Err(settings_error(anyhow::anyhow!(
                "Hermes configuration is managed by administrator policy"
            )));
        }
        to_value(alleycat_bridge_core::settings::write_one(&path, params).map_err(settings_error)?)
    }
    async fn handle_config_batch_write(
        &self,
        _ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let params = serde_json::from_value(params).map_err(|e| settings_error(e.into()))?;
        let _path = self.active_native_settings_path().await?.ok_or_else(|| {
            settings_error(anyhow::anyhow!(
                "Hermes gateway does not expose a settings writer"
            ))
        })?;
        let (_, managed, path) = self.native_settings_catalog().await?;
        if managed {
            return Err(settings_error(anyhow::anyhow!(
                "Hermes configuration is managed by administrator policy"
            )));
        }
        to_value(alleycat_bridge_core::settings::write(&path, params).map_err(settings_error)?)
    }
    async fn handle_config_requirements_read(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(ConfigRequirementsReadResponse::default())
    }

    async fn handle_mcp_server_status_list(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(ListMcpServerStatusResponse::default())
    }

    async fn handle_config_mcp_server_reload(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(McpServerRefreshResponse::default())
    }

    async fn handle_mcp_server_oauth_login(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(McpServerOauthLoginResponse {
            authorization_url: String::new(),
        })
    }

    async fn handle_skills_list(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
        to_value(SkillsListResponse { data: vec![] })
    }

    async fn handle_skills_remote_list(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(SkillsListResponse { data: vec![] })
    }

    async fn handle_skills_remote_export(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        Ok(json!({}))
    }

    async fn handle_skills_config_write(
        &self,
        _ctx: &Conn,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(SkillsConfigWriteResponse {
            effective_enabled: params
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        })
    }

    async fn handle_command_exec(&self, _ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let p: CommandExecParams = serde_json::from_value(params)
            .map_err(|e| rpc_error(-32602, format!("Invalid params: {e}")))?;
        if p.command.is_empty() {
            return error_response(-32602, "command/exec requires a non-empty command");
        }
        if p.tty || p.stream_stdin || p.stream_stdout_stderr || p.process_id.is_some() {
            return error_response(
                -32602,
                "command/exec streaming and TTY modes are not supported by the Hermes bridge; use buffered exec",
            );
        }

        let mut cmd = Command::new(&p.command[0]);
        cmd.args(&p.command[1..]);
        if let Some(cwd) = p.cwd {
            cmd.current_dir(cwd);
        }
        if let Some(env) = p.env {
            for (key, value) in env {
                match value {
                    Some(value) => {
                        cmd.env(key, value);
                    }
                    None => {
                        cmd.env_remove(key);
                    }
                }
            }
        }

        let timeout_ms = if p.disable_timeout {
            None
        } else {
            Some(p.timeout_ms.unwrap_or(30_000).clamp(1, 300_000) as u64)
        };
        let output_result = if let Some(timeout_ms) = timeout_ms {
            tokio::time::timeout(Duration::from_millis(timeout_ms), cmd.output())
                .await
                .map_err(|_| rpc_error(-32603, "command/exec timed out"))?
        } else {
            cmd.output().await
        };
        let output = output_result
            .map_err(|e| rpc_error(-32603, format!("command/exec failed to spawn: {e}")))?;
        let mut stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let mut stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if !p.disable_output_cap {
            let cap = p.output_bytes_cap.unwrap_or(1_048_576);
            truncate_string(&mut stdout, cap);
            truncate_string(&mut stderr, cap);
        }
        to_value(CommandExecResponse {
            exit_code: output.status.code().unwrap_or(-1),
            stdout,
            stderr,
        })
    }

    async fn handle_command_exec_write(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(CommandExecWriteResponse::default())
    }

    async fn handle_command_exec_terminate(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(CommandExecTerminateResponse::default())
    }

    async fn handle_command_exec_resize(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(CommandExecResizeResponse::default())
    }

    async fn handle_mock_experimental_method(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        Ok(json!({}))
    }

    async fn handle_experimental_feature_list(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        Ok(json!({ "data": [], "nextCursor": null }))
    }

    async fn handle_collaboration_mode_list(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        Ok(json!({ "data": [] }))
    }

    async fn handle_feedback_upload(
        &self,
        _ctx: &Conn,
        _params: Value,
    ) -> Result<Value, JsonRpcError> {
        to_value(alleycat_codex_proto::account::FeedbackUploadResponse::default())
    }
}

impl HermesBridge {
    async fn dispatch_turn_api(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        session_id: &str,
        params: &TurnStartParams,
    ) -> Result<Value, JsonRpcError> {
        let text = user_text(&params.input);
        let cwd = params
            .cwd
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .or_else(|| {
                self.index
                    .get_by_thread(thread_id)
                    .and_then(|binding| binding.cwd)
            });
        let selection = params.model.clone().or_else(|| {
            self.index
                .get_by_thread(thread_id)
                .and_then(|binding| binding.model)
        });
        let (model, provider) = hermes_model_selection(selection.as_deref());
        let request = CreateRunRequest {
            input: text,
            session_id: Some(session_id.to_string()),
            cwd,
            model,
            provider,
        };
        let run = match self.api_client.create_run(request).await {
            Ok(run) => run,
            Err(e) => {
                let message = format!("Hermes API error: {e}");
                self.emit_turn_completed(ctx, thread_id, turn_id, None, Some(message.clone()))
                    .await;
                return error_response(-32603, &message);
            }
        };
        if let Some(ref hermes_session_id) = run.session_id {
            self.index.update_after_turn(
                thread_id,
                Some(hermes_session_id.clone()),
                params.model.clone(),
                None,
                None,
                epoch_ms(),
            );
            self.persist_index()?;
        }
        self.state.insert(
            thread_id.to_string(),
            ActiveTurn {
                turn_id: turn_id.to_string(),
                thread_id: thread_id.to_string(),
                hermes_session_id: session_id.to_string(),
                run_id: Some(run.run_id.clone()),
            },
        );

        let agent_item_id = format!("item_{}", random_hex(8));
        self.emit_agent_started(ctx, thread_id, turn_id, &agent_item_id)
            .await;
        let response_turn = in_progress_turn(turn_id);
        let auto_approve = matches!(params.approval_policy.as_ref(), Some(AskForApproval::Never));
        let bridge = self.clone();
        let ctx = ctx.clone();
        let thread_id = thread_id.to_string();
        let turn_id = turn_id.to_string();
        tokio::spawn(async move {
            bridge
                .pump_api_events(
                    ctx,
                    thread_id,
                    turn_id,
                    agent_item_id,
                    run.run_id,
                    auto_approve,
                )
                .await;
        });
        to_value(TurnStartResponse {
            turn: response_turn,
        })
    }

    async fn pump_api_events(
        &self,
        ctx: Conn,
        thread_id: String,
        turn_id: String,
        agent_item_id: String,
        run_id: String,
        auto_approve: bool,
    ) {
        let mut full_text = String::new();
        let mut terminal = false;
        let resp = match self.api_client.events_stream(&run_id).await {
            Ok(resp) => resp,
            Err(e) => {
                let message = format!("Hermes events error: {e}");
                self.emit_agent_completed(&ctx, &thread_id, &turn_id, &agent_item_id, "")
                    .await;
                self.emit_turn_completed(&ctx, &thread_id, &turn_id, None, Some(message))
                    .await;
                return;
            }
        };
        let mut body = String::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(e) => {
                    let message = format!("Hermes SSE error: {e}");
                    self.emit_agent_completed(
                        &ctx,
                        &thread_id,
                        &turn_id,
                        &agent_item_id,
                        &full_text,
                    )
                    .await;
                    self.emit_turn_completed(&ctx, &thread_id, &turn_id, None, Some(message))
                        .await;
                    return;
                }
            };
            body.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(idx) = body.find("\n\n") {
                let complete = body[..idx + 2].to_string();
                body = body[idx + 2..].to_string();
                for event in crate::sse::parse_sse_frames(&complete) {
                    if let Some(delta) = event.message_delta() {
                        full_text.push_str(&delta);
                        self.emit_agent_delta(&ctx, &thread_id, &turn_id, &agent_item_id, &delta)
                            .await;
                    } else if let Some(error) = event.terminal_error() {
                        self.emit_agent_completed(
                            &ctx,
                            &thread_id,
                            &turn_id,
                            &agent_item_id,
                            &full_text,
                        )
                        .await;
                        self.emit_turn_completed(
                            &ctx,
                            &thread_id,
                            &turn_id,
                            None,
                            Some(format!("Hermes API error: {error}")),
                        )
                        .await;
                        return;
                    } else if event.event == "approval.request" {
                        if auto_approve {
                            if let Err(e) = self.api_client.approve_run_once(&run_id).await {
                                let message = format!("Hermes approval error: {e}");
                                self.emit_agent_completed(
                                    &ctx,
                                    &thread_id,
                                    &turn_id,
                                    &agent_item_id,
                                    &full_text,
                                )
                                .await;
                                self.emit_turn_completed(
                                    &ctx,
                                    &thread_id,
                                    &turn_id,
                                    None,
                                    Some(message),
                                )
                                .await;
                                return;
                            }
                        } else if !auto_approve {
                            self.emit_agent_completed(
                                &ctx,
                                &thread_id,
                                &turn_id,
                                &agent_item_id,
                                &full_text,
                            )
                            .await;
                            self.emit_turn_completed(
                                &ctx,
                                &thread_id,
                                &turn_id,
                                None,
                                Some("Hermes approval required".to_string()),
                            )
                            .await;
                            return;
                        }
                    } else if event.is_terminal_success() {
                        if full_text.is_empty()
                            && let Some(output) = event.data.get("output").and_then(Value::as_str)
                        {
                            full_text.push_str(output);
                            self.emit_agent_delta(
                                &ctx,
                                &thread_id,
                                &turn_id,
                                &agent_item_id,
                                output,
                            )
                            .await;
                        }
                        terminal = true;
                    }
                }
                if terminal {
                    break;
                }
            }
            if terminal {
                break;
            }
        }
        if !terminal && !body.trim().is_empty() {
            for event in crate::sse::parse_sse_frames(&body) {
                if let Some(delta) = event.message_delta() {
                    full_text.push_str(&delta);
                    self.emit_agent_delta(&ctx, &thread_id, &turn_id, &agent_item_id, &delta)
                        .await;
                }
            }
        }
        self.emit_agent_completed(&ctx, &thread_id, &turn_id, &agent_item_id, &full_text)
            .await;
        self.emit_turn_completed(&ctx, &thread_id, &turn_id, Some(&full_text), None)
            .await;
    }

    async fn dispatch_turn_cli(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        params: &TurnStartParams,
    ) -> Result<Value, JsonRpcError> {
        let text = user_text(&params.input);
        let binding = self.index.get_by_thread(thread_id);
        let (bin, session_id) = match &self.config.mode {
            crate::config::HermesMode::Cli { bin }
            | crate::config::HermesMode::Auto { bin, .. } => (
                bin.clone().unwrap_or_else(|| "hermes".to_string()),
                binding.as_ref().and_then(|b| b.cli_session_id.clone()),
            ),
            crate::config::HermesMode::Api { .. } => ("hermes".to_string(), None),
        };
        let model = params
            .model
            .clone()
            .or_else(|| binding.as_ref().and_then(|b| b.model.clone()));
        let cwd = params
            .cwd
            .clone()
            .or_else(|| binding.and_then(|b| b.cwd.map(PathBuf::from)));
        match crate::cli_adapter::run_hermes_cli(
            &bin,
            &text,
            session_id.as_deref(),
            cwd.as_ref(),
            model.as_deref(),
        )
        .await
        {
            Ok(output) => {
                if let Some(mut binding) = self.index.get_by_thread(thread_id) {
                    binding.cli_session_id = output.session_id;
                    self.index.upsert(binding);
                    self.persist_index()?;
                }
                self.emit_synthetic_completion(
                    ctx,
                    thread_id,
                    turn_id,
                    output.stdout.trim_end_matches('\n'),
                )
                .await;
                to_value(TurnStartResponse {
                    turn: completed_turn(turn_id),
                })
            }
            Err(e) => {
                self.emit_turn_completed(
                    ctx,
                    thread_id,
                    turn_id,
                    None,
                    Some(format!("Hermes CLI error: {e}")),
                )
                .await;
                error_response(-32603, &format!("Hermes CLI error: {e}"))
            }
        }
    }

    async fn emit_user_message(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        input: &[UserInput],
    ) {
        let item_id = format!("item_{}", random_hex(8));
        let item = ThreadItem::UserMessage {
            id: item_id,
            content: input.to_vec(),
        };
        self.push_logged_item(thread_id, turn_id, item.clone());
        let _ = ctx.notifier().send_notification(
            "item/started",
            ItemStartedNotification {
                item: item.clone(),
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                parent_item_id: None,
            },
        );
        let _ = ctx.notifier().send_notification(
            "item/completed",
            ItemCompletedNotification {
                item,
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                parent_item_id: None,
            },
        );
    }

    async fn emit_agent_started(&self, ctx: &Conn, thread_id: &str, turn_id: &str, item_id: &str) {
        let item = ThreadItem::AgentMessage {
            id: item_id.to_string(),
            text: String::new(),
            phase: None,
            memory_citation: None,
        };
        let _ = ctx.notifier().send_notification(
            "item/started",
            ItemStartedNotification {
                item,
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                parent_item_id: None,
            },
        );
    }

    async fn emit_agent_delta(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        delta: &str,
    ) {
        let _ = ctx.notifier().send_notification(
            "item/agentMessage/delta",
            AgentMessageDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                item_id: item_id.to_string(),
                delta: delta.to_string(),
                parent_item_id: None,
            },
        );
    }

    async fn emit_agent_completed(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text: &str,
    ) {
        let item = ThreadItem::AgentMessage {
            id: item_id.to_string(),
            text: text.to_string(),
            phase: None,
            memory_citation: None,
        };
        self.push_logged_item(thread_id, turn_id, item.clone());
        let _ = ctx.notifier().send_notification(
            "item/completed",
            ItemCompletedNotification {
                item,
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                parent_item_id: None,
            },
        );
    }

    async fn emit_turn_completed(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        _text: Option<&str>,
        error: Option<String>,
    ) {
        let status = if error.is_some() {
            TurnStatus::Failed
        } else {
            TurnStatus::Completed
        };
        let turn = Turn {
            id: turn_id.to_string(),
            items: vec![],
            items_view: "full".to_string(),
            status,
            error: error
                .clone()
                .map(|message| alleycat_codex_proto::common::TurnError {
                    message,
                    codex_error_info: None,
                    additional_details: None,
                }),
            started_at: Some(epoch_ms()),
            completed_at: Some(epoch_ms()),
            duration_ms: None,
        };
        self.complete_logged_turn(thread_id, turn_id, error);
        let _ = ctx.notifier().send_notification(
            "turn/completed",
            TurnCompletedNotification {
                thread_id: thread_id.to_string(),
                turn,
            },
        );
        self.state.remove(thread_id);
    }

    async fn emit_synthetic_completion(
        &self,
        ctx: &Conn,
        thread_id: &str,
        turn_id: &str,
        text: &str,
    ) {
        let item_id = format!("item_{}", random_hex(8));
        self.emit_agent_started(ctx, thread_id, turn_id, &item_id)
            .await;
        self.emit_agent_delta(ctx, thread_id, turn_id, &item_id, text)
            .await;
        self.emit_agent_completed(ctx, thread_id, turn_id, &item_id, text)
            .await;
        self.emit_turn_completed(ctx, thread_id, turn_id, Some(text), None)
            .await;
    }
}

fn hermes_settings_path() -> anyhow::Result<std::path::PathBuf> {
    match std::env::var_os("HERMES_HOME") {
        Some(home) => Ok(std::path::PathBuf::from(home).join("config.yaml")),
        None => alleycat_bridge_core::settings::home_path(".hermes/config.yaml"),
    }
}
fn settings_error(e: anyhow::Error) -> JsonRpcError {
    JsonRpcError {
        code: -32602,
        message: e.to_string(),
        data: None,
    }
}

#[cfg(test)]
mod model_catalog_tests {
    use super::*;
    #[test]
    fn provider_inventory_and_dispatch_preserve_nested_model_ids() {
        let ids = gateway_model_ids(
            &json!({"provider":"openrouter","model":"anthropic/new-model","providers":[
                {"slug":"other","authenticated":true,"models":["same-name"]},
                {"slug":"openrouter","authenticated":true,"models":["anthropic/new-model"]},
                {"slug":"unauthenticated","authenticated":false,"models":["unavailable"]}
            ]}),
        )
        .unwrap();
        assert_eq!(ids[0], "hermes/openrouter/anthropic/new-model");
        assert_eq!(ids.len(), 2);
        assert_eq!(
            hermes_model_selection(Some(&ids[0])),
            (
                Some("anthropic/new-model".into()),
                Some("openrouter".into())
            )
        );
        assert_eq!(
            hermes_model_selection(Some("anthropic/new-model")),
            (Some("anthropic/new-model".into()), None)
        );
        assert_eq!(hermes_model_selection(Some("hermes-agent")), (None, None));
    }
    #[tokio::test]
    async fn unavailable_catalog_is_an_error_instead_of_default_only_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let registry = alleycat_bridge_core::SessionRegistry::new(Default::default());
        let conn = Conn::from_session(registry.get_or_create("test".into(), "hermes"));
        // Port allocated then closed to produce a deterministic connection refusal.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        for mode in [
            crate::config::HermesMode::Auto { api_base: endpoint, bin: Some("/missing-hermes-test".into()) },
            crate::config::HermesMode::Cli { bin: Some("/missing-hermes-test".into()) },
        ] {
            let bridge = HermesBridge::new(HermesBridgeConfig {
                mode, state_dir: Some(dir.path().to_string_lossy().into_owned()),
            });
            assert!(bridge.handle_model_list(&conn, json!({})).await.is_err());
        }
    }

    #[test]
    fn gateway_catalog_tracks_route_changes_without_fabricated_models() {
        assert_eq!(
            gateway_model_ids(
                &json!({"data":[{"id":"hermes-agent"},{"id":"new-route"},{"id":"new-route"}]})
            )
            .unwrap(),
            vec!["hermes-agent", "new-route"]
        );
        assert_eq!(
            gateway_model_ids(&json!({"data":[{"id":"replacement"}]})).unwrap(),
            vec!["replacement"]
        );
        assert!(gateway_model_ids(&json!({"data":[]})).is_err());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn cli_first_turn_and_restart_use_native_session_identity() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("hermes-fake");
        let args = dir.path().join("args.log");
        std::fs::write(&bin, format!(r#"#!/bin/sh
printf '%s\n' "$@" > '{}'
while [ "$#" -gt 0 ]; do
    if [ "$1" = --usage-file ]; then
        shift
        printf '%s' '{{"session_id":"native-session"}}' > "$1"
    fi
    shift
done
printf 'OK\n'
"#, args.display())).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = HermesBridgeConfig {
            mode: crate::config::HermesMode::Cli { bin: Some(bin.to_string_lossy().into()) },
            state_dir: Some(dir.path().to_string_lossy().into()),
        };
        let registry = alleycat_bridge_core::SessionRegistry::new(Default::default());
        let conn = Conn::from_session(registry.get_or_create("test".into(), "hermes"));
        let bridge = HermesBridge::new(config.clone());
        let start = bridge.handle_thread_start(&conn, json!({"cwd":dir.path()})).await.unwrap();
        let id = start["thread"]["id"].as_str().unwrap();
        let turn = json!({"threadId":id,"input":[{"type":"text","text":"hello"}]});
        bridge.handle_turn_start(&conn, turn.clone()).await.unwrap();
        assert!(!std::fs::read_to_string(&args).unwrap().contains("--resume"));
        drop(bridge);
        let bridge = HermesBridge::new(config);
        assert_eq!(bridge.index.get_by_thread(id).unwrap().cli_session_id.as_deref(), Some("native-session"));
        bridge.handle_turn_start(&conn, turn).await.unwrap();
        assert!(std::fs::read_to_string(args).unwrap().contains("--resume\nnative-session"));
    }
}
