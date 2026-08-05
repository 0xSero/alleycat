use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use alleycat_bridge_core::{
    Bridge, Conn, JsonRpcError, LocalLauncher, ProcessLauncher, ThreadIndex as CoreThreadIndex,
    encode_backwards_cursor, error_codes, resolve_list_limit,
};
use alleycat_codex_proto as p;
use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::Utc;
use dashmap::DashMap;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

use crate::command_exec;
use crate::index::{self, AmpSessionRef, IndexEntry, entry_to_thread};
use crate::process::{AmpProcess, AmpSpawnConfig, result_error_message};
use crate::state::{ConnectionState, ThreadDefaults};

const DEFAULT_AMP_BIN: &str = "amp";
const MODEL_PROVIDER: &str = "amp";
const DEFAULT_MODEL: &str = "medium";
const USER_AGENT: &str = concat!("alleycat-amp-bridge/", env!("CARGO_PKG_VERSION"));

pub type ThreadIndex = CoreThreadIndex<AmpSessionRef>;
type ActiveTurns = Arc<Mutex<HashMap<String, ActiveTurn>>>;

pub struct AmpBridge {
    amp_bin: PathBuf,
    launcher: Arc<dyn ProcessLauncher>,
    codex_home: PathBuf,
    transcripts_dir: PathBuf,
    thread_index: Arc<ThreadIndex>,
    per_conn: DashMap<String, Arc<ConnectionState>>,
    active_turns: ActiveTurns,
    dangerously_allow_all: bool,
}

pub struct AmpBridgeBuilder {
    amp_bin: Option<PathBuf>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    codex_home: Option<PathBuf>,
    transcripts_dir: Option<PathBuf>,
    dangerously_allow_all: bool,
}

#[derive(Clone)]
struct ActiveTurn {
    turn_id: String,
    process: Option<Arc<AmpProcess>>,
    started_at: i64,
}

struct ThreadStartShape {
    thread: p::Thread,
    model: String,
    model_provider: String,
    cwd: String,
    approval_policy: p::AskForApproval,
    approvals_reviewer: p::ApprovalsReviewer,
    sandbox: p::SandboxPolicy,
}

impl AmpBridge {
    pub fn builder() -> AmpBridgeBuilder {
        AmpBridgeBuilder::default()
    }

    pub fn codex_home(&self) -> &Path {
        &self.codex_home
    }

    fn per_conn(&self, ctx: &Conn) -> Arc<ConnectionState> {
        let session = ctx.session();
        let key = format!("{}:{}", session.agent, session.node_id);
        if let Some(existing) = self.per_conn.get(&key) {
            return Arc::clone(existing.value());
        }
        let state = Arc::new(ConnectionState::new(
            Arc::clone(ctx.session()),
            Arc::clone(&self.thread_index) as Arc<dyn crate::state::ThreadIndexHandle>,
            ThreadDefaults::default(),
            Some(Arc::clone(&self.launcher)),
        ));
        let entry = self
            .per_conn
            .entry(key)
            .or_insert_with(|| Arc::clone(&state));
        Arc::clone(entry.value())
    }
}

impl Default for AmpBridgeBuilder {
    fn default() -> Self {
        Self {
            amp_bin: None,
            launcher: None,
            codex_home: None,
            transcripts_dir: None,
            // Amp's documented default is to run tools without prompting.
            dangerously_allow_all: true,
        }
    }
}

impl AmpBridgeBuilder {
    pub fn agent_bin(mut self, bin: impl Into<PathBuf>) -> Self {
        self.amp_bin = Some(bin.into());
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

    pub fn transcripts_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.transcripts_dir = Some(dir.into());
        self
    }

    pub fn dangerously_allow_all(mut self, allow: bool) -> Self {
        self.dangerously_allow_all = allow;
        self
    }

    pub fn from_env(mut self) -> Self {
        if self.amp_bin.is_none() {
            if let Some(bin) = std::env::var_os("AMP_BRIDGE_AMP_BIN")
                .or_else(|| std::env::var_os("AMP_BRIDGE_BIN"))
            {
                self.amp_bin = Some(PathBuf::from(bin));
            }
        }
        if self.codex_home.is_none() {
            if let Some(home) = std::env::var_os("CODEX_HOME").filter(|v| !v.is_empty()) {
                self.codex_home = Some(PathBuf::from(home));
            }
        }
        if self.transcripts_dir.is_none() {
            if let Some(dir) =
                std::env::var_os("AMP_BRIDGE_TRANSCRIPTS_DIR").filter(|v| !v.is_empty())
            {
                self.transcripts_dir = Some(PathBuf::from(dir));
            }
        }
        if let Ok(value) = std::env::var("AMP_BRIDGE_DANGEROUSLY_ALLOW_ALL") {
            self.dangerously_allow_all = matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }
        self
    }

    pub async fn build(self) -> Result<Arc<AmpBridge>> {
        let codex_home = self.codex_home.unwrap_or_else(default_codex_home);
        tokio::fs::create_dir_all(&codex_home)
            .await
            .with_context(|| format!("creating {}", codex_home.display()))?;
        let transcripts_dir = self
            .transcripts_dir
            .unwrap_or_else(|| codex_home.join("amp-transcripts"));
        tokio::fs::create_dir_all(&transcripts_dir)
            .await
            .with_context(|| format!("creating {}", transcripts_dir.display()))?;

        let thread_index = index::open_and_hydrate(&codex_home).await?;

        Ok(Arc::new(AmpBridge {
            amp_bin: self
                .amp_bin
                .unwrap_or_else(|| PathBuf::from(DEFAULT_AMP_BIN)),
            launcher: self
                .launcher
                .unwrap_or_else(|| Arc::new(LocalLauncher) as Arc<dyn ProcessLauncher>),
            codex_home,
            transcripts_dir,
            thread_index,
            per_conn: DashMap::new(),
            active_turns: Arc::new(Mutex::new(HashMap::new())),
            dangerously_allow_all: self.dangerously_allow_all,
        }))
    }
}

#[async_trait]
impl Bridge for AmpBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let typed: p::InitializeParams = decode(params)?;
        let state = self.per_conn(ctx);
        state.set_capabilities(
            Some(typed.client_info.name),
            typed.client_info.title,
            Some(typed.client_info.version),
            typed.capabilities.as_ref(),
        );
        ok(p::InitializeResponse {
            user_agent: USER_AGENT.to_string(),
            codex_home: self.codex_home.to_string_lossy().into_owned(),
            platform_family: platform_family().to_string(),
            platform_os: std::env::consts::OS.to_string(),
        })
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let state = self.per_conn(ctx);
        match method {
            "account/read" => {
                let _typed: p::GetAccountParams = decode_or_default(params)?;
                ok(p::GetAccountResponse {
                    account: Some(p::Account::ApiKey {}),
                    requires_openai_auth: false,
                })
            }
            "account/rateLimits/read" => ok(p::GetAccountRateLimitsResponse::default()),
            "account/login/start" => {
                let _typed: p::LoginAccountParams = decode(params)?;
                ok(p::LoginAccountResponse::ApiKey {})
            }
            "account/login/cancel" => {
                let _typed: p::CancelLoginAccountParams = decode(params)?;
                ok(p::CancelLoginAccountResponse {
                    status: p::CancelLoginAccountStatus::NotFound,
                })
            }
            "account/logout" => ok(p::LogoutAccountResponse::default()),
            "feedback/upload" => {
                let _typed: p::FeedbackUploadParams = decode(params)?;
                ok(p::FeedbackUploadResponse::default())
            }
            "config/read" => {
                let typed: p::ConfigReadParams = decode_or_default(params)?;
                ok(p::ConfigReadResponse {
                    config: json!({
                        "model_provider": MODEL_PROVIDER,
                        "model": DEFAULT_MODEL,
                        "cwd": typed.cwd,
                        "approval_policy": if self.dangerously_allow_all { "never" } else { "on-request" },
                    }),
                    origins: HashMap::new(),
                    layers: typed.include_layers.then(Vec::new),
                })
            }
            "config/value/write" => {
                let _typed: p::ConfigValueWriteParams = decode(params)?;
                ok(config_write_response(&self.codex_home))
            }
            "config/batchWrite" => {
                let _typed: p::ConfigBatchWriteParams = decode(params)?;
                ok(config_write_response(&self.codex_home))
            }
            "configRequirements/read" => ok(p::ConfigRequirementsReadResponse::default()),
            "mcpServerStatus/list" => {
                let _typed: p::ListMcpServerStatusParams = decode_or_default(params)?;
                let data = state
                    .caches()
                    .mcp_servers
                    .into_iter()
                    .map(|(name, status)| p::McpServerStatus {
                        name,
                        tools: json!({}),
                        resources: Vec::new(),
                        resource_templates: Vec::new(),
                        auth_status: json!({"status": status}),
                    })
                    .collect();
                ok(p::ListMcpServerStatusResponse {
                    data,
                    next_cursor: None,
                })
            }
            "config/mcpServer/reload" => ok(p::McpServerRefreshResponse::default()),
            "mcpServer/oauth/login" => Err(method_not_found("mcpServer/oauth/login")),
            "mock/experimentalMethod" => {
                let typed: p::MockExperimentalMethodParams = decode_or_default(params)?;
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
                let typed: p::ModelListParams = decode_or_default(params)?;
                ok(p::ModelListResponse {
                    data: amp_models(typed.include_hidden.unwrap_or(false)),
                    next_cursor: None,
                })
            }
            "skills/list" => {
                let typed: p::SkillsListParams = decode_or_default(params)?;
                let data = typed
                    .cwds
                    .into_iter()
                    .map(|cwd| p::SkillsListEntry {
                        cwd,
                        skills: Vec::new(),
                        errors: Vec::new(),
                    })
                    .collect();
                ok(p::SkillsListResponse { data })
            }
            "skills/remote/list" => Ok(json!({"data":[],"nextCursor":null})),
            "skills/remote/export" => Ok(json!({"data":[],"nextCursor":null})),
            "skills/config/write" => {
                let typed: p::SkillsConfigWriteParams = decode(params)?;
                ok(p::SkillsConfigWriteResponse {
                    effective_enabled: typed.enabled,
                })
            }
            "command/exec" => {
                let typed: p::CommandExecParams = decode(params)?;
                command_exec::handle_command_exec(&state, typed)
                    .await
                    .map_err(|err| JsonRpcError {
                        code: err.rpc_code(),
                        message: err.to_string(),
                        data: None,
                    })
                    .and_then(ok)
            }
            "command/exec/terminate" => {
                let typed: p::CommandExecTerminateParams = decode(params)?;
                ok(command_exec::handle_command_exec_terminate(&state, typed).await)
            }
            "command/exec/write" => {
                let typed: p::CommandExecWriteParams = decode(params)?;
                command_exec::handle_command_exec_write(&state, typed)
                    .await
                    .map_err(|err| JsonRpcError {
                        code: err.rpc_code(),
                        message: err.to_string(),
                        data: None,
                    })
                    .and_then(ok)
            }
            "command/exec/resize" => {
                let typed: p::CommandExecResizeParams = decode(params)?;
                command_exec::handle_command_exec_resize(&state, typed)
                    .await
                    .map_err(|err| JsonRpcError {
                        code: err.rpc_code(),
                        message: err.to_string(),
                        data: None,
                    })
                    .and_then(ok)
            }
            "thread/start" => {
                let typed: p::ThreadStartParams = decode(params)?;
                self.handle_thread_start(&state, typed).await
            }
            "thread/resume" => {
                let typed: p::ThreadResumeParams = decode(params)?;
                self.handle_thread_resume(&state, typed).await
            }
            "thread/fork" => {
                let _typed: p::ThreadForkParams = decode(params)?;
                Err(method_not_found("thread/fork"))
            }
            "thread/archive" => {
                let typed: p::ThreadArchiveParams = decode(params)?;
                self.handle_thread_archive(&state, typed).await
            }
            "thread/unarchive" => {
                let typed: p::ThreadUnarchiveParams = decode(params)?;
                self.handle_thread_unarchive(&state, typed).await
            }
            "thread/name/set" => {
                let typed: p::ThreadSetNameParams = decode(params)?;
                self.handle_thread_name_set(&state, typed).await
            }
            "thread/compact/start" => {
                let _typed: p::ThreadCompactStartParams = decode(params)?;
                ok(p::ThreadCompactStartResponse::default())
            }
            "thread/rollback" => {
                let _typed: p::ThreadRollbackParams = decode(params)?;
                Err(method_not_found("thread/rollback"))
            }
            "thread/list" => {
                let typed: p::ThreadListParams = decode_or_default(params)?;
                self.handle_thread_list(typed).await
            }
            "thread/loaded/list" => {
                let _typed: p::ThreadLoadedListParams = decode_or_default(params)?;
                let data = self.active_turns.lock().await.keys().cloned().collect();
                ok(p::ThreadLoadedListResponse {
                    data,
                    next_cursor: None,
                })
            }
            "thread/read" => {
                let typed: p::ThreadReadParams = decode(params)?;
                self.handle_thread_read(&state, typed).await
            }
            "thread/turns/list" => {
                let typed: p::ThreadTurnsListParams = decode(params)?;
                self.handle_thread_turns_list(&state, typed).await
            }
            "thread/backgroundTerminals/clean" => {
                let _typed: p::ThreadBackgroundTerminalsCleanParams = decode(params)?;
                ok(p::ThreadBackgroundTerminalsCleanResponse::default())
            }
            "turn/start" => {
                let typed: p::TurnStartParams = decode(params)?;
                self.handle_turn_start(&state, typed).await
            }
            "turn/steer" => {
                let typed: p::TurnSteerParams = decode(params)?;
                self.handle_turn_steer(&state, typed).await
            }
            "turn/interrupt" => {
                let typed: p::TurnInterruptParams = decode(params)?;
                self.handle_turn_interrupt(&state, typed).await
            }
            "review/start" => {
                let _typed: p::ReviewStartParams = decode(params)?;
                Err(method_not_found("review/start"))
            }
            other => Err(method_not_found(other)),
        }
    }

    async fn notification(&self, _ctx: &Conn, method: &str, params: Value) {
        tracing::debug!(method, params = %params, "amp bridge ignored client notification");
    }
}

impl AmpBridge {
    async fn handle_thread_start(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadStartParams,
    ) -> Result<Value, JsonRpcError> {
        let cwd = resolve_cwd(params.cwd.as_deref())?;
        let now = Utc::now().timestamp_millis();
        let thread_id = Uuid::now_v7().to_string();
        let model = normalize_model(params.model.as_deref());
        let approval_policy = params
            .approval_policy
            .clone()
            .unwrap_or_else(|| default_approval_policy(self.dangerously_allow_all));
        let approvals_reviewer = params
            .approvals_reviewer
            .unwrap_or(p::ApprovalsReviewer::AutoReview);
        let sandbox_mode = params.sandbox.unwrap_or(p::SandboxMode::WorkspaceWrite);
        state.update_defaults(|defaults| {
            defaults.model = Some(model.clone());
            defaults.model_provider = Some(MODEL_PROVIDER.to_string());
            defaults.approval_policy = Some(approval_policy.clone());
            defaults.approvals_reviewer = Some(approvals_reviewer);
            defaults.sandbox = Some(sandbox_mode);
            defaults.service_name = params.service_name.clone();
            defaults.system_prompt = params
                .developer_instructions
                .clone()
                .or(params.base_instructions.clone());
        });

        let entry = IndexEntry {
            thread_id: thread_id.clone(),
            cwd: cwd.clone(),
            created_at: now,
            updated_at: now,
            archived: false,
            name: None,
            preview: "(new Amp thread)".to_string(),
            forked_from_id: None,
            model_provider: MODEL_PROVIDER.to_string(),
            source: p::ThreadSourceKind::AppServer,
            metadata: AmpSessionRef {
                amp_thread_id: None,
                amp_thread_path: Some(transcript_path(&self.transcripts_dir, &thread_id)),
                model: Some(model.clone()),
                reasoning_effort: None,
            },
        };
        self.thread_index
            .insert(entry.clone())
            .await
            .map_err(|err| internal(err.to_string()))?;

        let shape = ThreadStartShape {
            thread: entry_to_thread(&entry),
            model,
            model_provider: MODEL_PROVIDER.to_string(),
            cwd,
            approval_policy,
            approvals_reviewer,
            sandbox: sandbox_policy(sandbox_mode),
        };
        emit(
            state,
            p::ServerNotification::ThreadStarted(p::ThreadStartedNotification {
                thread: shape.thread.clone(),
            }),
        );
        ok(thread_start_response(shape))
    }

    async fn handle_thread_resume(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadResumeParams,
    ) -> Result<Value, JsonRpcError> {
        let Some(mut entry) = self.thread_index.lookup(&params.thread_id).await else {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        };
        let mut thread = entry_to_thread(&entry);
        if !params.exclude_turns {
            thread.turns = self
                .all_turns(state, &params.thread_id)
                .await
                .map_err(|err| internal(err.to_string()))?;
        }
        let defaults = state.defaults();
        let old_model = entry.metadata.model.clone();
        let model = normalize_model(
            params
                .model
                .as_deref()
                .or(entry.metadata.model.as_deref())
                .or(defaults.model.as_deref()),
        );
        let model_changed = old_model.as_deref() != Some(model.as_str());
        if model_changed {
            entry.metadata.model = Some(model.clone());
            self.thread_index
                .insert(entry.clone())
                .await
                .map_err(|err| internal(err.to_string()))?;
        }
        let requested_approval_policy = params.approval_policy.clone();
        let requested_approvals_reviewer = params.approvals_reviewer;
        let requested_sandbox_mode = params.sandbox;
        let approval_policy = requested_approval_policy
            .clone()
            .or(defaults.approval_policy.clone())
            .unwrap_or_else(|| default_approval_policy(self.dangerously_allow_all));
        let approvals_reviewer = requested_approvals_reviewer
            .or(defaults.approvals_reviewer)
            .unwrap_or(p::ApprovalsReviewer::AutoReview);
        let sandbox_mode = requested_sandbox_mode
            .or(defaults.sandbox)
            .unwrap_or(p::SandboxMode::WorkspaceWrite);
        if requested_approval_policy.is_some()
            || requested_approvals_reviewer.is_some()
            || requested_sandbox_mode.is_some()
        {
            state.update_defaults(|defaults| {
                if requested_approval_policy.is_some() {
                    defaults.approval_policy = Some(approval_policy.clone());
                }
                if requested_approvals_reviewer.is_some() {
                    defaults.approvals_reviewer = Some(approvals_reviewer);
                }
                if requested_sandbox_mode.is_some() {
                    defaults.sandbox = Some(sandbox_mode);
                }
            });
        }
        ok(p::ThreadResumeResponse {
            thread,
            model,
            model_provider: MODEL_PROVIDER.to_string(),
            service_tier: params.service_tier,
            cwd: params.cwd.unwrap_or(entry.cwd),
            instruction_sources: Vec::new(),
            approval_policy,
            approvals_reviewer,
            sandbox: sandbox_policy(sandbox_mode),
            permission_profile: params.permission_profile,
            active_permission_profile: None,
            reasoning_effort: None,
        })
    }

    async fn handle_thread_archive(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadArchiveParams,
    ) -> Result<Value, JsonRpcError> {
        let changed = self
            .thread_index
            .set_archived(&params.thread_id, true)
            .await
            .map_err(|err| internal(err.to_string()))?;
        if !changed {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        }
        emit(
            state,
            p::ServerNotification::ThreadArchived(p::ThreadIdOnly {
                thread_id: params.thread_id,
            }),
        );
        ok(p::ThreadArchiveResponse::default())
    }

    async fn handle_thread_unarchive(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadUnarchiveParams,
    ) -> Result<Value, JsonRpcError> {
        let changed = self
            .thread_index
            .set_archived(&params.thread_id, false)
            .await
            .map_err(|err| internal(err.to_string()))?;
        if !changed {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        }
        emit(
            state,
            p::ServerNotification::ThreadUnarchived(p::ThreadIdOnly {
                thread_id: params.thread_id.clone(),
            }),
        );
        let entry = self
            .thread_index
            .lookup(&params.thread_id)
            .await
            .ok_or_else(|| internal("thread disappeared after unarchive"))?;
        ok(p::ThreadUnarchiveResponse {
            thread: entry_to_thread(&entry),
        })
    }

    async fn handle_thread_name_set(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadSetNameParams,
    ) -> Result<Value, JsonRpcError> {
        let name = (!params.name.trim().is_empty()).then(|| params.name.trim().to_string());
        let changed = self
            .thread_index
            .set_name(&params.thread_id, name.clone())
            .await
            .map_err(|err| internal(err.to_string()))?;
        if !changed {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        }
        emit(
            state,
            p::ServerNotification::ThreadNameUpdated(p::ThreadNameUpdatedNotification {
                thread_id: params.thread_id,
                thread_name: name,
            }),
        );
        ok(p::ThreadSetNameResponse::default())
    }

    async fn handle_thread_list(&self, params: p::ThreadListParams) -> Result<Value, JsonRpcError> {
        let filter = list_filter(&params);
        let sort = index::ListSort {
            key: params.sort_key.unwrap_or(p::ThreadSortKey::UpdatedAt),
            direction: params.sort_direction.unwrap_or(p::SortDirection::Desc),
        };
        let limit = Some(resolve_list_limit(params.limit));
        let page = self
            .thread_index
            .list(&filter, sort, params.cursor.as_deref(), limit)
            .await
            .map_err(|err| invalid_params(err.to_string()))?;
        let backwards_cursor = page
            .data
            .first()
            .map(|entry| encode_backwards_cursor(entry, sort));
        ok(p::ThreadListResponse {
            data: page.data.iter().map(entry_to_thread).collect(),
            next_cursor: page.next_cursor,
            backwards_cursor,
        })
    }

    async fn handle_thread_read(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadReadParams,
    ) -> Result<Value, JsonRpcError> {
        let Some(entry) = self.thread_index.lookup(&params.thread_id).await else {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        };
        let mut thread = entry_to_thread(&entry);
        if params.include_turns {
            thread.turns = self
                .all_turns(state, &params.thread_id)
                .await
                .map_err(|err| internal(err.to_string()))?;
        }
        ok(p::ThreadReadResponse { thread })
    }

    async fn handle_thread_turns_list(
        &self,
        state: &Arc<ConnectionState>,
        params: p::ThreadTurnsListParams,
    ) -> Result<Value, JsonRpcError> {
        if self.thread_index.lookup(&params.thread_id).await.is_none() {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        }
        let mut turns = self
            .all_turns(state, &params.thread_id)
            .await
            .map_err(|err| internal(err.to_string()))?;
        if params.sort_direction == Some(p::SortDirection::Desc) {
            turns.reverse();
        }
        let start = params
            .cursor
            .as_deref()
            .and_then(|cursor| cursor.parse::<usize>().ok())
            .unwrap_or(0)
            .min(turns.len());
        let limit = resolve_list_limit(params.limit) as usize;
        let end = (start + limit).min(turns.len());
        let next_cursor = (end < turns.len()).then(|| end.to_string());
        ok(p::ThreadTurnsListResponse {
            data: turns[start..end].to_vec(),
            next_cursor,
            backwards_cursor: (start > 0).then(|| start.saturating_sub(limit).to_string()),
        })
    }

    async fn handle_turn_start(
        &self,
        state: &Arc<ConnectionState>,
        params: p::TurnStartParams,
    ) -> Result<Value, JsonRpcError> {
        let Some(mut entry) = self.thread_index.lookup(&params.thread_id).await else {
            return Err(invalid_params(format!(
                "unknown amp thread `{}`",
                params.thread_id
            )));
        };
        let cwd = params
            .cwd
            .clone()
            .unwrap_or_else(|| PathBuf::from(&entry.cwd));
        let input = translate_user_input(&params.input, false)
            .map_err(|err| invalid_params(format!("input translation failed: {err}")))?;
        let defaults = state.defaults();
        let old_model = entry.metadata.model.clone();
        let mode = normalize_model(
            params
                .model
                .as_deref()
                .or(entry.metadata.model.as_deref())
                .or(defaults.model.as_deref()),
        );
        let entry_changed = old_model.as_deref() != Some(mode.as_str());
        if entry_changed {
            entry.metadata.model = Some(mode.clone());
        }
        let approval_policy = params
            .approval_policy
            .clone()
            .or(defaults.approval_policy.clone())
            .unwrap_or_else(|| default_approval_policy(self.dangerously_allow_all));
        let dangerously_allow_all = matches!(approval_policy, p::AskForApproval::Never);
        let turn_id = Uuid::now_v7().to_string();
        let started_at = now_secs();
        reserve_active_turn(
            &self.active_turns,
            &params.thread_id,
            turn_id.clone(),
            started_at,
        )
        .await?;

        if entry_changed {
            if let Err(err) = self.thread_index.insert(entry.clone()).await {
                remove_active_turn_if_matches(&self.active_turns, &params.thread_id, &turn_id)
                    .await;
                return Err(internal(err.to_string()));
            }
        }

        let process = match AmpProcess::launch(
            Arc::clone(&self.launcher),
            AmpSpawnConfig {
                amp_bin: self.amp_bin.clone(),
                cwd,
                amp_thread_id: entry.metadata.amp_thread_id.clone(),
                mode,
                dangerously_allow_all,
            },
        )
        .await
        {
            Ok(process) => process,
            Err(err) => {
                remove_active_turn_if_matches(&self.active_turns, &params.thread_id, &turn_id)
                    .await;
                return Err(internal(format!("spawning amp: {err:#}")));
            }
        };

        let events_rx = process.subscribe();
        if !attach_active_turn_process(
            &self.active_turns,
            &params.thread_id,
            &turn_id,
            Arc::clone(&process),
        )
        .await
        {
            process.shutdown().await;
            return Err(internal(
                "active turn reservation disappeared before amp launch completed",
            ));
        }

        if let Err(err) = process.send_serialized(&input).await {
            remove_active_turn_if_matches(&self.active_turns, &params.thread_id, &turn_id).await;
            process.shutdown().await;
            return Err(internal(err.to_string()));
        }

        let turn_for_notif = p::Turn {
            id: turn_id.clone(),
            items: Vec::new(),
            items_view: p::default_items_view(),
            status: p::TurnStatus::InProgress,
            error: None,
            started_at: Some(started_at),
            completed_at: None,
            duration_ms: None,
        };
        let mut turn = turn_for_notif.clone();
        turn.started_at = None;

        emit_status(
            state,
            &params.thread_id,
            p::ThreadStatus::Active {
                active_flags: vec![],
            },
        );
        emit(
            state,
            p::ServerNotification::TurnStarted(p::TurnStartedNotification {
                thread_id: params.thread_id.clone(),
                turn: turn_for_notif,
            }),
        );
        state.record_turn_started(&params.thread_id, turn_id.clone(), started_at);

        let user_message_item = p::ThreadItem::UserMessage {
            id: Uuid::now_v7().to_string(),
            content: params.input.clone(),
        };
        emit_item_lifecycle(state, &params.thread_id, &turn_id, &user_message_item, None);
        state.record_item(&params.thread_id, &turn_id, user_message_item.clone());

        if let Some(preview) = first_text_preview(&params.input) {
            let _ = self
                .thread_index
                .update_preview_and_updated_at(&params.thread_id, preview, Utc::now())
                .await;
        }

        spawn_event_pump(EventPumpArgs {
            state: Arc::clone(state),
            thread_id: params.thread_id.clone(),
            turn_id: turn_id.clone(),
            process,
            events_rx,
            started_at,
            active_turns: Arc::clone(&self.active_turns),
            thread_index: Arc::clone(&self.thread_index),
            transcripts_dir: self.transcripts_dir.clone(),
            initial_items: vec![user_message_item],
        });

        ok(p::TurnStartResponse { turn })
    }

    async fn handle_turn_steer(
        &self,
        state: &Arc<ConnectionState>,
        params: p::TurnSteerParams,
    ) -> Result<Value, JsonRpcError> {
        let active = self
            .active_turns
            .lock()
            .await
            .get(&params.thread_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("no active turn for `{}`", params.thread_id)))?;
        if active.turn_id != params.expected_turn_id {
            return Err(invalid_params(format!(
                "expected_turn_id `{}` does not match active turn `{}`",
                params.expected_turn_id, active.turn_id
            )));
        }
        let process = active
            .process
            .clone()
            .ok_or_else(|| internal("active amp turn is still launching"))?;
        let input = translate_user_input(&params.input, true)
            .map_err(|err| invalid_params(format!("input translation failed: {err}")))?;
        process
            .send_serialized(&input)
            .await
            .map_err(|err| internal(err.to_string()))?;

        let item = p::ThreadItem::UserMessage {
            id: Uuid::now_v7().to_string(),
            content: params.input,
        };
        emit_item_lifecycle(state, &params.thread_id, &active.turn_id, &item, None);
        state.record_item(&params.thread_id, &active.turn_id, item);

        ok(p::TurnSteerResponse {
            turn_id: active.turn_id,
        })
    }

    async fn handle_turn_interrupt(
        &self,
        state: &Arc<ConnectionState>,
        params: p::TurnInterruptParams,
    ) -> Result<Value, JsonRpcError> {
        let active = {
            let mut active_turns = self.active_turns.lock().await;
            let active = active_turns
                .get(&params.thread_id)
                .cloned()
                .ok_or_else(|| {
                    invalid_params(format!("no active turn for `{}`", params.thread_id))
                })?;
            if active.turn_id != params.turn_id {
                return Err(invalid_params(format!(
                    "turn_id `{}` does not match active turn `{}`",
                    params.turn_id, active.turn_id
                )));
            }
            if active.process.is_none() {
                return Err(invalid_params(format!(
                    "turn `{}` is still launching",
                    params.turn_id
                )));
            }
            active_turns
                .remove(&params.thread_id)
                .expect("active turn exists")
        };
        let process = active
            .process
            .clone()
            .expect("validated active turn has process");
        process.shutdown().await;
        let completed_at = now_secs();
        let error = p::TurnError {
            message: "amp turn interrupted".to_string(),
            codex_error_info: None,
            additional_details: None,
        };
        let turn = p::Turn {
            id: active.turn_id.clone(),
            items: state
                .thread_log(&params.thread_id)
                .last()
                .map(|t| t.items.clone())
                .unwrap_or_default(),
            items_view: p::default_items_view(),
            status: p::TurnStatus::Interrupted,
            error: Some(error.clone()),
            started_at: Some(active.started_at),
            completed_at: Some(completed_at),
            duration_ms: Some((completed_at - active.started_at).max(0) * 1000),
        };
        state.record_turn_completed(
            &params.thread_id,
            &active.turn_id,
            completed_at,
            p::TurnStatus::Interrupted,
            Some(error),
        );
        emit_status(state, &params.thread_id, p::ThreadStatus::Idle);
        emit(
            state,
            p::ServerNotification::TurnCompleted(p::TurnCompletedNotification {
                thread_id: params.thread_id,
                turn,
            }),
        );
        ok(p::TurnInterruptResponse::default())
    }

    async fn all_turns(
        &self,
        state: &Arc<ConnectionState>,
        thread_id: &str,
    ) -> Result<Vec<p::Turn>> {
        let mut turns =
            load_transcript_turns(&transcript_path(&self.transcripts_dir, thread_id)).await?;
        let seen = turns
            .iter()
            .map(|t| t.id.clone())
            .collect::<std::collections::HashSet<_>>();
        turns.extend(
            state
                .thread_log(thread_id)
                .into_iter()
                .filter(|t| !seen.contains(&t.id)),
        );
        Ok(turns)
    }
}

async fn reserve_active_turn(
    active_turns: &ActiveTurns,
    thread_id: &str,
    turn_id: String,
    started_at: i64,
) -> Result<(), JsonRpcError> {
    let mut active_turns = active_turns.lock().await;
    if active_turns.contains_key(thread_id) {
        return Err(invalid_params(format!(
            "thread `{thread_id}` already has an active amp turn"
        )));
    }
    active_turns.insert(
        thread_id.to_string(),
        ActiveTurn {
            turn_id,
            process: None,
            started_at,
        },
    );
    Ok(())
}

async fn attach_active_turn_process(
    active_turns: &ActiveTurns,
    thread_id: &str,
    turn_id: &str,
    process: Arc<AmpProcess>,
) -> bool {
    let mut active_turns = active_turns.lock().await;
    let Some(active) = active_turns.get_mut(thread_id) else {
        return false;
    };
    if active.turn_id != turn_id {
        return false;
    }
    active.process = Some(process);
    true
}

async fn remove_active_turn_if_matches(
    active_turns: &ActiveTurns,
    thread_id: &str,
    turn_id: &str,
) -> Option<ActiveTurn> {
    let mut active_turns = active_turns.lock().await;
    if active_turns
        .get(thread_id)
        .is_some_and(|active| active.turn_id == turn_id)
    {
        active_turns.remove(thread_id)
    } else {
        None
    }
}

struct EventPumpArgs {
    state: Arc<ConnectionState>,
    thread_id: String,
    turn_id: String,
    process: Arc<AmpProcess>,
    events_rx: broadcast::Receiver<Value>,
    started_at: i64,
    active_turns: ActiveTurns,
    thread_index: Arc<ThreadIndex>,
    transcripts_dir: PathBuf,
    initial_items: Vec<p::ThreadItem>,
}

fn spawn_event_pump(args: EventPumpArgs) {
    tokio::spawn(async move {
        run_event_pump(args).await;
    });
}

async fn run_event_pump(mut args: EventPumpArgs) {
    let mut translator = AmpTurnTranslator::new(
        args.thread_id.clone(),
        args.turn_id.clone(),
        args.initial_items,
    );
    let mut error_message: Option<String> = None;
    let mut result_seen = false;

    loop {
        let value = match args.events_rx.recv().await {
            Ok(value) => value,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(thread_id = %args.thread_id, turn_id = %args.turn_id, "amp event pump lagged by {n} events");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                error_message = Some("amp process exited before result".to_string());
                break;
            }
        };

        if value.get("type").and_then(Value::as_str) == Some("system")
            && value.get("subtype").and_then(Value::as_str) == Some("init")
        {
            args.state.refresh_init_cache(&value);
            if let Some(amp_thread_id) = value.get("session_id").and_then(Value::as_str) {
                let _ =
                    update_amp_thread_id(&args.thread_index, &args.thread_id, amp_thread_id).await;
            }
        }

        let outcome = translator.translate(&value);
        for item in outcome.completed_items {
            args.state.record_item(&args.thread_id, &args.turn_id, item);
        }
        for notification in outcome.notifications {
            emit(&args.state, notification);
        }
        if outcome.close_stdin {
            args.process.close_stdin().await;
        }
        if let Some(message) = outcome.error_message {
            error_message = Some(message);
        }
        if outcome.result_seen {
            result_seen = true;
            break;
        }
    }

    if !result_seen && error_message.is_none() {
        error_message = Some("amp process ended without a result envelope".to_string());
    }

    let completed_at = now_secs();
    let status = if error_message.is_some() {
        p::TurnStatus::Failed
    } else {
        p::TurnStatus::Completed
    };
    let error = error_message.map(|message| p::TurnError {
        message,
        codex_error_info: None,
        additional_details: None,
    });
    let turn = p::Turn {
        id: args.turn_id.clone(),
        items: translator.items,
        items_view: p::default_items_view(),
        status,
        error: error.clone(),
        started_at: Some(args.started_at),
        completed_at: Some(completed_at),
        duration_ms: Some((completed_at - args.started_at).max(0) * 1000),
    };

    let should_complete =
        remove_active_turn_if_matches(&args.active_turns, &args.thread_id, &args.turn_id)
            .await
            .is_some();
    args.process.shutdown().await;
    if should_complete {
        args.state.record_turn_completed(
            &args.thread_id,
            &args.turn_id,
            completed_at,
            status,
            error,
        );
        let _ = append_transcript_turn(
            &transcript_path(&args.transcripts_dir, &args.thread_id),
            &turn,
        )
        .await;
        if let Some(preview) = transcript_preview(&turn) {
            let _ = args
                .thread_index
                .update_preview_and_updated_at(&args.thread_id, preview, Utc::now())
                .await;
        }
        emit_status(&args.state, &args.thread_id, p::ThreadStatus::Idle);
        emit(
            &args.state,
            p::ServerNotification::TurnCompleted(p::TurnCompletedNotification {
                thread_id: args.thread_id,
                turn,
            }),
        );
    }
}

#[derive(Default)]
struct TranslateOutcome {
    notifications: Vec<p::ServerNotification>,
    completed_items: Vec<p::ThreadItem>,
    close_stdin: bool,
    result_seen: bool,
    error_message: Option<String>,
}

struct AmpTurnTranslator {
    thread_id: String,
    turn_id: String,
    items: Vec<p::ThreadItem>,
    pending_tools: HashMap<String, PendingTool>,
}

struct PendingTool {
    id: String,
    name: String,
    input: Value,
    parent_item_id: Option<String>,
}

impl AmpTurnTranslator {
    fn new(thread_id: String, turn_id: String, initial_items: Vec<p::ThreadItem>) -> Self {
        Self {
            thread_id,
            turn_id,
            items: initial_items,
            pending_tools: HashMap::new(),
        }
    }

    fn translate(&mut self, value: &Value) -> TranslateOutcome {
        match value.get("type").and_then(Value::as_str) {
            Some("assistant") => self.translate_assistant(value),
            Some("user") => self.translate_user(value),
            Some("result") => self.translate_result(value),
            _ => TranslateOutcome::default(),
        }
    }

    fn translate_assistant(&mut self, value: &Value) -> TranslateOutcome {
        let mut out = TranslateOutcome::default();
        let parent = value
            .get("parent_tool_use_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let content = value
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for block in content {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let text = block
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if text.is_empty() {
                        continue;
                    }
                    let item_id = Uuid::now_v7().to_string();
                    let started = p::ThreadItem::AgentMessage {
                        id: item_id.clone(),
                        text: String::new(),
                        phase: Some(Value::String("final_answer".to_string())),
                        memory_citation: None,
                    };
                    out.notifications.push(p::ServerNotification::ItemStarted(
                        p::ItemStartedNotification {
                            item: started,
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: parent.clone(),
                        },
                    ));
                    out.notifications
                        .push(p::ServerNotification::AgentMessageDelta(
                            p::AgentMessageDeltaNotification {
                                thread_id: self.thread_id.clone(),
                                turn_id: self.turn_id.clone(),
                                item_id: item_id.clone(),
                                delta: text.clone(),
                                parent_item_id: parent.clone(),
                            },
                        ));
                    let completed = p::ThreadItem::AgentMessage {
                        id: item_id,
                        text,
                        phase: Some(Value::String("final_answer".to_string())),
                        memory_citation: None,
                    };
                    self.items.push(completed.clone());
                    out.completed_items.push(completed.clone());
                    out.notifications.push(p::ServerNotification::ItemCompleted(
                        p::ItemCompletedNotification {
                            item: completed,
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: parent.clone(),
                        },
                    ));
                }
                Some("thinking") | Some("redacted_thinking") => {
                    let text = block
                        .get("thinking")
                        .or_else(|| block.get("data"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if text.is_empty() {
                        continue;
                    }
                    let item_id = Uuid::now_v7().to_string();
                    out.notifications.push(p::ServerNotification::ItemStarted(
                        p::ItemStartedNotification {
                            item: p::ThreadItem::Reasoning {
                                id: item_id.clone(),
                                summary: Vec::new(),
                                content: Vec::new(),
                            },
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: parent.clone(),
                        },
                    ));
                    out.notifications
                        .push(p::ServerNotification::ReasoningTextDelta(
                            p::ReasoningTextDeltaNotification {
                                thread_id: self.thread_id.clone(),
                                turn_id: self.turn_id.clone(),
                                item_id: item_id.clone(),
                                delta: text.clone(),
                                content_index: 0,
                                parent_item_id: parent.clone(),
                            },
                        ));
                    let completed = p::ThreadItem::Reasoning {
                        id: item_id,
                        summary: Vec::new(),
                        content: vec![text],
                    };
                    self.items.push(completed.clone());
                    out.completed_items.push(completed.clone());
                    out.notifications.push(p::ServerNotification::ItemCompleted(
                        p::ItemCompletedNotification {
                            item: completed,
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: parent.clone(),
                        },
                    ));
                }
                Some("tool_use") => {
                    let id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| Uuid::now_v7().to_string());
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
                    self.pending_tools.insert(
                        id.clone(),
                        PendingTool {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                            parent_item_id: parent.clone(),
                        },
                    );
                    out.notifications.push(p::ServerNotification::ItemStarted(
                        p::ItemStartedNotification {
                            item: dynamic_tool_item(
                                &id,
                                &name,
                                input,
                                p::DynamicToolCallStatus::InProgress,
                                None,
                                None,
                            ),
                            thread_id: self.thread_id.clone(),
                            turn_id: self.turn_id.clone(),
                            parent_item_id: parent.clone(),
                        },
                    ));
                }
                _ => {}
            }
        }
        if let Some(usage) = value.get("message").and_then(|m| m.get("usage")) {
            out.notifications.push(token_usage_notification(
                &self.thread_id,
                &self.turn_id,
                usage,
            ));
        }
        let stop_reason = value
            .get("message")
            .and_then(|m| m.get("stop_reason"))
            .and_then(Value::as_str);
        out.close_stdin = matches!(stop_reason, Some("end_turn" | "max_tokens") | None);
        out
    }

    fn translate_user(&mut self, value: &Value) -> TranslateOutcome {
        let mut out = TranslateOutcome::default();
        let parent = value
            .get("parent_tool_use_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let content = value
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for block in content {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            let Some(tool_use_id) = block.get("tool_use_id").and_then(Value::as_str) else {
                continue;
            };
            let content = block
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let is_error = block
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let pending = self
                .pending_tools
                .remove(tool_use_id)
                .unwrap_or(PendingTool {
                    id: tool_use_id.to_string(),
                    name: "unknown".to_string(),
                    input: json!({}),
                    parent_item_id: parent.clone(),
                });
            let item = dynamic_tool_item(
                &pending.id,
                &pending.name,
                pending.input,
                if is_error {
                    p::DynamicToolCallStatus::Failed
                } else {
                    p::DynamicToolCallStatus::Completed
                },
                Some(vec![json!({"type": "inputText", "text": content})]),
                Some(!is_error),
            );
            self.items.push(item.clone());
            out.completed_items.push(item.clone());
            out.notifications.push(p::ServerNotification::ItemCompleted(
                p::ItemCompletedNotification {
                    item,
                    thread_id: self.thread_id.clone(),
                    turn_id: self.turn_id.clone(),
                    parent_item_id: pending.parent_item_id,
                },
            ));
        }
        out
    }

    fn translate_result(&mut self, value: &Value) -> TranslateOutcome {
        let mut out = TranslateOutcome {
            result_seen: true,
            error_message: result_error_message(value),
            ..Default::default()
        };
        if let Some(usage) = value.get("usage") {
            out.notifications.push(token_usage_notification(
                &self.thread_id,
                &self.turn_id,
                usage,
            ));
        }
        for pending in self.pending_tools.drain().map(|(_, v)| v) {
            let item = dynamic_tool_item(
                &pending.id,
                &pending.name,
                pending.input,
                p::DynamicToolCallStatus::Failed,
                Some(vec![
                    json!({"type": "inputText", "text": "amp result arrived before tool result"}),
                ]),
                Some(false),
            );
            self.items.push(item.clone());
            out.completed_items.push(item.clone());
            out.notifications.push(p::ServerNotification::ItemCompleted(
                p::ItemCompletedNotification {
                    item,
                    thread_id: self.thread_id.clone(),
                    turn_id: self.turn_id.clone(),
                    parent_item_id: pending.parent_item_id,
                },
            ));
        }
        out
    }
}

fn dynamic_tool_item(
    id: &str,
    name: &str,
    arguments: Value,
    status: p::DynamicToolCallStatus,
    content_items: Option<Vec<Value>>,
    success: Option<bool>,
) -> p::ThreadItem {
    p::ThreadItem::DynamicToolCall {
        id: id.to_string(),
        namespace: Some(MODEL_PROVIDER.to_string()),
        tool: name.to_string(),
        arguments,
        status,
        content_items,
        success,
        duration_ms: None,
    }
}

fn token_usage_notification(
    thread_id: &str,
    turn_id: &str,
    usage: &Value,
) -> p::ServerNotification {
    let input = usage
        .get("input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let cached = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let breakdown = p::TokenUsageBreakdown {
        total_tokens: input + cached + output,
        input_tokens: input,
        cached_input_tokens: cached,
        output_tokens: output,
        reasoning_output_tokens: 0,
    };
    p::ServerNotification::ThreadTokenUsageUpdated(p::ThreadTokenUsageUpdatedNotification {
        thread_id: thread_id.to_string(),
        turn_id: turn_id.to_string(),
        token_usage: p::ThreadTokenUsage {
            total: breakdown.clone(),
            last: breakdown,
            model_context_window: usage.get("max_tokens").and_then(Value::as_i64),
        },
    })
}

fn translate_user_input(inputs: &[p::UserInput], steer: bool) -> Result<Value> {
    if inputs.is_empty() {
        anyhow::bail!("input vector is empty");
    }
    let mut content = Vec::new();
    for input in inputs {
        match input {
            p::UserInput::Text { text, .. } => content.push(json!({"type": "text", "text": text})),
            p::UserInput::Skill { name, .. } => {
                content.push(json!({"type": "text", "text": format!("/{name}")}))
            }
            p::UserInput::Mention { name, .. } => {
                content.push(json!({"type": "text", "text": format!("@{name}")}))
            }
            p::UserInput::Image { url } => content.push(image_from_data_url(url)?),
            p::UserInput::LocalImage { path } => content.push(image_from_local_file(path)?),
        }
    }
    let mut envelope = json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": content,
        }
    });
    if steer {
        envelope["steer"] = Value::Bool(true);
    }
    Ok(envelope)
}

fn image_from_data_url(url: &str) -> Result<Value> {
    let body = url
        .strip_prefix("data:")
        .context("data URL did not start with 'data:'")?;
    let (mime_section, payload) = body
        .split_once(',')
        .context("data URL missing comma separator")?;
    let (media_type, is_base64) = match mime_section.rsplit_once(';') {
        Some((mime, "base64")) => (mime, true),
        _ => (mime_section, false),
    };
    if !is_base64 {
        anyhow::bail!("data URL is not base64 encoded");
    }
    let cleaned: String = payload
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let bytes = BASE64_STANDARD.decode(cleaned.as_bytes())?;
    Ok(json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": BASE64_STANDARD.encode(bytes),
        }
    }))
}

fn image_from_local_file(path: &Path) -> Result<Value> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let media_type = guess_image_mime(path)
        .with_context(|| format!("could not infer image media type for {}", path.display()))?;
    Ok(json!({
        "type": "image",
        "source_path": format!("file://{}", path.display()),
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": BASE64_STANDARD.encode(bytes),
        }
    }))
}

fn guess_image_mime(path: &Path) -> Option<&'static str> {
    Some(
        match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            _ => return None,
        },
    )
}

async fn update_amp_thread_id(
    thread_index: &Arc<ThreadIndex>,
    thread_id: &str,
    amp_thread_id: &str,
) -> Result<()> {
    let Some(mut entry) = thread_index.lookup(thread_id).await else {
        return Ok(());
    };
    if entry.metadata.amp_thread_id.as_deref() == Some(amp_thread_id) {
        return Ok(());
    }
    entry.metadata.amp_thread_id = Some(amp_thread_id.to_string());
    thread_index.insert(entry).await
}

async fn append_transcript_turn(path: &Path, turn: &p::Turn) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let line = serde_json::to_vec(turn)?;
    file.write_all(&line).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    Ok(())
}

async fn load_transcript_turns(path: &Path) -> Result<Vec<p::Turn>> {
    let text = match tokio::fs::read_to_string(path).await {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut turns = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        turns.push(serde_json::from_str(line)?);
    }
    Ok(turns)
}

fn transcript_path(root: &Path, thread_id: &str) -> PathBuf {
    root.join(format!("{thread_id}.jsonl"))
}

fn transcript_preview(turn: &p::Turn) -> Option<String> {
    turn.items.iter().rev().find_map(|item| match item {
        p::ThreadItem::AgentMessage { text, .. } if !text.trim().is_empty() => {
            Some(text.lines().next().unwrap_or("").trim().to_string())
        }
        p::ThreadItem::UserMessage { content, .. } => first_text_preview(content),
        _ => None,
    })
}

fn first_text_preview(inputs: &[p::UserInput]) -> Option<String> {
    inputs.iter().find_map(|input| match input {
        p::UserInput::Text { text, .. } if !text.trim().is_empty() => {
            Some(text.lines().next().unwrap_or("").trim().to_string())
        }
        _ => None,
    })
}

fn emit_item_lifecycle(
    state: &Arc<ConnectionState>,
    thread_id: &str,
    turn_id: &str,
    item: &p::ThreadItem,
    parent_item_id: Option<String>,
) {
    emit(
        state,
        p::ServerNotification::ItemStarted(p::ItemStartedNotification {
            item: item.clone(),
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            parent_item_id: parent_item_id.clone(),
        }),
    );
    emit(
        state,
        p::ServerNotification::ItemCompleted(p::ItemCompletedNotification {
            item: item.clone(),
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            parent_item_id,
        }),
    );
}

fn emit_status(state: &Arc<ConnectionState>, thread_id: &str, status: p::ThreadStatus) {
    emit(
        state,
        p::ServerNotification::ThreadStatusChanged(p::ThreadStatusChangedNotification {
            thread_id: thread_id.to_string(),
            status,
        }),
    );
}

fn emit(state: &Arc<ConnectionState>, notif: p::ServerNotification) {
    let value = serde_json::to_value(&notif).expect("ServerNotification serializes");
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if !state.should_emit(&method) {
        return;
    }
    let msg = p::JsonRpcMessage::Notification(p::JsonRpcNotification {
        jsonrpc: p::JsonRpcVersion,
        method,
        params: value.get("params").cloned(),
    });
    let _ = state.send(msg);
}

fn list_filter(params: &p::ThreadListParams) -> index::ListFilter {
    index::ListFilter {
        archived: params.archived,
        cwds: params.cwd.as_ref().and_then(cwd_filter_values),
        search_term: params.search_term.clone(),
        model_providers: params.model_providers.clone(),
        source_kinds: params.source_kinds.clone(),
    }
}

fn cwd_filter_values(value: &Value) -> Option<Vec<String>> {
    if let Some(s) = value.as_str() {
        return Some(vec![s.to_string()]);
    }
    value.as_array().map(|values| {
        values
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()
    })
}

fn thread_start_response(shape: ThreadStartShape) -> p::ThreadStartResponse {
    p::ThreadStartResponse {
        thread: shape.thread,
        model: shape.model,
        model_provider: shape.model_provider,
        service_tier: None,
        cwd: shape.cwd,
        instruction_sources: Vec::new(),
        approval_policy: shape.approval_policy,
        approvals_reviewer: shape.approvals_reviewer,
        sandbox: shape.sandbox,
        permission_profile: None,
        active_permission_profile: None,
        reasoning_effort: None,
    }
}

fn config_write_response(codex_home: &Path) -> p::ConfigWriteResponse {
    p::ConfigWriteResponse {
        status: p::WriteStatus::Ok,
        version: "0".to_string(),
        file_path: codex_home
            .join("config.toml")
            .to_string_lossy()
            .into_owned(),
        overridden_metadata: None,
    }
}

fn amp_models(_include_hidden: bool) -> Vec<p::Model> {
    AMP_VISIBLE_MODES
        .into_iter()
        .map(|mode| (mode, false))
        .map(|(model, hidden)| p::Model {
            id: model.to_string(),
            model: model.to_string(),
            upgrade: None,
            upgrade_info: None,
            availability_nux: None,
            display_name: model.to_string(),
            description: amp_mode_description(model).to_string(),
            hidden,
            supported_reasoning_efforts: Vec::new(),
            default_reasoning_effort: p::ReasoningEffort::None,
            input_modalities: vec![
                Value::String("text".to_string()),
                Value::String("image".to_string()),
            ],
            supports_personality: false,
            additional_speed_tiers: Vec::new(),
            service_tiers: Vec::new(),
            is_default: model == DEFAULT_MODEL,
        })
        .collect()
}

const AMP_VISIBLE_MODES: [&str; 4] = ["low", "medium", "high", "ultra"];

fn normalize_model(model: Option<&str>) -> String {
    let mode = model
        .map(|value| {
            let lower = value.trim().to_ascii_lowercase();
            lower
                .trim_start_matches("amp/")
                .trim_start_matches("amp:")
                .to_string()
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());
    match mode.as_str() {
        "rush" => "low".to_string(),
        "smart" | "deep" => "medium".to_string(),
        "large" => "ultra".to_string(),
        _ if is_supported_amp_mode(&mode) => mode,
        _ => DEFAULT_MODEL.to_string(),
    }
}

fn is_supported_amp_mode(mode: &str) -> bool {
    AMP_VISIBLE_MODES.contains(&mode)
}

fn amp_mode_description(mode: &str) -> &'static str {
    match mode {
        "low" => "Fast responses for simple, well-defined tasks.",
        "medium" => "Balanced performance for everyday coding tasks.",
        "high" => "More reasoning for complex coding tasks.",
        "ultra" => "Maximum capability for the hardest coding tasks.",
        _ => "Amp agent mode",
    }
}

fn default_approval_policy(dangerously_allow_all: bool) -> p::AskForApproval {
    if dangerously_allow_all {
        p::AskForApproval::Never
    } else {
        p::AskForApproval::OnRequest
    }
}

fn sandbox_policy(mode: p::SandboxMode) -> p::SandboxPolicy {
    match mode {
        p::SandboxMode::ReadOnly => json!({"type": "readOnly"}),
        p::SandboxMode::WorkspaceWrite => json!({"type": "workspaceWrite"}),
        p::SandboxMode::DangerFullAccess => json!({"type": "dangerFullAccess"}),
    }
}

fn resolve_cwd(input: Option<&str>) -> Result<String, JsonRpcError> {
    let path = match input.filter(|s| !s.trim().is_empty()) {
        Some(path) => PathBuf::from(path),
        None => std::env::current_dir().map_err(|err| internal(err.to_string()))?,
    };
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|err| internal(err.to_string()))?
            .join(path)
    };
    Ok(path.to_string_lossy().into_owned())
}

fn default_codex_home() -> PathBuf {
    if let Some(home) = directories::UserDirs::new() {
        return home.home_dir().join(".codex").join("amp-bridge");
    }
    PathBuf::from(".codex").join("amp-bridge")
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn platform_family() -> &'static str {
    if cfg!(windows) {
        "windows"
    } else if cfg!(unix) {
        "unix"
    } else {
        "unknown"
    }
}

fn ok<T: serde::Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|err| internal(err.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, JsonRpcError> {
    serde_json::from_value(value).map_err(|err| invalid_params(err.to_string()))
}

fn decode_or_default<T>(value: Value) -> Result<T, JsonRpcError>
where
    T: serde::de::DeserializeOwned + Default,
{
    if value.is_null() {
        Ok(T::default())
    } else {
        decode(value)
    }
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

fn method_not_found(method: &str) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: format!("method `{method}` is not implemented"),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_current_and_legacy_amp_modes() {
        for mode in AMP_VISIBLE_MODES {
            assert_eq!(normalize_model(Some(mode)), mode);
        }
        assert_eq!(normalize_model(None), "medium");
        assert_eq!(normalize_model(Some("rush")), "low");
        assert_eq!(normalize_model(Some("smart")), "medium");
        assert_eq!(normalize_model(Some("deep")), "medium");
        assert_eq!(normalize_model(Some("large")), "ultra");
        assert_eq!(normalize_model(Some("amp:SMART")), "medium");
    }

    #[test]
    fn stream_input_uses_only_documented_fields() {
        let envelope = translate_user_input(
            &[p::UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            false,
        )
        .unwrap();
        let fields = envelope.as_object().unwrap().keys().collect::<Vec<_>>();
        assert_eq!(fields, vec!["message", "type"]);

        let steered = translate_user_input(
            &[p::UserInput::Text {
                text: "more".to_string(),
                text_elements: Vec::new(),
            }],
            true,
        )
        .unwrap();
        let fields = steered.as_object().unwrap().keys().collect::<Vec<_>>();
        assert_eq!(fields, vec!["message", "steer", "type"]);
    }
}
