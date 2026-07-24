use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use alleycat_bridge_core::{
    Bridge, ChildProcess, Conn, JsonRpcError, LocalLauncher, ProcessLauncher, ProcessRole,
    ProcessSpec, StdioMode, error_codes,
};
use alleycat_codex_proto as p;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::index::{self, DroidHydrator, DroidSessionRef, IndexEntry, ListFilter, ListSort};
use crate::process::{DroidProcess, DroidSpawnConfig};
use crate::translate::{CompletedTurn, DroidTurnTranslator};

const DEFAULT_DROID_BIN: &str = "droid";
const MODEL_PROVIDER: &str = "droid";
const DEFAULT_MODEL: &str = "claude-sonnet-4-5-20250929";
const USER_AGENT: &str = concat!("alleycat-droid-bridge/", env!("CARGO_PKG_VERSION"));
const DEFAULT_OUTPUT_BYTES_CAP: usize = 256 * 1024;
const DEFAULT_TIMEOUT_MS: i64 = 60_000;

pub struct DroidBridge {
    droid_bin: PathBuf,
    launcher: Arc<dyn ProcessLauncher>,
    codex_home: PathBuf,
    thread_index: Arc<index::ThreadIndex>,
    factory_sessions_dir: Option<PathBuf>,
    threads: Arc<Mutex<HashMap<String, ThreadRecord>>>,
    processes: Arc<Mutex<HashMap<String, Arc<DroidProcess>>>>,
    initial_hydration_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
struct ThreadRecord {
    id: String,
    cwd: String,
    name: Option<String>,
    preview: String,
    created_at: i64,
    updated_at: i64,
    path: Option<String>,
    model: String,
    approval_policy: p::AskForApproval,
    sandbox: p::SandboxPolicy,
    turns: Vec<Value>,
}

#[derive(Default)]
pub struct DroidBridgeBuilder {
    agent_bin: Option<PathBuf>,
    launcher: Option<Arc<dyn ProcessLauncher>>,
    codex_home: Option<PathBuf>,
    factory_sessions_dir: Option<PathBuf>,
    defer_initial_hydration: bool,
}

impl DroidBridge {
    pub fn builder() -> DroidBridgeBuilder {
        DroidBridgeBuilder::default()
    }
}

impl DroidBridgeBuilder {
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

    pub fn factory_sessions_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.factory_sessions_dir = Some(dir.into());
        self
    }

    /// Open the persisted thread index immediately and hydrate Factory's
    /// session history in the background. The default remains eager so
    /// standalone bridge callers keep their existing startup contract.
    pub fn defer_initial_hydration(mut self, enabled: bool) -> Self {
        self.defer_initial_hydration = enabled;
        self
    }

    pub fn from_env(mut self) -> Self {
        if self.agent_bin.is_none() {
            if let Some(bin) = std::env::var_os("DROID_BRIDGE_DROID_BIN")
                .or_else(|| std::env::var_os("DROID_BRIDGE_BIN"))
            {
                self.agent_bin = Some(PathBuf::from(bin));
            }
        }
        if self.codex_home.is_none() {
            if let Some(home) = std::env::var_os("CODEX_HOME").filter(|v| !v.is_empty()) {
                self.codex_home = Some(PathBuf::from(home));
            }
        }
        if self.factory_sessions_dir.is_none() {
            if let Some(dir) =
                std::env::var_os("DROID_BRIDGE_FACTORY_SESSIONS_DIR").filter(|v| !v.is_empty())
            {
                self.factory_sessions_dir = Some(PathBuf::from(dir));
            }
        }
        self
    }

    pub async fn build(self) -> Result<Arc<DroidBridge>> {
        let codex_home = self.codex_home.unwrap_or_else(default_codex_home);
        if let Err(err) = tokio::fs::create_dir_all(&codex_home).await {
            tracing::warn!(?codex_home, %err, "failed to create codex_home; continuing");
        }
        let factory_sessions_dir = self.factory_sessions_dir;
        let (thread_index, initial_hydration_task) = if self.defer_initial_hydration {
            let thread_index = index::ThreadIndex::open_at(codex_home.join("threads.json")).await?;
            let hydration_index = Arc::clone(&thread_index);
            let hydrator = match factory_sessions_dir.clone() {
                Some(dir) => DroidHydrator::with_sessions_dir(dir),
                None => DroidHydrator::new(),
            };
            let task = tokio::spawn(async move {
                let started = std::time::Instant::now();
                match hydration_index.hydrate_from(&hydrator).await {
                    Ok(added) => {
                        tracing::info!(
                            added,
                            elapsed_ms = started.elapsed().as_millis(),
                            "deferred droid hydration completed"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(%err, "deferred droid hydration failed");
                    }
                }
            });
            (thread_index, Some(task))
        } else {
            (
                index::open_and_hydrate(&codex_home, factory_sessions_dir.clone()).await?,
                None,
            )
        };
        Ok(Arc::new(DroidBridge {
            droid_bin: self
                .agent_bin
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DROID_BIN)),
            launcher: self
                .launcher
                .unwrap_or_else(|| Arc::new(LocalLauncher) as Arc<dyn ProcessLauncher>),
            codex_home,
            thread_index,
            factory_sessions_dir,
            threads: Arc::new(Mutex::new(HashMap::new())),
            processes: Arc::new(Mutex::new(HashMap::new())),
            initial_hydration_task: Mutex::new(initial_hydration_task),
        }))
    }
}

#[async_trait]
impl Bridge for DroidBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        ctx.set_initialize_capabilities(&params);
        let _typed: p::InitializeParams = decode(params)?;
        ok(p::InitializeResponse {
            user_agent: USER_AGENT.to_string(),
            codex_home: self.codex_home.to_string_lossy().into_owned(),
            platform_family: platform_family().to_string(),
            platform_os: platform_os().to_string(),
        })
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "account/read" => {
                let _typed: p::GetAccountParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
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
                let typed: p::ConfigReadParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                let config = json!({
                    "model_provider": MODEL_PROVIDER,
                    "model": DEFAULT_MODEL,
                    "cwd": typed.cwd,
                });
                ok(p::ConfigReadResponse {
                    config,
                    origins: HashMap::new(),
                    layers: typed.include_layers.then(Vec::new),
                })
            }
            "config/value/write" => {
                let _typed: p::ConfigValueWriteParams = decode(params)?;
                ok(p::ConfigWriteResponse {
                    status: p::WriteStatus::Ok,
                    version: "0".to_string(),
                    file_path: self
                        .codex_home
                        .join("config.toml")
                        .to_string_lossy()
                        .into_owned(),
                    overridden_metadata: None,
                })
            }
            "config/batchWrite" => {
                let _typed: p::ConfigBatchWriteParams = decode(params)?;
                ok(p::ConfigWriteResponse {
                    status: p::WriteStatus::Ok,
                    version: "0".to_string(),
                    file_path: self
                        .codex_home
                        .join("config.toml")
                        .to_string_lossy()
                        .into_owned(),
                    overridden_metadata: None,
                })
            }
            "configRequirements/read" => ok(p::ConfigRequirementsReadResponse::default()),
            "mcpServerStatus/list" => {
                let _typed: p::ListMcpServerStatusParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                ok(p::ListMcpServerStatusResponse {
                    data: Vec::new(),
                    next_cursor: None,
                })
            }
            "config/mcpServer/reload" => ok(p::McpServerRefreshResponse::default()),
            "mcpServer/oauth/login" => Err(method_not_found("mcpServer/oauth/login")),
            "mock/experimentalMethod" => {
                let typed: p::MockExperimentalMethodParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
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
                let _typed: p::ModelListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                ok(p::ModelListResponse {
                    data: droid_models(),
                    next_cursor: None,
                })
            }
            "skills/list" => {
                let typed: p::SkillsListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                let data = if typed.cwds.is_empty() {
                    Vec::new()
                } else {
                    typed
                        .cwds
                        .into_iter()
                        .map(|cwd| p::SkillsListEntry {
                            cwd,
                            skills: Vec::new(),
                            errors: Vec::new(),
                        })
                        .collect()
                };
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
                self.handle_command_exec(typed).await
            }
            "command/exec/terminate" => {
                let _typed: p::CommandExecTerminateParams = decode(params)?;
                ok(p::CommandExecTerminateResponse::default())
            }
            "command/exec/write" => {
                let _typed: p::CommandExecWriteParams = decode(params)?;
                Err(method_not_found("command/exec/write"))
            }
            "command/exec/resize" => {
                let _typed: p::CommandExecResizeParams = decode(params)?;
                Err(method_not_found("command/exec/resize"))
            }
            "thread/start" => {
                let typed: p::ThreadStartParams = decode(params)?;
                self.handle_thread_start(ctx, typed).await
            }
            "thread/resume" => {
                let typed: p::ThreadResumeParams = decode(params)?;
                self.handle_thread_resume(ctx, typed).await
            }
            "thread/fork" => {
                let _typed: p::ThreadForkParams = decode(params)?;
                Err(method_not_found("thread/fork"))
            }
            "thread/archive" => {
                let typed: p::ThreadArchiveParams = decode(params)?;
                self.handle_thread_archive(ctx, typed).await
            }
            "thread/unarchive" => {
                let typed: p::ThreadUnarchiveParams = decode(params)?;
                self.handle_thread_unarchive(ctx, typed).await
            }
            "thread/name/set" => {
                let typed: p::ThreadSetNameParams = decode(params)?;
                self.handle_thread_name_set(ctx, typed).await
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
                let typed: p::ThreadListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                self.handle_thread_list(typed).await
            }
            "thread/loaded/list" => {
                let _typed: p::ThreadLoadedListParams = if params.is_null() {
                    Default::default()
                } else {
                    decode(params)?
                };
                let ids = self
                    .processes
                    .lock()
                    .await
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                ok(p::ThreadLoadedListResponse {
                    data: ids,
                    next_cursor: None,
                })
            }
            "thread/read" => {
                let typed: p::ThreadReadParams = decode(params)?;
                self.handle_thread_read(typed).await
            }
            "thread/turns/list" => {
                let typed: p::ThreadTurnsListParams = decode(params)?;
                self.handle_thread_turns_list(typed).await
            }
            "thread/backgroundTerminals/clean" => {
                let _typed: p::ThreadBackgroundTerminalsCleanParams = decode(params)?;
                ok(p::ThreadBackgroundTerminalsCleanResponse::default())
            }
            "turn/start" => {
                let typed: p::TurnStartParams = decode(params)?;
                self.handle_turn_start(ctx, typed).await
            }
            "turn/steer" => {
                let typed: p::TurnSteerParams = decode(params)?;
                self.handle_turn_steer(ctx, typed).await
            }
            "turn/interrupt" => {
                let typed: p::TurnInterruptParams = decode(params)?;
                self.handle_turn_interrupt(typed).await
            }
            "review/start" => {
                let _typed: p::ReviewStartParams = decode(params)?;
                Err(method_not_found("review/start"))
            }
            other => Err(method_not_found(other)),
        }
    }

    async fn notification(&self, _ctx: &Conn, method: &str, params: Value) {
        tracing::debug!(method, params = %params, "droid bridge ignored client notification");
    }

    async fn shutdown(&self) {
        let task = self.initial_hydration_task.lock().await.take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }
}

impl DroidBridge {
    async fn handle_thread_start(
        &self,
        ctx: &Conn,
        params: p::ThreadStartParams,
    ) -> Result<Value, JsonRpcError> {
        let cwd = resolve_cwd(params.cwd.as_deref())?;
        let thread_id = Uuid::now_v7().to_string();
        let model = normalize_model(params.model.as_deref()).unwrap_or(DEFAULT_MODEL.to_string());
        let approval_policy = params
            .approval_policy
            .unwrap_or(p::AskForApproval::OnRequest);
        let sandbox = sandbox_value(params.sandbox);
        let now = now_millis();
        let session_path = self.session_path_for(&cwd, &thread_id).await;
        let record = ThreadRecord {
            id: thread_id.clone(),
            cwd: cwd.to_string_lossy().into_owned(),
            name: params.service_name.clone(),
            preview: String::new(),
            created_at: now,
            updated_at: now,
            path: session_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            model: model.clone(),
            approval_policy: approval_policy.clone(),
            sandbox,
            turns: Vec::new(),
        };
        self.spawn_process(
            &thread_id,
            &cwd,
            Some(model.clone()),
            &approval_policy,
            SessionOpenMode::Initialize,
        )
        .await?;
        self.threads
            .lock()
            .await
            .insert(thread_id.clone(), record.clone());
        if let Some(path) = session_path {
            let entry = index_entry_from_record(&record, path);
            if let Err(err) = self.thread_index.insert(entry).await {
                tracing::warn!(thread_id, %err, "failed to index droid thread");
            }
        }

        if ctx.should_emit("thread/started") {
            let _ = ctx.notifier().send_notification(
                "thread/started",
                json!({"thread": thread_json(&record, false)}),
            );
        }

        ok(thread_attach_response(&record, &model))
    }

    async fn handle_thread_resume(
        &self,
        ctx: &Conn,
        params: p::ThreadResumeParams,
    ) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        let mut record =
            if let Some(record) = self.threads.lock().await.get(&params.thread_id).cloned() {
                record
            } else {
                let entry = self
                    .thread_index
                    .lookup(&params.thread_id)
                    .await
                    .ok_or_else(|| {
                        invalid_params(format!("thread `{}` not found", params.thread_id))
                    })?;
                self.record_from_entry(&entry, &params).await
            };
        if let Some(model) = normalize_model(params.model.as_deref()) {
            record.model = model;
        }
        if params.exclude_turns {
            record.turns.clear();
        } else if record.turns.is_empty()
            && let Some(path) = record.path.as_deref()
        {
            record.turns = self.transcript_turn_values(Path::new(path)).await?;
        }
        let cwd = PathBuf::from(&record.cwd);
        self.spawn_process(
            &record.id,
            &cwd,
            Some(record.model.clone()),
            &record.approval_policy,
            SessionOpenMode::Load,
        )
        .await?;
        self.threads
            .lock()
            .await
            .insert(record.id.clone(), record.clone());
        let _ = ctx;
        ok(thread_attach_response(&record, &record.model))
    }

    async fn handle_thread_archive(
        &self,
        ctx: &Conn,
        params: p::ThreadArchiveParams,
    ) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        let changed = self
            .thread_index
            .set_archived(&params.thread_id, true)
            .await
            .map_err(internal_err)?;
        if !changed {
            return Err(invalid_params(format!(
                "thread `{}` not found",
                params.thread_id
            )));
        }
        if ctx.should_emit("thread/archived") {
            let _ = ctx
                .notifier()
                .send_notification("thread/archived", json!({"threadId": params.thread_id}));
        }
        ok(p::ThreadArchiveResponse::default())
    }

    async fn handle_thread_unarchive(
        &self,
        ctx: &Conn,
        params: p::ThreadUnarchiveParams,
    ) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        let changed = self
            .thread_index
            .set_archived(&params.thread_id, false)
            .await
            .map_err(internal_err)?;
        if !changed {
            return Err(invalid_params(format!(
                "thread `{}` not found",
                params.thread_id
            )));
        }
        let entry = self
            .thread_index
            .lookup(&params.thread_id)
            .await
            .ok_or_else(|| invalid_params(format!("thread `{}` not found", params.thread_id)))?;
        if ctx.should_emit("thread/unarchived") {
            let _ = ctx
                .notifier()
                .send_notification("thread/unarchived", json!({"threadId": params.thread_id}));
        }
        ok(p::ThreadUnarchiveResponse {
            thread: index::thread_from_entry(&entry),
        })
    }

    async fn handle_thread_name_set(
        &self,
        ctx: &Conn,
        params: p::ThreadSetNameParams,
    ) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        let trimmed = params.name.trim().to_string();
        let stored = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.clone())
        };
        let changed = self
            .thread_index
            .set_name(&params.thread_id, stored.clone())
            .await
            .map_err(internal_err)?;
        if !changed {
            return Err(invalid_params(format!(
                "thread `{}` not found",
                params.thread_id
            )));
        }
        let process = self.processes.lock().await.get(&params.thread_id).cloned();
        if let Some(process) = process {
            let _ = process
                .request("droid.rename_session", json!({ "title": trimmed }))
                .await;
        }
        {
            let mut threads = self.threads.lock().await;
            if let Some(record) = threads.get_mut(&params.thread_id) {
                record.name = stored.clone();
                record.updated_at = now_millis();
            }
        }
        if ctx.should_emit("thread/name/updated") {
            let _ = ctx.notifier().send_notification(
                "thread/name/updated",
                json!({"threadId": params.thread_id, "threadName": stored}),
            );
        }
        ok(p::ThreadSetNameResponse::default())
    }

    async fn handle_thread_list(&self, params: p::ThreadListParams) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        let filter = ListFilter {
            archived: Some(params.archived.unwrap_or(false)),
            cwds: parse_cwd_filter(&params.cwd),
            search_term: params.search_term.clone(),
            model_providers: params.model_providers.clone(),
            source_kinds: params.source_kinds.clone(),
        };
        let sort = ListSort {
            key: params.sort_key.unwrap_or(p::ThreadSortKey::CreatedAt),
            direction: params.sort_direction.unwrap_or(p::SortDirection::Desc),
        };
        let limit = alleycat_bridge_core::resolve_list_limit(params.limit);
        let page = self
            .thread_index
            .list(&filter, sort, params.cursor.as_deref(), Some(limit))
            .await
            .map_err(internal_err)?;
        let backwards_cursor = page
            .data
            .first()
            .map(|entry| alleycat_bridge_core::encode_backwards_cursor(entry, sort));
        let loaded = self
            .processes
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let data = page
            .data
            .into_iter()
            .map(|entry| {
                let mut thread = index::thread_from_entry(&entry);
                if loaded.contains(&thread.id) {
                    thread.status = p::ThreadStatus::Idle;
                }
                thread
            })
            .collect();
        ok(p::ThreadListResponse {
            data,
            next_cursor: page.next_cursor,
            backwards_cursor,
        })
    }

    async fn handle_thread_read(&self, params: p::ThreadReadParams) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        if let Some(record) = self.threads.lock().await.get(&params.thread_id).cloned() {
            let mut thread = record_to_thread(&record, false)?;
            if params.include_turns {
                thread.turns = self.turns_for_record(&record).await?;
            }
            return ok(p::ThreadReadResponse { thread });
        }
        let entry = self
            .thread_index
            .lookup(&params.thread_id)
            .await
            .ok_or_else(|| invalid_params(format!("thread `{}` not found", params.thread_id)))?;
        let mut thread = index::thread_from_entry(&entry);
        if params.include_turns {
            thread.turns = index::transcript_turns(&entry.metadata.droid_session_path)
                .await
                .map_err(internal_err)?;
        }
        ok(p::ThreadReadResponse { thread })
    }

    async fn handle_thread_turns_list(
        &self,
        params: p::ThreadTurnsListParams,
    ) -> Result<Value, JsonRpcError> {
        self.refresh_thread_index().await?;
        let mut turns =
            if let Some(record) = self.threads.lock().await.get(&params.thread_id).cloned() {
                self.turns_for_record(&record).await?
            } else {
                let entry = self
                    .thread_index
                    .lookup(&params.thread_id)
                    .await
                    .ok_or_else(|| {
                        invalid_params(format!("thread `{}` not found", params.thread_id))
                    })?;
                index::transcript_turns(&entry.metadata.droid_session_path)
                    .await
                    .map_err(internal_err)?
            };
        if matches!(
            params.sort_direction.unwrap_or(p::SortDirection::Desc),
            p::SortDirection::Desc
        ) {
            turns.reverse();
        }
        if let Some(limit) = params.limit
            && (limit as usize) < turns.len()
        {
            turns.truncate(limit as usize);
        }
        ok(p::ThreadTurnsListResponse {
            data: turns,
            next_cursor: None,
            backwards_cursor: None,
        })
    }

    async fn handle_turn_start(
        &self,
        ctx: &Conn,
        params: p::TurnStartParams,
    ) -> Result<Value, JsonRpcError> {
        let record = self
            .threads
            .lock()
            .await
            .get(&params.thread_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("thread `{}` not found", params.thread_id)))?;
        let process = self
            .processes
            .lock()
            .await
            .get(&params.thread_id)
            .cloned()
            .ok_or_else(|| {
                invalid_params(format!("thread `{}` is not loaded", params.thread_id))
            })?;
        let prompt = input_to_text(&params.input);
        let turn_id = Uuid::now_v7().to_string();
        let started_at = now_secs();
        let turn_for_notif = turn_json(
            &turn_id,
            Vec::new(),
            "inProgress",
            Some(started_at),
            None,
            None,
        );
        let turn_for_response = turn_json(&turn_id, Vec::new(), "inProgress", None, None, None);

        if ctx.should_emit("turn/started") {
            let _ = ctx.notifier().send_notification(
                "turn/started",
                json!({"threadId": params.thread_id, "turn": turn_for_notif}),
            );
        }

        let mut rx = process.subscribe();
        let notifier = ctx.notifier().clone();
        let threads = Arc::clone(&self.threads);
        let thread_index = Arc::clone(&self.thread_index);
        let thread_id = params.thread_id.clone();
        let cwd = record.cwd.clone();
        let turn_id_for_pump = turn_id.clone();
        tokio::spawn(async move {
            let mut translator =
                DroidTurnTranslator::new(&thread_id, &turn_id_for_pump, cwd, started_at);
            loop {
                match rx.recv().await {
                    Ok(frame) => {
                        for (method, payload) in translator.translate_frame(&frame) {
                            let _ = notifier.send_notification(method, payload);
                        }
                        if let Some(done) = translator.completed() {
                            record_completed_turn(&threads, &thread_index, &thread_id, done).await;
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        process
            .request(
                "droid.add_user_message",
                json!({
                    "text": prompt,
                    "images": [],
                    "files": []
                }),
            )
            .await
            .map_err(internal_err)?;
        Ok(json!({"turn": turn_for_response}))
    }

    async fn handle_turn_steer(
        &self,
        ctx: &Conn,
        params: p::TurnSteerParams,
    ) -> Result<Value, JsonRpcError> {
        let start = p::TurnStartParams {
            thread_id: params.thread_id,
            input: params.input,
            responsesapi_client_metadata: params.responsesapi_client_metadata,
            ..Default::default()
        };
        let response = self.handle_turn_start(ctx, start).await?;
        Ok(json!({
            "turnId": response.pointer("/turn/id").and_then(Value::as_str).unwrap_or("")
        }))
    }

    async fn handle_turn_interrupt(
        &self,
        params: p::TurnInterruptParams,
    ) -> Result<Value, JsonRpcError> {
        if let Some(process) = self.processes.lock().await.get(&params.thread_id).cloned() {
            let _ = process.request("droid.interrupt_session", json!({})).await;
        }
        ok(p::TurnInterruptResponse::default())
    }

    async fn handle_command_exec(
        &self,
        params: p::CommandExecParams,
    ) -> Result<Value, JsonRpcError> {
        if params.command.is_empty() {
            return Err(invalid_params("empty command argv"));
        }
        if params.tty || params.stream_stdin || params.stream_stdout_stderr {
            return Err(invalid_params(
                "droid-bridge command/exec supports buffered non-tty exec only",
            ));
        }
        if params.disable_output_cap && params.output_bytes_cap.is_some() {
            return Err(invalid_params(
                "disableOutputCap cannot be combined with outputBytesCap",
            ));
        }
        if params.disable_timeout && params.timeout_ms.is_some() {
            return Err(invalid_params(
                "disableTimeout cannot be combined with timeoutMs",
            ));
        }
        let mut env = Vec::<(OsString, OsString)>::new();
        if let Some(map) = params.env {
            for (key, value) in map {
                if let Some(value) = value {
                    env.push((key.into(), value.into()));
                }
            }
        }
        let spec = ProcessSpec {
            role: ProcessRole::ToolCommand,
            program: params.command[0].clone().into(),
            args: params.command[1..].iter().map(Into::into).collect(),
            cwd: params.cwd,
            env,
            env_clear: false,
            stdin: StdioMode::Null,
            stdout: StdioMode::Piped,
            stderr: StdioMode::Piped,
        };
        let mut child = self.launcher.launch(spec).await.map_err(internal_err)?;
        let cap = if params.disable_output_cap {
            usize::MAX
        } else {
            params.output_bytes_cap.unwrap_or(DEFAULT_OUTPUT_BYTES_CAP)
        };
        let timeout = if params.disable_timeout {
            None
        } else {
            Some(Duration::from_millis(
                params.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).max(0) as u64,
            ))
        };
        let stdout = child
            .take_stdout()
            .ok_or_else(|| internal("child stdout was not piped"))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| internal("child stderr was not piped"))?;
        let stdout_task = tokio::spawn(read_capped(stdout, cap));
        let stderr_task = tokio::spawn(read_capped(stderr, cap));
        let status = wait_child(&mut child, timeout).await?;
        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();
        ok(p::CommandExecResponse {
            exit_code: status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }

    async fn spawn_process(
        &self,
        thread_id: &str,
        cwd: &Path,
        model: Option<String>,
        approval_policy: &p::AskForApproval,
        open_mode: SessionOpenMode,
    ) -> Result<Arc<DroidProcess>, JsonRpcError> {
        if let Some(existing) = self.processes.lock().await.get(thread_id).cloned() {
            return Ok(existing);
        }
        let auto_level = autonomy_level(approval_policy).to_string();
        let process = DroidProcess::launch(
            Arc::clone(&self.launcher),
            DroidSpawnConfig {
                thread_id: thread_id.to_string(),
                cwd: cwd.to_path_buf(),
                droid_bin: self.droid_bin.clone(),
                model: model.clone(),
                auto_level: auto_level.clone(),
            },
        )
        .await
        .map_err(internal_err)?;
        tracing::debug!(
            thread_id = process.thread_id(),
            cwd = %process.cwd().display(),
            "spawned droid process"
        );
        let (method, params) = match open_mode {
            SessionOpenMode::Initialize => (
                "droid.initialize_session",
                json!({
                    "sessionId": thread_id,
                    "cwd": cwd.to_string_lossy(),
                    "machineId": "alleycat-droid-bridge",
                    "autonomyLevel": auto_level,
                    "modelId": model,
                }),
            ),
            SessionOpenMode::Load => (
                "droid.load_session",
                json!({
                    "sessionId": thread_id,
                }),
            ),
        };
        process
            .request(method, params)
            .await
            .map_err(internal_err)?;
        self.processes
            .lock()
            .await
            .insert(thread_id.to_string(), Arc::clone(&process));
        Ok(process)
    }

    async fn refresh_thread_index(&self) -> Result<(), JsonRpcError> {
        if self.initial_hydration_covers_refresh().await {
            return Ok(());
        }
        let hydrator = self.hydrator();
        let scanned = hydrator.scan_sessions().await;
        for info in scanned {
            let mut entry = index::entry_from_droid(&info);
            let should_insert =
                if let Some(existing) = self.thread_index.lookup(&entry.thread_id).await {
                    entry.archived = existing.archived;
                    entry.name = existing.name.clone().or(entry.name);
                    entry.forked_from_id = existing.forked_from_id.clone();
                    existing != entry
                } else {
                    true
                };
            if should_insert {
                self.thread_index
                    .insert(entry)
                    .await
                    .map_err(internal_err)?;
            }
        }
        Ok(())
    }

    /// A deferred initial scan is already authoritative for this refresh.
    /// While it is running, serve the persisted cache instead of launching a
    /// duplicate full-disk scan. Once it finishes, consume the task handle;
    /// later explicit refreshes resume their normal behavior.
    async fn initial_hydration_covers_refresh(&self) -> bool {
        let finished_task = {
            let mut guard = self.initial_hydration_task.lock().await;
            match guard.as_ref() {
                Some(task) if task.is_finished() => guard.take(),
                Some(_) => return true,
                None => return false,
            }
        };
        if let Some(task) = finished_task
            && let Err(err) = task.await
            && !err.is_cancelled()
        {
            tracing::warn!(%err, "deferred droid hydration task failed");
        }
        true
    }

    #[cfg(test)]
    async fn wait_for_initial_hydration(&self) {
        let task = self.initial_hydration_task.lock().await.take();
        if let Some(task) = task {
            task.await.expect("deferred hydration task should join");
        }
    }

    fn hydrator(&self) -> DroidHydrator {
        match &self.factory_sessions_dir {
            Some(dir) => DroidHydrator::with_sessions_dir(dir.clone()),
            None => DroidHydrator::new(),
        }
    }

    async fn session_path_for(&self, cwd: &Path, thread_id: &str) -> Option<PathBuf> {
        index::session_path_for(cwd, thread_id, self.factory_sessions_dir.as_deref()).await
    }

    async fn record_from_entry(
        &self,
        entry: &IndexEntry,
        params: &p::ThreadResumeParams,
    ) -> ThreadRecord {
        let model = if let Some(model) = normalize_model(params.model.as_deref()) {
            model
        } else {
            index::session_model(&entry.metadata.droid_session_path)
                .await
                .unwrap_or_else(|| DEFAULT_MODEL.to_string())
        };
        ThreadRecord {
            id: entry.thread_id.clone(),
            cwd: entry.cwd.clone(),
            name: entry.name.clone(),
            preview: entry.preview.clone(),
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            path: Some(
                entry
                    .metadata
                    .droid_session_path
                    .to_string_lossy()
                    .into_owned(),
            ),
            model,
            approval_policy: params
                .approval_policy
                .clone()
                .unwrap_or(p::AskForApproval::OnRequest),
            sandbox: sandbox_value(params.sandbox.clone()),
            turns: Vec::new(),
        }
    }

    async fn transcript_turn_values(&self, path: &Path) -> Result<Vec<Value>, JsonRpcError> {
        let turns = index::transcript_turns(path).await.map_err(internal_err)?;
        turns
            .into_iter()
            .map(|turn| serde_json::to_value(turn).map_err(|err| internal(err.to_string())))
            .collect()
    }

    async fn turns_for_record(&self, record: &ThreadRecord) -> Result<Vec<p::Turn>, JsonRpcError> {
        if record.turns.is_empty()
            && let Some(path) = record.path.as_deref()
        {
            return index::transcript_turns(Path::new(path))
                .await
                .map_err(internal_err);
        }
        record_turns(&record.turns)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionOpenMode {
    Initialize,
    Load,
}

async fn record_completed_turn(
    threads: &Arc<Mutex<HashMap<String, ThreadRecord>>>,
    thread_index: &Arc<index::ThreadIndex>,
    thread_id: &str,
    done: CompletedTurn,
) {
    let preview = last_text_preview(&done.items);
    let updated_at_millis = done.completed_at * 1000;
    let mut threads = threads.lock().await;
    if let Some(record) = threads.get_mut(thread_id) {
        record.preview = preview.clone();
        record.updated_at = updated_at_millis;
        record.turns.push(completed_turn_json(
            &done.turn_id,
            done.items,
            done.completed_at - (done.duration_ms / 1000),
            done.completed_at,
            done.duration_ms,
        ));
    }
    drop(threads);
    let updated_at =
        DateTime::<Utc>::from_timestamp_millis(updated_at_millis).unwrap_or_else(Utc::now);
    if let Err(err) = thread_index
        .update_preview_and_updated_at(thread_id, preview, updated_at)
        .await
    {
        tracing::warn!(thread_id, %err, "failed to update droid thread index after turn");
    }
}

fn thread_json(record: &ThreadRecord, include_turns: bool) -> Value {
    json!({
        "id": record.id,
        "sessionId": record.id,
        "forkedFromId": null,
        "preview": record.preview,
        "ephemeral": false,
        "modelProvider": MODEL_PROVIDER,
        "createdAt": record.created_at,
        "updatedAt": record.updated_at,
        "status": { "type": "idle" },
        "path": record.path,
        "cwd": record.cwd,
        "cliVersion": "",
        "source": "appServer",
        "threadSource": null,
        "agentNickname": null,
        "agentRole": null,
        "gitInfo": null,
        "name": record.name,
        "turns": if include_turns { json!(record.turns) } else { json!([]) },
    })
}

fn thread_attach_response(record: &ThreadRecord, model: &str) -> Value {
    json!({
        "thread": thread_json(record, true),
        "model": model,
        "modelProvider": MODEL_PROVIDER,
        "serviceTier": null,
        "cwd": record.cwd,
        "instructionSources": [],
        "approvalPolicy": record.approval_policy,
        "approvalsReviewer": "user",
        "sandbox": record.sandbox,
        "permissionProfile": { "type": "disabled" },
        "activePermissionProfile": null,
        "reasoningEffort": "high",
    })
}

fn index_entry_from_record(record: &ThreadRecord, path: PathBuf) -> IndexEntry {
    IndexEntry {
        thread_id: record.id.clone(),
        cwd: record.cwd.clone(),
        created_at: record.created_at,
        updated_at: record.updated_at,
        archived: false,
        name: record.name.clone(),
        preview: record.preview.clone(),
        forked_from_id: None,
        model_provider: MODEL_PROVIDER.to_string(),
        source: p::ThreadSourceKind::AppServer,
        metadata: DroidSessionRef {
            droid_session_path: path,
            droid_session_id: record.id.clone(),
        },
    }
}

fn record_to_thread(record: &ThreadRecord, include_turns: bool) -> Result<p::Thread, JsonRpcError> {
    serde_json::from_value(thread_json(record, include_turns))
        .map_err(|err| internal(err.to_string()))
}

fn record_turns(turns: &[Value]) -> Result<Vec<p::Turn>, JsonRpcError> {
    turns
        .iter()
        .cloned()
        .map(|turn| serde_json::from_value(turn).map_err(|err| internal(err.to_string())))
        .collect()
}

fn turn_json(
    id: &str,
    items: Vec<Value>,
    status: &str,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    duration_ms: Option<i64>,
) -> Value {
    json!({
        "id": id,
        "items": items,
        "itemsView": "full",
        "status": status,
        "error": null,
        "startedAt": started_at,
        "completedAt": completed_at,
        "durationMs": duration_ms,
    })
}

fn completed_turn_json(
    id: &str,
    items: Vec<Value>,
    started_at: i64,
    completed_at: i64,
    duration_ms: i64,
) -> Value {
    turn_json(
        id,
        items,
        "completed",
        Some(started_at),
        Some(completed_at),
        Some(duration_ms),
    )
}

fn input_to_text(input: &[p::UserInput]) -> String {
    let mut parts = Vec::new();
    for part in input {
        match part {
            p::UserInput::Text { text, .. } => parts.push(text.clone()),
            p::UserInput::Image { url } => parts.push(format!("[image: {url}]")),
            p::UserInput::LocalImage { path } => {
                parts.push(format!("[local image: {}]", path.display()))
            }
            p::UserInput::Skill { name, path } => {
                parts.push(format!("[skill: {name} at {}]", path.display()))
            }
            p::UserInput::Mention { name, path } => {
                parts.push(format!("[mention: {name} at {path}]"))
            }
        }
    }
    parts.join("\n\n")
}

fn last_text_preview(items: &[Value]) -> String {
    items
        .iter()
        .rev()
        .find_map(|item| item.get("text").and_then(Value::as_str))
        .unwrap_or("")
        .chars()
        .take(240)
        .collect()
}

fn droid_models() -> Vec<p::Model> {
    vec![
        model(
            DEFAULT_MODEL,
            "Claude Sonnet 4.5",
            true,
            p::ReasoningEffort::None,
        ),
        model(
            "claude-opus-4-7",
            "Claude Opus 4.7",
            false,
            p::ReasoningEffort::High,
        ),
        model(
            "claude-sonnet-4-6",
            "Claude Sonnet 4.6",
            false,
            p::ReasoningEffort::High,
        ),
        model(
            "claude-haiku-4-5-20251001",
            "Claude Haiku 4.5",
            false,
            p::ReasoningEffort::None,
        ),
        model("gpt-5.5", "GPT-5.5", false, p::ReasoningEffort::Medium),
        model(
            "gpt-5.5-fast",
            "GPT-5.5 Fast Mode",
            false,
            p::ReasoningEffort::Medium,
        ),
        model("gpt-5.4", "GPT-5.4", false, p::ReasoningEffort::High),
        model(
            "gpt-5.4-mini",
            "GPT-5.4 Mini",
            false,
            p::ReasoningEffort::High,
        ),
        model(
            "gpt-5.3-codex",
            "GPT-5.3 Codex",
            false,
            p::ReasoningEffort::High,
        ),
        model(
            "gemini-3.1-pro-preview",
            "Gemini 3.1 Pro",
            false,
            p::ReasoningEffort::High,
        ),
        model("glm-5.1", "GLM 5.1", false, p::ReasoningEffort::Medium),
        model("kimi-k2.6", "Kimi K2.6", false, p::ReasoningEffort::High),
    ]
}

fn model(id: &str, display: &str, is_default: bool, effort: p::ReasoningEffort) -> p::Model {
    p::Model {
        id: id.to_string(),
        model: id.to_string(),
        upgrade: None,
        upgrade_info: None,
        availability_nux: None,
        display_name: display.to_string(),
        description: "Factory Droid model".to_string(),
        hidden: false,
        supported_reasoning_efforts: vec![
            p::ReasoningEffortOption {
                reasoning_effort: p::ReasoningEffort::None,
                description: "No extended reasoning".to_string(),
            },
            p::ReasoningEffortOption {
                reasoning_effort: p::ReasoningEffort::Minimal,
                description: "Lowest latency".to_string(),
            },
            p::ReasoningEffortOption {
                reasoning_effort: p::ReasoningEffort::Low,
                description: "Brief reasoning".to_string(),
            },
            p::ReasoningEffortOption {
                reasoning_effort: p::ReasoningEffort::Medium,
                description: "Default reasoning".to_string(),
            },
            p::ReasoningEffortOption {
                reasoning_effort: p::ReasoningEffort::High,
                description: "Maximum reasoning".to_string(),
            },
        ],
        default_reasoning_effort: effort,
        input_modalities: vec![json!("text"), json!("image")],
        supports_personality: false,
        additional_speed_tiers: Vec::new(),
        service_tiers: vec![p::ModelServiceTier {
            id: "standard".to_string(),
            name: "Standard".to_string(),
            description: "Default bridge tier".to_string(),
        }],
        is_default,
    }
}

fn normalize_model(model: Option<&str>) -> Option<String> {
    let model = model?.trim();
    Some(model.strip_prefix("droid/").unwrap_or(model).to_string())
}

fn sandbox_value(mode: Option<p::SandboxMode>) -> p::SandboxPolicy {
    match mode {
        Some(p::SandboxMode::ReadOnly) => json!({ "type": "readOnly" }),
        Some(p::SandboxMode::DangerFullAccess) => json!({ "type": "dangerFullAccess" }),
        Some(p::SandboxMode::WorkspaceWrite) | None => json!({ "type": "workspaceWrite" }),
    }
}

fn autonomy_level(policy: &p::AskForApproval) -> &'static str {
    match policy {
        p::AskForApproval::Never => "high",
        p::AskForApproval::OnFailure => "medium",
        p::AskForApproval::OnRequest => "medium",
        p::AskForApproval::UnlessTrusted | p::AskForApproval::Granular(_) => "low",
    }
}

fn parse_cwd_filter(value: &Option<Value>) -> Option<Vec<String>> {
    match value.as_ref()? {
        Value::String(s) => Some(vec![s.clone()]),
        Value::Array(values) => Some(
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect(),
        ),
        _ => None,
    }
}

async fn wait_child(
    child: &mut Box<dyn ChildProcess>,
    timeout_dur: Option<Duration>,
) -> Result<std::process::ExitStatus, JsonRpcError> {
    if let Some(duration) = timeout_dur {
        match tokio::time::timeout(duration, child.wait()).await {
            Ok(result) => result.map_err(internal_err),
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                Err(internal("command/exec timed out"))
            }
        }
    } else {
        child.wait().await.map_err(internal_err)
    }
}

async fn read_capped(mut stream: alleycat_bridge_core::ChildStdout, cap: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let _ = read_capped_inner(&mut stream, &mut out, cap).await;
    out
}

async fn read_capped_inner<R>(stream: &mut R, out: &mut Vec<u8>, cap: usize) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buf = [0u8; 8192];
    while out.len() < cap {
        let read_limit = std::cmp::min(buf.len(), cap - out.len());
        let n = stream.read(&mut buf[..read_limit]).await?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(())
}

fn resolve_cwd(value: Option<&str>) -> Result<PathBuf, JsonRpcError> {
    match value {
        Some(cwd) if !cwd.is_empty() => Ok(PathBuf::from(cwd)),
        _ => std::env::current_dir().map_err(internal_err),
    }
}

fn default_codex_home() -> PathBuf {
    if let Some(dirs) = directories::ProjectDirs::from("", "", "codex") {
        dirs.config_dir().join("droid-bridge")
    } else {
        PathBuf::from(".codex/droid-bridge")
    }
}

fn platform_family() -> &'static str {
    if cfg!(target_family = "windows") {
        "windows"
    } else {
        "unix"
    }
}

fn platform_os() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        std::env::consts::OS
    }
}

fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn ok<T: serde::Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|err| internal(err.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, JsonRpcError> {
    serde_json::from_value(value).map_err(|err| invalid_params(format!("invalid params: {err}")))
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

fn internal_err<E: std::fmt::Display>(err: E) -> JsonRpcError {
    internal(format!("{err:#}"))
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

    fn write_session(root: &Path, file_id: &str, session_id: &str) -> PathBuf {
        let project_dir = root.join("-tmp-work");
        std::fs::create_dir_all(&project_dir).unwrap();
        let path = project_dir.join(format!("{file_id}.jsonl"));
        std::fs::write(
            &path,
            format!(
                "{{\"type\":\"session_start\",\"id\":\"{session_id}\",\"cwd\":\"/tmp/work\"}}\n\
                 {{\"type\":\"message\",\"timestamp\":\"2026-07-24T00:00:00Z\",\"message\":{{\"role\":\"user\",\"content\":\"hello\"}}}}\n"
            ),
        )
        .unwrap();
        path
    }

    #[tokio::test]
    async fn eager_hydration_remains_the_builder_default() {
        let home = tempfile::tempdir().unwrap();
        let sessions = tempfile::tempdir().unwrap();
        write_session(sessions.path(), "fresh", "fresh");

        let bridge = DroidBridge::builder()
            .codex_home(home.path())
            .factory_sessions_dir(sessions.path())
            .build()
            .await
            .unwrap();

        assert!(bridge.thread_index.lookup("fresh").await.is_some());
        assert!(bridge.initial_hydration_task.lock().await.is_none());
    }

    #[tokio::test]
    async fn deferred_hydration_opens_cache_then_adds_scanned_sessions() {
        let home = tempfile::tempdir().unwrap();
        let sessions = tempfile::tempdir().unwrap();
        let cached_path = sessions.path().join("cached.jsonl");
        let cached_index = index::ThreadIndex::open_at(home.path().join("threads.json"))
            .await
            .unwrap();
        cached_index
            .insert(IndexEntry {
                thread_id: "cached".into(),
                cwd: "/tmp/work".into(),
                created_at: 1,
                updated_at: 1,
                archived: false,
                name: None,
                preview: "cached".into(),
                forked_from_id: None,
                model_provider: MODEL_PROVIDER.into(),
                source: p::ThreadSourceKind::AppServer,
                metadata: DroidSessionRef {
                    droid_session_path: cached_path,
                    droid_session_id: "cached".into(),
                },
            })
            .await
            .unwrap();
        drop(cached_index);
        write_session(sessions.path(), "fresh", "fresh");

        let bridge = DroidBridge::builder()
            .codex_home(home.path())
            .factory_sessions_dir(sessions.path())
            .defer_initial_hydration(true)
            .build()
            .await
            .unwrap();

        assert!(bridge.thread_index.lookup("cached").await.is_some());
        bridge.wait_for_initial_hydration().await;
        assert!(bridge.thread_index.lookup("fresh").await.is_some());
    }

    #[tokio::test]
    async fn shutdown_consumes_the_deferred_hydration_task() {
        let home = tempfile::tempdir().unwrap();
        let sessions = tempfile::tempdir().unwrap();
        let bridge = DroidBridge::builder()
            .codex_home(home.path())
            .factory_sessions_dir(sessions.path())
            .defer_initial_hydration(true)
            .build()
            .await
            .unwrap();

        assert!(bridge.initial_hydration_task.lock().await.is_some());
        Bridge::shutdown(bridge.as_ref()).await;
        assert!(bridge.initial_hydration_task.lock().await.is_none());
    }
}
