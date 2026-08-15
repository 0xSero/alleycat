use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alleycat_bridge_core::{Bridge, Conn, JsonRpcError};
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::index::{OpencodeBinding, ThreadIndex};
use crate::opencode_client::OpencodeClient;
use crate::opencode_proc::OpencodeRuntime;
use crate::pty::PtyState;
use crate::sse::SseConsumer;
use crate::state::{ActiveTurn, BridgeState};
use crate::translate::{
    input::codex_input_to_parts, parts::message_to_turn_items_with_context, tool::ToolPartContext,
};

pub struct OpencodeBridge {
    // Keep the runtime alive for the bridge's lifetime. Dropping it triggers
    // `kill_on_drop` on the opencode child; if it dropped at the end of
    // `new()` (the previous behavior, after partial-moving its fields) the
    // child would be SIGKILL'd the instant the bridge was constructed and
    // the SSE consumer would immediately error with "connection reset".
    _runtime: OpencodeRuntime,
    client: OpencodeClient,
    index: Arc<ThreadIndex>,
    state: Arc<BridgeState>,
    pty: Arc<PtyState>,
    sse: SseConsumer,
}

impl OpencodeBridge {
    pub async fn new(runtime: OpencodeRuntime) -> anyhow::Result<Self> {
        let state_dir = std::env::var_os("ALLEYCAT_BRIDGE_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("alleycat-opencode-bridge"));
        Self::new_with_state_dir(runtime, state_dir).await
    }

    pub async fn new_with_state_dir(
        runtime: OpencodeRuntime,
        state_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        let index = Arc::new(ThreadIndex::open(state_dir.join("threads.json")).await?);
        let client = OpencodeClient::new(runtime.base_url.clone(), runtime.auth_token.clone());
        let sse = SseConsumer::spawn(client.clone());
        Ok(Self {
            _runtime: runtime,
            client,
            index,
            state: Arc::new(BridgeState::default()),
            pty: Arc::new(PtyState::new()),
            sse,
        })
    }

    /// Shape-parity entry point with `PiBridge::builder()` /
    /// `ClaudeBridge::builder()`. Either feed it an already-constructed
    /// `OpencodeRuntime` (e.g. `OpencodeRuntime::external(...)`) or call
    /// `.from_env()` to defer construction until `build()`.
    pub fn builder() -> OpencodeBridgeBuilder {
        OpencodeBridgeBuilder::default()
    }

    /// Subscribe a per-connection task that translates SSE events into codex
    /// notifications on `ctx`. The task ends when the connection closes (the
    /// notifier `send_notification` will start failing, but the broadcast
    /// receiver simply continues and any send failure is dropped).
    fn spawn_event_pump(&self, ctx: &Conn) {
        let mut rx = self.sse.subscribe();
        let index = Arc::clone(&self.index);
        let state = Arc::clone(&self.state);
        let pty = Arc::clone(&self.pty);
        let client = self.client.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let rc = crate::translate::events::RouteContext {
                            conn: &ctx,
                            index: &index,
                            state: &state,
                            client: &client,
                            pty_state: &pty,
                        };
                        crate::translate::events::route_event(rc, (*event).clone()).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("opencode SSE subscriber lagged, skipped {skipped} events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn handle_thread_start(&self, params: Value) -> Result<Value, JsonRpcError> {
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            });
        let title = params
            .get("serviceName")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let permission = params.get("approvalPolicy").and_then(permission_from_codex);
        let session = self
            .client
            .create_session(title, permission)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let mut binding = self
            .index
            .bind_session(&session)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        binding.directory = cwd.clone();
        self.index
            .insert(binding.clone())
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let thread = binding_to_thread(&binding);
        Ok(json!({
            "thread": thread,
            "model": params.get("model").and_then(Value::as_str).unwrap_or("opencode"),
            "modelProvider": params.get("modelProvider").and_then(Value::as_str).unwrap_or("opencode"),
            "cwd": cwd,
            "instructionSources": [],
            "approvalPolicy": params.get("approvalPolicy").cloned().unwrap_or(json!("untrusted")),
            "approvalsReviewer": params.get("approvalsReviewer").cloned().unwrap_or(json!("user")),
            // codex `SandboxPolicy` is tagged on `type` (e.g.
            // `{type:"workspaceWrite"}`), not `mode`. See
            // codex-rs/protocol/src/protocol.rs:SandboxPolicy.
            "sandbox": {"type":"workspaceWrite"},
            // Synthesize codex-default values for the optional config
            // fields opencode doesn't model itself. Without these the
            // wire shape diverges (codex emits content where bridges
            // emit null), even though the fields ARE present.
            "permissionProfile": params
                .get("permissionProfile")
                .cloned()
                .unwrap_or_else(|| json!({"type": "disabled"})),
            "activePermissionProfile": null,
            "reasoningEffort": params
                .get("reasoningEffort")
                .cloned()
                .unwrap_or_else(|| json!("high")),
            "serviceTier": null,
        }))
    }

    async fn handle_turn_start(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let thread_id = params
            .get("threadId")
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::invalid_params("threadId is required"))?;
        let binding = self
            .index
            .by_thread(thread_id)
            .ok_or_else(|| JsonRpcError::invalid_params(format!("unknown thread `{thread_id}`")))?;
        let input = params
            .get("input")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let parts = codex_input_to_parts(&input);
        let turn_id = format!("turn-{}", now_secs());
        let turn_started_at = now_secs();
        self.state.set_active_turn(
            thread_id.to_string(),
            ActiveTurn {
                turn_id: turn_id.clone(),
                model: params
                    .get("model")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                session_id: Some(binding.session_id.clone()),
                current_assistant_message_id: None,
                started_at: turn_started_at,
            },
        );
        let turn = json!({
            "id": turn_id,
            "items": [],
            "itemsView": "full",
            "status": "inProgress",
            "error": null,
            "startedAt": null,
            "completedAt": null,
            "durationMs": null,
        });
        let started_turn = json!({
            "id": turn_id,
            "items": [],
            "itemsView": "full",
            "status": "inProgress",
            "error": null,
            "startedAt": turn_started_at,
            "completedAt": null,
            "durationMs": null,
        });
        let _ = ctx.notifier().send_notification(
            "turn/started",
            json!({"threadId":thread_id,"turn":started_turn}),
        );
        let model = params.get("model").and_then(Value::as_str).map(split_model);
        let mut body = json!({ "parts": parts });
        if let Some((provider_id, model_id)) = model {
            body["model"] = json!({"providerID":provider_id,"modelID":model_id});
        }
        // Async dispatch: opencode acks the prompt with 204 immediately and
        // streams every subsequent item over SSE. The bridge replies to codex
        // here with `status:"inProgress"` and an empty item list — the SSE
        // pump is responsible for `item/started`, deltas, `item/completed`,
        // and the final `turn/completed` (gated on `session.idle`).
        self.client
            .prompt_async(&binding.session_id, body)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({ "turn": turn }))
    }

    async fn handle_turn_steer(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let response = self.handle_turn_start(ctx, params).await?;
        let turn_id = response
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::internal("turn/start response missing turn id"))?;
        Ok(json!({ "turnId": turn_id }))
    }

    async fn handle_thread_compact_start(&self, params: Value) -> Result<Value, JsonRpcError> {
        let binding = binding_from_params(&self.index, &params)?;
        let (provider_id, model_id) = self
            .resolve_model(params.get("model").and_then(Value::as_str))
            .await?;
        self.client
            .summarize_session(&binding.session_id, &provider_id, &model_id)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({}))
    }

    async fn handle_thread_rollback(&self, params: Value) -> Result<Value, JsonRpcError> {
        let num_turns = params
            .get("numTurns")
            .or_else(|| params.get("num_turns"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if num_turns == 0 {
            return Err(JsonRpcError::invalid_params("numTurns must be >= 1"));
        }
        let binding = binding_from_params(&self.index, &params)?;
        let messages = self
            .client
            .list_messages(&binding.session_id)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let user_ids: Vec<String> = messages
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|message| {
                let role = message.pointer("/info/role").and_then(Value::as_str)?;
                if role != "user" {
                    return None;
                }
                message
                    .pointer("/info/id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect();
        let target_index = user_ids
            .len()
            .checked_sub(num_turns as usize)
            .ok_or_else(|| {
                JsonRpcError::invalid_params(format!(
                    "thread has {} user messages; cannot rollback {num_turns}",
                    user_ids.len()
                ))
            })?;
        let target_id = user_ids
            .get(target_index)
            .ok_or_else(|| JsonRpcError::internal("rollback anchor out of range"))?;
        let reverted_session = self
            .client
            .revert_session(&binding.session_id, target_id)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let messages_after = self
            .client
            .list_messages(&binding.session_id)
            .await
            .unwrap_or(Value::Array(Vec::new()));
        let raw_messages = match reverted_session.get("revert") {
            Some(revert) => apply_revert_to_messages(
                messages_after.as_array().cloned().unwrap_or_default(),
                revert,
            ),
            None => messages_after.as_array().cloned().unwrap_or_default(),
        };
        let turns = collapse_messages_to_turns(
            raw_messages,
            Some(&binding.directory),
            Some(&binding.thread_id),
        );
        let mut thread = binding_to_thread(&binding);
        thread["turns"] = json!(turns);
        Ok(json!({ "thread": thread }))
    }

    async fn handle_thread_fork(&self, params: Value) -> Result<Value, JsonRpcError> {
        let binding = binding_from_params(&self.index, &params)?;
        let messages = self
            .client
            .list_messages(&binding.session_id)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let leaf_user_id = messages
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .rev()
            .find_map(|message| {
                let role = message.pointer("/info/role").and_then(Value::as_str)?;
                if role != "user" {
                    return None;
                }
                message
                    .pointer("/info/id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });
        let new_session = self
            .client
            .fork_session(&binding.session_id, leaf_user_id.as_deref())
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let mut new_binding = self
            .index
            .bind_session(&new_session)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        if new_binding.directory.is_empty() {
            new_binding.directory = binding.directory.clone();
            self.index
                .insert(new_binding.clone())
                .await
                .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        }
        let mut thread = binding_to_thread(&new_binding);
        thread["forkedFromId"] = json!(binding.thread_id);
        let model = params
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("opencode")
            .to_string();
        let model_provider = params
            .get("modelProvider")
            .and_then(Value::as_str)
            .unwrap_or("opencode")
            .to_string();
        Ok(json!({
            "thread": thread,
            "model": model,
            "modelProvider": model_provider,
            "cwd": new_binding.directory,
            "instructionSources": [],
            "approvalPolicy": params.get("approvalPolicy").cloned().unwrap_or(json!("untrusted")),
            "approvalsReviewer": params.get("approvalsReviewer").cloned().unwrap_or(json!("user")),
            "sandbox": {"type":"workspaceWrite"},
            "permissionProfile": params
                .get("permissionProfile")
                .cloned()
                .unwrap_or_else(|| json!({"type": "disabled"})),
            "activePermissionProfile": null,
            "reasoningEffort": params
                .get("reasoningEffort")
                .cloned()
                .unwrap_or_else(|| json!("high")),
            "serviceTier": null
        }))
    }

    async fn resolve_model(
        &self,
        explicit: Option<&str>,
    ) -> Result<(String, String), JsonRpcError> {
        if let Some(model) = explicit {
            let (provider_id, model_id) = split_model(model);
            return Ok((provider_id.to_string(), model_id.to_string()));
        }
        let providers = self
            .client
            .get("/config/providers")
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        providers
            .get("default")
            .and_then(Value::as_object)
            .and_then(|map| map.iter().next())
            .and_then(|(provider_id, model_id)| {
                model_id
                    .as_str()
                    .map(|id| (provider_id.clone(), id.to_string()))
            })
            .ok_or_else(|| {
                JsonRpcError::invalid_params(
                    "no model specified and no default provider/model configured",
                )
            })
    }

    async fn handle_thread_list(&self, params: Value) -> Result<Value, JsonRpcError> {
        // Parse the codex `ThreadListParams` shape. Opencode-bridge stays in
        // raw-Value style (no `alleycat-codex-proto` dep) to match the rest of
        // this file, but the filter/sort/pagination semantics mirror the
        // typed handlers in pi/claude-bridge.
        let cwd_filter = parse_cwd_filter_value(params.get("cwd"));
        let search_term = params
            .get("searchTerm")
            .and_then(Value::as_str)
            .map(str::to_string);
        // Codex semantics: omitted `archived` means "non-archived only"
        // (`unwrap_or(false)`), not "all".
        let archived_filter = params
            .get("archived")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let model_provider_filter: Option<Vec<String>> = string_array(params.get("modelProviders"));
        let source_kind_filter: Option<Vec<String>> = string_array(params.get("sourceKinds"));
        let sort_key = match params.get("sortKey").and_then(Value::as_str) {
            Some("updated_at") => SortKey::UpdatedAt,
            // Schema/codex-rs default is `created_at`.
            _ => SortKey::CreatedAt,
        };
        let sort_descending = !matches!(
            params.get("sortDirection").and_then(Value::as_str),
            Some("asc")
        );
        let cursor = params
            .get("cursor")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .and_then(decode_list_cursor);
        let limit = alleycat_bridge_core::resolve_list_limit(
            params
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as u32),
        ) as usize;
        // `useStateDbOnly` is accepted but a no-op for opencode: the upstream
        // HTTP API is the only state store we ever consult; there's no JSONL
        // rollout to scan-and-repair from. Read it just to silence linters.
        let _ = params.get("useStateDbOnly");

        // Fetch from upstream, then apply cwd filtering against the bridge's
        // local binding. `thread/start` can receive a Codex cwd override that
        // opencode itself does not store on the session, so forwarding
        // `directory=` would make a valid Codex thread disappear.
        let mut upstream_path = "/session".to_string();
        let mut query = Vec::new();
        if let Some(term) = search_term.as_deref() {
            query.push(format!("search={}", encode_query(term)));
        }
        if !query.is_empty() {
            upstream_path.push('?');
            upstream_path.push_str(&query.join("&"));
        }
        let sessions = self
            .client
            .get(&upstream_path)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let mut raw_sessions = sessions.as_array().cloned().unwrap_or_default();

        // Aggregate sessions across every project worktree. Opencode scopes
        // sessions by project (each non-default project gets a hashed
        // `projectID`), so a bare `GET /session` only returns the default
        // project's sessions — sessions created in other worktrees (e.g.
        // `/srv/agents/workspaces/opencode`) would never appear in the list.
        // Enumerate projects via `GET /project` and union each project's
        // `GET /session?directory=<worktree>` page with the default list.
        let mut seen_ids: std::collections::HashSet<String> = raw_sessions
            .iter()
            .filter_map(|session| session.get("id").and_then(Value::as_str).map(str::to_string))
            .collect();
        let query_suffix = if query.is_empty() {
            String::new()
        } else {
            format!("&{}", query.join("&"))
        };
        if let Ok(projects) = self.client.get("/project").await
            && let Some(project_list) = projects.as_array()
        {
            for project in project_list {
                let Some(worktree) = project.get("worktree").and_then(Value::as_str) else {
                    continue;
                };
                if worktree.trim().is_empty() {
                    continue;
                }
                let path = format!(
                    "/session?directory={}{}",
                    encode_query(worktree),
                    query_suffix
                );
                if let Ok(page) = self.client.get(&path).await
                    && let Some(page_array) = page.as_array()
                {
                    for session in page_array {
                        let id = session
                            .get("id")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_default();
                        if id.is_empty() || seen_ids.insert(id.clone()) {
                            raw_sessions.push(session.clone());
                        }
                    }
                }
            }
        }
        let upstream_count = raw_sessions.len();

        // Bind every fetched session into the local thread index so the
        // synthesized thread ids stay stable across calls. Bindings carry the
        // shape we need for local filter/sort.
        let mut bindings = Vec::with_capacity(raw_sessions.len());
        for session in raw_sessions {
            let binding = self
                .index
                .bind_session(&session)
                .await
                .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
            bindings.push(binding);
        }

        // Local filtering for everything opencode's HTTP API can't express.
        let term_lower = search_term.as_deref().map(str::to_lowercase);
        let bindings: Vec<_> = bindings
            .into_iter()
            .filter(|b| b.archived == archived_filter)
            .filter(|b| match cwd_filter.as_deref() {
                Some(cwds) => cwds.iter().any(|c| c == &b.directory),
                None => true,
            })
            .filter(|_b| match &model_provider_filter {
                // Opencode session metadata doesn't carry a provider; the
                // bridge tags every binding's wire `modelProvider` as
                // "opencode". Match against that token.
                Some(want) if !want.is_empty() => want.iter().any(|p| p == "opencode"),
                _ => true,
            })
            .filter(|_b| match &source_kind_filter {
                // Opencode is always sourced from the app-server; if the
                // caller's filter excludes that, return nothing.
                Some(want) if !want.is_empty() => {
                    want.iter().any(|s| s == "appServer" || s == "app_server")
                }
                _ => true,
            })
            .filter(|b| match term_lower.as_deref() {
                Some(needle) => {
                    let in_preview = b.preview.to_lowercase().contains(needle);
                    let in_name = b
                        .name
                        .as_deref()
                        .map(|n| n.to_lowercase().contains(needle))
                        .unwrap_or(false);
                    in_preview || in_name
                }
                None => true,
            })
            .collect();

        let mut bindings = bindings;
        bindings.sort_by(|a, b| {
            let (ak, bk) = match sort_key {
                SortKey::CreatedAt => (a.created_at, b.created_at),
                SortKey::UpdatedAt => (a.updated_at, b.updated_at),
            };
            let primary = ak.cmp(&bk);
            let primary = if sort_descending {
                primary.reverse()
            } else {
                primary
            };
            // Tiebreaker on thread_id keeps pagination deterministic across
            // collisions on the timestamp axis — same rule bridge-core uses.
            primary.then_with(|| a.thread_id.cmp(&b.thread_id))
        });

        let starting = match cursor {
            Some(ref c) => bindings
                .iter()
                .position(|b| cursor_after_binding(b, c, sort_key, sort_descending))
                .unwrap_or(bindings.len()),
            None => 0,
        };
        let end = (starting + limit).min(bindings.len());
        let page = &bindings[starting..end];

        let next_cursor = if end < bindings.len() {
            page.last().map(|b| encode_list_cursor(b, sort_key))
        } else {
            None
        };
        let backwards_cursor = page.first().map(|b| encode_list_cursor(b, sort_key));

        let data: Vec<Value> = page.iter().map(binding_to_thread).collect();
        tracing::info!(
            params = %params,
            opencode_path = %upstream_path,
            upstream_count,
            filtered = bindings.len(),
            returned = data.len(),
            "thread/list",
        );
        Ok(json!({
            "data": data,
            "nextCursor": next_cursor,
            "backwardsCursor": backwards_cursor,
        }))
    }

    async fn handle_thread_resume_or_read(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let thread_id = params
            .get("threadId")
            .or_else(|| params.get("thread_id"))
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::invalid_params("threadId is required"))?;
        let binding = self
            .index
            .by_thread(thread_id)
            .ok_or_else(|| JsonRpcError::invalid_params("unknown thread"))?;
        let exclude_turns = params
            .get("excludeTurns")
            .or_else(|| params.get("exclude_turns"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let (turns, is_active) = if exclude_turns {
            // Paginated hydration path (`excludeTurns: true`): return an empty
            // turn list so the client pages history via `thread/turns/list`
            // instead of us embedding the entire archive. Derive the concrete
            // status from a bounded window of the latest messages rather than
            // pulling every turn.
            let (window, _) = self
                .client
                .list_messages_page(&binding.session_id, Some(4), None)
                .await
                .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
            let window_turns = collapse_messages_to_turns(
                window,
                Some(&binding.directory),
                Some(&binding.thread_id),
            );
            let is_active = window_turns
                .last()
                .and_then(|turn| turn.get("status"))
                .and_then(Value::as_str)
                .map(|status| status == "inProgress")
                .unwrap_or(false);
            (Vec::new(), is_active)
        } else {
            let messages = self
                .client
                .get(&format!("/session/{}/message", binding.session_id))
                .await
                .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
            let turns = collapse_messages_to_turns(
                messages.as_array().cloned().unwrap_or_default(),
                Some(&binding.directory),
                Some(&binding.thread_id),
            );
            let is_active = turns
                .last()
                .and_then(|turn| turn.get("status"))
                .and_then(Value::as_str)
                .map(|status| status == "inProgress")
                .unwrap_or(false);
            (turns, is_active)
        };
        let mut thread = binding_to_thread(&binding);
        thread["turns"] = json!(turns);
        // A loaded thread must report a concrete status, not the list default
        // `notLoaded` (which the client maps to a perpetual "connecting" state
        // and breaks the conversation UI). Tail in-progress turn => active.
        thread["status"] = if is_active {
            json!({"type": "active", "active_flags": []})
        } else {
            json!({"type": "idle"})
        };
        if method == "thread/read" {
            Ok(json!({ "thread": thread }))
        } else {
            // Match upstream `ThreadResumeResponse` exactly. Missing any
            // required field (approvalPolicy, approvalsReviewer, sandbox,
            // serviceTier, reasoningEffort) makes the phone reject the
            // entire response with "deserialize typed RPC response: missing
            // field …", which surfaces as a "Home Action Failed" alert and
            // the thread won't open. Opencode has no notion of approvals
            // or sandboxes, so we synthesize the most permissive defaults.
            Ok(json!({
                "thread": thread,
                "model": "opencode",
                "modelProvider": "opencode",
                "serviceTier": null,
                "cwd": binding.directory,
                "instructionSources": [],
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandbox": {"type": "dangerFullAccess"},
                "permissionProfile": {"type": "disabled"},
                "activePermissionProfile": null,
                "reasoningEffort": "high",
            }))
        }
    }

    /// `thread/turns/list` — paginated turn-by-turn history for a single
    /// thread. iOS uses this on thread open as the canonical way to hydrate
    /// the conversation. Upstream pagination is by `cursor` + `limit`; we
    /// translate it onto opencode's native message pagination (legacy
    /// `GET /session/:id/message?limit=&before=` with the `x-next-cursor`
    /// response header) instead of materializing the whole archive and
    /// slicing locally.
    async fn handle_thread_turns_list(&self, params: Value) -> Result<Value, JsonRpcError> {
        let binding = binding_from_params(&self.index, &params)?;
        let turn_limit = params
            .get("limit")
            .and_then(Value::as_u64)
            .map(|v| v as u32);
        let cursor = params
            .get("cursor")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        // Fetch a bounded window of messages using opencode's native message
        // pagination, then collapse into turns. A turn is a user message plus
        // its assistant follow-ups, so request a multiple of the turn count to
        // cover a page without retrieving the entire conversation.
        let message_limit = turn_limit.map(|t| t.saturating_mul(4).max(4)).unwrap_or(50);
        let (messages, next_cursor) = self
            .client
            .list_messages_page(&binding.session_id, Some(message_limit), cursor.as_deref())
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let mut turns = collapse_messages_to_turns(
            messages,
            Some(&binding.directory),
            Some(&binding.thread_id),
        );
        // Default sort direction is `desc` (newest first) per the codex
        // wire spec.
        let descending = params
            .get("sortDirection")
            .and_then(Value::as_str)
            .map(|s| s == "desc")
            .unwrap_or(true);
        if descending {
            turns.reverse();
        }
        if let Some(limit) = turn_limit {
            turns.truncate(limit as usize);
        }
        Ok(json!({
            "data": turns,
            "nextCursor": next_cursor,
            "backwardsCursor": null,
        }))
    }

    async fn handle_thread_archive(&self, params: Value) -> Result<Value, JsonRpcError> {
        let mut binding = binding_from_params(&self.index, &params)?;
        self.client
            .patch(
                &format!("/session/{}", binding.session_id),
                json!({"time":{"archived":now_secs() * 1000}}),
            )
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        binding.archived = true;
        binding.updated_at = now_secs();
        self.index
            .insert(binding)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({}))
    }

    async fn handle_thread_unarchive(&self, params: Value) -> Result<Value, JsonRpcError> {
        let mut binding = binding_from_params(&self.index, &params)?;
        // Opencode's current HTTP update schema accepts an archive timestamp
        // but has no way to clear it back to null. Preserve the Codex-visible
        // state in the bridge index so clients that unarchive through this
        // protocol see the expected thread state.
        binding.archived = false;
        binding.updated_at = now_secs();
        self.index
            .insert(binding.clone())
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({"thread":binding_to_thread(&binding)}))
    }

    async fn handle_thread_name_set(&self, params: Value) -> Result<Value, JsonRpcError> {
        let binding = binding_from_params(&self.index, &params)?;
        let name = params.get("name").and_then(Value::as_str).unwrap_or("");
        self.client
            .patch(
                &format!("/session/{}", binding.session_id),
                json!({"title":name}),
            )
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({}))
    }

    async fn handle_turn_interrupt(&self, params: Value) -> Result<Value, JsonRpcError> {
        let binding = binding_from_params(&self.index, &params)?;
        self.client
            .post(&format!("/session/{}/abort", binding.session_id), json!({}))
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({}))
    }

    async fn handle_model_list(&self) -> Result<Value, JsonRpcError> {
        let configured = self
            .client
            .get("/config/providers")
            .await
            .unwrap_or(json!({}));
        let mut models = flatten_models(configured);
        if models.is_empty() {
            let providers = self.client.get("/provider").await.unwrap_or(json!({}));
            models = flatten_models(providers);
        }
        if models.is_empty() {
            models.push(default_model_entry("opencode", "opencode", "OpenCode"));
        }
        Ok(json!({"data":models,"nextCursor":null}))
    }

    async fn handle_config_read(&self) -> Result<Value, JsonRpcError> {
        Ok(json!({
            "config": self.client.get("/config").await.unwrap_or(json!({})),
            "origins": {},
        }))
    }

    async fn handle_config_write(&self, params: Value) -> Result<Value, JsonRpcError> {
        let _ = self.client.patch("/config", params).await;
        Ok(json!({}))
    }

    async fn handle_mcp_server_status_list(&self) -> Result<Value, JsonRpcError> {
        let raw = self.client.get("/mcp").await.unwrap_or(json!([]));
        Ok(json!({"data":mcp_statuses(raw),"nextCursor":null}))
    }
}

#[async_trait]
impl Bridge for OpencodeBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        ctx.set_initialize_capabilities(&params);
        Ok(json!({
            "userAgent": concat!("alleycat-opencode-bridge/", env!("CARGO_PKG_VERSION")),
            "codexHome": std::env::temp_dir().join("alleycat-opencode-bridge").to_string_lossy(),
            "platformFamily": std::env::consts::FAMILY,
            "platformOs": std::env::consts::OS
        }))
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "thread/start" => self.handle_thread_start(params).await,
            "thread/list" => self.handle_thread_list(params).await,
            "thread/resume" | "thread/read" => {
                self.handle_thread_resume_or_read(method, params).await
            }
            "thread/turns/list" => self.handle_thread_turns_list(params).await,
            "thread/archive" => self.handle_thread_archive(params).await,
            "thread/unarchive" => self.handle_thread_unarchive(params).await,
            "thread/name/set" => self.handle_thread_name_set(params).await,
            "turn/start" => self.handle_turn_start(ctx, params).await,
            "turn/interrupt" => self.handle_turn_interrupt(params).await,
            "turn/steer" => self.handle_turn_steer(ctx, params).await,
            "model/list" => self.handle_model_list().await,
            "config/read" => self.handle_config_read().await,
            "config/value/write" | "config/batchWrite" => self.handle_config_write(params).await,
            "configRequirements/read" => Ok(json!({"requirements":null})),
            "mcpServerStatus/list" => self.handle_mcp_server_status_list().await,
            "config/mcpServer/reload" => Ok(json!({})),
            "mcpServer/oauth/login" => Ok(json!({"authorizationUrl":""})),
            "skills/list" => Ok(json!({"data":[]})),
            "skills/remote/list" | "skills/remote/export" => Ok(json!({"data":[]})),
            "skills/config/write" => Ok(json!({
                "effectiveEnabled": params.get("enabled").and_then(Value::as_bool).unwrap_or(false)
            })),
            "account/read" => Ok(json!({"account":{"type":"apiKey"},"requiresOpenaiAuth":false})),
            "account/rateLimits/read" => Ok(json!({"rateLimits":[]})),
            "account/login/start"
            | "account/login/cancel"
            | "account/logout"
            | "feedback/upload" => Ok(json!({})),
            "experimentalFeature/list" => Ok(json!({"data":[],"nextCursor":null})),
            "collaborationMode/list" => Ok(json!({"data":[]})),
            "thread/loaded/list" => Ok(json!({"data":[],"nextCursor":null})),
            "thread/backgroundTerminals/clean" => Ok(json!({})),
            "thread/compact/start" => self.handle_thread_compact_start(params).await,
            "thread/rollback" => self.handle_thread_rollback(params).await,
            "thread/fork" => self.handle_thread_fork(params).await,
            "review/start" => Err(JsonRpcError::method_not_found("review/start")),
            "command/exec" => self.handle_command_exec(ctx, params).await,
            "command/exec/write" => self.handle_command_exec_write(params).await,
            "command/exec/terminate" => self.handle_command_exec_terminate(params).await,
            "command/exec/resize" => self.handle_command_exec_resize(params).await,
            "mock/experimentalMethod" => {
                Ok(json!({"echoed":params.get("value").cloned().unwrap_or(Value::Null)}))
            }
            other => Err(JsonRpcError::method_not_found(other)),
        }
    }

    async fn notification(&self, ctx: &Conn, method: &str, _params: Value) {
        if method != "initialized" {
            return;
        }
        self.spawn_event_pump(ctx);
    }
}

impl OpencodeBridge {
    /// `command/exec` — buffered or streaming run of an opencode PTY. The
    /// command is executed via `POST /pty` (which returns immediately) and
    /// the bridge then opens a long-lived websocket against `/pty/{id}/connect`
    /// to capture output. Resolution comes from the SSE `pty.exited` event,
    /// which `route_pty_event` routes back into the per-process exit oneshot.
    ///
    /// Codex's `tty:true` and `streamStdoutStderr:true` both imply chunked
    /// stdout via `command/exec/outputDelta`; opencode's PTY transport is
    /// inherently a single combined stream, so streaming chunks are emitted
    /// with `stream:"stdout"`. `stderr` in the final response is always empty.
    async fn handle_command_exec(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let command = params
            .get("command")
            .and_then(Value::as_array)
            .ok_or_else(|| JsonRpcError::invalid_params("command array is required"))?;
        let program = command
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::invalid_params("command must be non-empty"))?
            .to_string();
        let args = command
            .iter()
            .skip(1)
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            });
        let stream_output = params
            .get("streamStdoutStderr")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || params.get("tty").and_then(Value::as_bool).unwrap_or(false);
        let supplied_process_id = params
            .get("processId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        let create_body = json!({"command":program,"args":args,"cwd":cwd});
        let pty_info = self
            .client
            .pty_create(create_body)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        let pty_id = pty_info
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::internal("opencode /pty returned no id"))?
            .to_string();
        let process_id = supplied_process_id.unwrap_or_else(|| format!("pty-{pty_id}"));

        let process = self
            .pty
            .register(&self.client, process_id.clone(), pty_id.clone());

        // If the caller asked for streaming, fan output deltas out as
        // notifications until either the broadcast closes or the exit fires.
        if stream_output {
            self.spawn_output_delta_pump(ctx, &process);
        }

        let exit_rx = process
            .take_exit_rx()
            .ok_or_else(|| JsonRpcError::internal("pty exit receiver already taken"))?;
        let exit_code = exit_rx.await.unwrap_or(-1);

        let stdout = if stream_output {
            String::new()
        } else {
            String::from_utf8_lossy(&process.snapshot_output()).to_string()
        };

        // Best-effort cleanup: remove the bridge-side registration. The
        // opencode PTY itself is cleaned up by opencode when the process
        // exits, but a `DELETE /pty/{id}` here keeps the registry tidy.
        self.pty.remove(&process_id);
        let _ = self.client.pty_remove(&pty_id).await;

        Ok(json!({
            "exitCode": exit_code,
            "stdout": stdout,
            "stderr": "",
        }))
    }

    fn spawn_output_delta_pump(&self, ctx: &Conn, process: &Arc<crate::pty::PtyProcess>) {
        let mut rx = process.subscribe_stream();
        let process_id = process.process_id.clone();
        let notifier = ctx.notifier().clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(bytes) => {
                        let _ = notifier.send_notification(
                            "command/exec/outputDelta",
                            crate::pty::output_delta_payload(&process_id, "stdout", &bytes),
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            %process_id,
                            skipped,
                            "command/exec output subscriber lagged",
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn handle_command_exec_write(&self, params: Value) -> Result<Value, JsonRpcError> {
        use base64::Engine;
        let process_id = params
            .get("processId")
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::invalid_params("processId is required"))?;
        let process = self.pty.get_by_process(process_id).ok_or_else(|| {
            JsonRpcError::invalid_params(format!("unknown processId `{process_id}`"))
        })?;
        if let Some(delta) = params.get("deltaBase64").and_then(Value::as_str) {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(delta)
                .map_err(|err| JsonRpcError::invalid_params(format!("deltaBase64: {err}")))?;
            process
                .write(bytes)
                .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        }
        if params
            .get("closeStdin")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            process
                .close_stdin()
                .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        }
        Ok(json!({}))
    }

    async fn handle_command_exec_terminate(&self, params: Value) -> Result<Value, JsonRpcError> {
        let process_id = params
            .get("processId")
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::invalid_params("processId is required"))?;
        let pty_id = self
            .pty
            .get_by_process(process_id)
            .map(|process| process.pty_id.clone());
        self.pty.remove(process_id);
        if let Some(pty_id) = pty_id {
            let _ = self.client.pty_remove(&pty_id).await;
        }
        Ok(json!({}))
    }

    async fn handle_command_exec_resize(&self, params: Value) -> Result<Value, JsonRpcError> {
        let process_id = params
            .get("processId")
            .and_then(Value::as_str)
            .ok_or_else(|| JsonRpcError::invalid_params("processId is required"))?;
        let process = self.pty.get_by_process(process_id).ok_or_else(|| {
            JsonRpcError::invalid_params(format!("unknown processId `{process_id}`"))
        })?;
        let rows = params
            .pointer("/size/rows")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        let cols = params
            .pointer("/size/cols")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        if rows == 0 || cols == 0 {
            return Err(JsonRpcError::invalid_params(
                "size.rows and size.cols must be positive",
            ));
        }
        self.client
            .pty_resize(&process.pty_id, rows, cols)
            .await
            .map_err(|err| JsonRpcError::internal(format!("{err:#}")))?;
        Ok(json!({}))
    }
}

/// Either an explicit runtime or a deferred env-driven one. The deferred form
/// only spawns/probes the opencode backend at `build()` time, so callers can
/// configure env vars right up to the moment the bridge starts.
enum RuntimeSource {
    Explicit(OpencodeRuntime),
    FromEnv,
}

#[derive(Default)]
pub struct OpencodeBridgeBuilder {
    runtime: Option<RuntimeSource>,
    state_dir: Option<PathBuf>,
}

impl OpencodeBridgeBuilder {
    pub fn runtime(mut self, runtime: OpencodeRuntime) -> Self {
        self.runtime = Some(RuntimeSource::Explicit(runtime));
        self
    }

    /// Defer runtime construction until `build()`; reads the same env vars
    /// `OpencodeRuntime::start_from_env` honors today.
    pub fn from_env(mut self) -> Self {
        self.runtime = Some(RuntimeSource::FromEnv);
        self
    }

    pub fn state_dir(mut self, state_dir: impl Into<PathBuf>) -> Self {
        self.state_dir = Some(state_dir.into());
        self
    }

    pub async fn build(self) -> anyhow::Result<Arc<OpencodeBridge>> {
        let runtime = match self.runtime {
            Some(RuntimeSource::Explicit(rt)) => rt,
            Some(RuntimeSource::FromEnv) | None => OpencodeRuntime::start_from_env().await?,
        };
        let bridge = match self.state_dir {
            Some(state_dir) => OpencodeBridge::new_with_state_dir(runtime, state_dir).await?,
            None => OpencodeBridge::new(runtime).await?,
        };
        Ok(Arc::new(bridge))
    }
}

fn binding_from_params(
    index: &ThreadIndex,
    params: &Value,
) -> Result<OpencodeBinding, JsonRpcError> {
    let thread_id = params
        .get("threadId")
        .or_else(|| params.get("thread_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| JsonRpcError::invalid_params("threadId is required"))?;
    index
        .by_thread(thread_id)
        .ok_or_else(|| JsonRpcError::invalid_params("unknown thread"))
}

fn apply_revert_to_messages(mut messages: Vec<Value>, revert: &Value) -> Vec<Value> {
    let Some(message_id) = revert.get("messageID").and_then(Value::as_str) else {
        return messages;
    };
    if let Some(idx) = messages
        .iter()
        .position(|message| message.pointer("/info/id").and_then(Value::as_str) == Some(message_id))
    {
        messages.truncate(idx);
        return messages;
    }
    messages
        .into_iter()
        .filter(|message| {
            message
                .pointer("/info/id")
                .and_then(Value::as_str)
                .map(|id| id < message_id)
                .unwrap_or(true)
        })
        .collect()
}

fn binding_to_thread(binding: &OpencodeBinding) -> Value {
    let path = format!("opencode://session/{}", binding.session_id);
    let git_info = alleycat_bridge_core::git_info_for_cwd(&binding.directory)
        .and_then(|info| serde_json::to_value(info).ok())
        .unwrap_or(Value::Null);
    json!({
        "id": binding.thread_id,
        "sessionId": binding.session_id,
        "forkedFromId": null,
        "preview": binding.preview,
        "ephemeral": false,
        "modelProvider": "opencode",
        "createdAt": binding.created_at,
        "updatedAt": binding.updated_at,
        // Codex `ThreadStatus` is a serde-tagged enum (`tag = "type"`).
        // A bare string here makes the connected client reject the
        // entire response. Mirror the events.rs projection.
        "status": {"type": "notLoaded"},
        "path": path,
        "cwd": binding.directory,
        "cliVersion": concat!("alleycat-opencode-bridge/", env!("CARGO_PKG_VERSION")),
        "source": "appServer",
        "threadSource": null,
        "gitInfo": git_info,
        "name": binding.name,
        "turns": []
    })
}

fn permission_from_codex(value: &Value) -> Option<Value> {
    let action = match value.as_str()? {
        "never" => "deny",
        "on-request" | "on-failure" | "untrusted" => "ask",
        _ => "allow",
    };
    Some(json!([{"permission":"*","pattern":"*","action":action}]))
}

fn split_model(model: &str) -> (&str, &str) {
    model.split_once('/').unwrap_or(("opencode", model))
}

fn flatten_models(providers: Value) -> Vec<Value> {
    let defaults = provider_defaults(&providers);
    let provider_list = providers
        .get("providers")
        .or_else(|| providers.get("all"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut models: Vec<Value> = provider_list
        .into_iter()
        .flat_map(|provider| {
            let provider_id = provider
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("opencode")
                .to_string();
            let defaults = defaults.clone();
            models_for_provider(provider)
                .into_iter()
                .map(move |(model_id, model)| {
                    let is_default = defaults
                        .iter()
                        .any(|(p, m)| p == &provider_id && m == &model_id);
                    model_entry(&provider_id, &model_id, &model, is_default)
                })
        })
        .collect();
    if !models.is_empty()
        && !models
            .iter()
            .any(|model| model.get("isDefault").and_then(Value::as_bool) == Some(true))
        && let Some(first) = models.first_mut()
    {
        first["isDefault"] = json!(true);
    }
    models
}

fn provider_defaults(providers: &Value) -> Vec<(String, String)> {
    providers
        .get("default")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|map| {
            map.iter().filter_map(|(provider_id, model_id)| {
                model_id
                    .as_str()
                    .map(|model_id| (provider_id.clone(), model_id.to_string()))
            })
        })
        .collect()
}

fn models_for_provider(provider: Value) -> Vec<(String, Value)> {
    match provider.get("models") {
        Some(Value::Array(models)) => models
            .iter()
            .cloned()
            .map(|model| {
                let id = model
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("model")
                    .to_string();
                (id, model)
            })
            .collect(),
        Some(Value::Object(models)) => models
            .iter()
            .map(|(key, model)| {
                let id = model
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or(key)
                    .to_string();
                (id, model.clone())
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn model_entry(provider_id: &str, model_id: &str, model: &Value, is_default: bool) -> Value {
    json!({
        "id": format!("{provider_id}/{model_id}"),
        "model": model_id,
        "displayName": model.get("name").and_then(Value::as_str).unwrap_or(model_id),
        "description": model.get("description").and_then(Value::as_str).unwrap_or(""),
        "hidden": false,
        "supportedReasoningEfforts": reasoning_efforts_for_model(model),
        "defaultReasoningEffort": "medium",
        "inputModalities": input_modalities_for_model(model),
        "supportsPersonality": false,
        "additionalSpeedTiers": [],
        "serviceTiers": [{
            "id": "standard",
            "name": "Standard",
            "description": "Default bridge service tier"
        }],
        "isDefault": is_default
    })
}

fn reasoning_efforts_for_model(model: &Value) -> Value {
    let supports_reasoning = model
        .pointer("/capabilities/reasoning")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let efforts = if supports_reasoning {
        vec!["minimal", "low", "medium", "high"]
    } else {
        vec!["medium"]
    };
    json!(
        efforts
            .into_iter()
            .map(|reasoning_effort| json!({
                "reasoningEffort": reasoning_effort,
                "description": ""
            }))
            .collect::<Vec<_>>()
    )
}

fn input_modalities_for_model(model: &Value) -> Value {
    let input = model
        .pointer("/capabilities/input")
        .and_then(Value::as_object);
    let mut modalities = vec![json!("text")];
    if input
        .and_then(|input| input.get("image"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        modalities.push(json!("image"));
    }
    Value::Array(modalities)
}

fn default_model_entry(provider_id: &str, model_id: &str, display_name: &str) -> Value {
    json!({
        "id": format!("{provider_id}/{model_id}"),
        "model": model_id,
        "displayName": display_name,
        "description": "",
        "hidden": false,
        "supportedReasoningEfforts": [{
            "reasoningEffort": "medium",
            "description": ""
        }],
        "defaultReasoningEffort": "medium",
        "inputModalities": ["text"],
        "supportsPersonality": false,
        "additionalSpeedTiers": [],
        "serviceTiers": [{
            "id": "standard",
            "name": "Standard",
            "description": "Default bridge service tier"
        }],
        "isDefault": true
    })
}

fn mcp_statuses(raw: Value) -> Vec<Value> {
    match raw {
        Value::Array(items) => items,
        Value::Object(map) => map
            .into_iter()
            .map(|(name, value)| {
                json!({
                    "name": value.get("name").and_then(Value::as_str).unwrap_or(&name),
                    "tools": value.get("tools").cloned().unwrap_or_else(|| json!({})),
                    "resources": value.get("resources").cloned().unwrap_or_else(|| json!([])),
                    "resourceTemplates": value
                        .get("resourceTemplates")
                        .or_else(|| value.get("resource_templates"))
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                    "authStatus": value
                        .get("authStatus")
                        .or_else(|| value.get("auth_status"))
                        .cloned()
                        .unwrap_or_else(|| json!("unsupported")),
                })
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Group an ordered list of opencode messages into codex-shape `Turn`s.
///
/// Codex models a turn as "one user prompt + the assistant's full response"
/// — a single `Turn` in `thread/read.turns[]` contains both the
/// `userMessage` item and every assistant-side item (reasoning, tool calls,
/// agentMessage) that came back. Opencode stores each as a separate
/// `Message` row, so we walk the array and fold consecutive
/// `role: "assistant"` messages into the preceding `role: "user"` turn.
///
/// Timing collapses too: the turn's `startedAt` is the user message's
/// `info.time.created`; `completedAt` is the LAST assistant message's
/// `info.time.completed` (or the user's `time.created` if no assistant
/// followed yet, marking the turn `inProgress`).
fn collapse_messages_to_turns(
    messages: Vec<Value>,
    default_cwd: Option<&str>,
    sender_thread_id: Option<&str>,
) -> Vec<Value> {
    let tool_context = ToolPartContext {
        cwd: default_cwd,
        sender_thread_id,
        include_side_channel_items: true,
    };
    let mut turns: Vec<Value> = Vec::new();
    for message in messages {
        let role = message
            .pointer("/info/role")
            .and_then(Value::as_str)
            .unwrap_or("");
        if role == "user" {
            turns.push(turn_from_user_message(&message, tool_context));
        } else if let Some(turn) = turns.last_mut() {
            // Fold this assistant message into the most recent user-anchored
            // turn. We append items, refresh completedAt/durationMs/status,
            // and surface any per-message error.
            fold_assistant_into_turn(turn, &message, tool_context);
        } else {
            // Stand-alone assistant message with no prior user (shouldn't
            // happen in normal opencode sessions, but keep the data rather
            // than silently dropping). Emit it as its own turn.
            turns.push(turn_from_assistant_only(&message, tool_context));
        }
    }
    turns
}

fn turn_from_user_message(message: &Value, tool_context: ToolPartContext<'_>) -> Value {
    let id = message
        .pointer("/info/id")
        .and_then(Value::as_str)
        .unwrap_or("message")
        .to_string();
    let started_at = message
        .pointer("/info/time/created")
        .and_then(Value::as_i64);
    // User-role messages aren't "running" work; opencode stores them
    // synchronously and never writes `time.completed`. Anchor them to the
    // user's `time.created` so the wire shape stays non-null until an
    // assistant message folds in and refreshes the completion fields.
    let items = message_to_turn_items_with_context(message, tool_context);
    json!({
        "id": id,
        "items": items,
        "itemsView": "full",
        // Without an assistant follow-up the turn is genuinely in-progress
        // from codex's perspective — the user just sent a prompt and is
        // waiting for the model. fold_assistant_into_turn() bumps this to
        // "completed" once the first assistant message arrives.
        "status": "inProgress",
        "error": Value::Null,
        "startedAt": started_at,
        "completedAt": Value::Null,
        "durationMs": Value::Null,
    })
}

fn turn_from_assistant_only(message: &Value, tool_context: ToolPartContext<'_>) -> Value {
    let id = message
        .pointer("/info/id")
        .and_then(Value::as_str)
        .unwrap_or("message")
        .to_string();
    let started_at = message
        .pointer("/info/time/created")
        .and_then(Value::as_i64);
    let completed_at = message
        .pointer("/info/time/completed")
        .and_then(Value::as_i64);
    let duration_ms = match (started_at, completed_at) {
        (Some(s), Some(c)) if c >= s => Some(c - s),
        _ => None,
    };
    json!({
        "id": id,
        "items": message_to_turn_items_with_context(message, tool_context),
        "itemsView": "full",
        "status": if completed_at.is_some() { "completed" } else { "inProgress" },
        "error": opencode_message_error(message),
        "startedAt": started_at,
        "completedAt": completed_at,
        "durationMs": duration_ms,
    })
}

fn fold_assistant_into_turn(turn: &mut Value, message: &Value, tool_context: ToolPartContext<'_>) {
    // Append the assistant's items to the existing turn's `items` array.
    if let Some(items) = turn.get_mut("items").and_then(Value::as_array_mut) {
        for item in message_to_turn_items_with_context(message, tool_context) {
            items.push(item);
        }
    }
    // Refresh completion timing from this assistant message. We want the
    // LAST assistant message's completion time, so always overwrite (later
    // assistants supersede earlier ones).
    let completed_at = message
        .pointer("/info/time/completed")
        .and_then(Value::as_i64);
    let started_at = turn.get("startedAt").and_then(Value::as_i64);
    let duration_ms = match (started_at, completed_at) {
        (Some(s), Some(c)) if c >= s => Some(c - s),
        _ => None,
    };
    if let Some(slot) = turn.get_mut("completedAt") {
        *slot = json!(completed_at);
    }
    if let Some(slot) = turn.get_mut("durationMs") {
        *slot = json!(duration_ms);
    }
    if let Some(slot) = turn.get_mut("status") {
        *slot = json!(if completed_at.is_some() {
            "completed"
        } else {
            "inProgress"
        });
    }
    // Surface a per-turn error if this assistant message reported one.
    // Earlier-message errors stay (we only overwrite if the new one is
    // non-null), so a successful assistant after a failed retry doesn't
    // mask the failure history.
    if let Some(err) = opencode_message_error(message)
        && let Some(slot) = turn.get_mut("error")
    {
        *slot = err;
    }
}

fn opencode_message_error(message: &Value) -> Option<Value> {
    let err = message.pointer("/info/error")?;
    let msg = err
        .pointer("/data/message")
        .and_then(Value::as_str)
        .or_else(|| err.get("message").and_then(Value::as_str))
        .unwrap_or_default();
    if msg.is_empty() {
        return None;
    }
    Some(json!({ "message": msg }))
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn encode_query(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[derive(Debug, Clone, Copy)]
enum SortKey {
    CreatedAt,
    UpdatedAt,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ListCursorPayload {
    /// `created_at` or `updated_at` depending on the sort key the cursor
    /// was minted with.
    ts: i64,
    id: String,
}

/// `cwd` may be a JSON string, a JSON array of strings, or absent. Any other
/// shape is treated as no filter (rather than an error) for parity with the
/// pi/claude `parse_cwd_filter` helper.
fn parse_cwd_filter_value(value: Option<&Value>) -> Option<Vec<String>> {
    let v = value?;
    match v {
        Value::String(s) => Some(vec![s.clone()]),
        Value::Array(arr) => Some(
            arr.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

/// Read a JSON `Value` as `Option<Vec<String>>`: `null`/missing → `None`,
/// non-array → `None`, otherwise the string-typed elements.
fn string_array(value: Option<&Value>) -> Option<Vec<String>> {
    let arr = value?.as_array()?;
    Some(
        arr.iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
    )
}

fn encode_list_cursor(binding: &OpencodeBinding, key: SortKey) -> String {
    use base64::Engine;
    let ts = match key {
        SortKey::CreatedAt => binding.created_at,
        SortKey::UpdatedAt => binding.updated_at,
    };
    let payload = ListCursorPayload {
        ts,
        id: binding.thread_id.clone(),
    };
    let json = serde_json::to_vec(&payload).expect("ListCursorPayload always serializes");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

fn decode_list_cursor(raw: &str) -> Option<ListCursorPayload> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn cursor_after_binding(
    binding: &OpencodeBinding,
    cursor: &ListCursorPayload,
    key: SortKey,
    descending: bool,
) -> bool {
    let entry_ts = match key {
        SortKey::CreatedAt => binding.created_at,
        SortKey::UpdatedAt => binding.updated_at,
    };
    let primary = entry_ts.cmp(&cursor.ts);
    let primary = if descending {
        primary.reverse()
    } else {
        primary
    };
    match primary {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            binding.thread_id.cmp(&cursor.id) == std::cmp::Ordering::Greater
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_binding() -> OpencodeBinding {
        OpencodeBinding {
            thread_id: "thread".to_string(),
            session_id: "session".to_string(),
            directory: "/tmp".to_string(),
            workspace_id: None,
            archived: false,
            name: Some("Greeting".to_string()),
            created_at: 0,
            updated_at: 0,
            preview: String::new(),
        }
    }

    #[test]
    fn binding_to_thread_emits_tagged_status() {
        let projected = binding_to_thread(&sample_binding());
        assert_eq!(projected["status"], json!({"type": "notLoaded"}));
    }
}
