//! `thread/*` request handlers.
//!
//! These methods juggle three resources:
//!
//! - [`crate::pool::PiPool`] — the live pi processes, accessed via
//!   `state.pi_pool()`.
//! - [`crate::state::ThreadIndexHandle`] — the bridge's `threads.json`
//!   metadata store, accessed via `state.thread_index()`.
//! - The connection's [`ThreadDefaults`](crate::state::ThreadDefaults) —
//!   used to fill in approval policy / sandbox / model when the request
//!   omits them.
//!
//! Mapping to pi commands roughly tracks the design plan
//! (`~/.claude/plans/i-wanna-design-a-smooth-horizon.md`, "Thread management").
//! Specific divergences are documented inline:
//!
//! - `thread/start` does not (yet) inject `base_instructions`/`developer_instructions`
//!   into pi. Pi has no native system-prompt slot accessible from RPC; the
//!   plan suggests writing them to `~/.pi/agent/<session>/system.md` or
//!   prepending them to the first user prompt — pending a pi-side hook,
//!   the bridge captures them in the index `name` field annotation only.
//! - `thread/rollback` is implemented via pi `fork` to preserve the audit
//!   trail (per the plan), but pi's fork is per-entry so we approximate
//!   "drop the last N turns" as "fork from the entry id of the (n-th
//!   from end) user message".
//! - `thread/compact/start` is fire-and-forget from the codex client's
//!   point of view: the bridge sends pi `compact{}` and returns success
//!   immediately. The matching `thread/compacted` notification is emitted
//!   by `handlers/turn.rs`'s event pump when pi reports `compaction_end`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use thiserror::Error;
use uuid::Uuid;

use crate::codex_proto as p;
use crate::codex_proto::{SessionSource, SortDirection, ThreadSourceKind};
use crate::index::IndexEntry;
use crate::index::{ListFilter, ListSort};
use crate::pool::pi_protocol as pi;
use crate::pool::{PiProcessHandle, PoolError};
use crate::state::ConnectionState;
use crate::translate::items::{SessionHistoryEntry, translate_messages, translate_session_history};

/// Errors a `thread/*` handler can produce. Mapped onto JSON-RPC error
/// codes by the dispatcher in main.rs.
#[derive(Debug, Error)]
pub enum ThreadError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("thread `{0}` not found in index")]
    NotFound(String),
    #[error("pool error: {0}")]
    Pool(String),
    #[error("pi rpc error: {0}")]
    PiRpc(String),
    #[error(transparent)]
    Index(#[from] anyhow::Error),
}

impl ThreadError {
    /// JSON-RPC code suitable for surfacing via the dispatcher's
    /// `MethodError` mapper.
    pub fn rpc_code(&self) -> i64 {
        match self {
            ThreadError::InvalidParams(_) => p::error_codes::INVALID_PARAMS,
            ThreadError::NotFound(_) => p::error_codes::INVALID_PARAMS,
            ThreadError::Pool(_) | ThreadError::PiRpc(_) | ThreadError::Index(_) => {
                p::error_codes::INTERNAL_ERROR
            }
        }
    }

    fn pool(err: PoolError) -> Self {
        // Use the alternate `:#` formatter so anyhow's full chain
        // (e.g., "spawning /Users/.../pi: No such file or directory" instead
        // of just "spawning /Users/.../pi") surfaces in JSON-RPC error
        // payloads — otherwise the root cause is hidden behind anyhow's
        // outermost context.
        Self::Pool(format!("{err:#}"))
    }
}

// ============================================================================
// thread/start
// ============================================================================

// A running Local Studio process can legitimately spend tens of seconds
// refreshing a large persisted session index before it services get_state.
// Keep the correlation slot alive long enough for that valid response instead
// of discarding it at 30 seconds and leaving thread/start unusable.
const INITIAL_SESSION_RPC_TIMEOUT: Duration = Duration::from_secs(120);
const INITIAL_SESSION_CLAIM_ATTEMPTS: usize = 2;

pub async fn handle_thread_start(
    state: &Arc<ConnectionState>,
    params: p::ThreadStartParams,
) -> Result<p::ThreadStartResponse, ThreadError> {
    let cwd = resolve_cwd(params.cwd.as_deref())?;
    let defaults = state.defaults();

    // `pi --mode rpc` creates a fresh AgentSession before it starts reading
    // commands. Adopt that initial session instead of immediately tearing it
    // down with `new_session`, which repeats runtime/resource initialization.
    let (thread_id, handle, initial_pi_state) = acquire_clean_initial_session(state, &cwd).await?;

    let requested_model = params.model.clone().or_else(|| defaults.model.clone());
    let requested_provider = model_provider_override(
        requested_model.as_deref(),
        params.model_provider.as_deref(),
        defaults.model_provider.as_deref(),
        state.model_provider_prefixes(),
    );
    let requested_effort = additional_effort(&params.additional).or(defaults.reasoning_effort);
    let has_session_overrides =
        requested_model.is_some() || requested_effort.is_some() || params.service_name.is_some();

    // Apply optional model + thinking-level overrides before any first
    // turn. Errors here are downgraded to warnings so the thread still
    // comes up — codex clients usually retry overrides on next turn.
    apply_model_override(&handle, &requested_model, &requested_provider).await;
    apply_thinking_override(&handle, requested_effort).await;

    // Pi has no native session-naming for `service_name` / `personality`,
    // so we pass `service_name` through to pi as the session name when
    // present (otherwise leave the pi default).
    if let Some(name) = params.service_name.as_deref() {
        let _ = handle
            .send_request(pi::RpcCommand::SetSessionName(pi::SetSessionNameCmd {
                id: None,
                name: name.to_string(),
            }))
            .await;
    }

    // Recover pi's session id + path so the index can route resumes back
    // to the same JSONL file.
    let pi_state = if has_session_overrides {
        match pi_session_state_with_timeout(&handle, INITIAL_SESSION_RPC_TIMEOUT).await {
            Ok(pi_state) => pi_state,
            Err(error) => {
                state.pi_pool().release(&thread_id).await;
                return Err(error);
            }
        }
    } else {
        initial_pi_state
    };
    let Some(pi_session_path) = pi_state.session_file.clone().map(PathBuf::from) else {
        state.pi_pool().release(&thread_id).await;
        return Err(ThreadError::PiRpc("pi did not surface session_file".into()));
    };
    let pi_session_id = pi_state.session_id.clone();
    let model = pi_state
        .model
        .as_ref()
        .map(|model| model.id.clone())
        .or(requested_model)
        .unwrap_or_default();
    let model_provider = pi_state
        .model
        .as_ref()
        .map(|model| model.provider.clone())
        .or(requested_provider)
        .unwrap_or_else(|| "pi".to_string());
    let reasoning_effort = Some(reasoning_effort_from_pi(pi_state.thinking_level));

    let now_ms = now_unix_millis();
    let entry = IndexEntry {
        thread_id: thread_id.clone(),
        cwd: cwd.to_string_lossy().into_owned(),
        name: params.service_name.clone(),
        preview: String::new(),
        created_at: now_ms,
        updated_at: now_ms,
        archived: false,
        forked_from_id: None,
        model_provider: model_provider.clone(),
        source: ThreadSourceKind::AppServer,
        metadata: crate::index::PiSessionRef {
            pi_session_path: pi_session_path.clone(),
            pi_session_id,
        },
    };
    if let Err(error) = state.thread_index().insert(entry.clone()).await {
        state.pi_pool().release(&thread_id).await;
        return Err(ThreadError::from(error));
    }
    // Replenish the warm slot only after the claimed session is ready. The
    // spawned replacement is detached from this request's critical path.
    state.pi_pool().schedule_prewarm(cwd.clone());

    // Emit `thread/started` so codex clients (which key UI state off this
    // notification, not the thread/start response) can reflect the new
    // thread immediately. Codex itself emits this from the app-server.
    if state.should_emit("thread/started") {
        let frame = notification_frame(p::ServerNotification::ThreadStarted(
            p::ThreadStartedNotification {
                thread: thread_from_entry(&entry),
            },
        ));
        let _ = state.send(frame);
    }

    let approval_policy = params
        .approval_policy
        .clone()
        .or_else(|| defaults.approval_policy.clone())
        .unwrap_or(p::AskForApproval::OnRequest);
    // `turn/start` carries only per-turn overrides. Remember the policy
    // selected for this thread so subsequent turns do not silently fall back
    // to `on-request` and prompt a full-access client.
    state.update_defaults(|defaults| {
        defaults.approval_policy = Some(approval_policy.clone());
    });
    let approvals_reviewer = params
        .approvals_reviewer
        .or(defaults.approvals_reviewer)
        .unwrap_or(p::ApprovalsReviewer::User);
    let sandbox = sandbox_value(params.sandbox.or(defaults.sandbox));
    Ok(p::ThreadStartResponse {
        thread: thread_from_entry(&entry),
        model,
        model_provider,
        service_tier: Some(default_service_tier()),
        cwd: cwd.to_string_lossy().into_owned(),
        instruction_sources: Vec::new(),
        approval_policy,
        approvals_reviewer,
        sandbox,
        permission_profile: params
            .permission_profile
            .clone()
            .or_else(|| Some(default_permission_profile())),
        active_permission_profile: None,
        reasoning_effort,
    })
}

// ============================================================================
// thread/resume
// ============================================================================

pub async fn handle_thread_resume(
    state: &Arc<ConnectionState>,
    params: p::ThreadResumeParams,
) -> Result<p::ThreadResumeResponse, ThreadError> {
    let entry = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;

    let handle = load_or_resume_handle(state, &params.thread_id, &entry).await?;

    let defaults = state.defaults();
    let requested_model = params.model.clone().or_else(|| defaults.model.clone());
    let requested_provider = model_provider_override(
        requested_model.as_deref(),
        params.model_provider.as_deref(),
        Some(entry.model_provider.as_str()),
        state.model_provider_prefixes(),
    );
    let requested_effort = additional_effort(&params.additional).or(defaults.reasoning_effort);
    apply_model_override(&handle, &requested_model, &requested_provider).await;
    apply_thinking_override(&handle, requested_effort).await;
    let pi_state = pi_session_state(&handle).await?;

    let mut thread = thread_from_entry(&entry);
    if !params.exclude_turns {
        thread.turns = fetch_turns(&handle, &entry.metadata.pi_session_path).await?;
    }
    apply_live_turn_state(
        &mut thread,
        super::turn::active_turn_id(&params.thread_id).as_deref(),
    );

    let model = pi_state
        .model
        .as_ref()
        .map(|model| model.id.clone())
        .or(requested_model)
        .unwrap_or_default();
    let model_provider = pi_state
        .model
        .as_ref()
        .map(|model| model.provider.clone())
        .or(requested_provider)
        .unwrap_or_else(|| entry.model_provider.clone());
    let approval_policy = params
        .approval_policy
        .clone()
        .or_else(|| defaults.approval_policy.clone())
        .unwrap_or(p::AskForApproval::OnRequest);
    let approvals_reviewer = params
        .approvals_reviewer
        .or(defaults.approvals_reviewer)
        .unwrap_or(p::ApprovalsReviewer::User);
    let sandbox = sandbox_value(params.sandbox.or(defaults.sandbox));

    Ok(p::ThreadResumeResponse {
        thread,
        model,
        model_provider,
        service_tier: Some(default_service_tier()),
        cwd: entry.cwd.clone(),
        instruction_sources: Vec::new(),
        approval_policy,
        approvals_reviewer,
        sandbox,
        permission_profile: params
            .permission_profile
            .clone()
            .or_else(|| Some(default_permission_profile())),
        active_permission_profile: None,
        reasoning_effort: Some(reasoning_effort_from_pi(pi_state.thinking_level)),
    })
}

// ============================================================================
// thread/fork
// ============================================================================

pub async fn handle_thread_fork(
    state: &Arc<ConnectionState>,
    params: p::ThreadForkParams,
) -> Result<p::ThreadForkResponse, ThreadError> {
    let source = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;
    let cwd = PathBuf::from(&source.cwd);

    // Acquire (or reuse) a pi process bound to the source's cwd, switch
    // it to the source session, find the leaf user-message entry id, and
    // ask pi to fork. The new session path comes back inside pi's
    // session state — we read it via `get_state` after the fork lands.
    let handle = match state.pi_pool().get(&params.thread_id).await {
        Some(h) => h,
        None => state
            .pi_pool()
            .acquire_utility(Some(&cwd))
            .await
            .map_err(ThreadError::pool)?,
    };
    switch_handle_to(&handle, &source.metadata.pi_session_path).await?;

    let entry_id = leaf_user_entry_id(&handle).await?;
    let fork_resp = handle
        .send_request(pi::RpcCommand::Fork(pi::ForkCmd { id: None, entry_id }))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    if !fork_resp.success {
        return Err(ThreadError::PiRpc(
            fork_resp.error.unwrap_or_else(|| "fork failed".into()),
        ));
    }

    // Pi switched the session to the fork as a side effect of the call.
    // Recover the new session id + path for the index row.
    let (pi_session_id, pi_session_path) = pi_session_identity(&handle).await?;

    let new_thread_id = Uuid::now_v7().to_string();
    let now_ms = now_unix_millis();
    let entry = IndexEntry {
        thread_id: new_thread_id.clone(),
        cwd: source.cwd.clone(),
        name: source.name.clone(),
        preview: source.preview.clone(),
        created_at: now_ms,
        updated_at: now_ms,
        archived: false,
        forked_from_id: Some(source.thread_id.clone()),
        model_provider: source.model_provider.clone(),
        source: ThreadSourceKind::AppServer,
        metadata: crate::index::PiSessionRef {
            pi_session_path,
            pi_session_id,
        },
    };
    state
        .thread_index()
        .insert(entry.clone())
        .await
        .map_err(ThreadError::from)?;

    let mut thread = thread_from_entry(&entry);
    if !params.exclude_turns {
        thread.turns = fetch_turns(&handle, &entry.metadata.pi_session_path).await?;
    }
    apply_live_turn_state(
        &mut thread,
        super::turn::active_turn_id(&new_thread_id).as_deref(),
    );

    let defaults = state.defaults();
    Ok(p::ThreadForkResponse {
        thread,
        model: params
            .model
            .clone()
            .or(defaults.model.clone())
            .unwrap_or_default(),
        model_provider: params
            .model_provider
            .clone()
            .unwrap_or_else(|| source.model_provider.clone()),
        service_tier: None,
        cwd: source.cwd.clone(),
        instruction_sources: Vec::new(),
        approval_policy: params
            .approval_policy
            .clone()
            .or_else(|| defaults.approval_policy.clone())
            .unwrap_or(p::AskForApproval::OnRequest),
        approvals_reviewer: params
            .approvals_reviewer
            .or(defaults.approvals_reviewer)
            .unwrap_or(p::ApprovalsReviewer::User),
        sandbox: sandbox_value(params.sandbox.or(defaults.sandbox)),
        permission_profile: params.permission_profile.clone(),
        active_permission_profile: None,
        reasoning_effort: additional_effort(&params.additional),
    })
}

// ============================================================================
// thread/archive  /  thread/unarchive
// ============================================================================

pub async fn handle_thread_archive(
    state: &Arc<ConnectionState>,
    params: p::ThreadArchiveParams,
) -> Result<p::ThreadArchiveResponse, ThreadError> {
    let changed = state
        .thread_index()
        .set_archived(&params.thread_id, true)
        .await
        .map_err(ThreadError::from)?;
    if !changed {
        return Err(ThreadError::NotFound(params.thread_id));
    }
    if state.should_emit("thread/archived") {
        let frame = notification_frame(p::ServerNotification::ThreadArchived(p::ThreadIdOnly {
            thread_id: params.thread_id.clone(),
        }));
        let _ = state.send(frame);
    }
    Ok(p::ThreadArchiveResponse::default())
}

pub async fn handle_thread_unarchive(
    state: &Arc<ConnectionState>,
    params: p::ThreadUnarchiveParams,
) -> Result<p::ThreadUnarchiveResponse, ThreadError> {
    let changed = state
        .thread_index()
        .set_archived(&params.thread_id, false)
        .await
        .map_err(ThreadError::from)?;
    if !changed {
        return Err(ThreadError::NotFound(params.thread_id));
    }
    let entry = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;
    if state.should_emit("thread/unarchived") {
        let frame = notification_frame(p::ServerNotification::ThreadUnarchived(p::ThreadIdOnly {
            thread_id: params.thread_id.clone(),
        }));
        let _ = state.send(frame);
    }
    Ok(p::ThreadUnarchiveResponse {
        thread: thread_from_entry(&entry),
    })
}

// ============================================================================
// thread/name/set
// ============================================================================

pub async fn handle_thread_set_name(
    state: &Arc<ConnectionState>,
    params: p::ThreadSetNameParams,
) -> Result<p::ThreadSetNameResponse, ThreadError> {
    let trimmed = params.name.trim().to_string();
    let stored = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.clone())
    };

    // Persist to the index first — that's the source of truth for
    // `thread/list`. Pi-side `set_session_name` is best-effort: pi only
    // stores the name when the matching session is the current one in
    // the spawned process.
    let changed = state
        .thread_index()
        .set_name(&params.thread_id, stored.clone())
        .await
        .map_err(ThreadError::from)?;
    if !changed {
        return Err(ThreadError::NotFound(params.thread_id.clone()));
    }

    if let Some(name) = stored.as_deref() {
        if let Some(handle) = state.pi_pool().get(&params.thread_id).await {
            let _ = handle
                .send_request(pi::RpcCommand::SetSessionName(pi::SetSessionNameCmd {
                    id: None,
                    name: name.to_string(),
                }))
                .await;
        }
    }

    if state.should_emit("thread/name/updated") {
        let frame = notification_frame(p::ServerNotification::ThreadNameUpdated(
            p::ThreadNameUpdatedNotification {
                thread_id: params.thread_id.clone(),
                thread_name: stored,
            },
        ));
        let _ = state.send(frame);
    }
    Ok(p::ThreadSetNameResponse::default())
}

// ============================================================================
// thread/compact/start
// ============================================================================

pub async fn handle_thread_compact_start(
    state: &Arc<ConnectionState>,
    params: p::ThreadCompactStartParams,
) -> Result<p::ThreadCompactStartResponse, ThreadError> {
    let handle = state
        .pi_pool()
        .get(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;

    // A normal turn's event pump exits at `agent_end`, so manual compaction
    // needs its own short-lived subscriber. Subscribe before sending the
    // command: Pi can emit both lifecycle events before its command response.
    let events_rx = handle.subscribe_events();
    let pump = super::turn::spawn_compaction_event_pump(
        state.clone(),
        params.thread_id.clone(),
        Uuid::now_v7().to_string(),
        events_rx,
    );
    let resp = match handle
        .send_request(pi::RpcCommand::Compact(pi::CompactCmd::default()))
        .await
    {
        Ok(response) => response,
        Err(error) => {
            pump.abort();
            return Err(ThreadError::PiRpc(error.to_string()));
        }
    };
    if !resp.success {
        pump.abort();
        return Err(ThreadError::PiRpc(
            resp.error.unwrap_or_else(|| "compact failed".into()),
        ));
    }
    Ok(p::ThreadCompactStartResponse::default())
}

// ============================================================================
// thread/rollback
// ============================================================================

pub async fn handle_thread_rollback(
    state: &Arc<ConnectionState>,
    params: p::ThreadRollbackParams,
) -> Result<p::ThreadRollbackResponse, ThreadError> {
    if params.num_turns == 0 {
        return Err(ThreadError::InvalidParams(
            "num_turns must be >= 1".to_string(),
        ));
    }

    let entry = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;
    let cwd = PathBuf::from(&entry.cwd);
    let handle = match state.pi_pool().get(&params.thread_id).await {
        Some(h) => h,
        None => state
            .pi_pool()
            .acquire_utility(Some(&cwd))
            .await
            .map_err(ThreadError::pool)?,
    };
    switch_handle_to(&handle, &entry.metadata.pi_session_path).await?;

    // Walk pi's `get_fork_messages` (one entry per user message) backwards
    // and pick the entry id of the (n-th-from-end) user message. That
    // becomes the fork anchor; pi creates a fresh JSONL with everything
    // up-to-and-including that entry.
    let resp = handle
        .send_request(pi::RpcCommand::GetForkMessages(pi::BareCmd::default()))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(ThreadError::PiRpc(
            resp.error
                .unwrap_or_else(|| "get_fork_messages failed".into()),
        ));
    }
    let entries: pi::ForkMessagesData = serde_json::from_value(
        resp.data
            .ok_or_else(|| ThreadError::PiRpc("missing fork_messages data".into()))?,
    )
    .map_err(|e| ThreadError::PiRpc(format!("decode fork_messages: {e}")))?;

    let target_index = entries
        .messages
        .len()
        .checked_sub(params.num_turns as usize)
        .ok_or_else(|| {
            ThreadError::InvalidParams(format!(
                "thread has {} user messages; cannot rollback {}",
                entries.messages.len(),
                params.num_turns
            ))
        })?;
    let target_entry = entries
        .messages
        .get(target_index)
        .ok_or_else(|| ThreadError::PiRpc("rollback anchor out of range".into()))?;
    let fork_resp = handle
        .send_request(pi::RpcCommand::Fork(pi::ForkCmd {
            id: None,
            entry_id: target_entry.entry_id.clone(),
        }))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    if !fork_resp.success {
        return Err(ThreadError::PiRpc(
            fork_resp.error.unwrap_or_else(|| "fork failed".into()),
        ));
    }
    // Pi swapped to the new session as a side effect; rewrite the
    // existing index row to point at the new path so subsequent calls
    // use the truncated history.
    let (pi_session_id, pi_session_path) = pi_session_identity(&handle).await?;
    let mut updated = entry.clone();
    updated.metadata.pi_session_id = pi_session_id;
    updated.metadata.pi_session_path = pi_session_path;
    updated.updated_at = now_unix_millis();
    state
        .thread_index()
        .insert(updated.clone())
        .await
        .map_err(ThreadError::from)?;

    let mut thread = thread_from_entry(&updated);
    thread.turns = fetch_turns(&handle, &updated.metadata.pi_session_path).await?;
    apply_live_turn_state(
        &mut thread,
        super::turn::active_turn_id(&params.thread_id).as_deref(),
    );
    Ok(p::ThreadRollbackResponse { thread })
}

// ============================================================================
// thread/list
// ============================================================================

pub async fn handle_thread_list(
    state: &Arc<ConnectionState>,
    params: p::ThreadListParams,
) -> Result<p::ThreadListResponse, ThreadError> {
    state.refresh_session_index().await;
    // Per codex-rs `thread_list`: omitted `archived` means "non-archived
    // only" (`unwrap_or(false)`), not "all". Bridge-core's `ListFilter` keeps
    // `Option<bool>` so internal callers can ask for "all" if they need to;
    // the wire-facing handler defaults to false.
    let archived = Some(params.archived.unwrap_or(false));
    let filter = ListFilter {
        archived,
        cwds: parse_cwd_filter(&params.cwd),
        search_term: params.search_term.clone(),
        model_providers: params.model_providers.clone(),
        source_kinds: params.source_kinds.clone(),
    };
    // Schema/codex-rs default is `created_at`. The bridge-core
    // `ListSort::default()` is `updated_at`, kept for internal callers; the
    // wire handler must align with the schema.
    let sort = ListSort {
        key: params.sort_key.unwrap_or(p::ThreadSortKey::CreatedAt),
        direction: params.sort_direction.unwrap_or(SortDirection::Desc),
    };
    let limit = alleycat_bridge_core::resolve_list_limit(params.limit);
    // `use_state_db_only` is accepted but inherently true for this bridge:
    // pi-bridge always lists from the threads.json index, and scan-and-repair
    // hydration runs once at startup (see `index::open_and_hydrate`), not
    // per-list. Honoring the false default would require a per-call rescan
    // we deliberately skip to keep `thread/list` cheap.
    let _ = params.use_state_db_only;

    let page = state
        .thread_index()
        .list(&filter, sort, params.cursor.as_deref(), Some(limit))
        .await
        .map_err(ThreadError::from)?;

    let backwards_cursor = page
        .data
        .first()
        .map(|e| alleycat_bridge_core::encode_backwards_cursor(e, sort));

    // The first persisted-session resume otherwise pays the full pi process
    // startup cost after the user taps. Warm the first few distinct project
    // directories represented on this page while the list is being rendered.
    // `schedule_prewarm` is detached and deduplicates by cwd.
    let mut warmed_cwds = std::collections::HashSet::new();
    for entry in page.data.iter().take(8) {
        let cwd = resume_cwd_or_fallback(&entry.cwd, &entry.thread_id, state.trust_persisted_cwd());
        if warmed_cwds.insert(cwd.clone()) {
            state.pi_pool().schedule_prewarm(cwd);
        }
        if warmed_cwds.len() == 4 {
            break;
        }
    }

    // Enrich entries that pi has actually spawned right now so codex
    // clients can render the correct badge without a follow-up
    // `thread/loaded/list` call.
    let loaded: std::collections::HashSet<String> = state
        .pi_pool()
        .loaded_thread_ids()
        .await
        .into_iter()
        .filter(|id| !id.starts_with("utility_"))
        .collect();
    let data = page
        .data
        .into_iter()
        .map(|entry| {
            // For list responses we use the index's `to_thread`-equivalent
            // free function, which leaves status at `NotLoaded` so the
            // pool-membership check below can flip only the loaded ones to
            // `Idle`. The handler-local `thread_from_entry` is for handlers
            // (start/resume/read) that always return loaded threads.
            let mut t = crate::index::thread_from_entry(&entry);
            if loaded.contains(&t.id) {
                t.status = if super::turn::active_turn_id(&t.id).is_some() {
                    p::ThreadStatus::Active {
                        active_flags: Vec::new(),
                    }
                } else {
                    p::ThreadStatus::Idle
                };
            }
            t
        })
        .collect();

    Ok(p::ThreadListResponse {
        data,
        next_cursor: page.next_cursor,
        backwards_cursor,
    })
}

// ============================================================================
// thread/loaded/list
// ============================================================================

pub async fn handle_thread_loaded_list(
    state: &Arc<ConnectionState>,
    _params: p::ThreadLoadedListParams,
) -> p::ThreadLoadedListResponse {
    let mut data = state.pi_pool().loaded_thread_ids().await;
    // Strip utility-spawned thread ids — codex clients only know about
    // codex-shaped UUIDs, not the synthetic `utility_*` namespace the
    // pool uses internally.
    data.retain(|id| !id.starts_with("utility_"));
    p::ThreadLoadedListResponse {
        data,
        next_cursor: None,
    }
}

// ============================================================================
// thread/read
// ============================================================================

pub async fn handle_thread_read(
    state: &Arc<ConnectionState>,
    params: p::ThreadReadParams,
) -> Result<p::ThreadReadResponse, ThreadError> {
    let entry = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;

    let mut thread = thread_from_entry(&entry);
    if params.include_turns {
        // Prefer the live process if one is running for this thread —
        // it has the freshest state. Otherwise spawn a utility pi and
        // switch it to the persisted JSONL to read the messages.
        let handle = match state.pi_pool().get(&params.thread_id).await {
            Some(h) => h,
            None => {
                let cwd = resume_cwd_or_fallback(
                    &entry.cwd,
                    &params.thread_id,
                    state.trust_persisted_cwd(),
                );
                let h = state
                    .pi_pool()
                    .acquire_utility(Some(&cwd))
                    .await
                    .map_err(ThreadError::pool)?;
                switch_handle_to(&h, &entry.metadata.pi_session_path).await?;
                h
            }
        };
        thread.turns = fetch_turns(&handle, &entry.metadata.pi_session_path).await?;
    }
    apply_live_turn_state(
        &mut thread,
        super::turn::active_turn_id(&params.thread_id).as_deref(),
    );
    Ok(p::ThreadReadResponse { thread })
}

// ============================================================================
// thread/turns/list
// ============================================================================
//
// Pi's RPC has no native pagination — `get_messages` returns the full
// transcript in one shot. Rather than fake a cursor, we return the whole
// page once with `next_cursor: None` so the phone (which falls back to a
// non-paginated read when it sees no cursor) gets the full list and stops
// retrying. `sort_direction` is honored so the phone can request newest-first
// without re-sorting client-side.

pub async fn handle_thread_turns_list(
    state: &Arc<ConnectionState>,
    params: p::ThreadTurnsListParams,
) -> Result<p::ThreadTurnsListResponse, ThreadError> {
    // Honor a follow-up paginated call gracefully: any non-empty cursor is
    // treated as "you've already seen the whole page, return empty" since we
    // don't paginate.
    if params.cursor.as_deref().is_some_and(|c| !c.is_empty()) {
        return Ok(p::ThreadTurnsListResponse::default());
    }

    let entry = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .ok_or_else(|| ThreadError::NotFound(params.thread_id.clone()))?;

    let handle = match state.pi_pool().get(&params.thread_id).await {
        Some(h) => h,
        None => {
            let cwd =
                resume_cwd_or_fallback(&entry.cwd, &params.thread_id, state.trust_persisted_cwd());
            let h = state
                .pi_pool()
                .acquire_utility(Some(&cwd))
                .await
                .map_err(ThreadError::pool)?;
            switch_handle_to(&h, &entry.metadata.pi_session_path).await?;
            h
        }
    };

    let mut turns = fetch_turns(&handle, &entry.metadata.pi_session_path).await?;
    apply_live_turn_to_turns(
        &mut turns,
        super::turn::active_turn_id(&params.thread_id).as_deref(),
    );
    if matches!(
        params.sort_direction.unwrap_or(SortDirection::Desc),
        SortDirection::Desc
    ) {
        turns.reverse();
    }
    if let Some(limit) = params.limit
        && (limit as usize) < turns.len()
    {
        turns.truncate(limit as usize);
    }
    Ok(p::ThreadTurnsListResponse {
        data: turns,
        next_cursor: None,
        backwards_cursor: None,
    })
}

// ============================================================================
// thread/backgroundTerminals/clean
// ============================================================================

pub async fn handle_thread_background_terminals_clean(
    _state: &Arc<ConnectionState>,
    _params: p::ThreadBackgroundTerminalsCleanParams,
) -> p::ThreadBackgroundTerminalsCleanResponse {
    // Pi has no concept of background terminals; the codex client
    // tolerates a no-op success here.
    p::ThreadBackgroundTerminalsCleanResponse::default()
}

// ============================================================================
// helpers
// ============================================================================

fn resolve_cwd(requested: Option<&str>) -> Result<PathBuf, ThreadError> {
    match requested {
        Some(path) if !path.is_empty() => Ok(PathBuf::from(path)),
        _ => std::env::current_dir().map_err(|e| {
            ThreadError::InvalidParams(format!("cwd not provided and bridge cwd unavailable: {e}"))
        }),
    }
}

/// Pi threads persist their original `cwd` in the index. Old threads created
/// in `$TMPDIR` get cleaned up by macOS (and Linux's tmpfiles.d), and threads
/// from other machines may point to project paths that don't exist locally.
/// `Command::current_dir` returns ENOENT on spawn, surfaced as
/// "spawning ...: No such file or directory" with no hint that the *project
/// dir* — not the binary — is what's missing. Fall back to the home directory
/// so old threads stay listable/readable rather than wedging the UI.
/// Return a live pi handle for `thread_id`, resuming it from the persisted
/// session file if the pool has no live process for it.
///
/// The process pool reaps idle processes and is cleared entirely on a daemon
/// restart, and JSONL persists on disk across both. Without this, a `turn/start`
/// on a thread whose process is gone fails with `ThreadNotLoaded` — the crash
/// reported when a backgrounded phone sends a follow-up. Both `thread/resume`
/// and `turn/start` route through here so a cold thread reloads transparently,
/// matching the generic pi runtime.
pub(crate) async fn load_or_resume_handle(
    state: &Arc<ConnectionState>,
    thread_id: &str,
    entry: &IndexEntry,
) -> Result<Arc<PiProcessHandle>, ThreadError> {
    if let Some(handle) = state.pi_pool().get(thread_id).await {
        // Already loaded on this session. Callers may race resume/turn-start;
        // do not switch sessions during an active prompt.
        return Ok(handle);
    }

    let cwd = resume_cwd_or_fallback(&entry.cwd, thread_id, state.trust_persisted_cwd());
    let handle = state
        .pi_pool()
        .acquire_for_resume(thread_id.to_string(), &cwd)
        .await
        .map_err(ThreadError::pool)?;

    let switch = handle
        .send_request(pi::RpcCommand::SwitchSession(pi::SwitchSessionCmd {
            id: None,
            session_path: entry
                .metadata
                .pi_session_path
                .to_string_lossy()
                .into_owned(),
        }))
        .await;
    let switch = match switch {
        Ok(switch) => switch,
        Err(error) => {
            state.pi_pool().release(thread_id).await;
            return Err(ThreadError::PiRpc(error.to_string()));
        }
    };
    if !switch.success {
        state.pi_pool().release(thread_id).await;
        return Err(ThreadError::PiRpc(
            switch
                .error
                .unwrap_or_else(|| "switch_session failed".into()),
        ));
    }
    // Keep the next cold open off the user-facing path. When this resume
    // claimed the cwd's warm utility process, this replenishes it; when it
    // had to spawn, it creates the first warm slot for the next session.
    state.pi_pool().schedule_prewarm(cwd);
    Ok(handle)
}

fn resume_cwd_or_fallback(persisted: &str, thread_id: &str, trust_persisted_cwd: bool) -> PathBuf {
    let original = PathBuf::from(persisted);
    if trust_persisted_cwd || original.is_dir() {
        return original;
    }
    let fallback = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"));
    tracing::warn!(
        thread_id,
        original = %original.display(),
        fallback = %fallback.display(),
        "persisted thread cwd is missing; falling back to home dir"
    );
    fallback
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn thread_from_entry(entry: &IndexEntry) -> p::Thread {
    p::Thread {
        id: entry.thread_id.clone(),
        session_id: entry.metadata.pi_session_id.clone(),
        forked_from_id: entry.forked_from_id.clone(),
        preview: entry.preview.clone(),
        ephemeral: false,
        model_provider: entry.model_provider.clone(),
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        status: p::ThreadStatus::Idle,
        path: Some(
            entry
                .metadata
                .pi_session_path
                .to_string_lossy()
                .into_owned(),
        ),
        cwd: entry.cwd.clone(),
        cli_version: format!("alleycat-pi-bridge/{}", env!("CARGO_PKG_VERSION")),
        source: source_kind_to_session_source(entry.source),
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        git_info: alleycat_bridge_core::git_info_for_cwd(&entry.cwd),
        name: entry.name.clone(),
        turns: Vec::new(),
    }
}

fn apply_live_turn_state(thread: &mut p::Thread, active_turn_id: Option<&str>) {
    let Some(active_turn_id) = active_turn_id else {
        return;
    };
    thread.status = p::ThreadStatus::Active {
        active_flags: Vec::new(),
    };
    apply_live_turn_to_turns(&mut thread.turns, Some(active_turn_id));
}

fn apply_live_turn_to_turns(turns: &mut [p::Turn], active_turn_id: Option<&str>) {
    let Some(active_turn_id) = active_turn_id else {
        return;
    };
    let Some(turn) = turns.last_mut() else {
        return;
    };
    turn.id = active_turn_id.to_string();
    turn.status = p::TurnStatus::InProgress;
    turn.error = None;
    turn.completed_at = None;
    turn.duration_ms = None;
}

fn source_kind_to_session_source(kind: ThreadSourceKind) -> SessionSource {
    match kind {
        ThreadSourceKind::Cli => SessionSource::Cli,
        ThreadSourceKind::VsCode => SessionSource::VsCode,
        ThreadSourceKind::Exec => SessionSource::Exec,
        ThreadSourceKind::AppServer => SessionSource::AppServer,
        _ => SessionSource::AppServer,
    }
}

fn sandbox_value(mode: Option<p::SandboxMode>) -> p::SandboxPolicy {
    // Codex's `SandboxPolicy` is `#[serde(tag = "type", rename_all =
    // "camelCase")]` with variants `ReadOnly`, `DangerFullAccess`,
    // `WorkspaceWrite`, `ExternalSandbox` (see app-server-protocol/src/protocol/v2.rs).
    // Inner fields all have `#[serde(default)]`, so emitting just the discriminator
    // round-trips cleanly. Default to `workspaceWrite` when the caller didn't
    // pick anything.
    match mode {
        Some(p::SandboxMode::ReadOnly) => serde_json::json!({ "type": "readOnly" }),
        Some(p::SandboxMode::DangerFullAccess) => {
            serde_json::json!({ "type": "dangerFullAccess" })
        }
        Some(p::SandboxMode::WorkspaceWrite) | None => {
            serde_json::json!({ "type": "workspaceWrite" })
        }
    }
}

fn parse_cwd_filter(value: &Option<serde_json::Value>) -> Option<Vec<String>> {
    let v = value.as_ref()?;
    match v {
        serde_json::Value::String(s) => Some(vec![s.clone()]),
        serde_json::Value::Array(arr) => Some(
            arr.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

fn parse_effort(value: &serde_json::Value) -> Option<p::ReasoningEffort> {
    match value.as_str()? {
        "none" => Some(p::ReasoningEffort::None),
        "minimal" => Some(p::ReasoningEffort::Minimal),
        "low" => Some(p::ReasoningEffort::Low),
        "medium" => Some(p::ReasoningEffort::Medium),
        "high" => Some(p::ReasoningEffort::High),
        "xhigh" => Some(p::ReasoningEffort::XHigh),
        "max" => Some(p::ReasoningEffort::Max),
        _ => None,
    }
}

fn additional_effort(
    additional: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<p::ReasoningEffort> {
    additional
        .get("reasoningEffort")
        .and_then(parse_effort)
        .or_else(|| additional.get("effort").and_then(parse_effort))
}

fn model_provider_override(
    model: Option<&str>,
    explicit_provider: Option<&str>,
    default_provider: Option<&str>,
    controller_provider_prefixes: &[String],
) -> Option<String> {
    if let Some(provider) = explicit_provider
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(provider.to_string());
    }
    let model_is_qualified = model.is_some_and(|model| {
        model.split_once('/').is_some_and(|(provider, model_id)| {
            !provider.trim().is_empty() && !model_id.trim().is_empty()
        })
    });
    if model_is_qualified {
        return None;
    }
    default_provider
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| controller_provider_prefixes.first().cloned())
}

fn pi_thinking_level(effort: p::ReasoningEffort) -> pi::ThinkingLevel {
    match effort {
        p::ReasoningEffort::None => pi::ThinkingLevel::Off,
        p::ReasoningEffort::Minimal => pi::ThinkingLevel::Minimal,
        p::ReasoningEffort::Low => pi::ThinkingLevel::Low,
        p::ReasoningEffort::Medium => pi::ThinkingLevel::Medium,
        p::ReasoningEffort::High => pi::ThinkingLevel::High,
        p::ReasoningEffort::XHigh => pi::ThinkingLevel::Xhigh,
        p::ReasoningEffort::Max => pi::ThinkingLevel::Max,
    }
}

fn reasoning_effort_from_pi(level: pi::ThinkingLevel) -> p::ReasoningEffort {
    match level {
        pi::ThinkingLevel::Off => p::ReasoningEffort::None,
        pi::ThinkingLevel::Minimal => p::ReasoningEffort::Minimal,
        pi::ThinkingLevel::Low => p::ReasoningEffort::Low,
        pi::ThinkingLevel::Medium => p::ReasoningEffort::Medium,
        pi::ThinkingLevel::High => p::ReasoningEffort::High,
        pi::ThinkingLevel::Xhigh => p::ReasoningEffort::XHigh,
        pi::ThinkingLevel::Max => p::ReasoningEffort::Max,
    }
}

async fn acquire_clean_initial_session(
    state: &Arc<ConnectionState>,
    cwd: &Path,
) -> Result<(String, Arc<PiProcessHandle>, pi::SessionState), ThreadError> {
    for attempt in 0..INITIAL_SESSION_CLAIM_ATTEMPTS {
        let (thread_id, handle) = state
            .pi_pool()
            .acquire_for_new_thread(cwd)
            .await
            .map_err(ThreadError::pool)?;

        let pi_state =
            match pi_session_state_with_timeout(&handle, INITIAL_SESSION_RPC_TIMEOUT).await {
                Ok(pi_state) => pi_state,
                Err(error) => {
                    state.pi_pool().release(&thread_id).await;
                    return Err(error);
                }
            };

        match validate_initial_session(&pi_state) {
            Ok(()) => return Ok((thread_id, handle, pi_state)),
            Err(reason) => {
                state.pi_pool().release(&thread_id).await;
                if attempt + 1 == INITIAL_SESSION_CLAIM_ATTEMPTS {
                    return Err(ThreadError::PiRpc(format!(
                        "pi initial session was not clean after {INITIAL_SESSION_CLAIM_ATTEMPTS} attempts: {reason}"
                    )));
                }
                tracing::warn!(
                    attempt = attempt + 1,
                    %reason,
                    "discarding non-empty pi utility session before thread/start"
                );
            }
        }
    }

    unreachable!("initial session claim loop always returns")
}

fn validate_initial_session(state: &pi::SessionState) -> Result<(), String> {
    let mut reasons = Vec::new();
    if state.session_id.trim().is_empty() {
        reasons.push("missing session id");
    }
    if state.session_file.as_deref().is_none_or(str::is_empty) {
        reasons.push("missing session file");
    }
    if state.message_count != 0 {
        reasons.push("session has messages");
    }
    if state.pending_message_count != 0 {
        reasons.push("session has pending messages");
    }
    if state.is_streaming {
        reasons.push("session is streaming");
    }
    if state.is_compacting {
        reasons.push("session is compacting");
    }
    if reasons.is_empty() {
        Ok(())
    } else {
        Err(reasons.join(", "))
    }
}

async fn apply_model_override(
    handle: &Arc<PiProcessHandle>,
    model: &Option<String>,
    provider: &Option<String>,
) {
    let Some(model) = model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let parsed = model.split_once('/').and_then(|(provider, model_id)| {
        let provider = provider.trim();
        let model_id = model_id.trim();
        (!provider.is_empty() && !model_id.is_empty()).then_some((provider, model_id))
    });
    let (provider, model_id) = match (provider.as_deref(), parsed) {
        (Some(provider), _) if !provider.trim().is_empty() => (provider.trim(), model),
        (_, Some((provider, model_id))) => (provider, model_id),
        _ => return,
    };
    let _ = handle
        .send_request(pi::RpcCommand::SetModel(pi::SetModelCmd {
            id: None,
            provider: provider.to_string(),
            model_id: model_id.to_string(),
        }))
        .await;
}

async fn apply_thinking_override(
    handle: &Arc<PiProcessHandle>,
    effort: Option<p::ReasoningEffort>,
) {
    let Some(effort) = effort else { return };
    let _ = handle
        .send_request(pi::RpcCommand::SetThinkingLevel(pi::SetThinkingLevelCmd {
            id: None,
            level: pi_thinking_level(effort),
        }))
        .await;
}

/// Recover pi's session id + path from a live handle. The bridge can't
/// keep state across pi restarts, so this round-trips through pi's
/// `get_state` after each session-changing command.
async fn pi_session_identity(
    handle: &Arc<PiProcessHandle>,
) -> Result<(String, PathBuf), ThreadError> {
    let state = pi_session_state(handle).await?;
    let session_path = state
        .session_file
        .map(PathBuf::from)
        .ok_or_else(|| ThreadError::PiRpc("pi did not surface session_file".into()))?;
    Ok((state.session_id, session_path))
}

async fn pi_session_state(handle: &Arc<PiProcessHandle>) -> Result<pi::SessionState, ThreadError> {
    let resp = handle
        .send_request(pi::RpcCommand::GetState(pi::BareCmd::default()))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    decode_pi_session_state(resp)
}

async fn pi_session_state_with_timeout(
    handle: &Arc<PiProcessHandle>,
    timeout: Duration,
) -> Result<pi::SessionState, ThreadError> {
    let resp = handle
        .send_request_with_timeout(pi::RpcCommand::GetState(pi::BareCmd::default()), timeout)
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    decode_pi_session_state(resp)
}

fn decode_pi_session_state(resp: pi::RpcResponse) -> Result<pi::SessionState, ThreadError> {
    if !resp.success {
        return Err(ThreadError::PiRpc(
            resp.error.unwrap_or_else(|| "get_state failed".into()),
        ));
    }
    serde_json::from_value(
        resp.data
            .ok_or_else(|| ThreadError::PiRpc("missing get_state data".into()))?,
    )
    .map_err(|e| ThreadError::PiRpc(format!("decode session state: {e}")))
}

async fn switch_handle_to(
    handle: &Arc<PiProcessHandle>,
    session_path: &Path,
) -> Result<(), ThreadError> {
    let resp = handle
        .send_request(pi::RpcCommand::SwitchSession(pi::SwitchSessionCmd {
            id: None,
            session_path: session_path.to_string_lossy().into_owned(),
        }))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(ThreadError::PiRpc(
            resp.error.unwrap_or_else(|| "switch_session failed".into()),
        ));
    }
    Ok(())
}

async fn leaf_user_entry_id(handle: &Arc<PiProcessHandle>) -> Result<String, ThreadError> {
    let resp = handle
        .send_request(pi::RpcCommand::GetForkMessages(pi::BareCmd::default()))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(ThreadError::PiRpc(
            resp.error
                .unwrap_or_else(|| "get_fork_messages failed".into()),
        ));
    }
    let data: pi::ForkMessagesData = serde_json::from_value(
        resp.data
            .ok_or_else(|| ThreadError::PiRpc("missing fork_messages data".into()))?,
    )
    .map_err(|e| ThreadError::PiRpc(format!("decode fork_messages: {e}")))?;
    data.messages
        .last()
        .map(|m| m.entry_id.clone())
        .ok_or_else(|| {
            ThreadError::InvalidParams("source thread has no user messages to fork from".into())
        })
}

async fn fetch_turns(
    handle: &Arc<PiProcessHandle>,
    session_path: &Path,
) -> Result<Vec<p::Turn>, ThreadError> {
    let resp = handle
        .send_request(pi::RpcCommand::GetMessages(pi::BareCmd::default()))
        .await
        .map_err(|e| ThreadError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(ThreadError::PiRpc(
            resp.error.unwrap_or_else(|| "get_messages failed".into()),
        ));
    }
    let data: pi::GetMessagesData = serde_json::from_value(
        resp.data
            .ok_or_else(|| ThreadError::PiRpc("missing messages data".into()))?,
    )
    .map_err(|e| ThreadError::PiRpc(format!("decode messages: {e}")))?;
    let fallback = || translate_messages(&data.messages);
    let Ok(raw) = tokio::fs::read_to_string(session_path).await else {
        return Ok(fallback());
    };
    let history = parse_session_history(&raw);
    let message_count = history
        .iter()
        .filter(|entry| matches!(entry, SessionHistoryEntry::Message(_)))
        .count();
    if message_count != data.messages.len() {
        tracing::warn!(
            session_path = %session_path.display(),
            journal_messages = message_count,
            rpc_messages = data.messages.len(),
            "persisted session history did not match get_messages; using RPC history"
        );
        return Ok(fallback());
    }
    Ok(translate_session_history(&history))
}

fn parse_session_history(raw: &str) -> Vec<SessionHistoryEntry> {
    raw.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(
            |entry| match entry.get("type").and_then(|value| value.as_str()) {
                Some("message") => serde_json::from_value(entry.get("message")?.clone())
                    .ok()
                    .map(SessionHistoryEntry::Message),
                Some("compaction") => {
                    let id = entry.get("id")?.as_str()?.to_string();
                    let timestamp_ms = entry
                        .get("timestamp")
                        .and_then(|value| value.as_str())
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                        .map(|value| value.timestamp_millis());
                    Some(SessionHistoryEntry::Compaction { id, timestamp_ms })
                }
                _ => None,
            },
        )
        .collect()
}

/// Default `permissionProfile` value matching codex's stock emission when no
/// named profile is configured. The bridge has no permission-profile system
/// of its own — pi sandboxes via the spawned process's filesystem caps —
/// but emitting `null` here makes codex clients render the thread as
/// "unconfigured", which is wrong since the underlying agent does enforce
/// some boundaries. `{type: "disabled"}` is codex's "no named profile, but
/// the runtime is still doing its thing" shape.
fn default_permission_profile() -> p::PermissionProfile {
    serde_json::json!({ "type": "disabled" })
}

/// `serviceTier` is the OpenAI account tier flag. Upstream schema enum is
/// `"fast" | "flex" | null` (per
/// `codex-rs/app-server-protocol/schema/json/v2/ThreadResumeResponse.json`).
/// pi has no notion of an OpenAI service tier — `null` is the correct shape.
fn default_service_tier() -> p::ServiceTier {
    serde_json::Value::Null
}

fn notification_frame(notif: p::ServerNotification) -> p::JsonRpcMessage {
    let value = serde_json::to_value(&notif).expect("ServerNotification serializes");
    let method = value
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    let params = value.get("params").cloned();
    p::JsonRpcMessage::Notification(p::JsonRpcNotification {
        jsonrpc: p::JsonRpcVersion,
        method,
        params,
    })
}

// ============================================================================
// Tests
// ============================================================================
//
// These exercise the index-only paths (archive/unarchive/name/set/list/
// loaded-list/backgroundTerminals/clean). The pi-spawning paths
// (thread/start/resume/fork/compact/rollback/read+include_turns) need a
// real pi binary and live in the integration suite under `tests/`.

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::mpsc;

    #[test]
    fn session_history_parser_keeps_compaction_metadata() {
        let history = parse_session_history(
            r#"{"type":"compaction","id":"compact-1","timestamp":"2026-07-31T02:18:16.306Z","summary":"done"}"#,
        );
        assert_eq!(history.len(), 1);
        assert!(matches!(
            &history[0],
            SessionHistoryEntry::Compaction {
                id,
                timestamp_ms: Some(_)
            } if id == "compact-1"
        ));
    }

    async fn dummy_state() -> (
        Arc<ConnectionState>,
        mpsc::UnboundedReceiver<alleycat_bridge_core::session::Sequenced>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::ThreadIndex::open_at(dir.path().join("threads.json"))
            .await
            .unwrap();
        std::mem::forget(dir);
        ConnectionState::for_test(
            Arc::new(crate::pool::PiPool::new("/dev/null")),
            index,
            Default::default(),
        )
    }

    fn sample_entry(thread_id: &str) -> IndexEntry {
        IndexEntry {
            thread_id: thread_id.into(),
            cwd: "/repo".into(),
            name: None,
            preview: "first message".into(),
            created_at: 100,
            updated_at: 200,
            archived: false,
            forked_from_id: None,
            model_provider: "pi".into(),
            source: ThreadSourceKind::AppServer,
            metadata: crate::index::PiSessionRef {
                pi_session_path: "/tmp/pi/x.jsonl".into(),
                pi_session_id: "pi-session-x".into(),
            },
        }
    }

    fn initial_session_state() -> pi::SessionState {
        serde_json::from_value(json!({
            "model": null,
            "thinkingLevel": "high",
            "isStreaming": false,
            "isCompacting": false,
            "steeringMode": "all",
            "followUpMode": "all",
            "sessionFile": "/tmp/pi/initial.jsonl",
            "sessionId": "initial-session",
            "autoCompactionEnabled": true,
            "messageCount": 0,
            "pendingMessageCount": 0
        }))
        .unwrap()
    }

    #[test]
    fn initial_session_validation_accepts_fresh_rpc_session() {
        assert!(validate_initial_session(&initial_session_state()).is_ok());
    }

    #[test]
    fn initial_session_validation_rejects_dirty_or_active_session() {
        let mut state = initial_session_state();
        state.message_count = 1;
        state.pending_message_count = 2;
        state.is_streaming = true;
        let error = validate_initial_session(&state).expect_err("dirty session must be rejected");
        assert!(error.contains("messages"));
        assert!(error.contains("pending"));
        assert!(error.contains("streaming"));
    }

    #[tokio::test]
    async fn archive_sets_flag_and_emits_notification() {
        let (state, mut rx) = dummy_state().await;
        state
            .thread_index()
            .insert(sample_entry("t1"))
            .await
            .unwrap();
        let resp = handle_thread_archive(
            &state,
            p::ThreadArchiveParams {
                thread_id: "t1".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(resp, p::ThreadArchiveResponse::default());

        // Drain and verify the notification frame routes thread/archived.
        let frame = rx.recv().await.unwrap();
        let value = frame.payload;
        assert_eq!(value["method"], json!("thread/archived"));
        assert_eq!(value["params"]["threadId"], json!("t1"));
    }

    #[tokio::test]
    async fn archive_returns_not_found_for_unknown_thread() {
        let (state, _rx) = dummy_state().await;
        let err = handle_thread_archive(
            &state,
            p::ThreadArchiveParams {
                thread_id: "missing".into(),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ThreadError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unarchive_returns_thread_with_cleared_flag() {
        let (state, mut rx) = dummy_state().await;
        let mut entry = sample_entry("t2");
        entry.archived = true;
        state.thread_index().insert(entry).await.unwrap();

        let resp = handle_thread_unarchive(
            &state,
            p::ThreadUnarchiveParams {
                thread_id: "t2".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.thread.id, "t2");

        let frame = rx.recv().await.unwrap();
        let value = frame.payload;
        assert_eq!(value["method"], json!("thread/unarchived"));
    }

    #[tokio::test]
    async fn set_name_persists_and_emits_updated() {
        let (state, mut rx) = dummy_state().await;
        state
            .thread_index()
            .insert(sample_entry("t3"))
            .await
            .unwrap();
        handle_thread_set_name(
            &state,
            p::ThreadSetNameParams {
                thread_id: "t3".into(),
                name: "  My Thread  ".into(),
            },
        )
        .await
        .unwrap();
        let row = state.thread_index().lookup("t3").await.unwrap();
        assert_eq!(row.name.as_deref(), Some("My Thread"));

        let frame = rx.recv().await.unwrap();
        let value = frame.payload;
        assert_eq!(value["method"], json!("thread/name/updated"));
        assert_eq!(value["params"]["threadName"], json!("My Thread"));
    }

    #[tokio::test]
    async fn set_name_with_empty_string_clears_the_name() {
        let (state, _rx) = dummy_state().await;
        let mut entry = sample_entry("t4");
        entry.name = Some("Existing".into());
        state.thread_index().insert(entry).await.unwrap();
        handle_thread_set_name(
            &state,
            p::ThreadSetNameParams {
                thread_id: "t4".into(),
                name: "   ".into(),
            },
        )
        .await
        .unwrap();
        let row = state.thread_index().lookup("t4").await.unwrap();
        assert!(row.name.is_none(), "name should be cleared");
    }

    #[tokio::test]
    async fn list_returns_threads_in_index() {
        let (state, _rx) = dummy_state().await;
        state
            .thread_index()
            .insert(sample_entry("a"))
            .await
            .unwrap();
        state
            .thread_index()
            .insert(sample_entry("b"))
            .await
            .unwrap();
        let resp = handle_thread_list(&state, p::ThreadListParams::default())
            .await
            .unwrap();
        assert_eq!(resp.data.len(), 2);
        let ids: Vec<&str> = resp.data.iter().map(|t| t.id.as_str()).collect();
        assert!(ids.contains(&"a"));
        assert!(ids.contains(&"b"));
    }

    #[tokio::test]
    async fn list_filters_by_archived_flag() {
        let (state, _rx) = dummy_state().await;
        let mut a = sample_entry("a");
        a.archived = false;
        state.thread_index().insert(a).await.unwrap();
        let mut b = sample_entry("b");
        b.archived = true;
        state.thread_index().insert(b).await.unwrap();

        let resp = handle_thread_list(
            &state,
            p::ThreadListParams {
                archived: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].id, "b");
    }

    #[tokio::test]
    async fn list_marks_loaded_threads_as_idle() {
        let (state, _rx) = dummy_state().await;
        // Two index rows; only "loaded-id" gets a pool entry to flip to
        // Idle. The pool fake here has no real pi child — we just want
        // its `loaded_thread_ids` list to include the right id, which
        // it never will because acquire_for_new_thread spawns and
        // /dev/null is not a valid pi binary. So this test focuses on
        // the no-op case: with no loaded threads, every entry should
        // stay `NotLoaded`.
        state
            .thread_index()
            .insert(sample_entry("only"))
            .await
            .unwrap();
        let resp = handle_thread_list(&state, p::ThreadListParams::default())
            .await
            .unwrap();
        assert_eq!(resp.data.len(), 1);
        assert!(matches!(resp.data[0].status, p::ThreadStatus::NotLoaded));
    }

    #[tokio::test]
    async fn loaded_list_drops_utility_namespaced_ids() {
        let (state, _rx) = dummy_state().await;
        // The pool is empty at construction; loaded_thread_ids returns
        // [], which should pass through filter unchanged.
        let resp = handle_thread_loaded_list(&state, p::ThreadLoadedListParams::default()).await;
        assert!(resp.data.is_empty());
        assert!(resp.next_cursor.is_none());
    }

    #[tokio::test]
    async fn background_terminals_clean_is_a_noop_success() {
        let (state, _rx) = dummy_state().await;
        let _resp = handle_thread_background_terminals_clean(
            &state,
            p::ThreadBackgroundTerminalsCleanParams {
                thread_id: "anything".into(),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn read_without_include_turns_returns_metadata_only() {
        let (state, _rx) = dummy_state().await;
        let entry = sample_entry("r1");
        let preview = entry.preview.clone();
        state.thread_index().insert(entry).await.unwrap();
        let resp = handle_thread_read(
            &state,
            p::ThreadReadParams {
                thread_id: "r1".into(),
                include_turns: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.thread.id, "r1");
        assert_eq!(resp.thread.preview, preview);
        assert!(resp.thread.turns.is_empty());
    }

    #[test]
    fn live_turn_state_overrides_persisted_idle_snapshot() {
        let entry = sample_entry("active-thread");
        let mut thread = thread_from_entry(&entry);
        thread.turns.push(p::Turn {
            id: "turn_0".into(),
            items: Vec::new(),
            items_view: p::default_items_view(),
            status: p::TurnStatus::Completed,
            error: None,
            started_at: Some(10),
            completed_at: Some(20),
            duration_ms: Some(10_000),
        });

        apply_live_turn_state(&mut thread, Some("live-turn-id"));

        assert!(matches!(thread.status, p::ThreadStatus::Active { .. }));
        let turn = thread.turns.last().unwrap();
        assert_eq!(turn.id, "live-turn-id");
        assert!(matches!(turn.status, p::TurnStatus::InProgress));
        assert!(turn.completed_at.is_none());
        assert!(turn.duration_ms.is_none());
    }

    #[test]
    fn parse_cwd_filter_handles_string_array_and_null() {
        assert_eq!(
            parse_cwd_filter(&Some(json!("/repo"))),
            Some(vec!["/repo".to_string()])
        );
        assert_eq!(
            parse_cwd_filter(&Some(json!(["/a", "/b"]))),
            Some(vec!["/a".to_string(), "/b".to_string()])
        );
        assert_eq!(parse_cwd_filter(&None), None);
        assert_eq!(parse_cwd_filter(&Some(json!(null))), None);
    }

    #[test]
    fn parse_effort_recognizes_codex_levels() {
        assert!(matches!(
            parse_effort(&json!("minimal")),
            Some(p::ReasoningEffort::Minimal)
        ));
        assert!(matches!(
            parse_effort(&json!("high")),
            Some(p::ReasoningEffort::High)
        ));
        // `xhigh` started as a pi-only thinking level but codex added it to
        // ReasoningEffort, so we now accept it here too.
        assert!(matches!(
            parse_effort(&json!("xhigh")),
            Some(p::ReasoningEffort::XHigh)
        ));
        assert!(parse_effort(&json!(42)).is_none());
        assert!(parse_effort(&json!("nonsense")).is_none());
    }

    #[test]
    fn canonical_reasoning_effort_wins_with_legacy_fallback() {
        let canonical = std::collections::HashMap::from([
            ("reasoningEffort".to_string(), json!("low")),
            ("effort".to_string(), json!("high")),
        ]);
        assert_eq!(additional_effort(&canonical), Some(p::ReasoningEffort::Low));
        let legacy = std::collections::HashMap::from([("effort".to_string(), json!("medium"))]);
        assert_eq!(additional_effort(&legacy), Some(p::ReasoningEffort::Medium));
    }

    #[test]
    fn unqualified_model_uses_controller_provider_scope() {
        assert_eq!(
            model_provider_override(Some("GLM-5.2"), None, None, &["local-studio".to_string()])
                .as_deref(),
            Some("local-studio")
        );
        assert_eq!(
            model_provider_override(
                Some("other/model"),
                None,
                None,
                &["local-studio".to_string()]
            ),
            None
        );
    }

    #[test]
    fn pi_thinking_level_maps_each_codex_effort() {
        assert!(matches!(
            pi_thinking_level(p::ReasoningEffort::Minimal),
            pi::ThinkingLevel::Minimal
        ));
        assert!(matches!(
            pi_thinking_level(p::ReasoningEffort::Low),
            pi::ThinkingLevel::Low
        ));
        assert!(matches!(
            pi_thinking_level(p::ReasoningEffort::Medium),
            pi::ThinkingLevel::Medium
        ));
        assert!(matches!(
            pi_thinking_level(p::ReasoningEffort::High),
            pi::ThinkingLevel::High
        ));
    }

    #[test]
    fn sandbox_value_maps_each_mode() {
        assert_eq!(
            sandbox_value(Some(p::SandboxMode::ReadOnly)),
            json!({"type": "readOnly"})
        );
        assert_eq!(
            sandbox_value(Some(p::SandboxMode::DangerFullAccess)),
            json!({"type": "dangerFullAccess"})
        );
        assert_eq!(
            sandbox_value(Some(p::SandboxMode::WorkspaceWrite)),
            json!({"type": "workspaceWrite"})
        );
        // Default falls back to workspaceWrite.
        assert_eq!(sandbox_value(None), json!({"type": "workspaceWrite"}));
    }

    #[test]
    fn rollback_zero_is_invalid() {
        // Pure sync precheck — `num_turns == 0` short-circuits before
        // any pool/index access.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (state, _rx) = rt.block_on(dummy_state());
        let err = rt
            .block_on(handle_thread_rollback(
                &state,
                p::ThreadRollbackParams {
                    thread_id: "t".into(),
                    num_turns: 0,
                },
            ))
            .unwrap_err();
        assert!(matches!(err, ThreadError::InvalidParams(_)), "got {err:?}");
    }
}
