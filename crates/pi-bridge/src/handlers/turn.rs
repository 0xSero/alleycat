//! `turn/*` and `review/*` request handlers.
//!
//! `turn/start` is the single most complex handler in the bridge. The
//! flow:
//!
//! 1. Look up the pi process for `thread_id` (no auto-spawn — the codex
//!    client is expected to call `thread/start`/`thread/resume` first).
//! 2. Apply optional model/effort/sandbox/approval-policy overrides.
//! 3. Translate `UserInput[]` → pi prompt + images via
//!    [`crate::translate::input::translate_user_input`].
//! 4. Mint a fresh codex `turn_id` and store it in the per-thread
//!    [`ACTIVE_TURNS`] table so `turn/steer`/`turn/interrupt` can validate
//!    `expected_turn_id`.
//! 5. Emit `turn/started` (subject to opt-out filter) with an empty
//!    `Turn`.
//! 6. Send pi `prompt`. **Pi `prompt` is fire-and-forget**: it acks
//!    `{success:true}` after preflight and continues asynchronously,
//!    streaming `AgentEvent`s on stdout. We don't await `agent_end`
//!    inside the handler.
//! 7. Spawn a background pump task that subscribes to the pi event
//!    channel, runs every event through [`EventTranslatorState::translate`],
//!    and forwards the resulting [`ServerNotification`]s to the codex
//!    client. The pump exits on pi `agent_end`, emits `turn/completed`,
//!    and clears the active-turn slot.
//! 8. Return `TurnStartResponse { turn }` with status `InProgress`.
//!
//! Approval bridging (#18) lives inside the pump:
//!
//! - For pi `tool_execution_start { toolName: "bash", … }`, when the
//!   thread's `approval_policy` requires it, the pump intercepts the
//!   matching `item/started` notification, requests
//!   `item/commandExecution/requestApproval` from the codex client, and
//!   either forwards the original `item/started` (approve) or sends pi
//!   `abort` (cancel) / suppresses the rest of the tool call's events
//!   (decline). See `approval.rs` rustdoc for the post-hoc-approval
//!   limitation.
//! - For pi `extension_ui_request`, the pump translates to codex
//!   `item/tool/requestUserInput`, awaits the answer, and writes back
//!   `extension_ui_response` to the originating pi handle.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex as SyncMutex;
use std::time::{Duration, SystemTime};

use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::approval;
use crate::codex_proto as p;
use crate::pool::PiProcessHandle;
use crate::pool::pi_protocol as pi;
use crate::state::ConnectionState;
use crate::translate::events::EventTranslatorState;
use crate::translate::input::translate_user_input;
use crate::translate::items::{translate_messages, user_item_id};
use crate::translate::turn_terminal_state;

/// Per-thread active-turn registry. Pi only allows one active turn per
/// process, so this is a 1:1 map. Keyed by codex `thread_id`.
static ACTIVE_TURNS: LazyLock<SyncMutex<HashMap<String, ActiveTurn>>> =
    LazyLock::new(|| SyncMutex::new(HashMap::new()));

#[derive(Clone)]
struct ActiveTurn {
    turn_id: String,
    /// Approval policy frozen at `turn/start` time. Stored for future
    /// `turn/steer` re-validation when policy can change mid-turn; the
    /// pump uses its own copy via `EventPumpArgs.approval_policy`.
    #[allow(dead_code)]
    approval_policy: p::AskForApproval,
}

#[derive(Debug, Error)]
pub enum TurnError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("thread `{0}` is not loaded; call thread/start or thread/resume first")]
    ThreadNotLoaded(String),
    #[error("expected_turn_id `{expected}` does not match active turn `{actual}`")]
    TurnIdMismatch { expected: String, actual: String },
    #[error("no active turn for thread `{0}`")]
    NoActiveTurn(String),
    #[error("thread `{thread_id}` already has active turn `{turn_id}`")]
    TurnAlreadyActive { thread_id: String, turn_id: String },
    #[error("input translation failed: {0}")]
    InputTranslation(String),
    #[error("pi rpc error: {0}")]
    PiRpc(String),
    #[error("review/start is not implemented in pi-bridge v1")]
    ReviewUnsupported,
}

impl TurnError {
    pub fn rpc_code(&self) -> i64 {
        match self {
            TurnError::InvalidParams(_)
            | TurnError::TurnIdMismatch { .. }
            | TurnError::ThreadNotLoaded(_)
            | TurnError::NoActiveTurn(_)
            | TurnError::TurnAlreadyActive { .. }
            | TurnError::InputTranslation(_) => p::error_codes::INVALID_PARAMS,
            TurnError::ReviewUnsupported => p::error_codes::METHOD_NOT_FOUND,
            TurnError::PiRpc(_) => p::error_codes::INTERNAL_ERROR,
        }
    }
}

// ============================================================================
// turn/start
// ============================================================================

pub async fn handle_turn_start(
    state: &Arc<ConnectionState>,
    params: p::TurnStartParams,
) -> Result<p::TurnStartResponse, TurnError> {
    // Resume the thread from its persisted session if the pool has no live
    // process for it (idle-reaped, or cleared by a daemon restart). Without
    // this, a follow-up turn from a backgrounded client fails with
    // `ThreadNotLoaded` instead of continuing the conversation.
    let handle = match state.pi_pool().get(&params.thread_id).await {
        Some(handle) => handle,
        None => {
            let entry = state
                .thread_index()
                .lookup(&params.thread_id)
                .await
                .ok_or_else(|| TurnError::ThreadNotLoaded(params.thread_id.clone()))?;
            crate::handlers::thread::load_or_resume_handle(state, &params.thread_id, &entry)
                .await
                .map_err(|e| match e {
                    crate::handlers::thread::ThreadError::NotFound(id) => {
                        TurnError::ThreadNotLoaded(id)
                    }
                    other => TurnError::PiRpc(other.to_string()),
                })?
        }
    };

    apply_overrides(&handle, &params).await;

    let prompt = translate_user_input(&params.input)
        .map_err(|e| TurnError::InputTranslation(e.to_string()))?;
    let index_preview = state
        .thread_index()
        .lookup(&params.thread_id)
        .await
        .map(|entry| {
            if entry.preview.trim().is_empty() || entry.preview == "(no messages)" {
                prompt.message.clone()
            } else {
                entry.preview
            }
        })
        .unwrap_or_else(|| prompt.message.clone());

    let next_turn_index = next_turn_index(&handle).await?;

    let turn_id = Uuid::now_v7().to_string();
    let approval_policy = params
        .approval_policy
        .clone()
        .or(state.defaults().approval_policy)
        .unwrap_or(p::AskForApproval::OnRequest);

    register_active_turn(&params.thread_id, &turn_id, approval_policy.clone()).map_err(
        |active_turn_id| TurnError::TurnAlreadyActive {
            thread_id: params.thread_id.clone(),
            turn_id: active_turn_id,
        },
    )?;

    // Mark the pool entry active so the LRU reaper doesn't snipe a turn
    // mid-flight. Cleared by the pump on `agent_end`.
    state.pi_pool().mark_active(&params.thread_id).await;

    let started_at = now_unix_secs();
    // Codex shape: the `turn/start` *response* has `startedAt: null` (turn
    // is in-progress; client doesn't need a wall-clock yet) but the
    // `turn/started` *notification* carries the actual `startedAt`.
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

    if state.should_emit("turn/started") {
        let frame = notification_frame(p::ServerNotification::TurnStarted(
            p::TurnStartedNotification {
                thread_id: params.thread_id.clone(),
                turn: turn_for_notif,
            },
        ));
        let _ = state.send(frame);
    }

    // Echo the user input back as a userMessage item lifecycle, the way
    // codex itself does (see app-server-protocol/src/protocol/v2.rs:5330).
    // Clients render history from these item events; if we skip the echo,
    // the user's prompt never shows up in `thread/read`.
    let user_message_item = p::ThreadItem::UserMessage {
        id: user_item_id(next_turn_index),
        content: params.input.clone(),
    };
    if state.should_emit("item/started") {
        let frame = notification_frame(p::ServerNotification::ItemStarted(
            p::ItemStartedNotification {
                item: user_message_item.clone(),
                thread_id: params.thread_id.clone(),
                turn_id: turn_id.clone(),
                parent_item_id: None,
            },
        ));
        let _ = state.send(frame);
    }
    if state.should_emit("item/completed") {
        let frame = notification_frame(p::ServerNotification::ItemCompleted(
            p::ItemCompletedNotification {
                item: user_message_item,
                thread_id: params.thread_id.clone(),
                turn_id: turn_id.clone(),
                parent_item_id: None,
            },
        ));
        let _ = state.send(frame);
    }

    // Subscribe to pi events *before* sending prompt so we don't miss the
    // first `agent_start` even on slow pumps.
    let events_rx = handle.subscribe_events();

    // Send pi `prompt`. Per the pi RPC contract, `success: true` arrives
    // after preflight only — pi continues async.
    let resp = match handle
        .send_request(pi::RpcCommand::Prompt(pi::PromptCmd {
            id: None,
            message: prompt.message,
            images: prompt.images,
            streaming_behavior: None,
        }))
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let message = error.to_string();
            clear_active_turn(&params.thread_id);
            state.pi_pool().mark_idle(&params.thread_id).await;
            emit_turn_completed(
                state,
                &params.thread_id,
                &turn_id,
                started_at,
                p::TurnStatus::Failed,
                Some(p::TurnError {
                    message: message.clone(),
                    codex_error_info: None,
                    additional_details: None,
                }),
            );
            return Err(TurnError::PiRpc(message));
        }
    };
    if !resp.success {
        let message = resp.error.unwrap_or_else(|| "prompt failed".into());
        clear_active_turn(&params.thread_id);
        state.pi_pool().mark_idle(&params.thread_id).await;
        emit_turn_completed(
            state,
            &params.thread_id,
            &turn_id,
            started_at,
            p::TurnStatus::Failed,
            Some(p::TurnError {
                message: message.clone(),
                codex_error_info: None,
                additional_details: None,
            }),
        );
        return Err(TurnError::PiRpc(message));
    }
    spawn_event_pump(EventPumpArgs {
        state: Arc::clone(state),
        handle: Arc::clone(&handle),
        thread_id: params.thread_id.clone(),
        turn_id: turn_id.clone(),
        approval_policy,
        events_rx,
        started_at,
        turn_index: next_turn_index,
    });
    let summary_state = Arc::clone(state);
    let summary_thread_id = params.thread_id.clone();
    tokio::spawn(async move {
        if let Err(error) = summary_state
            .thread_index()
            .update_preview_and_updated_at(&summary_thread_id, index_preview, chrono::Utc::now())
            .await
        {
            tracing::warn!(
                thread_id = %summary_thread_id,
                %error,
                "failed to refresh pi thread list summary"
            );
        }
    });

    Ok(p::TurnStartResponse { turn })
}

// ============================================================================
// turn/steer
// ============================================================================

pub async fn handle_turn_steer(
    state: &Arc<ConnectionState>,
    params: p::TurnSteerParams,
) -> Result<p::TurnSteerResponse, TurnError> {
    let handle = state
        .pi_pool()
        .get(&params.thread_id)
        .await
        .ok_or_else(|| TurnError::ThreadNotLoaded(params.thread_id.clone()))?;

    let active = active_turn(&params.thread_id)
        .ok_or_else(|| TurnError::NoActiveTurn(params.thread_id.clone()))?;
    if active.turn_id != params.expected_turn_id {
        return Err(TurnError::TurnIdMismatch {
            expected: params.expected_turn_id,
            actual: active.turn_id,
        });
    }

    let prompt = translate_user_input(&params.input)
        .map_err(|e| TurnError::InputTranslation(e.to_string()))?;
    let resp = handle
        .send_request(pi::RpcCommand::Steer(pi::SteerCmd {
            id: None,
            message: prompt.message,
            images: prompt.images,
        }))
        .await
        .map_err(|e| TurnError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(TurnError::PiRpc(
            resp.error.unwrap_or_else(|| "steer failed".into()),
        ));
    }
    Ok(p::TurnSteerResponse {
        turn_id: active.turn_id,
    })
}

// ============================================================================
// turn/interrupt
// ============================================================================

pub async fn handle_turn_interrupt(
    state: &Arc<ConnectionState>,
    params: p::TurnInterruptParams,
) -> Result<p::TurnInterruptResponse, TurnError> {
    let handle = state
        .pi_pool()
        .get(&params.thread_id)
        .await
        .ok_or_else(|| TurnError::ThreadNotLoaded(params.thread_id.clone()))?;

    if let Some(active) = active_turn(&params.thread_id) {
        if active.turn_id != params.turn_id {
            return Err(TurnError::TurnIdMismatch {
                expected: params.turn_id,
                actual: active.turn_id,
            });
        }
    }
    // No active turn? Still send pi abort — pi treats it as a no-op when
    // nothing is running, and the codex client may legitimately race
    // turn/interrupt with a turn that just finished.

    let resp = handle
        .send_request(pi::RpcCommand::Abort(pi::BareCmd::default()))
        .await
        .map_err(|e| TurnError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(TurnError::PiRpc(
            resp.error.unwrap_or_else(|| "abort failed".into()),
        ));
    }
    Ok(p::TurnInterruptResponse::default())
}

// ============================================================================
// review/start
// ============================================================================

pub async fn handle_review_start(
    _state: &Arc<ConnectionState>,
    _params: p::ReviewStartParams,
) -> Result<p::ReviewStartResponse, TurnError> {
    // Pi has no review mode; faking it requires a custom prompt
    // template + EnteredReviewMode/ExitedReviewMode bookend items, which
    // the plan flags as "OR return error 'unimplemented' if simpler for
    // v1". Returning method_not_found here.
    Err(TurnError::ReviewUnsupported)
}

// ============================================================================
// helpers + event pump
// ============================================================================

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn register_active_turn(
    thread_id: &str,
    turn_id: &str,
    approval_policy: p::AskForApproval,
) -> Result<(), String> {
    let mut active_turns = ACTIVE_TURNS.lock().unwrap();
    if let Some(active) = active_turns.get(thread_id) {
        return Err(active.turn_id.clone());
    }
    active_turns.insert(
        thread_id.to_string(),
        ActiveTurn {
            turn_id: turn_id.to_string(),
            approval_policy,
        },
    );
    Ok(())
}

fn active_turn(thread_id: &str) -> Option<ActiveTurn> {
    ACTIVE_TURNS.lock().unwrap().get(thread_id).cloned()
}

pub(crate) fn active_turn_id(thread_id: &str) -> Option<String> {
    active_turn(thread_id).map(|turn| turn.turn_id)
}

fn clear_active_turn(thread_id: &str) {
    ACTIVE_TURNS.lock().unwrap().remove(thread_id);
}

async fn apply_overrides(handle: &Arc<PiProcessHandle>, params: &p::TurnStartParams) {
    if let Some((provider, model_id)) = params.model.as_deref().and_then(split_model_selection) {
        let _ = handle
            .send_request(pi::RpcCommand::SetModel(pi::SetModelCmd {
                id: None,
                provider: provider.to_string(),
                model_id: model_id.to_string(),
            }))
            .await;
    }
    if let Some(effort) = params.effort {
        let level = match effort {
            p::ReasoningEffort::None => pi::ThinkingLevel::Off,
            p::ReasoningEffort::Minimal => pi::ThinkingLevel::Minimal,
            p::ReasoningEffort::Low => pi::ThinkingLevel::Low,
            p::ReasoningEffort::Medium => pi::ThinkingLevel::Medium,
            p::ReasoningEffort::High => pi::ThinkingLevel::High,
            p::ReasoningEffort::XHigh => pi::ThinkingLevel::Xhigh,
            p::ReasoningEffort::Max => pi::ThinkingLevel::Max,
        };
        let _ = handle
            .send_request(pi::RpcCommand::SetThinkingLevel(pi::SetThinkingLevelCmd {
                id: None,
                level,
            }))
            .await;
    }
}

fn split_model_selection(model: &str) -> Option<(&str, &str)> {
    let (provider, model_id) = model.trim().split_once('/')?;
    let provider = provider.trim();
    let model_id = model_id.trim();
    (!provider.is_empty() && !model_id.is_empty()).then_some((provider, model_id))
}

async fn next_turn_index(handle: &Arc<PiProcessHandle>) -> Result<usize, TurnError> {
    let resp = handle
        .send_request(pi::RpcCommand::GetMessages(pi::BareCmd::default()))
        .await
        .map_err(|e| TurnError::PiRpc(e.to_string()))?;
    if !resp.success {
        return Err(TurnError::PiRpc(
            resp.error.unwrap_or_else(|| "get_messages failed".into()),
        ));
    }
    let data: pi::GetMessagesData = serde_json::from_value(
        resp.data
            .ok_or_else(|| TurnError::PiRpc("missing messages data".into()))?,
    )
    .map_err(|e| TurnError::PiRpc(format!("decode messages: {e}")))?;
    Ok(translate_messages(&data.messages).len())
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

struct EventPumpArgs {
    state: Arc<ConnectionState>,
    handle: Arc<PiProcessHandle>,
    thread_id: String,
    turn_id: String,
    approval_policy: p::AskForApproval,
    events_rx: broadcast::Receiver<pi::PiEvent>,
    started_at: i64,
    turn_index: usize,
}

fn spawn_event_pump(args: EventPumpArgs) {
    tokio::spawn(async move {
        run_event_pump(args).await;
    });
}

pub(crate) fn spawn_compaction_event_pump(
    state: Arc<ConnectionState>,
    thread_id: String,
    turn_id: String,
    mut events_rx: broadcast::Receiver<pi::PiEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut translator = EventTranslatorState::new(thread_id.clone(), turn_id);
        loop {
            let event = match tokio::time::timeout(Duration::from_secs(300), events_rx.recv()).await
            {
                Err(_) => {
                    tracing::warn!(
                        thread_id = %thread_id,
                        "compaction event pump timed out waiting for lifecycle completion"
                    );
                    return;
                }
                Ok(result) => match result {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(
                            thread_id = %thread_id,
                            "compaction event pump lagged by {count} events"
                        );
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                },
            };

            let finished = matches!(event, pi::PiEvent::CompactionEnd { .. });
            if matches!(
                event,
                pi::PiEvent::CompactionStart { .. } | pi::PiEvent::CompactionEnd { .. }
            ) {
                for notification in translator.translate(event) {
                    if state_should_emit(&state, &notification) {
                        let _ = state.send(notification_frame(notification));
                    }
                }
            }
            if finished {
                return;
            }
        }
    })
}

async fn run_event_pump(mut args: EventPumpArgs) {
    let mut translator = EventTranslatorState::with_turn_index(
        args.thread_id.clone(),
        args.turn_id.clone(),
        args.turn_index,
    );
    let mut stop_reason: Option<pi::StopReason> = None;
    let mut error_message: Option<String> = None;

    loop {
        let event = match args.events_rx.recv().await {
            Ok(ev) => ev,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(
                    thread_id = %args.thread_id,
                    turn_id = %args.turn_id,
                    "event pump lagged by {n} events; reconciling from agent history"
                );
                translator.mark_lagged();
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                // Pi process exited unexpectedly — fall through to emit
                // a synthetic agent_end.
                if stop_reason.is_none() && error_message.is_none() {
                    error_message = Some("pi process exited".into());
                }
                break;
            }
        };

        // Approval interception for bash tool calls. Pi has already
        // started running the command by the time we see this event;
        // the bridge can only either let the resulting item show up in
        // the codex transcript or hide it. See approval.rs rustdoc.
        if let pi::PiEvent::ToolExecutionStart {
            tool_name,
            args: tool_args,
            ..
        } = &event
        {
            if tool_name == "bash"
                && approval::should_request_approval(
                    &args.approval_policy,
                    approval::ApprovalKind::Command,
                )
            {
                let item_id = Uuid::now_v7().to_string();
                let cmd_str = tool_args
                    .get("command")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let outcome = approval::request_command_approval(
                    &args.state,
                    p::CommandExecutionRequestApprovalParams {
                        thread_id: args.thread_id.clone(),
                        turn_id: args.turn_id.clone(),
                        item_id,
                        command: cmd_str,
                        ..Default::default()
                    },
                    None,
                )
                .await;
                if let Ok(outcome) = outcome {
                    if outcome.should_abort_turn() {
                        let _ = args
                            .handle
                            .send_request(pi::RpcCommand::Abort(pi::BareCmd::default()))
                            .await;
                    }
                    // Approve / decline: we still forward pi's events
                    // since the command is already running. The codex
                    // client treats `decline` as "let it finish but
                    // don't auto-trust similar commands later" — that
                    // matches pi's behavior anyway.
                    let _ = outcome;
                } else {
                    tracing::warn!(
                        thread_id = %args.thread_id,
                        "approval request failed; forwarding tool item anyway"
                    );
                }
            }
        }

        // Extension UI bridging. Pi → codex → client → codex → pi.
        if let pi::PiEvent::ExtensionUiRequest(req) = &event {
            handle_extension_ui_request(&args, req).await;
            continue;
        }

        // Capture Pi's terminal reason before consuming the event for
        // translation. The translator closes items but the pump owns the
        // enclosing codex turn lifecycle.
        if let Some((reason, message)) = event_terminal_state(&event) {
            stop_reason = Some(reason);
            error_message = message;
        }

        let notifications = translator.translate(event.clone());
        for notif in notifications {
            if !state_should_emit(&args.state, &notif) {
                continue;
            }
            let frame = notification_frame(notif);
            let _ = args.state.send(frame);
        }

        if matches!(&event, pi::PiEvent::AgentEnd { .. }) {
            break;
        }
    }

    for notif in translator.finish() {
        if state_should_emit(&args.state, &notif) {
            let frame = notification_frame(notif);
            let _ = args.state.send(frame);
        }
    }

    let (status, error) = turn_terminal_state(stop_reason, error_message.as_deref());
    emit_turn_completed(
        &args.state,
        &args.thread_id,
        &args.turn_id,
        args.started_at,
        status,
        error,
    );

    clear_active_turn(&args.thread_id);
    args.state.pi_pool().mark_idle(&args.thread_id).await;
}

fn event_terminal_state(event: &pi::PiEvent) -> Option<(pi::StopReason, Option<String>)> {
    let assistant = match event {
        pi::PiEvent::MessageUpdate {
            assistant_message_event: pi::AssistantMessageEvent::Done { reason, message },
            ..
        } => return Some((*reason, normalized_error(message.error_message.as_deref()))),
        pi::PiEvent::MessageUpdate {
            assistant_message_event: pi::AssistantMessageEvent::Error { reason, error },
            ..
        } => return Some((*reason, normalized_error(error.error_message.as_deref()))),
        pi::PiEvent::MessageEnd {
            message: pi::AgentMessage::Assistant(message),
        }
        | pi::PiEvent::TurnEnd {
            message: pi::AgentMessage::Assistant(message),
            ..
        } => message,
        pi::PiEvent::AgentEnd { messages } => {
            messages.iter().rev().find_map(|message| match message {
                pi::AgentMessage::Assistant(message) => Some(message),
                _ => None,
            })?
        }
        _ => return None,
    };
    Some((
        assistant.stop_reason,
        normalized_error(assistant.error_message.as_deref()),
    ))
}

fn normalized_error(message: Option<&str>) -> Option<String> {
    message
        .map(str::trim)
        .filter(|message| !message.is_empty())
        .map(str::to_string)
}

fn emit_turn_completed(
    state: &Arc<ConnectionState>,
    thread_id: &str,
    turn_id: &str,
    started_at: i64,
    status: p::TurnStatus,
    error: Option<p::TurnError>,
) {
    let completed_at = now_unix_secs();
    let turn = p::Turn {
        id: turn_id.to_string(),
        items: Vec::new(),
        items_view: p::default_items_view(),
        status,
        error,
        started_at: Some(started_at),
        completed_at: Some(completed_at),
        duration_ms: Some(((completed_at - started_at) * 1000).max(0)),
    };
    if state.should_emit("turn/completed") {
        let frame = notification_frame(p::ServerNotification::TurnCompleted(
            p::TurnCompletedNotification {
                thread_id: thread_id.to_string(),
                turn,
            },
        ));
        let _ = state.send(frame);
    }
}

/// Map a `ServerNotification` to its `method` string and consult the
/// connection's opt-out list. Avoids serializing twice in the happy path
/// by using the enum's discriminant directly.
fn state_should_emit(state: &Arc<ConnectionState>, notif: &p::ServerNotification) -> bool {
    let method = match notif {
        p::ServerNotification::Error(_) => "error",
        p::ServerNotification::ThreadStarted(_) => "thread/started",
        p::ServerNotification::ThreadStatusChanged(_) => "thread/status/changed",
        p::ServerNotification::ThreadArchived(_) => "thread/archived",
        p::ServerNotification::ThreadUnarchived(_) => "thread/unarchived",
        p::ServerNotification::ThreadClosed(_) => "thread/closed",
        p::ServerNotification::SkillsChanged(_) => "skills/changed",
        p::ServerNotification::ThreadNameUpdated(_) => "thread/name/updated",
        p::ServerNotification::ThreadGoalCleared(_) => "thread/goal/cleared",
        p::ServerNotification::ThreadTokenUsageUpdated(_) => "thread/tokenUsage/updated",
        p::ServerNotification::TurnStarted(_) => "turn/started",
        p::ServerNotification::TurnCompleted(_) => "turn/completed",
        p::ServerNotification::TurnDiffUpdated(_) => "turn/diff/updated",
        p::ServerNotification::TurnPlanUpdated(_) => "turn/plan/updated",
        p::ServerNotification::HookStarted(_) => "hook/started",
        p::ServerNotification::HookCompleted(_) => "hook/completed",
        p::ServerNotification::ItemStarted(_) => "item/started",
        p::ServerNotification::ItemCompleted(_) => "item/completed",
        p::ServerNotification::AgentMessageDelta(_) => "item/agentMessage/delta",
        p::ServerNotification::ReasoningTextDelta(_) => "item/reasoning/textDelta",
        p::ServerNotification::ReasoningSummaryTextDelta(_) => "item/reasoning/summaryTextDelta",
        p::ServerNotification::ReasoningSummaryPartAdded(_) => "item/reasoning/summaryPartAdded",
        p::ServerNotification::CommandExecutionOutputDelta(_) => {
            "item/commandExecution/outputDelta"
        }
        p::ServerNotification::CommandExecOutputDelta(_) => "command/exec/outputDelta",
        p::ServerNotification::FileChangeOutputDelta(_) => "item/fileChange/outputDelta",
        p::ServerNotification::FileChangePatchUpdated(_) => "item/fileChange/patchUpdated",
        p::ServerNotification::McpToolCallProgress(_) => "item/mcpToolCall/progress",
        p::ServerNotification::DynamicToolCallArgumentsDelta(_) => {
            "item/dynamicToolCall/argumentsDelta"
        }
        p::ServerNotification::ContextCompacted(_) => "thread/compacted",
        p::ServerNotification::ModelRerouted(_) => "model/rerouted",
        p::ServerNotification::Warning(_) => "warning",
        p::ServerNotification::ConfigWarning(_) => "configWarning",
        p::ServerNotification::DeprecationNotice(_) => "deprecationNotice",
        p::ServerNotification::ServerRequestResolved(_) => "serverRequest/resolved",
        p::ServerNotification::McpServerStatusUpdated(_) => "mcpServer/startupStatus/updated",
        p::ServerNotification::AccountRateLimitsUpdated(_) => "account/rateLimits/updated",
        p::ServerNotification::RemoteControlStatusChanged(_) => "remoteControl/status/changed",
    };
    state.should_emit(method)
}

async fn handle_extension_ui_request(args: &EventPumpArgs, req: &pi::ExtensionUiRequest) {
    use pi::ExtensionUiRequest as R;

    // Translate pi's `select`/`input`/`confirm`/`editor` to a single
    // codex `requestUserInput` question, dispatch, and feed the answer
    // back to pi as `extension_ui_response`. Fire-and-forget UI hints
    // (notify, setStatus, setWidget, setTitle, setEditorText) get
    // dropped — they're pi-specific affordances that codex doesn't
    // model.
    let (pi_id, question) = match req {
        R::Select {
            id, title, options, ..
        } => (
            id.clone(),
            p::ToolRequestUserInputQuestion {
                id: "value".into(),
                header: String::new(),
                question: title.clone(),
                is_other: false,
                is_secret: false,
                options: Some(
                    options
                        .iter()
                        .map(|o| p::ToolRequestUserInputOption {
                            label: o.clone(),
                            description: String::new(),
                        })
                        .collect(),
                ),
            },
        ),
        R::Confirm {
            id, title, message, ..
        } => (
            id.clone(),
            p::ToolRequestUserInputQuestion {
                id: "value".into(),
                header: title.clone(),
                question: message.clone(),
                is_other: false,
                is_secret: false,
                options: Some(vec![
                    p::ToolRequestUserInputOption {
                        label: "yes".into(),
                        description: String::new(),
                    },
                    p::ToolRequestUserInputOption {
                        label: "no".into(),
                        description: String::new(),
                    },
                ]),
            },
        ),
        R::Input {
            id,
            title,
            placeholder,
            ..
        } => (
            id.clone(),
            p::ToolRequestUserInputQuestion {
                id: "value".into(),
                header: String::new(),
                question: title.clone(),
                is_other: placeholder.is_some(),
                is_secret: false,
                options: None,
            },
        ),
        R::Editor { id, title, .. } => (
            id.clone(),
            p::ToolRequestUserInputQuestion {
                id: "value".into(),
                header: String::new(),
                question: title.clone(),
                is_other: true,
                is_secret: false,
                options: None,
            },
        ),
        R::Notify { .. }
        | R::SetStatus { .. }
        | R::SetWidget { .. }
        | R::SetTitle { .. }
        | R::SetEditorText { .. } => {
            // Fire-and-forget pi UI hints. No codex equivalent.
            return;
        }
    };

    let item_id = Uuid::now_v7().to_string();
    let answers = approval::request_user_input(
        &args.state,
        args.thread_id.clone(),
        args.turn_id.clone(),
        item_id,
        vec![question],
        None,
    )
    .await;

    let response = match answers {
        Ok(map) => match map.get("value").and_then(|a| a.answers.first()).cloned() {
            Some(value) => {
                if matches!(req, pi::ExtensionUiRequest::Confirm { .. }) {
                    pi::ExtensionUiResponse::Confirmed {
                        id: pi_id,
                        confirmed: value == "yes",
                    }
                } else {
                    pi::ExtensionUiResponse::Value { id: pi_id, value }
                }
            }
            None => pi::ExtensionUiResponse::Cancelled {
                id: pi_id,
                cancelled: true,
            },
        },
        Err(err) => {
            tracing::warn!(%err, "extension_ui_request: codex client did not respond");
            pi::ExtensionUiResponse::Cancelled {
                id: pi_id,
                cancelled: true,
            }
        }
    };

    if let Err(err) = args
        .handle
        .send_notification(&pi::RpcCommand::ExtensionUiResponse(response))
    {
        tracing::warn!(%err, "failed to forward extension_ui_response to pi");
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    async fn dummy_state() -> Arc<ConnectionState> {
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::ThreadIndex::open_at(dir.path().join("threads.json"))
            .await
            .unwrap();
        std::mem::forget(dir);
        let (state, _rx) = ConnectionState::for_test(
            Arc::new(crate::pool::PiPool::new("/dev/null")),
            index,
            Default::default(),
        );
        state
    }

    #[tokio::test]
    async fn turn_start_returns_thread_not_loaded_when_pool_empty() {
        let state = dummy_state().await;
        let err = handle_turn_start(
            &state,
            p::TurnStartParams {
                thread_id: "missing".into(),
                input: vec![p::UserInput::Text {
                    text: "hi".into(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TurnError::ThreadNotLoaded(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn turn_steer_rejects_unknown_thread() {
        let state = dummy_state().await;
        let err = handle_turn_steer(
            &state,
            p::TurnSteerParams {
                thread_id: "missing".into(),
                input: vec![p::UserInput::Text {
                    text: "x".into(),
                    text_elements: Vec::new(),
                }],
                expected_turn_id: "any".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TurnError::ThreadNotLoaded(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn turn_interrupt_returns_thread_not_loaded_when_pool_empty() {
        let state = dummy_state().await;
        let err = handle_turn_interrupt(
            &state,
            p::TurnInterruptParams {
                thread_id: "missing".into(),
                turn_id: "tu".into(),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TurnError::ThreadNotLoaded(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn review_start_is_unsupported() {
        let state = dummy_state().await;
        let err = handle_review_start(
            &state,
            p::ReviewStartParams {
                thread_id: "t".into(),
                target: p::ReviewTarget::UncommittedChanges,
                delivery: None,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TurnError::ReviewUnsupported), "got {err:?}");
        assert_eq!(err.rpc_code(), p::error_codes::METHOD_NOT_FOUND);
    }

    #[test]
    fn active_turn_table_round_trip() {
        let thread_id = format!("test-{}", Uuid::now_v7());
        register_active_turn(&thread_id, "tu1", p::AskForApproval::OnRequest).unwrap();
        let active = active_turn(&thread_id).unwrap();
        assert_eq!(active.turn_id, "tu1");
        assert!(matches!(
            active.approval_policy,
            p::AskForApproval::OnRequest
        ));
        clear_active_turn(&thread_id);
        assert!(active_turn(&thread_id).is_none());
    }

    #[test]
    fn active_turn_registration_rejects_overlapping_turns() {
        let thread_id = format!("test-{}", Uuid::now_v7());
        register_active_turn(&thread_id, "turn-one", p::AskForApproval::Never).unwrap();

        let existing =
            register_active_turn(&thread_id, "turn-two", p::AskForApproval::Never).unwrap_err();

        assert_eq!(existing, "turn-one");
        assert_eq!(active_turn(&thread_id).unwrap().turn_id, "turn-one");
        clear_active_turn(&thread_id);
    }

    #[test]
    fn turn_error_rpc_codes() {
        assert_eq!(
            TurnError::InvalidParams("x".into()).rpc_code(),
            p::error_codes::INVALID_PARAMS
        );
        assert_eq!(
            TurnError::ReviewUnsupported.rpc_code(),
            p::error_codes::METHOD_NOT_FOUND
        );
        assert_eq!(
            TurnError::PiRpc("oops".into()).rpc_code(),
            p::error_codes::INTERNAL_ERROR
        );
    }

    #[test]
    fn agent_end_carries_the_final_assistant_outcome() {
        let messages: Vec<pi::AgentMessage> = serde_json::from_value(serde_json::json!([{
            "role": "assistant",
            "content": [],
            "api": "openai-completions",
            "provider": "local-studio",
            "model": "offline-model",
            "usage": {
                "input": 0,
                "output": 0,
                "cacheRead": 0,
                "cacheWrite": 0,
                "totalTokens": 0,
                "cost": {
                    "input": 0,
                    "output": 0,
                    "cacheRead": 0,
                    "cacheWrite": 0,
                    "total": 0
                }
            },
            "stopReason": "error",
            "errorMessage": "  model is not running  ",
            "timestamp": 1
        }]))
        .unwrap();

        assert_eq!(
            event_terminal_state(&pi::PiEvent::AgentEnd { messages }),
            Some((
                pi::StopReason::Error,
                Some("model is not running".to_string())
            ))
        );
    }
}
