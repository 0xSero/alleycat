use alleycat_bridge_core::Conn;
use serde_json::{Value, json};

use crate::approval;
use crate::index::ThreadIndex;
use crate::opencode_client::OpencodeClient;
use crate::pty::PtyState;
use crate::state::{BridgeState, PartKind, TokenUsageBreakdown};
use crate::translate::parts::message_to_turn_items_with_context;
use crate::translate::tool::{
    ToolPartContext, tool_part_side_notifications, tool_part_status_is_terminal,
    tool_part_to_item_with_context,
};

/// Bundle of references threaded through every SSE event handler in this
/// module. Fields are all `&` references; the struct derives `Copy` so handlers
/// can pass it through without churn at every call site.
///
/// Created once per inbound SSE frame in `handlers::spawn_event_pump`.
#[derive(Clone, Copy)]
pub struct RouteContext<'a> {
    pub conn: &'a Conn,
    pub index: &'a ThreadIndex,
    pub state: &'a BridgeState,
    pub client: &'a OpencodeClient,
    pub pty_state: &'a PtyState,
}

pub async fn route_event(rc: RouteContext<'_>, event: Value) {
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
    let props = event.get("properties").cloned().unwrap_or(Value::Null);

    // Session-less branches first — these don't require a thread binding.
    if route_global_event(rc, event_type, &props).await {
        return;
    }
    if route_pty_event(rc, event_type, &props) {
        return;
    }

    // `session.created` arrives BEFORE we have a thread binding when another
    // client (e.g. opencode TUI) initiated the session. Bind it first, then
    // continue to the main match so any session-scoped logic still runs.
    if event_type == "session.created" {
        handle_session_created(rc, &props).await;
        return;
    }

    let session_id = props
        .get("sessionID")
        .or_else(|| props.pointer("/info/id"))
        .and_then(Value::as_str);
    let Some(session_id) = session_id else {
        return;
    };
    let Some(thread_id) = rc.index.thread_for_session(session_id) else {
        return;
    };
    let active = rc.state.active_turn(&thread_id);
    let turn_id = active
        .as_ref()
        .map(|turn| turn.turn_id.clone())
        .unwrap_or_else(|| "opencode-turn".to_string());

    // step-finish parts surface as `message.part.updated` and feed token usage.
    if event_type == "message.part.updated" {
        let part = props.get("part").cloned().unwrap_or(Value::Null);
        if part.get("type").and_then(Value::as_str) == Some("step-finish") {
            emit_token_usage(rc, &thread_id, &turn_id, &part);
        }
    }

    match event_type {
        "session.status" => {
            // opencode session statuses are `{type: "idle" | "busy" |
            // "retry"}` (`packages/opencode/src/session/status.ts:10-23`).
            // codex `ThreadStatus` only knows `idle | active | notLoaded |
            // systemError`, so fold "busy"/"retry" onto `active` (the only
            // codex variant signaling "turn in progress").
            let raw = props.get("status").cloned().unwrap_or(Value::Null);
            let kind = raw.get("type").and_then(Value::as_str).unwrap_or("idle");
            let status = match kind {
                "idle" => json!({"type": "idle"}),
                "busy" | "retry" => json!({"type": "active", "activeFlags": []}),
                _ => json!({"type": "idle"}),
            };
            let _ = rc.conn.notifier().send_notification(
                "thread/status/changed",
                json!({"threadId":thread_id,"status":status}),
            );
        }
        "session.idle" => {
            if let Some(active) = rc.state.take_active_turn(&thread_id) {
                emit_idle_message_fallback(rc, &thread_id, &active.turn_id, &active).await;
                let completed_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(active.started_at);
                let duration_ms = (completed_at - active.started_at) * 1000;
                let _ = rc.conn.notifier().send_notification(
                    "turn/completed",
                    json!({
                        "threadId": thread_id,
                        "turn": {
                            "id": turn_id,
                            "items": [],
                            "itemsView": "full",
                            "status": "completed",
                            "error": null,
                            "startedAt": active.started_at,
                            "completedAt": completed_at,
                            "durationMs": duration_ms,
                        }
                    }),
                );
            }
        }
        "message.updated" => {
            handle_message_updated(rc, &props, &thread_id, &turn_id);
        }
        "message.removed" => {
            if let Some(message_id) = props.get("messageID").and_then(Value::as_str) {
                rc.state.forget_message(message_id);
            }
        }
        "message.part.updated" => {
            handle_message_part_updated(rc, &props, &thread_id, &turn_id);
        }
        "message.part.delta" => {
            handle_message_part_delta(rc, &props, &thread_id, &turn_id);
        }
        "todo.updated" => {
            let plan = props.get("todos").cloned().unwrap_or(json!([]));
            let _ = rc.conn.notifier().send_notification(
                "turn/plan/updated",
                json!({"threadId":thread_id,"turnId":turn_id,"plan":plan}),
            );
        }
        "session.compacted" => {
            let _ = rc.conn.notifier().send_notification(
                "thread/compacted",
                json!({"threadId":thread_id,"turnId":turn_id}),
            );
        }
        "session.error" => {
            let error = normalize_turn_error(props.get("error").unwrap_or(&Value::Null));
            let _ = rc.conn.notifier().send_notification(
                "error",
                json!({"threadId":thread_id,"turnId":turn_id,"error":error,"willRetry":false}),
            );
        }
        "session.updated" => {
            handle_session_updated(rc, &props, &thread_id).await;
        }
        "session.diff" => {
            let diff = props.get("diff").cloned().unwrap_or(json!([]));
            let unified = file_diffs_to_unified(&diff);
            let _ = rc.conn.notifier().send_notification(
                "turn/diff/updated",
                json!({"threadId":thread_id,"turnId":turn_id,"diff":unified}),
            );
        }
        "permission.asked" => {
            handle_permission_asked(rc, &thread_id, &turn_id, &props);
        }
        "question.asked" => {
            handle_question_asked(rc, &thread_id, &turn_id, &props);
        }
        _ => {}
    }
}

/// Spawn the codex `requestUserInput` round-trip for `question.asked`. Same
/// non-blocking pattern as approvals — questions can sit for minutes while
/// the user thinks.
fn handle_question_asked(rc: RouteContext<'_>, thread_id: &str, turn_id: &str, props: &Value) {
    let request_id = match props.get("id").and_then(Value::as_str) {
        Some(id) => id.to_string(),
        None => {
            tracing::warn!("question.asked missing `id` — dropping");
            return;
        }
    };
    let questions_in = props
        .get("questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if questions_in.is_empty() {
        tracing::warn!(%request_id, "question.asked with no questions — replying with empty answers");
        let client = rc.client.clone();
        tokio::spawn(async move {
            let _ = client.question_reply(&request_id, json!([])).await;
        });
        return;
    }

    // Encode each opencode question as a codex `ToolRequestUserInputQuestion`.
    // The id is the original index as a string so we can reconstruct the
    // ordered `Array<Array<string>>` reply opencode expects.
    let questions_out = questions_in
        .iter()
        .enumerate()
        .map(|(idx, q)| {
            let header = q
                .get("header")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let text = q
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let options = q
                .get("options")
                .and_then(Value::as_array)
                .map(|opts| {
                    opts.iter()
                        .map(|opt| {
                            json!({
                                "label": opt.get("label").and_then(Value::as_str).unwrap_or(""),
                                "description": opt.get("description").and_then(Value::as_str).unwrap_or(""),
                            })
                        })
                        .collect::<Vec<_>>()
                });
            let mut entry = json!({
                "id": idx.to_string(),
                "header": header,
                "question": text,
                "isOther": q.get("custom").and_then(Value::as_bool).unwrap_or(false),
                "isSecret": false,
            });
            entry["options"] = match options {
                Some(opts) => json!(opts),
                None => Value::Null,
            };
            entry
        })
        .collect::<Vec<_>>();

    let params = json!({
        "threadId": thread_id,
        "turnId": turn_id,
        "itemId": request_id,
        "questions": questions_out,
    });

    let notifier = rc.conn.notifier().clone();
    let client = rc.client.clone();
    let question_count = questions_in.len();
    tokio::spawn(async move {
        let result = notifier
            .request(
                "item/tool/requestUserInput".to_string(),
                params,
                approval::DEFAULT_APPROVAL_TIMEOUT,
            )
            .await;
        let answers = match result {
            Ok(value) => answers_in_question_order(&value, question_count),
            Err(error) => {
                tracing::warn!(?error, %request_id, "requestUserInput round-trip failed; replying with empty answers");
                vec![Vec::new(); question_count]
            }
        };
        let answers_json = json!(answers);
        if let Err(error) = client.question_reply(&request_id, answers_json).await {
            tracing::warn!(%error, %request_id, "POST /question/{}/reply failed", request_id);
        }
    });
}

/// Re-shape a codex `ToolRequestUserInputResponse` (`answers: { id: { answers:
/// [...] } }`) into opencode's `Reply.answers: Array<Array<string>>`. The
/// outer array is ordered by the original question index; missing ids fill
/// with empty arrays (opencode's `Question.RejectedError` is the alternative,
/// but reject is signalled separately by the `rejected` SSE branch).
fn answers_in_question_order(response: &Value, count: usize) -> Vec<Vec<String>> {
    let map = response.get("answers").and_then(Value::as_object);
    (0..count)
        .map(|idx| {
            let key = idx.to_string();
            map.and_then(|m| m.get(&key))
                .and_then(|entry| entry.get("answers"))
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .collect()
}

/// Spawn the codex `requestApproval` round-trip without blocking the SSE pump
/// (a single approval can take up to `DEFAULT_APPROVAL_TIMEOUT`, ~5 minutes).
fn handle_permission_asked(rc: RouteContext<'_>, thread_id: &str, turn_id: &str, props: &Value) {
    let request_id = match props.get("id").and_then(Value::as_str) {
        Some(id) => id.to_string(),
        None => {
            tracing::warn!("permission.asked missing `id` — dropping");
            return;
        }
    };
    let permission = props
        .get("permission")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let metadata = props.get("metadata").cloned().unwrap_or(Value::Null);
    let kind = approval::classify_permission(&permission, &metadata);

    let (method, params) = match kind {
        approval::PermissionKind::Command => {
            let command = metadata
                .get("command")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| {
                    metadata
                        .get("argv")
                        .and_then(|argv| argv.as_array())
                        .map(|argv| {
                            argv.iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(" ")
                        })
                });
            let cwd = metadata
                .get("cwd")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let mut params = json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": request_id,
                "approvalId": Value::Null,
                "reason": metadata.get("reason").cloned().unwrap_or(Value::Null),
            });
            if let Some(command) = command {
                params["command"] = json!(command);
            }
            if let Some(cwd) = cwd {
                params["cwd"] = json!(cwd);
            }
            ("item/commandExecution/requestApproval".to_string(), params)
        }
        approval::PermissionKind::FileChange => {
            let params = json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": request_id,
                "reason": metadata.get("reason").cloned().unwrap_or(Value::Null),
                "grantRoot": metadata.get("path").cloned().unwrap_or(Value::Null),
            });
            ("item/fileChange/requestApproval".to_string(), params)
        }
    };

    let notifier = rc.conn.notifier().clone();
    let client = rc.client.clone();
    tokio::spawn(async move {
        let outcome = match approval::request_approval(&notifier, &method, params, None).await {
            Ok(outcome) => outcome,
            Err(error) => {
                // Timeout/closed/malformed — forward a reject so opencode
                // doesn't block on us indefinitely. This matches pi-bridge's
                // "fail closed" stance.
                tracing::warn!(%error, %request_id, "approval round-trip failed; forwarding reject");
                approval::ApprovalOutcome::Rejected
            }
        };
        if let Err(error) = client
            .permission_reply(&request_id, outcome.as_opencode_reply(), None)
            .await
        {
            tracing::warn!(%error, %request_id, "POST /permission/{}/reply failed", request_id);
        }
    });
}

/// Handle `message.updated`. For assistant messages this drives the codex
/// `item/started` / `item/completed` lifecycle for the AgentMessage item:
/// `item/started` is emitted on first sighting (with an empty text body),
/// `item/completed` is emitted when `info.time.completed` is set with the
/// assistant text accumulated from prior `message.part.delta`s.
fn handle_message_updated(rc: RouteContext<'_>, props: &Value, thread_id: &str, turn_id: &str) {
    let info = match props.get("info") {
        Some(info) => info,
        None => return,
    };
    let message_id = match info.get("id").and_then(Value::as_str) {
        Some(id) => id,
        None => return,
    };
    let role = info.get("role").and_then(Value::as_str).unwrap_or("");
    if role != "assistant" {
        // User messages are echoed by the codex client when it sends `turn/start`;
        // bridging them here would double-render. Tool/agent activity arrives via
        // `message.part.updated` instead.
        return;
    }

    if rc.state.mark_message_started(message_id) {
        let _ = rc.conn.notifier().send_notification(
            "item/started",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "item": {
                    "type": "agentMessage",
                    "id": message_id,
                    "text": "",
                    "phase": "final_answer",
                    "memoryCitation": null,
                },
            }),
        );
        // Track the in-flight assistant message id on the active turn so other
        // SSE branches (e.g. T4 delta routing) can correlate without re-reading
        // every event's `messageID`.
        let mid = message_id.to_string();
        rc.state.update_active_turn(thread_id, |turn| {
            turn.current_assistant_message_id = Some(mid);
        });
    }

    let completed_at = info.pointer("/time/completed").and_then(Value::as_i64);
    if completed_at.is_some() {
        // Close any open reasoning items for this message before the
        // AgentMessage's `item/completed` so the lifecycle order matches
        // pi/claude/codex.
        for reasoning_part_id in rc.state.take_reasoning_parts(message_id) {
            let content = rc.state.take_reasoning_text(&reasoning_part_id);
            if rc.state.mark_part_completed(&reasoning_part_id) {
                let _ = rc.conn.notifier().send_notification(
                    "item/completed",
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": {
                            "type": "reasoning",
                            "id": reasoning_part_id,
                            "summary": [],
                            "content": if content.is_empty() { vec![] } else { vec![content] },
                        },
                    }),
                );
            }
            rc.state.forget_part(&reasoning_part_id);
        }
        let text = rc.state.take_message_text(message_id);
        let _ = rc.conn.notifier().send_notification(
            "item/completed",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "item": {
                    "type": "agentMessage",
                    "id": message_id,
                    "text": text,
                    "phase": "final_answer",
                    "memoryCitation": null,
                },
            }),
        );
        rc.state.mark_message_completed(message_id);
        rc.state.update_active_turn(thread_id, |turn| {
            if turn.current_assistant_message_id.as_deref() == Some(message_id) {
                turn.current_assistant_message_id = None;
            }
        });
    }
}

async fn emit_idle_message_fallback(
    rc: RouteContext<'_>,
    thread_id: &str,
    turn_id: &str,
    active: &crate::state::ActiveTurn,
) {
    let Some(session_id) = active.session_id.as_deref() else {
        return;
    };
    let Some(message) = latest_completed_assistant_message(rc.client, session_id).await else {
        return;
    };
    let Some(message_id) = message
        .pointer("/info/id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return;
    };
    let agent_message_completed = rc.state.message_completed(&message_id);
    let binding_cwd = rc
        .index
        .by_thread(thread_id)
        .map(|binding| binding.directory);
    let tool_context = ToolPartContext {
        cwd: binding_cwd.as_deref(),
        sender_thread_id: Some(thread_id),
        include_side_channel_items: false,
    };
    let mut items = message_to_turn_items_with_context(&message, tool_context);
    if !fallback_items_ready(&items) {
        for _ in 0..12 {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let Some(next_message) =
                latest_completed_assistant_message(rc.client, session_id).await
            else {
                continue;
            };
            if next_message.pointer("/info/id").and_then(Value::as_str) != Some(message_id.as_str())
            {
                continue;
            }
            let next_items = message_to_turn_items_with_context(&next_message, tool_context);
            let ready = fallback_items_ready(&next_items);
            items = next_items;
            if ready {
                break;
            }
        }
    }

    for item in items {
        let item_type = item.get("type").and_then(Value::as_str);
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or(message_id.as_str())
            .to_string();

        if item_type != Some("agentMessage") {
            if rc.state.mark_part_completed(&item_id) {
                if rc.state.mark_part_started(&item_id) {
                    let _ = rc.conn.notifier().send_notification(
                        "item/started",
                        json!({
                            "threadId": thread_id,
                            "turnId": turn_id,
                            "item": item.clone(),
                        }),
                    );
                }
                let _ = rc.conn.notifier().send_notification(
                    "item/completed",
                    json!({
                        "threadId": thread_id,
                        "turnId": turn_id,
                        "item": item,
                    }),
                );
                rc.state.forget_part(&item_id);
            }
            continue;
        }
        if agent_message_completed {
            continue;
        }
        let text = item
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if rc.state.mark_message_started(&item_id) {
            let _ = rc.conn.notifier().send_notification(
                "item/started",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": {
                        "type": "agentMessage",
                        "id": item_id,
                        "text": "",
                        "phase": "final_answer",
                        "memoryCitation": null,
                    },
                }),
            );
        }
        if !text.is_empty() {
            let _ = rc.conn.notifier().send_notification(
                "item/agentMessage/delta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": item_id,
                    "delta": text,
                }),
            );
        }
        let _ = rc.conn.notifier().send_notification(
            "item/completed",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "item": item,
            }),
        );
        rc.state.mark_message_completed(&item_id);
    }
}

async fn latest_completed_assistant_message(
    client: &OpencodeClient,
    session_id: &str,
) -> Option<Value> {
    let messages = client
        .get(&format!("/session/{session_id}/message"))
        .await
        .ok()?;
    messages.as_array()?.iter().rev().find_map(|message| {
        let role = message.pointer("/info/role").and_then(Value::as_str)?;
        if role != "assistant" {
            return None;
        }
        message
            .pointer("/info/time/completed")
            .and_then(Value::as_i64)?;
        Some(message.clone())
    })
}

fn contains_live_tool_item(items: &[Value]) -> bool {
    items.iter().any(|item| {
        matches!(
            item.get("type").and_then(Value::as_str),
            Some(
                "commandExecution"
                    | "fileChange"
                    | "mcpToolCall"
                    | "dynamicToolCall"
                    | "webSearch"
                    | "collabAgentToolCall"
            )
        )
    })
}

fn fallback_items_ready(items: &[Value]) -> bool {
    let has_agent_message = items
        .iter()
        .any(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"));
    has_agent_message && (!mentions_tool_activity(items) || contains_live_tool_item(items))
}

fn mentions_tool_activity(items: &[Value]) -> bool {
    items.iter().any(value_mentions_tool_activity)
}

fn value_mentions_tool_activity(value: &Value) -> bool {
    match value {
        Value::String(text) => {
            let text = text.to_ascii_lowercase();
            text.contains("command") || text.contains("shell") || text.contains("tool")
        }
        Value::Array(items) => items.iter().any(value_mentions_tool_activity),
        Value::Object(map) => map.values().any(value_mentions_tool_activity),
        _ => false,
    }
}

/// Classify a part by its `type` (and `tool` field for tool parts) and emit
/// `item/started` on first sighting, `item/completed` on terminal status. Also
/// caches the resulting `PartKind` so `message.part.delta` can route field
/// updates correctly (T4 wires the per-kind delta dispatch).
fn handle_message_part_updated(
    rc: RouteContext<'_>,
    props: &Value,
    thread_id: &str,
    turn_id: &str,
) {
    let part = match props.get("part") {
        Some(part) => part,
        None => return,
    };
    let part_id = match part.get("id").and_then(Value::as_str) {
        Some(id) => id,
        None => return,
    };
    let part_type = part.get("type").and_then(Value::as_str).unwrap_or("");
    let kind = classify_part(part);
    rc.state.set_part_kind(part_id, kind);
    let binding_cwd = rc
        .index
        .by_thread(thread_id)
        .map(|binding| binding.directory);
    let tool_context = ToolPartContext {
        cwd: binding_cwd.as_deref(),
        sender_thread_id: Some(thread_id),
        include_side_channel_items: false,
    };

    // Reasoning parts get their own codex Reasoning item. We bracket each
    // part with `item/started`/`item/completed`; the closing event is fired
    // when the parent message completes (in `handle_message_updated`),
    // matching what codex does for thinking-mode turns.
    if part_type == "reasoning" {
        if rc.state.mark_part_started(part_id) {
            if let Some(message_id) = part.get("messageID").and_then(Value::as_str) {
                rc.state.register_reasoning_part(message_id, part_id);
            }
            let _ = rc.conn.notifier().send_notification(
                "item/started",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": {"type":"reasoning","id":part_id,"summary":[],"content":[]},
                }),
            );
        }
        return;
    }

    if part_type != "tool" {
        // Text-and-other non-tool parts roll into the parent assistant
        // message; their lifecycle is handled in `handle_message_updated`.
        return;
    }

    if rc.state.mark_part_started(part_id)
        && let Some(item) = tool_part_to_item_with_context(part, tool_context)
    {
        let _ = rc.conn.notifier().send_notification(
            "item/started",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
                "item": item,
            }),
        );
    }

    if tool_part_status_is_terminal(part) {
        let first_completion = rc.state.mark_part_completed(part_id);
        if first_completion && let Some(item) = tool_part_to_item_with_context(part, tool_context) {
            let _ = rc.conn.notifier().send_notification(
                "item/completed",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "item": item,
                }),
            );
        }
        // Side-channel notifications (currently `turn/plan/updated` for
        // todowrite). Fires alongside the canonical item — or in place of
        // it for tools that return `None` from tool_part_to_item.
        if first_completion {
            for (method, params) in tool_part_side_notifications(part, thread_id, turn_id) {
                let _ = rc.conn.notifier().send_notification(method, params);
            }
        }
        rc.state.forget_part(part_id);
    }
}

/// Route a `message.part.delta` SSE event to the correct codex notification
/// using the cached `PartKind` set by `handle_message_part_updated` (T3). Also
/// keeps the per-message text accumulator in sync so the trailing
/// `item/completed` payload carries the full assistant text.
fn handle_message_part_delta(rc: RouteContext<'_>, props: &Value, thread_id: &str, turn_id: &str) {
    let field = props.get("field").and_then(Value::as_str).unwrap_or("");
    let delta = props.get("delta").and_then(Value::as_str).unwrap_or("");
    let part_id = props
        .get("partID")
        .and_then(Value::as_str)
        .unwrap_or("part");
    let message_id = props.get("messageID").and_then(Value::as_str);
    let kind = rc.state.part_kind(part_id);

    // Side-effect: accumulate text-part deltas so the eventual `item/completed`
    // (emitted by handle_message_updated on info.time.completed) carries the
    // full assistant text.
    if field == "text"
        && kind == PartKind::Text
        && let Some(message_id) = message_id
    {
        rc.state.append_message_text(message_id, delta);
    }

    match (kind, field) {
        (PartKind::Text, "text") => {
            // The codex `AgentMessage` item is keyed by message id (matching
            // the surrounding `item/started`/`item/completed`); per-part ids
            // are an opencode internal detail. Sending part_id here would
            // make a client tracking deltas-by-itemId fail to correlate.
            let item_id = message_id.unwrap_or(part_id);
            let _ = rc.conn.notifier().send_notification(
                "item/agentMessage/delta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": item_id,
                    "delta": delta,
                }),
            );
        }
        (PartKind::Reasoning, "text") => {
            // Accumulate so the trailing `item/completed` Reasoning carries
            // the full content.
            rc.state.append_reasoning_text(part_id, delta);
            let _ = rc.conn.notifier().send_notification(
                "item/reasoning/textDelta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": part_id,
                    "contentIndex": 0,
                    "delta": delta,
                }),
            );
        }
        (PartKind::ToolBash, "output") => {
            let _ = rc.conn.notifier().send_notification(
                "item/commandExecution/outputDelta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": part_id,
                    "delta": delta,
                }),
            );
        }
        (PartKind::ToolMcp, "output") => {
            let _ = rc.conn.notifier().send_notification(
                "item/mcpToolCall/progress",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": part_id,
                    "message": delta,
                }),
            );
        }
        (PartKind::ToolFileChange, "output") => {
            let _ = rc.conn.notifier().send_notification(
                "item/fileChange/outputDelta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": part_id,
                    "delta": delta,
                }),
            );
        }
        (PartKind::Other, "text") => {
            // Unknown part kind but a text-shaped delta — fall back to
            // assistant message so a delta arriving before its
            // `message.part.updated` doesn't get silently dropped.
            let _ = rc.conn.notifier().send_notification(
                "item/agentMessage/delta",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "itemId": part_id,
                    "delta": delta,
                }),
            );
        }
        _ => {
            tracing::debug!(
                ?kind,
                %field,
                %part_id,
                "dropping message.part.delta with no matching codex topic"
            );
        }
    }
}

/// Inspect a `Part` and return the `PartKind` that determines how the bridge
/// routes follow-up `message.part.delta` events. Pure function (no state) so
/// it can be unit-tested.
fn classify_part(part: &Value) -> PartKind {
    let part_type = part.get("type").and_then(Value::as_str).unwrap_or("");
    match part_type {
        "text" => PartKind::Text,
        "reasoning" => PartKind::Reasoning,
        "tool" => {
            let tool = part.get("tool").and_then(Value::as_str).unwrap_or("");
            if matches!(
                tool,
                "bash" | "read" | "glob" | "grep" | "codesearch" | "list" | "ls"
            ) {
                PartKind::ToolBash
            } else if matches!(tool, "write" | "edit" | "patch" | "apply_patch") {
                PartKind::ToolFileChange
            } else if tool.contains("__") {
                PartKind::ToolMcp
            } else {
                PartKind::ToolDynamic
            }
        }
        _ => PartKind::Other,
    }
}

/// `session.created` from opencode — bind it into the index (idempotent) and
/// emit codex `thread/started`. Used when another client (opencode TUI, a peer
/// bridge) creates a session we'll subsequently see updates for.
async fn handle_session_created(rc: RouteContext<'_>, props: &Value) {
    let info = match props.get("info") {
        Some(info) => info.clone(),
        None => return,
    };
    // Subagent sessions carry a non-empty `parentID` (top-level sessions
    // omit the field). They are conversation-scoped data, so they must not
    // be bound into the index or announced as top-level threads — the same
    // rule `thread/list` enforces.
    if info
        .get("parentID")
        .and_then(Value::as_str)
        .map(str::trim)
        .is_some_and(|parent| !parent.is_empty())
    {
        return;
    }
    let binding = match rc.index.bind_session(&info).await {
        Ok(binding) => binding,
        Err(error) => {
            tracing::warn!(?error, "failed to bind session from session.created");
            return;
        }
    };
    let _ = rc.conn.notifier().send_notification(
        "thread/started",
        json!({"thread": binding_to_thread(&binding)}),
    );
}

/// `session.updated` — diff the optional fields in `info` against the cached
/// binding and emit one notification per observed change. Currently emits
/// `thread/name/updated` for `title` changes, `thread/archived` /
/// `thread/unarchived` for `time.archived` set/cleared.
async fn handle_session_updated(rc: RouteContext<'_>, props: &Value, thread_id: &str) {
    let info = match props.get("info") {
        Some(info) => info,
        None => return,
    };
    let mut binding = match rc.index.by_thread(thread_id) {
        Some(binding) => binding,
        None => return,
    };
    let mut changed = false;

    // Title change. Only emit when the field is *present* in the event (absent
    // means "unchanged"); a JSON `null` is a clear, a string is a set/rename.
    if let Some(title_value) = info.get("title") {
        let new_name = title_value.as_str().map(ToOwned::to_owned);
        if new_name != binding.name {
            binding.name = new_name.clone();
            changed = true;
            let _ = rc.conn.notifier().send_notification(
                "thread/name/updated",
                json!({
                    "threadId": thread_id,
                    "threadName": new_name,
                }),
            );
        }
    }

    // Archive transitions. The `time` block is itself optional; presence of
    // `time.archived` (any value, including null) means archive state moved.
    if let Some(time) = info.get("time")
        && let Some(archived_value) = time.get("archived")
    {
        let now_archived = !archived_value.is_null();
        if now_archived != binding.archived {
            binding.archived = now_archived;
            changed = true;
            let method = if now_archived {
                "thread/archived"
            } else {
                "thread/unarchived"
            };
            let _ = rc
                .conn
                .notifier()
                .send_notification(method, json!({"threadId": thread_id}));
        }
    }

    if changed && let Err(error) = rc.index.insert(binding).await {
        tracing::warn!(?error, %thread_id, "failed to persist session.updated change");
    }
}

/// Concatenate an opencode `FileDiff[]` into one unified-diff string. The
/// codex `turn/diff/updated.diff` field expects a single string, so we wrap
/// each FileDiff in a "diff --git a/<file> b/<file>" header followed by the
/// original `patch` body.
fn file_diffs_to_unified(diffs: &Value) -> String {
    let Some(arr) = diffs.as_array() else {
        return String::new();
    };
    let mut out = String::new();
    for entry in arr {
        let file = entry.get("file").and_then(Value::as_str).unwrap_or("");
        let patch = entry.get("patch").and_then(Value::as_str).unwrap_or("");
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("diff --git a/");
        out.push_str(file);
        out.push_str(" b/");
        out.push_str(file);
        out.push('\n');
        out.push_str(patch);
        if !patch.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

/// Mirror of `handlers::binding_to_thread` — kept local to keep events.rs
/// independent of `handlers::*` (and to avoid making the handler-side helper
/// pub crate-wide just for one call site).
fn binding_to_thread(binding: &crate::index::OpencodeBinding) -> Value {
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
        // ThreadStatus is `#[serde(tag = "type")]` — must be an object,
        // not a bare string. Sending "notLoaded" here would make the
        // phone reject the entire thread/list response.
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

fn normalize_turn_error(raw: &Value) -> Value {
    let message = raw
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| raw.pointer("/data/message").and_then(Value::as_str))
        .unwrap_or("opencode error");
    json!({ "message": message })
}

/// Handle SSE events that don't carry a session id — bridge-wide notices like
/// MCP cache invalidation or installation update notices. Returns `true` when
/// the event was consumed here (routing should stop).
async fn route_global_event(rc: RouteContext<'_>, event_type: &str, props: &Value) -> bool {
    match event_type {
        "mcp.tools.changed" => {
            // Cache-invalidation hint for `model/list` / `mcpServerStatus/list`.
            // The bridge does not yet maintain those caches; once it does
            // (G2/T15), `rc.state.invalidate_mcp_cache()` will be called here.
            let _ = rc;
            true
        }
        "installation.update-available" => {
            let _ = rc
                .conn
                .notifier()
                .send_notification("deprecationNotice", deprecation_notice_for_update(props));
            true
        }
        _ => false,
    }
}

/// Route opencode `pty.*` SSE events into the per-process state machine. Each
/// chunk of buffered/streamed output flows through `PtyProcess` (driven by the
/// websocket task), so this handler only resolves lifecycle: `pty.exited`
/// fires the exit oneshot, and streaming subscribers fan output deltas out as
/// `command/exec/outputDelta` notifications. Returns `true` when the event
/// was a `pty.*` event so `route_event` should stop dispatching.
fn route_pty_event(rc: RouteContext<'_>, event_type: &str, props: &Value) -> bool {
    match event_type {
        "pty.created" | "pty.updated" | "pty.deleted" => true,
        "pty.exited" => {
            let pty_id = props.get("id").and_then(Value::as_str).unwrap_or("");
            let exit_code = props.get("exitCode").and_then(Value::as_i64).unwrap_or(-1) as i32;
            // Capture the streaming process_id (if registered) before we
            // clear the registry, so we can fire one final outputDelta-style
            // notification path if needed. Today we only resolve the exit
            // oneshot — the buffered handler builds its response from the
            // accumulated output buffer.
            rc.pty_state.finish(pty_id, exit_code);
            true
        }
        _ => false,
    }
}

/// Parse an opencode `step-finish` part's `tokens` block into the codex
/// `TokenUsageBreakdown` shape, accumulate against the thread's running
/// totals, and emit `thread/tokenUsage/updated`.
fn emit_token_usage(rc: RouteContext<'_>, thread_id: &str, turn_id: &str, part: &Value) {
    let last = step_finish_tokens(part);
    let snapshot = rc.state.record_token_usage(thread_id, last);
    let _ = rc.conn.notifier().send_notification(
        "thread/tokenUsage/updated",
        token_usage_payload(thread_id, turn_id, &snapshot),
    );
}

/// Pure function: extract a codex `TokenUsageBreakdown` from an opencode
/// `step-finish` part. Exposed to unit tests so we can verify the math
/// without standing up a `Conn`.
fn step_finish_tokens(part: &Value) -> TokenUsageBreakdown {
    let tokens = part.get("tokens").cloned().unwrap_or(Value::Null);
    let input = number_field(&tokens, "input");
    let output = number_field(&tokens, "output");
    let reasoning = number_field(&tokens, "reasoning");
    let cache_read = tokens
        .pointer("/cache/read")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    // Codex `total_tokens` is the sum of input+output (cached-input is a
    // subset of input). Prefer the opencode-supplied total when present,
    // else derive it.
    let total = number_field(&tokens, "total");
    let total = if total > 0 { total } else { input + output };
    TokenUsageBreakdown {
        total_tokens: total,
        input_tokens: input,
        cached_input_tokens: cache_read,
        output_tokens: output,
        reasoning_output_tokens: reasoning,
    }
}

fn token_usage_payload(
    thread_id: &str,
    turn_id: &str,
    snapshot: &crate::state::ThreadTokenUsageState,
) -> Value {
    json!({
        "threadId": thread_id,
        "turnId": turn_id,
        "tokenUsage": {
            "total": snapshot.total.to_json(),
            "last": snapshot.last.to_json(),
            "modelContextWindow": Value::Null,
        },
    })
}

fn number_field(value: &Value, key: &str) -> i64 {
    value
        .get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

/// Build the `deprecationNotice` payload for an `installation.update-available`
/// event. Pure function for unit tests.
fn deprecation_notice_for_update(props: &Value) -> Value {
    let version = props.get("version").and_then(Value::as_str).unwrap_or("");
    let message = if version.is_empty() {
        "An update is available for opencode.".to_string()
    } else {
        format!("opencode {version} is available.")
    };
    json!({
        "kind": "installationUpdateAvailable",
        "version": version,
        "message": message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BridgeState;
    use serde_json::json;

    #[test]
    fn step_finish_tokens_uses_supplied_total() {
        let part = json!({
            "type": "step-finish",
            "tokens": {
                "total": 200,
                "input": 90,
                "output": 100,
                "reasoning": 10,
                "cache": { "read": 7, "write": 0 }
            }
        });
        let bd = step_finish_tokens(&part);
        assert_eq!(bd.total_tokens, 200);
        assert_eq!(bd.input_tokens, 90);
        assert_eq!(bd.output_tokens, 100);
        assert_eq!(bd.reasoning_output_tokens, 10);
        assert_eq!(bd.cached_input_tokens, 7);
    }

    #[test]
    fn step_finish_tokens_derives_total_when_absent() {
        let part = json!({
            "type": "step-finish",
            "tokens": {
                "input": 25,
                "output": 50,
                "reasoning": 3,
                "cache": { "read": 0, "write": 0 }
            }
        });
        let bd = step_finish_tokens(&part);
        // 25 + 50
        assert_eq!(bd.total_tokens, 75);
    }

    #[test]
    fn step_finish_tokens_handles_missing_cache() {
        let part = json!({"type":"step-finish","tokens":{"input":1,"output":2,"reasoning":0}});
        let bd = step_finish_tokens(&part);
        assert_eq!(bd.cached_input_tokens, 0);
        assert_eq!(bd.total_tokens, 3);
    }

    #[test]
    fn token_usage_payload_shape_matches_codex() {
        let state = BridgeState::default();
        let snap = state.record_token_usage(
            "thr",
            TokenUsageBreakdown {
                total_tokens: 10,
                input_tokens: 6,
                cached_input_tokens: 1,
                output_tokens: 4,
                reasoning_output_tokens: 0,
            },
        );
        let payload = token_usage_payload("thr", "tu1", &snap);
        assert_eq!(payload["threadId"], "thr");
        assert_eq!(payload["turnId"], "tu1");
        assert_eq!(payload["tokenUsage"]["total"]["totalTokens"], 10);
        assert_eq!(payload["tokenUsage"]["last"]["totalTokens"], 10);
        assert!(payload["tokenUsage"]["modelContextWindow"].is_null());
    }

    #[test]
    fn deprecation_notice_includes_version() {
        let props = json!({"version":"1.2.3"});
        let payload = deprecation_notice_for_update(&props);
        assert_eq!(payload["kind"], "installationUpdateAvailable");
        assert_eq!(payload["version"], "1.2.3");
        assert!(
            payload["message"].as_str().unwrap().contains("1.2.3"),
            "message should mention version: {payload:?}"
        );
    }

    #[test]
    fn deprecation_notice_falls_back_when_version_missing() {
        let props = json!({});
        let payload = deprecation_notice_for_update(&props);
        assert_eq!(payload["version"], "");
        assert_eq!(payload["message"], "An update is available for opencode.");
    }

    #[test]
    fn answers_in_question_order_reorders_by_index() {
        let response = json!({
            "answers": {
                "1": { "answers": ["b1", "b2"] },
                "0": { "answers": ["a"] },
                "2": { "answers": [] }
            }
        });
        let ordered = answers_in_question_order(&response, 3);
        assert_eq!(
            ordered,
            vec![
                vec!["a".to_string()],
                vec!["b1".to_string(), "b2".to_string()],
                Vec::<String>::new()
            ]
        );
    }

    #[test]
    fn answers_in_question_order_pads_missing_with_empty() {
        let response = json!({"answers": {"0": {"answers":["yes"]}}});
        let ordered = answers_in_question_order(&response, 3);
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0], vec!["yes".to_string()]);
        assert!(ordered[1].is_empty());
        assert!(ordered[2].is_empty());
    }

    #[test]
    fn answers_in_question_order_handles_missing_top_level() {
        let response = json!({});
        let ordered = answers_in_question_order(&response, 2);
        assert_eq!(ordered, vec![Vec::<String>::new(), Vec::<String>::new()]);
    }

    #[test]
    fn file_diffs_to_unified_concatenates_with_git_headers() {
        let diffs = json!([
            {"file":"a.rs","patch":"@@ -1 +1 @@\n-x\n+y\n","additions":1,"deletions":1},
            {"file":"b.rs","patch":"@@ -0,0 +1 @@\n+z","additions":1,"deletions":0},
        ]);
        let unified = file_diffs_to_unified(&diffs);
        assert!(unified.starts_with("diff --git a/a.rs b/a.rs\n"));
        assert!(unified.contains("\ndiff --git a/b.rs b/b.rs\n"));
        // Patches without trailing newlines get one appended so subsequent
        // headers don't run together.
        assert!(unified.contains("+z\n"));
    }

    #[test]
    fn file_diffs_to_unified_handles_empty() {
        let unified = file_diffs_to_unified(&json!([]));
        assert!(unified.is_empty());
    }

    #[test]
    fn file_diffs_to_unified_returns_empty_for_non_array() {
        let unified = file_diffs_to_unified(&json!(null));
        assert!(unified.is_empty());
    }
}
