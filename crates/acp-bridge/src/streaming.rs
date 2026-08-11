//! Live streaming of ACP `session/update` notifications onto the codex
//! notification surface.
//!
//! While `session/prompt` runs we receive a flood of `session/update`
//! notifications — agent message chunks, thought chunks, tool calls,
//! tool call updates, plans, etc. Without streaming, those land in a
//! buffer that `handle_turn_start` drains AFTER the response, causing
//! iOS to render the whole turn at once at the end.
//!
//! `TurnStreamEmitter` is a stateful sink driven by a callback inside
//! `AcpClient::send_request_streaming`. For each `session/update` it
//! both:
//!   1. updates internal state (text accumulators, in-flight tool calls,
//!      plan/commands snapshots) — so `finish()` can return a complete
//!      summary the caller stores into `StoredTurn`,
//!   2. emits the matching codex notification(s) over `ctx.notifier()` so
//!      iOS sees content arrive live:
//!      * `agent_message_chunk` → `item/started` (empty `agentMessage`
//!        shell) on first chunk, `item/agentMessage/delta` per chunk,
//!        `item/completed` on flush.
//!      * `agent_thought_chunk` → same with `reasoning` + `item/reasoning/textDelta`.
//!      * `tool_call` → `item/started` with the fully-rendered codex
//!        `commandExecution` / `fileChange` / `dynamicToolCall` item.
//!      * `tool_call_update` with terminal status → `item/completed`
//!        with the updated item. Non-terminal updates are absorbed
//!        into state but don't emit (codex has no first-class "item
//!        updated" notification surface).
//!
//! `finish()` returns the full list of items the emitter rendered (in
//! stream order) plus any captured plan / available_commands. Callers
//! use that as the canonical turn payload so live stream IDs and
//! refresh IDs are guaranteed to match.

use std::collections::HashMap;

use serde_json::{Value, json};

use crate::translator::{ToolCallStatePublic, render_tool_call_public};

/// Notification sink: receives `(method, params)` pairs as the emitter
/// fires them. Production code passes a closure that hands the payload
/// to a `NotificationSender`; tests pass a closure that records into a
/// shared `Vec` for inspection.
pub type EmitFn = Box<dyn FnMut(&str, &Value) + Send + 'static>;

pub struct TurnStreamEmitter {
    emit: EmitFn,
    thread_id: String,
    turn_id: String,
    /// Items emitted in stream order. Includes terminal-state tool
    /// calls + closed text runs. In-flight runs are appended on flush.
    items: Vec<Value>,
    /// id → position in `items` for in-place mutation when an item
    /// receives a `tool_call_update`.
    item_index: HashMap<String, usize>,
    /// Currently-open text run (agent message OR reasoning).
    text: Option<TextRun>,
    /// Monotonic counter for stable text-run item ids.
    text_seq: usize,
    /// In-flight tool calls keyed by ACP toolCallId.
    tool_calls: HashMap<String, InflightToolCall>,
    /// Most-recent plan snapshot (raw ACP entries — caller maps to
    /// codex `TurnPlanStep`).
    plan_entries: Option<Vec<Value>>,
    /// Most-recent ACP `available_commands_update` payload.
    available_commands: Option<Vec<Value>>,
    /// Most-recent ACP `current_mode_update` modeId. ACP carries this
    /// as `{sessionUpdate:"current_mode_update", currentModeId: string}`.
    current_mode: Option<String>,
}

struct TextRun {
    kind: TextKind,
    item_id: String,
    accumulated: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextKind {
    AgentMessage,
    Reasoning,
}

struct InflightToolCall {
    state: ToolCallStatePublic,
    completed: bool,
}

/// Result of draining the emitter at end of turn.
pub struct StreamFinish {
    pub items: Vec<Value>,
    pub plan_entries: Option<Vec<Value>>,
    pub available_commands: Option<Vec<Value>>,
    pub current_mode: Option<String>,
}

impl TurnStreamEmitter {
    pub fn new<F>(emit: F, thread_id: String, turn_id: String) -> Self
    where
        F: FnMut(&str, &Value) + Send + 'static,
    {
        Self {
            emit: Box::new(emit),
            thread_id,
            turn_id,
            items: Vec::new(),
            item_index: HashMap::new(),
            text: None,
            text_seq: 0,
            tool_calls: HashMap::new(),
            plan_entries: None,
            available_commands: None,
            current_mode: None,
        }
    }

    /// Process one inbound `session/update` notification.
    pub fn ingest(&mut self, note: &Value) {
        if note.get("method").and_then(|v| v.as_str()) != Some("session/update") {
            return;
        }
        let update = match note.get("params").and_then(|p| p.get("update")) {
            Some(u) => u,
            None => return,
        };
        let kind = update
            .get("sessionUpdate")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        match kind {
            "agent_message_chunk" => {
                self.handle_text(TextKind::AgentMessage, update.get("content"))
            }
            "agent_thought_chunk" => self.handle_text(TextKind::Reasoning, update.get("content")),
            "user_message_chunk" => {
                // Devin sometimes echoes the user prompt as a chunk; we
                // already prepend our own canonical user item in
                // handle_turn_start, so just close any open agent run.
                self.flush_text();
            }
            "tool_call" => {
                self.flush_text();
                self.handle_tool_call(update);
            }
            "tool_call_update" => {
                self.flush_text();
                self.handle_tool_call_update(update);
            }
            "plan" => {
                self.flush_text();
                if let Some(entries) = update.get("entries").and_then(|v| v.as_array()) {
                    self.plan_entries = Some(entries.clone());
                }
            }
            "available_commands_update" => {
                if let Some(arr) = update.get("availableCommands").and_then(|v| v.as_array()) {
                    self.available_commands = Some(arr.clone());
                }
            }
            "current_mode_update" => {
                if let Some(id) = update.get("currentModeId").and_then(|v| v.as_str()) {
                    self.current_mode = Some(id.to_string());
                }
            }
            // usage_update, vendor-specific kinds: drop.
            _ => {}
        }
    }

    /// Close open text runs, emit item/completed for any still-pending
    /// tool calls, and return the consolidated turn payload.
    pub fn finish(mut self) -> StreamFinish {
        self.flush_text();
        let drained: Vec<(String, InflightToolCall)> = self.tool_calls.drain().collect();
        for (_id, infl) in drained {
            if !infl.completed {
                let item = render_tool_call_public(&infl.state);
                self.replace_item(&infl.state.item_id, item.clone());
                self.emit_item_completed(&item);
            }
        }
        StreamFinish {
            items: self.items,
            plan_entries: self.plan_entries,
            available_commands: self.available_commands,
            current_mode: self.current_mode,
        }
    }

    fn emit_item_started(&mut self, item: &Value, started_at_ms: i64) {
        let params = json!({
            "threadId": self.thread_id,
            "turnId": self.turn_id,
            "item": item,
            "startedAtMs": started_at_ms,
        });
        (self.emit)("item/started", &params);
    }

    fn emit_item_completed(&mut self, item: &Value) {
        let completed_at_ms = chrono::Utc::now().timestamp_millis();
        let params = json!({
            "threadId": self.thread_id,
            "turnId": self.turn_id,
            "item": item,
            "completedAtMs": completed_at_ms,
        });
        (self.emit)("item/completed", &params);
    }

    fn handle_text(&mut self, kind: TextKind, content: Option<&Value>) {
        let text = pluck_text(content);
        if text.is_empty() {
            return;
        }
        if let Some(run) = &self.text
            && run.kind != kind
        {
            self.flush_text();
        }
        if self.text.is_none() {
            let now_ms = chrono::Utc::now().timestamp_millis();
            let item_id = match kind {
                TextKind::AgentMessage => format!("acp-agent-{}", self.text_seq),
                TextKind::Reasoning => format!("acp-reasoning-{}", self.text_seq),
            };
            self.text_seq += 1;
            // Emit item/started with an empty shell so iOS reserves a
            // slot keyed by item_id; subsequent deltas fold in.
            let shell = match kind {
                TextKind::AgentMessage => json!({
                    "id": &item_id,
                    "type": "agentMessage",
                    "text": "",
                    "phase": null,
                    "memoryCitation": null,
                }),
                TextKind::Reasoning => json!({
                    "id": &item_id,
                    "type": "reasoning",
                    "summary": [],
                    "content": [""],
                }),
            };
            self.emit_item_started(&shell, now_ms);
            self.text = Some(TextRun {
                kind,
                item_id,
                accumulated: String::new(),
            });
        }

        // Mutate the text run and capture the delta params before
        // calling the closure — `self.emit` is `&mut self` and conflicts
        // with the live `&mut TextRun` borrow.
        let item_id_for_delta = {
            let run = self.text.as_mut().expect("text run set");
            run.accumulated.push_str(&text);
            run.item_id.clone()
        };
        let method = match kind {
            TextKind::AgentMessage => "item/agentMessage/delta",
            TextKind::Reasoning => "item/reasoning/textDelta",
        };
        let mut params = json!({
            "threadId": self.thread_id,
            "turnId": self.turn_id,
            "itemId": item_id_for_delta,
            "delta": text,
        });
        if kind == TextKind::Reasoning {
            params["contentIndex"] = json!(0);
        }
        (self.emit)(method, &params);
    }

    fn flush_text(&mut self) {
        let run = match self.text.take() {
            Some(r) => r,
            None => return,
        };
        let item = match run.kind {
            TextKind::AgentMessage => json!({
                "id": &run.item_id,
                "type": "agentMessage",
                "text": run.accumulated,
                "phase": null,
                "memoryCitation": null,
            }),
            TextKind::Reasoning => json!({
                "id": &run.item_id,
                "type": "reasoning",
                "summary": [],
                "content": [run.accumulated],
            }),
        };
        self.push_item(item.clone());
        self.emit_item_completed(&item);
    }

    fn handle_tool_call(&mut self, update: &Value) {
        let id = match update.get("toolCallId").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };
        let state = ToolCallStatePublic::from_announce(update);
        let item = render_tool_call_public(&state);
        let started_at = state.started_at_ms;
        self.emit_item_started(&item, started_at);
        self.push_item(item);
        self.tool_calls.insert(
            id,
            InflightToolCall {
                state,
                completed: false,
            },
        );
    }

    fn handle_tool_call_update(&mut self, update: &Value) {
        let id = match update.get("toolCallId").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };
        if !self.tool_calls.contains_key(&id) {
            // Update without an announce — treat as a fresh announce.
            self.handle_tool_call(update);
        }
        if !self.tool_calls.contains_key(&id) {
            return;
        }
        let (item, item_id, terminal) = {
            let infl = self.tool_calls.get_mut(&id).expect("inserted above");
            infl.state.merge_update(update);
            let item = render_tool_call_public(&infl.state);
            let item_id = infl.state.item_id.clone();
            let terminal = matches!(infl.state.status.as_str(), "completed" | "failed");
            if terminal {
                infl.completed = true;
            }
            (item, item_id, terminal)
        };
        // Keep `items` in sync so the post-response payload has the
        // latest snapshot. Emit item/completed only on terminal.
        self.replace_item(&item_id, item.clone());
        if terminal {
            self.emit_item_completed(&item);
        }
    }

    fn push_item(&mut self, item: Value) {
        let id = item
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let idx = self.items.len();
        self.items.push(item);
        if !id.is_empty() {
            self.item_index.insert(id, idx);
        }
    }

    fn replace_item(&mut self, id: &str, new_item: Value) {
        if let Some(&idx) = self.item_index.get(id) {
            self.items[idx] = new_item;
        } else {
            self.push_item(new_item);
        }
    }
}

fn pluck_text(content: Option<&Value>) -> String {
    let v = match content {
        Some(v) => v,
        None => return String::new(),
    };
    if let Some(s) = v.as_str() {
        return s.to_string();
    }
    if let Some(t) = v.get("text").and_then(|t| t.as_str()) {
        return t.to_string();
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Capture every emitted notification for inspection. Returns a
    /// shared handle the caller can clone into the emitter closure plus
    /// inspect after `finish()`.
    type Captured = Arc<Mutex<Vec<(String, Value)>>>;

    fn capturing_emitter() -> (TurnStreamEmitter, Captured) {
        let captured: Captured = Arc::new(Mutex::new(Vec::new()));
        let cap_for_cb = Arc::clone(&captured);
        let emitter = TurnStreamEmitter::new(
            move |method, params| {
                cap_for_cb
                    .lock()
                    .unwrap()
                    .push((method.to_string(), params.clone()));
            },
            "thread-x".to_string(),
            "turn-x".to_string(),
        );
        (emitter, captured)
    }

    fn note(kind: &str, content_text: &str) -> Value {
        json!({
            "method": "session/update",
            "params": {
                "sessionId": "s",
                "update": {
                    "sessionUpdate": kind,
                    "content": {"type": "text", "text": content_text},
                },
            },
        })
    }

    fn tool_call_note(id: &str, kind: &str, command: &str) -> Value {
        json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": id,
                "kind": kind,
                "title": "Ran command",
                "status": "pending",
                "rawInput": {"command": command},
                "content": []
            }}
        })
    }

    fn tool_update_note(id: &str, status: &str, output: &str) -> Value {
        json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": id,
                "status": status,
                "content": [{"type": "content", "content": {"type": "text", "text": output}}],
                "rawOutput": {"exitCode": 0}
            }}
        })
    }

    fn methods(captured: &Captured) -> Vec<String> {
        captured
            .lock()
            .unwrap()
            .iter()
            .map(|(m, _)| m.clone())
            .collect()
    }

    #[test]
    fn agent_message_chunks_emit_started_deltas_completed() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&note("agent_message_chunk", "Hel"));
        emitter.ingest(&note("agent_message_chunk", "lo"));
        let finish = emitter.finish();

        let ms = methods(&captured);
        assert_eq!(
            ms,
            vec![
                "item/started",
                "item/agentMessage/delta",
                "item/agentMessage/delta",
                "item/completed",
            ]
        );

        // Final item must accumulate the full text and live in finish.items.
        assert_eq!(finish.items.len(), 1);
        assert_eq!(finish.items[0]["type"], "agentMessage");
        assert_eq!(finish.items[0]["text"], "Hello");
    }

    #[test]
    fn switching_text_kind_closes_previous_run() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&note("agent_thought_chunk", "thinking"));
        emitter.ingest(&note("agent_message_chunk", "answer"));
        let finish = emitter.finish();

        let ms = methods(&captured);
        // reasoning started + delta + completed, then agentMessage same.
        assert_eq!(
            ms,
            vec![
                "item/started",
                "item/reasoning/textDelta",
                "item/completed",
                "item/started",
                "item/agentMessage/delta",
                "item/completed",
            ]
        );
        assert_eq!(finish.items.len(), 2);
        assert_eq!(finish.items[0]["type"], "reasoning");
        assert_eq!(finish.items[1]["type"], "agentMessage");
    }

    #[test]
    fn tool_call_announce_then_completed_emits_started_and_completed_once() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&tool_call_note("c1", "execute", "pwd"));
        emitter.ingest(&tool_update_note("c1", "in_progress", "/Users"));
        emitter.ingest(&tool_update_note("c1", "completed", "/Users/x\n"));
        let finish = emitter.finish();

        let ms = methods(&captured);
        assert_eq!(ms, vec!["item/started", "item/completed"]);
        assert_eq!(finish.items.len(), 1);
        assert_eq!(finish.items[0]["type"], "commandExecution");
        assert_eq!(finish.items[0]["status"], "completed");
    }

    #[test]
    fn tool_call_never_terminal_emits_completed_at_finish() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&tool_call_note("c1", "execute", "long_running"));
        emitter.ingest(&tool_update_note("c1", "in_progress", "..."));
        let finish = emitter.finish();

        let ms = methods(&captured);
        // started fired live, completed fired on finish() flush.
        assert_eq!(ms, vec!["item/started", "item/completed"]);
        assert_eq!(finish.items[0]["status"], "inProgress");
    }

    #[test]
    fn user_message_chunk_closes_open_agent_run() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&note("agent_message_chunk", "partial"));
        emitter.ingest(&note("user_message_chunk", "next prompt"));
        emitter.finish();
        let ms = methods(&captured);
        // agent text run gets started/delta/completed; user_message_chunk
        // is absorbed by handle_turn_start's own user item emit, not us.
        assert!(ms.contains(&"item/completed".to_string()));
    }

    #[test]
    fn plan_and_available_commands_and_mode_are_captured_but_not_emitted() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "plan",
                "entries": [{"content": "Step 1", "status": "pending"}],
            }}
        }));
        emitter.ingest(&json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "available_commands_update",
                "availableCommands": [{"name": "web", "description": "Web"}],
            }}
        }));
        emitter.ingest(&json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "current_mode_update",
                "currentModeId": "plan",
            }}
        }));
        let finish = emitter.finish();
        // No item/* notifications for these — they belong to other
        // codex surfaces (turn/plan/updated, skills/list, modes).
        assert!(captured.lock().unwrap().is_empty());
        assert_eq!(finish.plan_entries.as_ref().unwrap().len(), 1);
        assert_eq!(finish.available_commands.as_ref().unwrap().len(), 1);
        assert_eq!(finish.current_mode.as_deref(), Some("plan"));
    }

    #[test]
    fn tool_call_update_without_prior_announce_is_treated_as_announce() {
        let (mut emitter, captured) = capturing_emitter();
        emitter.ingest(&tool_update_note("orphan", "completed", "done"));
        let finish = emitter.finish();
        let ms = methods(&captured);
        // First update synthesizes an announce; the update then merges
        // status=completed and emits item/completed.
        assert!(ms.starts_with(&["item/started".to_string()]));
        assert!(ms.contains(&"item/completed".to_string()));
        assert_eq!(finish.items.len(), 1);
        assert_eq!(finish.items[0]["status"], "completed");
    }
}
