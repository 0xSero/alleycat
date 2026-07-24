//! Translate pi `AgentSessionEvent`s into codex JSON-RPC notifications.
//!
//! This is "the big match" called out in the bridge plan — pi emits a stream
//! of fine-grained events (text deltas, thinking deltas, tool start/update/end,
//! compaction, retry) that the codex client expects packaged into a smaller,
//! more domain-specific notification vocabulary. Translation requires state:
//!
//! - The codex turn we're inside (`thread_id` + `turn_id`).
//! - Per-pi-message-id we minted a codex `item_id` for, so subsequent deltas
//!   can attach to the right item.
//! - Per pi tool call id we remember the codex item kind we picked at
//!   `tool_execution_start`, so `tool_execution_end` can build the right
//!   `item/completed` payload.
//!
//! The translator does **not** own the I/O — it consumes
//! [`crate::pool::pi_protocol::PiEvent`] values and produces
//! [`crate::codex_proto::notifications::ServerNotification`] values that
//! `handlers/turn.rs` writes to the codex client.

use std::collections::HashMap;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::codex_proto::common::{TurnError, TurnStatus};
use crate::codex_proto::items::{
    CommandExecutionStatus, DynamicToolCallStatus, FileUpdateChange, McpToolCallError,
    McpToolCallResult, McpToolCallStatus, PatchApplyStatus, PatchChangeKind, ThreadItem,
};
use crate::codex_proto::notifications::{
    AgentMessageDeltaNotification, CommandExecutionOutputDeltaNotification,
    ContextCompactedNotification, ErrorNotification, ItemCompletedNotification,
    ItemStartedNotification, McpToolCallProgressNotification, ReasoningTextDeltaNotification,
    ServerNotification,
};
use crate::pool::pi_protocol::{
    AgentMessage, AssistantContentBlock, AssistantMessage, AssistantMessageEvent, PiEvent,
    StopReason as PiStopReason,
};
use crate::translate::items::{assistant_item_id, reasoning_item_id};
use crate::translate::tool_call::{CodexToolKind, classify};

/// Per-(thread, turn) state the translator carries between events.
///
/// Construct one when codex emits `turn/started` and feed every pi event
/// through [`Self::translate`] until pi's `agent_end` arrives. After that,
/// drop the state and rebuild it on the next `turn/start`.
#[derive(Debug)]
pub struct EventTranslatorState {
    thread_id: String,
    turn_id: String,
    turn_index: usize,
    open_message_item: Option<OpenItem>,
    open_reasoning_item: Option<OpenItem>,
    open_tool_calls: HashMap<String, OpenToolCall>,
    open_compaction_item_id: Option<String>,
}

#[derive(Debug, Clone)]
struct OpenItem {
    item_id: String,
    started: bool,
    saw_delta: bool,
}

#[derive(Debug, Clone)]
struct OpenToolCall {
    item_id: String,
    kind: CodexToolKind,
    /// Original `toolName` (`bash`, `read`, ...) and the args object pi
    /// passed at `ToolExecutionStart`. Carried into `ToolExecutionEnd`
    /// so the completion side can build canonical shapes that need both
    /// the request (eg `command_actions: [{type: read, path}]` for Read)
    /// and the response (`aggregated_output` for the file body).
    tool_name: String,
    args: Value,
}

impl EventTranslatorState {
    pub fn new(thread_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self::with_turn_index(thread_id, turn_id, 0)
    }

    pub fn with_turn_index(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        turn_index: usize,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            turn_index,
            open_message_item: None,
            open_reasoning_item: None,
            open_tool_calls: HashMap::new(),
            open_compaction_item_id: None,
        }
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    /// Translate a single pi event into zero or more codex notifications.
    pub fn translate(&mut self, event: PiEvent) -> Vec<ServerNotification> {
        match event {
            PiEvent::AgentStart => Vec::new(),
            PiEvent::TurnStart => Vec::new(),
            PiEvent::AgentEnd { .. } => self.translate_agent_end(),
            PiEvent::TurnEnd { .. } => Vec::new(),
            PiEvent::ThinkingLevelChanged { .. } => Vec::new(),

            PiEvent::MessageStart { message } => match message {
                AgentMessage::Assistant(a) => self.translate_message_start(a),
                _ => Vec::new(),
            },
            PiEvent::MessageUpdate {
                assistant_message_event,
                ..
            } => self.translate_message_update(assistant_message_event),
            PiEvent::MessageEnd { message } => match message {
                AgentMessage::Assistant(a) => self.translate_message_end(a),
                _ => Vec::new(),
            },

            PiEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => self.translate_tool_start(tool_call_id, tool_name, args),
            PiEvent::ToolExecutionUpdate {
                tool_call_id,
                tool_name,
                args: _,
                partial_result,
            } => self.translate_tool_update(tool_call_id, tool_name, partial_result),
            PiEvent::ToolExecutionEnd {
                tool_call_id,
                tool_name,
                result,
                is_error,
            } => self.translate_tool_end(tool_call_id, tool_name, result, is_error),

            PiEvent::QueueUpdate { .. } => Vec::new(),

            PiEvent::CompactionStart { reason: _ } => self.translate_compaction_start(),
            PiEvent::CompactionEnd {
                reason: _,
                result: _,
                aborted,
                will_retry,
                error_message,
            } => self.translate_compaction_end(aborted, will_retry, error_message),

            PiEvent::AutoRetryStart {
                attempt: _,
                max_attempts: _,
                delay_ms: _,
                error_message,
            } => vec![self.error_notification(error_message, true)],
            PiEvent::AutoRetryEnd {
                success,
                attempt: _,
                final_error,
            } => {
                if success {
                    Vec::new()
                } else {
                    vec![self.error_notification(
                        final_error.unwrap_or_else(|| "auto-retry failed".to_string()),
                        false,
                    )]
                }
            }

            // Extension UI requests are routed via approval.rs; this translator
            // does not synthesize them.
            PiEvent::ExtensionUiRequest(_) => Vec::new(),

            PiEvent::ExtensionError {
                extension_path,
                event,
                error,
            } => vec![self.error_notification(
                format!("extension {extension_path} raised an error handling {event}: {error}"),
                false,
            )],
        }
    }

    // ---- message lifecycle -------------------------------------------------

    fn translate_message_start(&mut self, message: AssistantMessage) -> Vec<ServerNotification> {
        let item_id = assistant_item_id(self.turn_index, message.timestamp);
        let text = extract_assistant_text(&message);
        let started = !text.is_empty();
        self.open_message_item = Some(OpenItem {
            item_id: item_id.clone(),
            started,
            saw_delta: false,
        });
        if started {
            vec![self.item_started(ThreadItem::AgentMessage {
                id: item_id,
                text,
                phase: Some(serde_json::Value::String("final_answer".into())),
                memory_citation: None,
            })]
        } else {
            Vec::new()
        }
    }

    fn translate_message_update(
        &mut self,
        event: AssistantMessageEvent,
    ) -> Vec<ServerNotification> {
        match event {
            AssistantMessageEvent::TextStart { .. } => Vec::new(),
            AssistantMessageEvent::TextDelta { delta, partial, .. } => {
                if self.open_message_item.is_none() {
                    let item_id = assistant_item_id(self.turn_index, partial.timestamp);
                    self.open_message_item = Some(OpenItem {
                        item_id,
                        started: false,
                        saw_delta: false,
                    });
                }

                let mut started_item = None;
                let item_id = {
                    let item = self
                        .open_message_item
                        .as_mut()
                        .expect("open_message_item was just initialized");
                    if !item.started {
                        item.started = true;
                        started_item = Some(ThreadItem::AgentMessage {
                            id: item.item_id.clone(),
                            text: String::new(),
                            phase: Some(serde_json::Value::String("final_answer".into())),
                            memory_citation: None,
                        });
                    }
                    item.saw_delta = true;
                    item.item_id.clone()
                };

                let mut out = Vec::new();
                if let Some(item) = started_item {
                    out.push(self.item_started(item));
                }
                out.push(ServerNotification::AgentMessageDelta(
                    AgentMessageDeltaNotification {
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        item_id,
                        delta,
                        parent_item_id: None,
                    },
                ));
                out
            }
            AssistantMessageEvent::TextEnd {
                content, partial, ..
            } => {
                if self.open_message_item.is_none() && !content.is_empty() {
                    let item_id = assistant_item_id(self.turn_index, partial.timestamp);
                    self.open_message_item = Some(OpenItem {
                        item_id,
                        started: false,
                        saw_delta: false,
                    });
                }
                self.ensure_agent_message_delta(content)
            }

            AssistantMessageEvent::ThinkingStart { partial, .. } => {
                let item_id = reasoning_item_id(self.turn_index, partial.timestamp);
                self.open_reasoning_item = Some(OpenItem {
                    item_id: item_id.clone(),
                    started: true,
                    saw_delta: false,
                });
                vec![self.item_started(ThreadItem::Reasoning {
                    id: item_id,
                    summary: Vec::new(),
                    content: Vec::new(),
                })]
            }
            AssistantMessageEvent::ThinkingDelta { delta, .. } => {
                let Some(item) = self.open_reasoning_item.as_mut() else {
                    return Vec::new();
                };
                item.saw_delta = true;
                vec![ServerNotification::ReasoningTextDelta(
                    ReasoningTextDeltaNotification {
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        item_id: item.item_id.clone(),
                        delta,
                        content_index: 0,
                        parent_item_id: None,
                    },
                )]
            }
            AssistantMessageEvent::ThinkingEnd { content, .. } => {
                if let Some(item) = self.open_reasoning_item.take() {
                    vec![self.item_completed(ThreadItem::Reasoning {
                        id: item.item_id,
                        summary: Vec::new(),
                        content: vec![content],
                    })]
                } else {
                    Vec::new()
                }
            }

            AssistantMessageEvent::ToolcallStart { .. }
            | AssistantMessageEvent::ToolcallDelta { .. }
            | AssistantMessageEvent::ToolcallEnd { .. } => Vec::new(),

            AssistantMessageEvent::Start { .. } => Vec::new(),
            AssistantMessageEvent::Done { .. } => Vec::new(),
            AssistantMessageEvent::Error { reason, error } => {
                let message = error.error_message.clone().unwrap_or_else(|| match reason {
                    PiStopReason::Aborted => "aborted".to_string(),
                    PiStopReason::Error => "error".to_string(),
                    _ => "stream error".to_string(),
                });
                vec![self.error_notification(message, false)]
            }
        }
    }

    fn translate_message_end(&mut self, message: AssistantMessage) -> Vec<ServerNotification> {
        let Some(item) = self.open_message_item.take() else {
            return Vec::new();
        };
        let text = extract_assistant_text(&message);
        if !item.started && text.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();
        if !item.started {
            out.push(self.item_started(ThreadItem::AgentMessage {
                id: item.item_id.clone(),
                text: String::new(),
                phase: Some(serde_json::Value::String("final_answer".into())),
                memory_citation: None,
            }));
        }
        if !item.saw_delta && !text.is_empty() {
            out.push(ServerNotification::AgentMessageDelta(
                AgentMessageDeltaNotification {
                    thread_id: self.thread_id.clone(),
                    turn_id: self.turn_id.clone(),
                    item_id: item.item_id.clone(),
                    delta: text.clone(),
                    parent_item_id: None,
                },
            ));
        }
        out.push(self.item_completed(ThreadItem::AgentMessage {
            id: item.item_id,
            text,
            phase: Some(serde_json::Value::String("final_answer".into())),
            memory_citation: None,
        }));
        out
    }

    // ---- tool execution ----------------------------------------------------

    fn translate_tool_start(
        &mut self,
        tool_call_id: String,
        tool_name: String,
        args: Value,
    ) -> Vec<ServerNotification> {
        let kind = classify(&tool_name);
        let item = self.tool_started_item(&kind, &tool_name, &tool_call_id, &args);
        self.open_tool_calls.insert(
            tool_call_id.clone(),
            OpenToolCall {
                item_id: tool_call_id,
                kind,
                tool_name,
                args,
            },
        );
        vec![self.item_started(item)]
    }

    fn translate_tool_update(
        &self,
        tool_call_id: String,
        _tool_name: String,
        partial_result: Value,
    ) -> Vec<ServerNotification> {
        let Some(open) = self.open_tool_calls.get(&tool_call_id) else {
            return Vec::new();
        };
        match &open.kind {
            CodexToolKind::CommandExecution => {
                let delta = stringify_partial_stdout(&partial_result);
                if delta.is_empty() {
                    return Vec::new();
                }
                vec![ServerNotification::CommandExecutionOutputDelta(
                    CommandExecutionOutputDeltaNotification {
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        item_id: open.item_id.clone(),
                        delta,
                        parent_item_id: None,
                    },
                )]
            }
            CodexToolKind::Mcp { .. } => {
                let message = stringify_progress_message(&partial_result);
                vec![ServerNotification::McpToolCallProgress(
                    McpToolCallProgressNotification {
                        thread_id: self.thread_id.clone(),
                        turn_id: self.turn_id.clone(),
                        item_id: open.item_id.clone(),
                        message,
                        parent_item_id: None,
                    },
                )]
            }
            // FileChange + exploration reads + Dynamic — no streaming
            // notification today. Exploration reads (read/grep/ls/find) are
            // synchronous in pi-mono so the body lands in one chunk on
            // ToolExecutionEnd.
            CodexToolKind::FileChange
            | CodexToolKind::ExplorationRead
            | CodexToolKind::ExplorationSearch
            | CodexToolKind::ExplorationList
            | CodexToolKind::Dynamic { .. } => Vec::new(),
        }
    }

    fn translate_tool_end(
        &mut self,
        tool_call_id: String,
        tool_name: String,
        result: Value,
        is_error: bool,
    ) -> Vec<ServerNotification> {
        let Some(open) = self.open_tool_calls.remove(&tool_call_id) else {
            tracing::warn!(
                ?tool_call_id,
                ?tool_name,
                "tool_execution_end with no matching start; dropping"
            );
            return Vec::new();
        };
        let item = self.tool_completed_item(&open, result, is_error);
        vec![self.item_completed(item)]
    }

    fn tool_started_item(
        &self,
        kind: &CodexToolKind,
        tool_name: &str,
        tool_call_id: &str,
        args: &Value,
    ) -> ThreadItem {
        match kind {
            CodexToolKind::CommandExecution => ThreadItem::CommandExecution {
                id: tool_call_id.to_string(),
                command: extract_command(args),
                cwd: String::new(),
                process_id: None,
                source: Default::default(),
                status: CommandExecutionStatus::InProgress,
                command_actions: Vec::new(),
                aggregated_output: None,
                exit_code: None,
                duration_ms: None,
            },
            CodexToolKind::FileChange => ThreadItem::FileChange {
                id: tool_call_id.to_string(),
                changes: synthesize_file_changes(tool_name, args),
                status: PatchApplyStatus::InProgress,
            },
            CodexToolKind::Mcp { server, tool } => ThreadItem::McpToolCall {
                id: tool_call_id.to_string(),
                server: server.clone(),
                tool: tool.clone(),
                status: McpToolCallStatus::InProgress,
                arguments: args.clone(),
                mcp_app_resource_uri: None,
                result: None,
                error: None,
                duration_ms: None,
            },
            CodexToolKind::ExplorationRead
            | CodexToolKind::ExplorationSearch
            | CodexToolKind::ExplorationList => build_exploration_command_item(
                kind,
                tool_name,
                tool_call_id,
                args,
                CommandExecutionStatus::InProgress,
                None,
            ),
            CodexToolKind::Dynamic { namespace, tool } => ThreadItem::DynamicToolCall {
                id: tool_call_id.to_string(),
                namespace: namespace.clone(),
                tool: tool.clone(),
                arguments: args.clone(),
                status: DynamicToolCallStatus::InProgress,
                content_items: None,
                success: None,
                duration_ms: None,
            },
        }
    }

    fn tool_completed_item(
        &self,
        open: &OpenToolCall,
        result: Value,
        is_error: bool,
    ) -> ThreadItem {
        match &open.kind {
            CodexToolKind::CommandExecution => {
                let aggregated = extract_bash_output(&result);
                let exit_code = extract_bash_exit_code(&result);
                let status = if is_error {
                    CommandExecutionStatus::Failed
                } else {
                    CommandExecutionStatus::Completed
                };
                ThreadItem::CommandExecution {
                    id: open.item_id.clone(),
                    command: extract_command(&open.args),
                    cwd: String::new(),
                    process_id: None,
                    source: Default::default(),
                    status,
                    command_actions: Vec::new(),
                    aggregated_output: aggregated.map(cap_aggregated_output),
                    exit_code,
                    duration_ms: None,
                }
            }
            CodexToolKind::FileChange => ThreadItem::FileChange {
                id: open.item_id.clone(),
                changes: synthesize_file_changes(&open.tool_name, &open.args),
                status: if is_error {
                    PatchApplyStatus::Failed
                } else {
                    PatchApplyStatus::Completed
                },
            },
            CodexToolKind::Mcp { server, tool } => {
                let (result_payload, error) = mcp_result_split(result, is_error);
                ThreadItem::McpToolCall {
                    id: open.item_id.clone(),
                    server: server.clone(),
                    tool: tool.clone(),
                    status: if is_error {
                        McpToolCallStatus::Failed
                    } else {
                        McpToolCallStatus::Completed
                    },
                    arguments: Value::Null,
                    mcp_app_resource_uri: None,
                    result: result_payload,
                    error,
                    duration_ms: None,
                }
            }
            CodexToolKind::ExplorationRead
            | CodexToolKind::ExplorationSearch
            | CodexToolKind::ExplorationList => {
                let body = extract_tool_text_output(&result).map(cap_aggregated_output);
                let status = if is_error {
                    CommandExecutionStatus::Failed
                } else {
                    CommandExecutionStatus::Completed
                };
                build_exploration_command_item(
                    &open.kind,
                    &open.tool_name,
                    &open.item_id,
                    &open.args,
                    status,
                    body,
                )
            }
            CodexToolKind::Dynamic { namespace, tool } => ThreadItem::DynamicToolCall {
                id: open.item_id.clone(),
                namespace: namespace.clone(),
                tool: tool.clone(),
                arguments: Value::Null,
                status: if is_error {
                    DynamicToolCallStatus::Failed
                } else {
                    DynamicToolCallStatus::Completed
                },
                content_items: Some(vec![result]),
                success: Some(!is_error),
                duration_ms: None,
            },
        }
    }

    // ---- compaction --------------------------------------------------------

    fn translate_compaction_start(&mut self) -> Vec<ServerNotification> {
        let id = new_item_id();
        self.open_compaction_item_id = Some(id.clone());
        vec![self.item_started(ThreadItem::ContextCompaction { id })]
    }

    fn translate_compaction_end(
        &mut self,
        aborted: bool,
        will_retry: bool,
        error_message: Option<String>,
    ) -> Vec<ServerNotification> {
        let Some(id) = self.open_compaction_item_id.take() else {
            return Vec::new();
        };
        let mut out = vec![self.item_completed(ThreadItem::ContextCompaction { id })];
        out.push(ServerNotification::ContextCompacted(
            ContextCompactedNotification {
                thread_id: self.thread_id.clone(),
                turn_id: self.turn_id.clone(),
            },
        ));
        if aborted || error_message.is_some() {
            let message = error_message.unwrap_or_else(|| "compaction aborted".to_string());
            out.push(self.error_notification(message, will_retry));
        }
        out
    }

    fn translate_agent_end(&mut self) -> Vec<ServerNotification> {
        let mut out = Vec::new();
        if let Some(item) = self.open_message_item.take() {
            if item.started {
                out.push(self.item_completed(ThreadItem::AgentMessage {
                    id: item.item_id,
                    text: String::new(),
                    phase: None,
                    memory_citation: None,
                }));
            }
        }
        if let Some(item) = self.open_reasoning_item.take() {
            out.push(self.item_completed(ThreadItem::Reasoning {
                id: item.item_id,
                summary: Vec::new(),
                content: Vec::new(),
            }));
        }
        out
    }

    fn ensure_agent_message_delta(&mut self, text: String) -> Vec<ServerNotification> {
        if text.is_empty() {
            return Vec::new();
        }
        let Some(item) = self.open_message_item.as_mut() else {
            return Vec::new();
        };
        if item.saw_delta {
            return Vec::new();
        }

        let mut started_item = None;
        if !item.started {
            item.started = true;
            started_item = Some(ThreadItem::AgentMessage {
                id: item.item_id.clone(),
                text: String::new(),
                phase: Some(serde_json::Value::String("final_answer".into())),
                memory_citation: None,
            });
        }
        item.saw_delta = true;
        let item_id = item.item_id.clone();

        let mut out = Vec::new();
        if let Some(item) = started_item {
            out.push(self.item_started(item));
        }
        out.push(ServerNotification::AgentMessageDelta(
            AgentMessageDeltaNotification {
                thread_id: self.thread_id.clone(),
                turn_id: self.turn_id.clone(),
                item_id,
                delta: text,
                parent_item_id: None,
            },
        ));
        out
    }

    // ---- helpers -----------------------------------------------------------

    fn item_started(&self, item: ThreadItem) -> ServerNotification {
        ServerNotification::ItemStarted(ItemStartedNotification {
            item,
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            parent_item_id: None,
        })
    }

    fn item_completed(&self, item: ThreadItem) -> ServerNotification {
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            item,
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            parent_item_id: None,
        })
    }

    fn error_notification(&self, message: String, will_retry: bool) -> ServerNotification {
        ServerNotification::Error(ErrorNotification {
            error: TurnError {
                message,
                codex_error_info: None,
                additional_details: None,
            },
            will_retry,
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
        })
    }
}

fn new_item_id() -> String {
    Uuid::now_v7().to_string()
}

fn extract_assistant_text(message: &AssistantMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|b| match b {
            AssistantContentBlock::Text(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect::<String>()
}

fn extract_command(args: &Value) -> String {
    args.get("command")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn stringify_partial_stdout(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_string();
    }
    for key in ["stdout", "output", "delta"] {
        if let Some(s) = value.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    String::new()
}

fn stringify_progress_message(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_string();
    }
    if let Some(s) = value.get("message").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    value.to_string()
}

fn extract_bash_output(result: &Value) -> Option<String> {
    extract_tool_text_output(result)
}

/// Cap aggregated_output at 256 KiB on a UTF-8 boundary. Multi-megabyte
/// `read` bodies otherwise inflate every notification round-trip.
const EXPLORATION_OUTPUT_CAP: usize = 256 * 1024;

/// Build `FileUpdateChange` entries from pi's `write` / `edit` /
/// `apply_patch` arguments. Mirrors the claude-side helper but matches
/// pi's snake_case keys (`path`, `oldText`/`newText`) instead of
/// claude's `file_path` / `old_string`. Returns empty Vec when args are
/// missing or unparseable.
pub(crate) fn synthesize_file_changes(tool_name: &str, args: &Value) -> Vec<FileUpdateChange> {
    match tool_name {
        "write" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if path.is_empty() {
                return Vec::new();
            }
            let content = args.get("content").and_then(Value::as_str).unwrap_or("");
            vec![FileUpdateChange {
                path,
                kind: PatchChangeKind::Add,
                diff: unified_addition(content),
            }]
        }
        "edit" => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if path.is_empty() {
                return Vec::new();
            }
            let edits = args
                .get("edits")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if edits.is_empty() {
                return Vec::new();
            }
            let mut diff = String::new();
            for edit in &edits {
                let old = edit.get("oldText").and_then(Value::as_str).unwrap_or("");
                let new = edit.get("newText").and_then(Value::as_str).unwrap_or("");
                diff.push_str(&unified_hunk(old, new));
            }
            vec![FileUpdateChange {
                path,
                kind: PatchChangeKind::Update { move_path: None },
                diff,
            }]
        }
        "apply_patch" => {
            // pi's `apply_patch` ships a unified-diff body in `args.patch`.
            // Pass it through as a single Update entry; the path lives
            // inside the diff body itself, but FileUpdateChange.path is
            // required, so we use an empty string when we can't extract
            // a single path. Best-effort.
            let patch = args.get("patch").and_then(Value::as_str).unwrap_or("");
            if patch.is_empty() {
                return Vec::new();
            }
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            vec![FileUpdateChange {
                path,
                kind: PatchChangeKind::Update { move_path: None },
                diff: patch.to_string(),
            }]
        }
        _ => Vec::new(),
    }
}

fn unified_hunk(old: &str, new: &str) -> String {
    let old_count = old.lines().count().max(1);
    let new_count = new.lines().count().max(1);
    let mut out = format!("@@ -1,{old_count} +1,{new_count} @@\n");
    for line in old.lines() {
        out.push('-');
        out.push_str(line);
        out.push('\n');
    }
    for line in new.lines() {
        out.push('+');
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn unified_addition(content: &str) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let count = lines.len().max(1);
    let mut out = format!("@@ -0,0 +1,{count} @@\n");
    for line in &lines {
        out.push('+');
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn cap_aggregated_output(mut text: String) -> String {
    if text.len() <= EXPLORATION_OUTPUT_CAP {
        return text;
    }
    let mut idx = EXPLORATION_OUTPUT_CAP;
    while idx > 0 && !text.is_char_boundary(idx) {
        idx -= 1;
    }
    text.truncate(idx);
    text.push_str("\n... [truncated]");
    text
}

/// Pull the human-readable body out of a pi tool result. Pi normalizes
/// most read/grep/ls results to one of:
/// - a plain string,
/// - `{"content": "..."}` or `{"output": "..."}`,
/// - or a content-array `[{"type":"text","text":"..."}]` (mirrors what
///   the model sees in toolResult).
/// Returns `None` only when no recognizable shape is present.
fn extract_tool_text_output(result: &Value) -> Option<String> {
    if let Some(s) = result.as_str() {
        return Some(s.to_string());
    }
    for key in ["content", "output", "text", "result"] {
        if let Some(s) = result.get(key).and_then(Value::as_str) {
            return Some(s.to_string());
        }
    }
    if let Some(arr) = result.get("content").and_then(Value::as_array) {
        let mut joined = String::new();
        for entry in arr {
            if let Some(text) = entry.get("text").and_then(Value::as_str) {
                if !joined.is_empty() && !joined.ends_with('\n') {
                    joined.push('\n');
                }
                joined.push_str(text);
            }
        }
        if !joined.is_empty() {
            return Some(joined);
        }
    }
    None
}

/// Build a `ThreadItem::CommandExecution` for one of the read-only pi
/// exploration tools (`read`, `grep`, `ls`, `find`). The same shape is
/// reused at start time (status=InProgress, output=None) and at
/// completion time (status from is_error, output filled from the
/// extracted text body).
fn build_exploration_command_item(
    kind: &CodexToolKind,
    tool_name: &str,
    item_id: &str,
    args: &Value,
    status: CommandExecutionStatus,
    aggregated_output: Option<String>,
) -> ThreadItem {
    let (command, command_actions) = match kind {
        CodexToolKind::ExplorationRead => {
            let path = args
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let command = format!("read {path}");
            let name = command_action_name(&path);
            let action = serde_json::json!({
                "type": "read",
                "command": command.clone(),
                "name": name,
                "path": path
            });
            (command, vec![action])
        }
        CodexToolKind::ExplorationSearch => {
            let pattern = args
                .get("pattern")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let path = args.get("path").and_then(Value::as_str);
            let command = format!("grep {pattern}");
            let mut action = serde_json::json!({
                "type": "search",
                "command": command.clone(),
                "query": pattern
            });
            if let Some(p) = path {
                action["path"] = Value::String(p.to_string());
            }
            (command, vec![action])
        }
        CodexToolKind::ExplorationList => {
            let pattern = args
                .get("pattern")
                .and_then(Value::as_str)
                .map(str::to_string);
            let path = args.get("path").and_then(Value::as_str).map(str::to_string);
            let display = match (&pattern, &path) {
                (Some(p), Some(d)) => format!("{tool_name} {p} {d}"),
                (Some(p), None) => format!("{tool_name} {p}"),
                (None, Some(d)) => format!("{tool_name} {d}"),
                _ => tool_name.to_string(),
            };
            let mut action = serde_json::json!({"type": "listFiles", "command": display.clone()});
            if let Some(p) = path {
                action["path"] = Value::String(p);
            }
            if let Some(p) = pattern {
                action["pattern"] = Value::String(p);
            }
            (display, vec![action])
        }
        _ => (String::new(), Vec::new()),
    };
    ThreadItem::CommandExecution {
        id: item_id.to_string(),
        command,
        cwd: String::new(),
        process_id: None,
        source: Default::default(),
        status,
        command_actions,
        aggregated_output,
        exit_code: None,
        duration_ms: None,
    }
}

fn command_action_name(path: &str) -> String {
    path.rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or("file")
        .to_string()
}

fn extract_bash_exit_code(result: &Value) -> Option<i32> {
    result
        .get("exitCode")
        .or_else(|| result.get("exit_code"))
        .and_then(|v| v.as_i64())
        .and_then(|n| i32::try_from(n).ok())
}

fn mcp_result_split(
    result: Value,
    is_error: bool,
) -> (Option<Box<McpToolCallResult>>, Option<McpToolCallError>) {
    if is_error {
        let message = result
            .get("message")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| result.to_string());
        let error = McpToolCallError {
            message,
            code: result.get("code").and_then(|v| v.as_i64()),
            data: Some(result),
        };
        return (None, Some(error));
    }
    let payload = McpToolCallResult {
        content: vec![json!({ "type": "text", "text": result.to_string() })],
        structured_content: Some(result),
        is_error: None,
        meta: None,
    };
    (Some(Box::new(payload)), None)
}

/// Helper for `handlers/turn.rs`: derive a `TurnStatus`/`TurnError` pair from
/// the optional error string carried out of the final pi event of a turn.
pub fn turn_status_from_agent_end(error_message: Option<&str>) -> (TurnStatus, Option<TurnError>) {
    if let Some(message) = error_message {
        (
            TurnStatus::Failed,
            Some(TurnError {
                message: message.to_string(),
                codex_error_info: None,
                additional_details: None,
            }),
        )
    } else {
        (TurnStatus::Completed, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::pi_protocol::{
        AssistantContentBlock, AssistantRole, CompactionReason, StopReason, TextContent, Usage,
        UsageCost,
    };

    fn state() -> EventTranslatorState {
        EventTranslatorState::with_turn_index("th_1", "tu_1", 7)
    }

    fn agent_msg(text: &str) -> AgentMessage {
        AgentMessage::Assistant(assistant_message(text))
    }

    fn item_id_from_started(notification: &ServerNotification) -> &str {
        match notification {
            ServerNotification::ItemStarted(n) => n.item.id(),
            other => panic!("expected ItemStarted, got {other:?}"),
        }
    }

    fn assistant_message(text: &str) -> AssistantMessage {
        AssistantMessage {
            role: AssistantRole::Assistant,
            content: vec![AssistantContentBlock::Text(TextContent {
                text: text.into(),
                text_signature: None,
            })],
            api: "openai-responses".into(),
            provider: "openai".into(),
            model: "gpt-5".into(),
            response_id: None,
            usage: Usage {
                input: 0,
                output: 0,
                cache_read: 0,
                cache_write: 0,
                total_tokens: 0,
                cost: UsageCost {
                    input: 0.0,
                    output: 0.0,
                    cache_read: 0.0,
                    cache_write: 0.0,
                    total: 0.0,
                },
            },
            stop_reason: StopReason::Stop,
            error_message: None,
            timestamp: 0,
        }
    }

    #[test]
    fn agent_start_and_turn_start_are_silent() {
        let mut s = state();
        assert!(s.translate(PiEvent::AgentStart).is_empty());
        assert!(s.translate(PiEvent::TurnStart).is_empty());
    }

    #[test]
    fn message_start_delays_empty_agent_message_until_text() {
        let mut s = state();
        let out = s.translate(PiEvent::MessageStart {
            message: agent_msg(""),
        });
        assert!(out.is_empty());
        assert!(s.open_message_item.is_some());
    }

    #[test]
    fn message_start_emits_non_empty_item_started_agent_message() {
        let mut s = state();
        let out = s.translate(PiEvent::MessageStart {
            message: agent_msg("hi"),
        });
        assert_eq!(out.len(), 1);
        match &out[0] {
            ServerNotification::ItemStarted(n) => match &n.item {
                ThreadItem::AgentMessage { id, text, .. } => {
                    assert_eq!(id, "assistant_7_0");
                    assert_eq!(text, "hi");
                }
                other => panic!("expected AgentMessage, got {other:?}"),
            },
            other => panic!("expected ItemStarted, got {other:?}"),
        }
        assert!(s.open_message_item.is_some());
    }

    #[test]
    fn text_delta_routes_to_agent_message_delta() {
        let mut s = state();
        let _ = s.translate(PiEvent::MessageStart {
            message: agent_msg(""),
        });
        let item_id = s.open_message_item.as_ref().unwrap().item_id.clone();

        let out = s.translate(PiEvent::MessageUpdate {
            message: agent_msg(""),
            assistant_message_event: AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "hi".into(),
                partial: assistant_message(""),
            },
        });
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0], ServerNotification::ItemStarted(_)));
        match &out[1] {
            ServerNotification::AgentMessageDelta(n) => {
                assert_eq!(n.thread_id, "th_1");
                assert_eq!(n.turn_id, "tu_1");
                assert_eq!(n.item_id, item_id);
                assert_eq!(n.delta, "hi");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn live_stream_item_ids_match_replay_ids_for_same_turn_index() {
        let mut live = EventTranslatorState::with_turn_index("th_1", "live_turn", 3);
        let mut message = assistant_message("answer");
        message.content.insert(
            0,
            AssistantContentBlock::Thinking(crate::pool::pi_protocol::ThinkingContent {
                thinking: "ponder".into(),
                thinking_signature: None,
                redacted: None,
            }),
        );
        message.timestamp = 42;

        let started = live.translate(PiEvent::MessageStart {
            message: AgentMessage::Assistant(message.clone()),
        });
        let thinking = live.translate(PiEvent::MessageUpdate {
            message: AgentMessage::Assistant(message.clone()),
            assistant_message_event: AssistantMessageEvent::ThinkingStart {
                content_index: 0,
                partial: message.clone(),
            },
        });

        let replay = crate::translate::items::translate_messages(&[
            AgentMessage::User(crate::pool::pi_protocol::UserMessage {
                role: crate::pool::pi_protocol::UserRole::User,
                content: crate::pool::pi_protocol::UserMessageContent::Text("q".into()),
                timestamp: 1,
            }),
            AgentMessage::User(crate::pool::pi_protocol::UserMessage {
                role: crate::pool::pi_protocol::UserRole::User,
                content: crate::pool::pi_protocol::UserMessageContent::Text("q".into()),
                timestamp: 2,
            }),
            AgentMessage::User(crate::pool::pi_protocol::UserMessage {
                role: crate::pool::pi_protocol::UserRole::User,
                content: crate::pool::pi_protocol::UserMessageContent::Text("q".into()),
                timestamp: 3,
            }),
            AgentMessage::User(crate::pool::pi_protocol::UserMessage {
                role: crate::pool::pi_protocol::UserRole::User,
                content: crate::pool::pi_protocol::UserMessageContent::Text("q".into()),
                timestamp: 4,
            }),
            AgentMessage::Assistant(message),
        ]);
        assert_eq!(item_id_from_started(&started[0]), replay[3].items[1].id());
        assert_eq!(item_id_from_started(&thinking[0]), replay[3].items[2].id());
    }

    #[test]
    fn thinking_lifecycle_emits_reasoning_item() {
        let mut s = state();
        let started = s.translate(PiEvent::MessageUpdate {
            message: agent_msg(""),
            assistant_message_event: AssistantMessageEvent::ThinkingStart {
                content_index: 0,
                partial: assistant_message(""),
            },
        });
        assert_eq!(started.len(), 1);
        match &started[0] {
            ServerNotification::ItemStarted(n) => match &n.item {
                ThreadItem::Reasoning { id, .. } => assert_eq!(id, "reasoning_7_0"),
                other => panic!("expected Reasoning, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }

        let delta = s.translate(PiEvent::MessageUpdate {
            message: agent_msg(""),
            assistant_message_event: AssistantMessageEvent::ThinkingDelta {
                content_index: 0,
                delta: "thinking…".into(),
                partial: assistant_message(""),
            },
        });
        match &delta[0] {
            ServerNotification::ReasoningTextDelta(n) => assert_eq!(n.delta, "thinking…"),
            other => panic!("unexpected {other:?}"),
        }

        let ended = s.translate(PiEvent::MessageUpdate {
            message: agent_msg(""),
            assistant_message_event: AssistantMessageEvent::ThinkingEnd {
                content_index: 0,
                content: "thinking done".into(),
                partial: assistant_message(""),
            },
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::Reasoning { content, .. } => {
                    assert_eq!(content, &vec!["thinking done".to_string()]);
                }
                other => panic!("expected Reasoning, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn bash_tool_lifecycle_emits_command_execution() {
        let mut s = state();
        let started = s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc1".into(),
            tool_name: "bash".into(),
            args: json!({"command": "ls -la"}),
        });
        match &started[0] {
            ServerNotification::ItemStarted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    id,
                    command,
                    status,
                    ..
                } => {
                    assert_eq!(id, "tc1");
                    assert_eq!(command, "ls -la");
                    assert_eq!(*status, CommandExecutionStatus::InProgress);
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }

        let updated = s.translate(PiEvent::ToolExecutionUpdate {
            tool_call_id: "tc1".into(),
            tool_name: "bash".into(),
            args: json!({}),
            partial_result: json!({"stdout": "line1\n"}),
        });
        match &updated[0] {
            ServerNotification::CommandExecutionOutputDelta(n) => {
                assert_eq!(n.delta, "line1\n");
                assert_eq!(n.item_id, "tc1");
            }
            other => panic!("unexpected {other:?}"),
        }

        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc1".into(),
            tool_name: "bash".into(),
            result: json!({"output": "line1\nline2\n", "exitCode": 0}),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    status,
                    aggregated_output,
                    exit_code,
                    ..
                } => {
                    assert_eq!(*status, CommandExecutionStatus::Completed);
                    assert_eq!(aggregated_output.as_deref(), Some("line1\nline2\n"));
                    assert_eq!(*exit_code, Some(0));
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
        assert!(s.open_tool_calls.is_empty());
    }

    #[test]
    fn bash_tool_completion_extracts_live_pi_content_array() {
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc1".into(),
            tool_name: "bash".into(),
            args: json!({"command": "printf marker"}),
        });

        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc1".into(),
            tool_name: "bash".into(),
            result: json!({"content": [{"type": "text", "text": "marker"}]}),
            is_error: false,
        });

        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    aggregated_output, ..
                } => assert_eq!(aggregated_output.as_deref(), Some("marker")),
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn mcp_tool_lifecycle_emits_mcp_tool_call() {
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc1".into(),
            tool_name: "github__create_issue".into(),
            args: json!({"title": "bug"}),
        });
        let progress = s.translate(PiEvent::ToolExecutionUpdate {
            tool_call_id: "tc1".into(),
            tool_name: "github__create_issue".into(),
            args: json!({}),
            partial_result: json!({"message": "calling GitHub..."}),
        });
        match &progress[0] {
            ServerNotification::McpToolCallProgress(n) => {
                assert_eq!(n.message, "calling GitHub...")
            }
            other => panic!("unexpected {other:?}"),
        }
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc1".into(),
            tool_name: "github__create_issue".into(),
            result: json!({"number": 123}),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::McpToolCall {
                    server,
                    tool,
                    status,
                    ..
                } => {
                    assert_eq!(server, "github");
                    assert_eq!(tool, "create_issue");
                    assert_eq!(*status, McpToolCallStatus::Completed);
                }
                other => panic!("expected McpToolCall, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn write_tool_classified_as_file_change() {
        let mut s = state();
        let started = s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc1".into(),
            tool_name: "write".into(),
            args: json!({"path": "/tmp/x"}),
        });
        match &started[0] {
            ServerNotification::ItemStarted(n) => {
                assert!(matches!(n.item, ThreadItem::FileChange { .. }))
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn write_tool_emits_filechange_with_addition_diff() {
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_w".into(),
            tool_name: "write".into(),
            args: json!({"path": "/tmp/x", "content": "line1\nline2\n"}),
        });
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc_w".into(),
            tool_name: "write".into(),
            result: json!("ok"),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::FileChange { changes, .. } => {
                    assert_eq!(changes.len(), 1);
                    assert_eq!(changes[0].path, "/tmp/x");
                    assert!(matches!(
                        changes[0].kind,
                        crate::codex_proto::items::PatchChangeKind::Add
                    ));
                    assert!(changes[0].diff.starts_with("@@ -0,0 +1,2 @@"));
                    assert!(changes[0].diff.contains("+line1"));
                    assert!(changes[0].diff.contains("+line2"));
                }
                other => panic!("expected FileChange, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn edit_tool_emits_filechange_with_hunks_from_edits_array() {
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_e".into(),
            tool_name: "edit".into(),
            args: json!({
                "path": "/tmp/x",
                "edits": [{"oldText": "foo\n", "newText": "bar\n"}]
            }),
        });
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc_e".into(),
            tool_name: "edit".into(),
            result: json!("1 replacement"),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::FileChange { changes, .. } => {
                    assert_eq!(changes.len(), 1);
                    assert_eq!(changes[0].path, "/tmp/x");
                    assert!(matches!(
                        changes[0].kind,
                        crate::codex_proto::items::PatchChangeKind::Update { .. }
                    ));
                    assert!(changes[0].diff.contains("-foo"));
                    assert!(changes[0].diff.contains("+bar"));
                }
                other => panic!("expected FileChange, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn unknown_tool_classified_as_dynamic() {
        // `read` is now canonical (ExplorationRead → CommandExecution).
        // Use a name that has no canonical mapping to keep the Dynamic
        // fallback under test. `multi_tool_use.parallel` is a good
        // representative — it should never appear at this layer (pi
        // flattens it), but if it ever surfaces we want it visible as
        // Dynamic rather than misclassified.
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc1".into(),
            tool_name: "multi_tool_use.parallel".into(),
            args: json!({"requests": []}),
        });
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc1".into(),
            tool_name: "multi_tool_use.parallel".into(),
            result: json!({"content": "hi"}),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::DynamicToolCall {
                    tool,
                    status,
                    success,
                    ..
                } => {
                    assert_eq!(tool, "multi_tool_use.parallel");
                    assert_eq!(*status, DynamicToolCallStatus::Completed);
                    assert_eq!(*success, Some(true));
                }
                other => panic!("expected DynamicToolCall, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn read_tool_lifecycle_emits_command_execution_with_read_action() {
        let mut s = state();
        let started = s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_read".into(),
            tool_name: "read".into(),
            args: json!({"path": ".pi-tool-demo.txt"}),
        });
        match &started[0] {
            ServerNotification::ItemStarted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    command,
                    command_actions,
                    status,
                    ..
                } => {
                    assert_eq!(command, "read .pi-tool-demo.txt");
                    assert_eq!(command_actions[0]["type"], "read");
                    assert_eq!(command_actions[0]["command"], "read .pi-tool-demo.txt");
                    assert_eq!(command_actions[0]["name"], ".pi-tool-demo.txt");
                    assert_eq!(command_actions[0]["path"], ".pi-tool-demo.txt");
                    assert_eq!(*status, CommandExecutionStatus::InProgress);
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
        // Pi's read result is a plain string body in practice; the
        // translator must populate aggregated_output and flip status.
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc_read".into(),
            tool_name: "read".into(),
            result: json!("demo line 1\ndemo line 2 (edited)\n"),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    command,
                    aggregated_output,
                    command_actions,
                    status,
                    ..
                } => {
                    assert_eq!(command, "read .pi-tool-demo.txt");
                    assert_eq!(
                        aggregated_output.as_deref(),
                        Some("demo line 1\ndemo line 2 (edited)\n")
                    );
                    assert_eq!(command_actions[0]["type"], "read");
                    assert_eq!(command_actions[0]["command"], "read .pi-tool-demo.txt");
                    assert_eq!(command_actions[0]["name"], ".pi-tool-demo.txt");
                    assert_eq!(*status, CommandExecutionStatus::Completed);
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn grep_tool_lifecycle_emits_search_action() {
        let mut s = state();
        let started = s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_grep".into(),
            tool_name: "grep".into(),
            args: json!({"pattern": "fn main", "path": "src"}),
        });
        match &started[0] {
            ServerNotification::ItemStarted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    command,
                    command_actions,
                    ..
                } => {
                    assert_eq!(command, "grep fn main");
                    assert_eq!(command_actions[0]["type"], "search");
                    assert_eq!(command_actions[0]["command"], "grep fn main");
                    assert_eq!(command_actions[0]["query"], "fn main");
                    assert_eq!(command_actions[0]["path"], "src");
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
        let _ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc_grep".into(),
            tool_name: "grep".into(),
            result: json!("src/main.rs:1:fn main()\n"),
            is_error: false,
        });
    }

    #[test]
    fn ls_tool_lifecycle_emits_list_files_action() {
        let mut s = state();
        let started = s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_ls".into(),
            tool_name: "ls".into(),
            args: json!({"path": "."}),
        });
        match &started[0] {
            ServerNotification::ItemStarted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    command,
                    command_actions,
                    ..
                } => {
                    assert_eq!(command, "ls .");
                    assert_eq!(command_actions[0]["type"], "listFiles");
                    assert_eq!(command_actions[0]["command"], "ls .");
                    assert_eq!(command_actions[0]["path"], ".");
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn read_caps_aggregated_output_at_256_kib() {
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_big".into(),
            tool_name: "read".into(),
            args: json!({"path": "/big.txt"}),
        });
        let big = "A".repeat(300 * 1024);
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc_big".into(),
            tool_name: "read".into(),
            result: Value::String(big),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    aggregated_output, ..
                } => {
                    let body = aggregated_output.as_deref().unwrap();
                    assert!(body.len() < 300 * 1024);
                    assert!(body.ends_with("[truncated]"));
                }
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn read_extracts_text_from_content_array() {
        // Pi sometimes normalizes results to the canonical
        // toolResult content-array shape (`[{"type":"text","text":"..."}]`).
        // The extraction helper should pull the body out either way.
        let mut s = state();
        s.translate(PiEvent::ToolExecutionStart {
            tool_call_id: "tc_arr".into(),
            tool_name: "read".into(),
            args: json!({"path": "/x"}),
        });
        let ended = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "tc_arr".into(),
            tool_name: "read".into(),
            result: json!({"content": [{"type":"text","text":"hello"}]}),
            is_error: false,
        });
        match &ended[0] {
            ServerNotification::ItemCompleted(n) => match &n.item {
                ThreadItem::CommandExecution {
                    aggregated_output, ..
                } => assert_eq!(aggregated_output.as_deref(), Some("hello")),
                other => panic!("expected CommandExecution, got {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn compaction_lifecycle_emits_item_and_thread_compacted() {
        let mut s = state();
        let started = s.translate(PiEvent::CompactionStart {
            reason: CompactionReason::Threshold,
        });
        match &started[0] {
            ServerNotification::ItemStarted(n) => {
                assert!(matches!(n.item, ThreadItem::ContextCompaction { .. }))
            }
            other => panic!("unexpected {other:?}"),
        }
        let ended = s.translate(PiEvent::CompactionEnd {
            reason: CompactionReason::Threshold,
            result: None,
            aborted: false,
            will_retry: false,
            error_message: None,
        });
        assert_eq!(ended.len(), 2);
        assert!(matches!(ended[0], ServerNotification::ItemCompleted(_)));
        assert!(matches!(ended[1], ServerNotification::ContextCompacted(_)));
    }

    #[test]
    fn compaction_aborted_also_emits_error() {
        let mut s = state();
        s.translate(PiEvent::CompactionStart {
            reason: CompactionReason::Threshold,
        });
        let ended = s.translate(PiEvent::CompactionEnd {
            reason: CompactionReason::Threshold,
            result: None,
            aborted: true,
            will_retry: true,
            error_message: Some("budget exceeded".into()),
        });
        assert_eq!(ended.len(), 3);
        match &ended[2] {
            ServerNotification::Error(e) => {
                assert_eq!(e.error.message, "budget exceeded");
                assert!(e.will_retry);
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn auto_retry_start_emits_retryable_error() {
        let mut s = state();
        let out = s.translate(PiEvent::AutoRetryStart {
            attempt: 1,
            max_attempts: 3,
            delay_ms: 1000,
            error_message: "rate limited".into(),
        });
        match &out[0] {
            ServerNotification::Error(e) => {
                assert_eq!(e.error.message, "rate limited");
                assert!(e.will_retry);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn auto_retry_end_failure_emits_terminal_error() {
        let mut s = state();
        let out = s.translate(PiEvent::AutoRetryEnd {
            success: false,
            attempt: 3,
            final_error: Some("gave up".into()),
        });
        match &out[0] {
            ServerNotification::Error(e) => {
                assert_eq!(e.error.message, "gave up");
                assert!(!e.will_retry);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn stray_tool_end_emits_nothing() {
        let mut s = state();
        let out = s.translate(PiEvent::ToolExecutionEnd {
            tool_call_id: "ghost".into(),
            tool_name: "bash".into(),
            result: json!({}),
            is_error: false,
        });
        assert!(out.is_empty());
    }

    #[test]
    fn agent_end_closes_dangling_message() {
        let mut s = state();
        s.translate(PiEvent::MessageStart {
            message: agent_msg(""),
        });
        let out = s.translate(PiEvent::AgentEnd {
            messages: Vec::new(),
        });
        assert!(out.is_empty());

        s.translate(PiEvent::MessageStart {
            message: agent_msg("hi"),
        });
        let out = s.translate(PiEvent::AgentEnd {
            messages: Vec::new(),
        });
        assert_eq!(out.len(), 1);
        match &out[0] {
            ServerNotification::ItemCompleted(n) => {
                assert!(matches!(n.item, ThreadItem::AgentMessage { .. }))
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn extension_error_translates_to_error_notification() {
        let mut s = state();
        let out = s.translate(PiEvent::ExtensionError {
            extension_path: "/x".into(),
            event: json!({"type": "boom"}),
            error: json!("kaboom"),
        });
        match &out[0] {
            ServerNotification::Error(e) => {
                assert!(e.error.message.contains("/x"));
                assert!(!e.will_retry);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
