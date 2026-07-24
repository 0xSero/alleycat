//! Translate a pi `AgentMessage[]` history (from `get_messages` or a JSONL
//! session replay) into codex `Vec<Turn>` of `Vec<ThreadItem>`. Used by
//! `thread/read{includeTurns:true}`, `thread/resume`, and the codex
//! `Thread.turns` field on responses that include history.
//!
//! Mapping (per the bridge design table):
//!
//! | pi message                          | codex item(s)                    |
//! |-------------------------------------|----------------------------------|
//! | `UserMessage { content }`           | one `ThreadItem::UserMessage`    |
//! | `AssistantMessage` text content     | one `ThreadItem::AgentMessage`   |
//! | `AssistantMessage` thinking content | one `ThreadItem::Reasoning`      |
//! | `AssistantMessage` tool calls       | one `CommandExecution` / `FileChange` / `McpToolCall` / `DynamicToolCall` per call |
//! | `ToolResultMessage`                 | folded into the matching tool-call item by `toolCallId` |
//!
//! Turn boundaries: pi sessions don't track turn boundaries explicitly. Each
//! user message starts a new turn; everything until (but not including) the
//! next user message lives in that turn.

use std::collections::HashMap;

use serde_json::Value;

use crate::codex_proto::common::TurnStatus;
use crate::codex_proto::items::{
    CommandExecutionStatus, DynamicToolCallStatus, McpToolCallError, McpToolCallResult,
    McpToolCallStatus, PatchApplyStatus, ThreadItem, UserInput,
};
use crate::codex_proto::thread::Turn;
use crate::pool::pi_protocol::{
    AgentMessage, AssistantContentBlock, AssistantMessage, ToolResultContentBlock,
    ToolResultMessage, UserContentBlock, UserMessage, UserMessageContent,
};
use crate::translate::tool_call::{CodexToolKind, classify};

/// Translate a flat pi message stream into the per-turn codex shape.
pub fn translate_messages(messages: &[AgentMessage]) -> Vec<Turn> {
    let tool_results = index_tool_results(messages);

    let mut turns: Vec<Turn> = Vec::new();
    let mut current_items: Vec<ThreadItem> = Vec::new();
    let mut current_started_at: Option<i64> = None;
    let mut current_completed_at: Option<i64> = None;

    for message in messages {
        match message {
            AgentMessage::User(user) => {
                flush_turn(
                    &mut turns,
                    &mut current_items,
                    &mut current_started_at,
                    &mut current_completed_at,
                );
                current_started_at = Some(user.timestamp);
                current_completed_at = Some(user.timestamp);
                current_items.push(user_message_to_item(user, turns.len()));
            }
            AgentMessage::Assistant(asst) => {
                if current_started_at.is_none() {
                    current_started_at = Some(asst.timestamp);
                }
                current_completed_at = Some(asst.timestamp);
                let turn_index = turns.len();
                push_assistant_items(asst, turn_index, &mut current_items, &tool_results);
            }
            AgentMessage::ToolResult(_) => {
                // Folded into the matching tool-call item via `tool_results`.
            }
            AgentMessage::Other(_) => {
                // Custom app messages have no codex representation.
            }
        }
    }

    flush_turn(
        &mut turns,
        &mut current_items,
        &mut current_started_at,
        &mut current_completed_at,
    );
    turns
}

fn flush_turn(
    turns: &mut Vec<Turn>,
    items: &mut Vec<ThreadItem>,
    started_at: &mut Option<i64>,
    completed_at: &mut Option<i64>,
) {
    if items.is_empty() {
        return;
    }
    let s = started_at.take();
    let c = completed_at.take();
    let duration_ms = match (s, c) {
        (Some(s), Some(c)) if c >= s => Some(c - s),
        _ => None,
    };
    turns.push(Turn {
        id: format!("turn_{}", turns.len()),
        items: std::mem::take(items),
        items_view: crate::codex_proto::default_items_view(),
        status: TurnStatus::Completed,
        error: None,
        started_at: s,
        completed_at: c,
        duration_ms,
    });
}

fn index_tool_results<'a>(messages: &'a [AgentMessage]) -> HashMap<&'a str, &'a ToolResultMessage> {
    let mut out = HashMap::new();
    for m in messages {
        if let AgentMessage::ToolResult(t) = m {
            out.insert(t.tool_call_id.as_str(), t);
        }
    }
    out
}

pub(crate) fn user_item_id(turn_index: usize) -> String {
    format!("user_{turn_index}")
}

pub(crate) fn assistant_item_id(turn_index: usize, _timestamp: i64) -> String {
    format!("assistant_{turn_index}")
}

pub(crate) fn reasoning_item_id(turn_index: usize, _timestamp: i64) -> String {
    format!("reasoning_{turn_index}")
}

fn user_message_to_item(message: &UserMessage, turn_index: usize) -> ThreadItem {
    let content = match &message.content {
        UserMessageContent::Text(s) => vec![UserInput::Text {
            text: s.clone(),
            text_elements: Vec::new(),
        }],
        UserMessageContent::Blocks(blocks) => blocks
            .iter()
            .map(|b| match b {
                UserContentBlock::Text(t) => UserInput::Text {
                    text: t.text.clone(),
                    text_elements: Vec::new(),
                },
                UserContentBlock::Image(img) => UserInput::Image {
                    url: format!("data:{};base64,{}", img.mime_type, img.data),
                },
            })
            .collect(),
    };
    ThreadItem::UserMessage {
        id: user_item_id(turn_index),
        content,
    }
}

fn push_assistant_items(
    message: &AssistantMessage,
    turn_index: usize,
    out: &mut Vec<ThreadItem>,
    tool_results: &HashMap<&str, &ToolResultMessage>,
) {
    let mut text = String::new();
    let mut thinking = String::new();
    let mut tool_calls = Vec::new();
    for block in &message.content {
        match block {
            AssistantContentBlock::Text(t) => text.push_str(&t.text),
            AssistantContentBlock::Thinking(t) => thinking.push_str(&t.thinking),
            AssistantContentBlock::ToolCall(tc) => tool_calls.push(tc),
        }
    }

    if !thinking.is_empty() {
        if let Some(ThreadItem::Reasoning { content, .. }) = out
            .iter_mut()
            .find(|item| matches!(item, ThreadItem::Reasoning { .. }))
        {
            if let Some(existing) = content.first_mut() {
                if !existing.is_empty() {
                    existing.push_str("\n\n");
                }
                existing.push_str(&thinking);
            } else {
                content.push(thinking);
            }
        } else {
            out.push(ThreadItem::Reasoning {
                id: reasoning_item_id(turn_index, message.timestamp),
                summary: Vec::new(),
                content: vec![thinking],
            });
        }
    }
    if !text.is_empty() {
        if let Some(ThreadItem::AgentMessage { text: existing, .. }) = out
            .iter_mut()
            .find(|item| matches!(item, ThreadItem::AgentMessage { .. }))
        {
            if !existing.is_empty() {
                existing.push_str("\n\n");
            }
            existing.push_str(&text);
        } else {
            out.push(ThreadItem::AgentMessage {
                id: assistant_item_id(turn_index, message.timestamp),
                text,
                phase: Some(serde_json::Value::String("final_answer".into())),
                memory_citation: None,
            });
        }
    }
    for tc in tool_calls {
        let kind = classify(&tc.name);
        let result = tool_results.get(tc.id.as_str()).copied();
        out.push(tool_call_to_item(&kind, tc.id.clone(), tc, result));
    }
}

fn tool_call_to_item(
    kind: &CodexToolKind,
    id: String,
    call: &crate::pool::pi_protocol::ToolCall,
    result: Option<&ToolResultMessage>,
) -> ThreadItem {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    match kind {
        CodexToolKind::CommandExecution => ThreadItem::CommandExecution {
            id,
            command: call
                .arguments
                .get("command")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cwd: String::new(),
            process_id: None,
            source: Default::default(),
            status: tool_status_command(result, is_error),
            command_actions: Vec::new(),
            aggregated_output: result.map(merge_tool_result_text),
            exit_code: None,
            duration_ms: None,
        },
        CodexToolKind::FileChange => ThreadItem::FileChange {
            id,
            changes: crate::translate::events::synthesize_file_changes(&call.name, &call.arguments),
            status: match result {
                None => PatchApplyStatus::InProgress,
                Some(_) if is_error => PatchApplyStatus::Failed,
                Some(_) => PatchApplyStatus::Completed,
            },
        },
        CodexToolKind::Mcp { server, tool } => ThreadItem::McpToolCall {
            id,
            server: server.clone(),
            tool: tool.clone(),
            status: match result {
                None => McpToolCallStatus::InProgress,
                Some(_) if is_error => McpToolCallStatus::Failed,
                Some(_) => McpToolCallStatus::Completed,
            },
            arguments: call.arguments.clone(),
            mcp_app_resource_uri: None,
            result: result.and_then(|r| {
                if r.is_error {
                    None
                } else {
                    Some(Box::new(McpToolCallResult {
                        content: r.content.iter().map(tool_result_block_to_value).collect(),
                        structured_content: r.details.clone(),
                        is_error: None,
                        meta: None,
                    }))
                }
            }),
            error: result.and_then(|r| {
                if r.is_error {
                    Some(McpToolCallError {
                        message: merge_tool_result_text(r),
                        code: None,
                        data: r.details.clone(),
                    })
                } else {
                    None
                }
            }),
            duration_ms: None,
        },
        CodexToolKind::ExplorationRead
        | CodexToolKind::ExplorationSearch
        | CodexToolKind::ExplorationList => {
            let body = result.map(|r| cap_aggregated_output_disk(merge_tool_result_text(r)));
            build_exploration_disk_item(
                kind,
                &call.name,
                id,
                &call.arguments,
                tool_status_command(result, is_error),
                body,
            )
        }
        CodexToolKind::Dynamic { namespace, tool } => ThreadItem::DynamicToolCall {
            id,
            namespace: namespace.clone(),
            tool: tool.clone(),
            arguments: call.arguments.clone(),
            status: match result {
                None => DynamicToolCallStatus::InProgress,
                Some(_) if is_error => DynamicToolCallStatus::Failed,
                Some(_) => DynamicToolCallStatus::Completed,
            },
            content_items: result
                .map(|r| r.content.iter().map(tool_result_block_to_value).collect()),
            success: result.map(|r| !r.is_error),
            duration_ms: None,
        },
    }
}

/// Disk-side counterpart of `build_exploration_command_item` in events.rs.
/// Mirrors the same command/command_actions shape so live-stream and disk
/// replay produce identical items.
fn build_exploration_disk_item(
    kind: &CodexToolKind,
    tool_name: &str,
    id: String,
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
        id,
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

const EXPLORATION_OUTPUT_CAP_DISK: usize = 256 * 1024;

fn cap_aggregated_output_disk(mut text: String) -> String {
    if text.len() <= EXPLORATION_OUTPUT_CAP_DISK {
        return text;
    }
    let mut idx = EXPLORATION_OUTPUT_CAP_DISK;
    while idx > 0 && !text.is_char_boundary(idx) {
        idx -= 1;
    }
    text.truncate(idx);
    text.push_str("\n... [truncated]");
    text
}

fn tool_status_command(
    result: Option<&ToolResultMessage>,
    is_error: bool,
) -> CommandExecutionStatus {
    match result {
        None => CommandExecutionStatus::InProgress,
        Some(_) if is_error => CommandExecutionStatus::Failed,
        Some(_) => CommandExecutionStatus::Completed,
    }
}

fn tool_result_block_to_value(block: &ToolResultContentBlock) -> Value {
    match block {
        ToolResultContentBlock::Text(t) => serde_json::json!({
            "type": "text",
            "text": t.text,
        }),
        ToolResultContentBlock::Image(img) => serde_json::json!({
            "type": "image",
            "data": img.data,
            "mimeType": img.mime_type,
        }),
    }
}

fn merge_tool_result_text(result: &ToolResultMessage) -> String {
    let mut out = String::new();
    for block in &result.content {
        if let ToolResultContentBlock::Text(t) = block {
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            out.push_str(&t.text);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::pi_protocol::{
        AssistantRole, ImageContent, StopReason, TextContent, ThinkingContent, ToolCall,
        ToolResultRole, Usage, UsageCost, UserRole,
    };
    use serde_json::json;

    fn user(content: UserMessageContent, ts: i64) -> AgentMessage {
        AgentMessage::User(UserMessage {
            role: UserRole::User,
            content,
            timestamp: ts,
        })
    }

    fn assistant_with_blocks(blocks: Vec<AssistantContentBlock>, ts: i64) -> AgentMessage {
        AgentMessage::Assistant(AssistantMessage {
            role: AssistantRole::Assistant,
            content: blocks,
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
            timestamp: ts,
        })
    }

    fn tool_result(
        tool_call_id: &str,
        tool_name: &str,
        text: &str,
        is_error: bool,
        ts: i64,
    ) -> AgentMessage {
        AgentMessage::ToolResult(ToolResultMessage {
            role: ToolResultRole::ToolResult,
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            content: vec![ToolResultContentBlock::Text(TextContent {
                text: text.into(),
                text_signature: None,
            })],
            details: None,
            is_error,
            timestamp: ts,
        })
    }

    #[test]
    fn empty_input_yields_no_turns() {
        assert!(translate_messages(&[]).is_empty());
    }

    #[test]
    fn user_message_starts_a_turn() {
        let turns = translate_messages(&[user(UserMessageContent::Text("hello".into()), 1_000)]);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 1);
        match &turns[0].items[0] {
            ThreadItem::UserMessage { id, content } => {
                assert_eq!(id, "user_0");
                assert_eq!(content.len(), 1);
                match &content[0] {
                    UserInput::Text { text, .. } => assert_eq!(text, "hello"),
                    other => panic!("expected text, got {other:?}"),
                }
            }
            other => panic!("expected UserMessage, got {other:?}"),
        }
        assert_eq!(turns[0].started_at, Some(1_000));
    }

    #[test]
    fn user_blocks_image_becomes_data_url() {
        let turns = translate_messages(&[user(
            UserMessageContent::Blocks(vec![
                UserContentBlock::Text(TextContent {
                    text: "see this".into(),
                    text_signature: None,
                }),
                UserContentBlock::Image(ImageContent {
                    data: "AAA".into(),
                    mime_type: "image/png".into(),
                }),
            ]),
            2_000,
        )]);
        match &turns[0].items[0] {
            ThreadItem::UserMessage { content, .. } => {
                assert_eq!(content.len(), 2);
                match &content[1] {
                    UserInput::Image { url } => {
                        assert_eq!(url, "data:image/png;base64,AAA")
                    }
                    other => panic!("expected image, got {other:?}"),
                }
            }
            other => panic!("expected UserMessage, got {other:?}"),
        }
    }

    #[test]
    fn assistant_text_thinking_emits_two_items() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("q".into()), 1),
            assistant_with_blocks(
                vec![
                    AssistantContentBlock::Thinking(ThinkingContent {
                        thinking: "ponder".into(),
                        thinking_signature: None,
                        redacted: None,
                    }),
                    AssistantContentBlock::Text(TextContent {
                        text: "answer".into(),
                        text_signature: None,
                    }),
                ],
                2,
            ),
        ]);
        assert_eq!(turns.len(), 1);
        let kinds: Vec<_> = turns[0]
            .items
            .iter()
            .map(|i| match i {
                ThreadItem::UserMessage { .. } => "user",
                ThreadItem::AgentMessage { .. } => "agent",
                ThreadItem::Reasoning { .. } => "reasoning",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds, vec!["user", "reasoning", "agent"]);
        assert_eq!(turns[0].items[0].id(), "user_0");
        assert_eq!(turns[0].items[1].id(), "reasoning_0");
        assert_eq!(turns[0].items[2].id(), "assistant_0");
    }

    #[test]
    fn assistant_tool_cycles_share_one_reasoning_and_message_item() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("q".into()), 1),
            assistant_with_blocks(
                vec![
                    AssistantContentBlock::Thinking(ThinkingContent {
                        thinking: "first thought".into(),
                        thinking_signature: None,
                        redacted: None,
                    }),
                    AssistantContentBlock::Text(TextContent {
                        text: "first update".into(),
                        text_signature: None,
                    }),
                ],
                2,
            ),
            assistant_with_blocks(
                vec![
                    AssistantContentBlock::Thinking(ThinkingContent {
                        thinking: "second thought".into(),
                        thinking_signature: None,
                        redacted: None,
                    }),
                    AssistantContentBlock::Text(TextContent {
                        text: "final answer".into(),
                        text_signature: None,
                    }),
                ],
                3,
            ),
        ]);

        assert_eq!(turns[0].items.len(), 3);
        match &turns[0].items[1] {
            ThreadItem::Reasoning { content, .. } => {
                assert_eq!(content, &["first thought\n\nsecond thought"]);
            }
            other => panic!("expected Reasoning, got {other:?}"),
        }
        match &turns[0].items[2] {
            ThreadItem::AgentMessage { text, .. } => {
                assert_eq!(text, "first update\n\nfinal answer");
            }
            other => panic!("expected AgentMessage, got {other:?}"),
        }
    }

    #[test]
    fn tool_call_paired_with_result_completes_command_execution() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("ls".into()), 1),
            assistant_with_blocks(
                vec![AssistantContentBlock::ToolCall(ToolCall {
                    id: "tc1".into(),
                    name: "bash".into(),
                    arguments: json!({"command":"ls"}),
                    thought_signature: None,
                })],
                2,
            ),
            tool_result("tc1", "bash", "file1\nfile2\n", false, 3),
        ]);
        assert_eq!(turns.len(), 1);
        let exec_item = turns[0]
            .items
            .iter()
            .find(|i| matches!(i, ThreadItem::CommandExecution { .. }))
            .expect("expected CommandExecution item");
        match exec_item {
            ThreadItem::CommandExecution {
                command,
                aggregated_output,
                status,
                ..
            } => {
                assert_eq!(command, "ls");
                assert_eq!(*status, CommandExecutionStatus::Completed);
                assert_eq!(aggregated_output.as_deref(), Some("file1\nfile2\n"));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn unmatched_tool_call_stays_in_progress() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("ls".into()), 1),
            assistant_with_blocks(
                vec![AssistantContentBlock::ToolCall(ToolCall {
                    id: "tc1".into(),
                    name: "bash".into(),
                    arguments: json!({"command":"ls"}),
                    thought_signature: None,
                })],
                2,
            ),
        ]);
        let status = turns[0]
            .items
            .iter()
            .find_map(|i| match i {
                ThreadItem::CommandExecution { status, .. } => Some(status),
                _ => None,
            })
            .unwrap();
        assert_eq!(*status, CommandExecutionStatus::InProgress);
    }

    #[test]
    fn tool_call_failed_when_result_marks_is_error() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("write x".into()), 1),
            assistant_with_blocks(
                vec![AssistantContentBlock::ToolCall(ToolCall {
                    id: "w1".into(),
                    name: "write".into(),
                    arguments: json!({"path":"/x"}),
                    thought_signature: None,
                })],
                2,
            ),
            tool_result("w1", "write", "permission denied", true, 3),
        ]);
        let item = turns[0]
            .items
            .iter()
            .find(|i| matches!(i, ThreadItem::FileChange { .. }))
            .unwrap();
        match item {
            ThreadItem::FileChange { status, .. } => {
                assert_eq!(*status, PatchApplyStatus::Failed);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn mcp_tool_call_with_result() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("file an issue".into()), 1),
            assistant_with_blocks(
                vec![AssistantContentBlock::ToolCall(ToolCall {
                    id: "mcp1".into(),
                    name: "github__create_issue".into(),
                    arguments: json!({"title":"bug"}),
                    thought_signature: None,
                })],
                2,
            ),
            tool_result("mcp1", "github__create_issue", "{\"number\":1}", false, 3),
        ]);
        let item = turns[0]
            .items
            .iter()
            .find(|i| matches!(i, ThreadItem::McpToolCall { .. }))
            .unwrap();
        match item {
            ThreadItem::McpToolCall {
                server,
                tool,
                status,
                result,
                error,
                ..
            } => {
                assert_eq!(server, "github");
                assert_eq!(tool, "create_issue");
                assert_eq!(*status, McpToolCallStatus::Completed);
                assert!(result.is_some());
                assert!(error.is_none());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn multiple_user_messages_split_into_multiple_turns() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("first".into()), 1),
            assistant_with_blocks(
                vec![AssistantContentBlock::Text(TextContent {
                    text: "ack1".into(),
                    text_signature: None,
                })],
                2,
            ),
            user(UserMessageContent::Text("second".into()), 3),
            assistant_with_blocks(
                vec![AssistantContentBlock::Text(TextContent {
                    text: "ack2".into(),
                    text_signature: None,
                })],
                4,
            ),
        ]);
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].items.len(), 2);
        assert_eq!(turns[1].items.len(), 2);
        assert_eq!(turns[0].started_at, Some(1));
        assert_eq!(turns[0].completed_at, Some(2));
        assert_eq!(turns[1].started_at, Some(3));
        assert_eq!(turns[1].completed_at, Some(4));
    }

    #[test]
    fn other_message_variant_skipped() {
        let turns = translate_messages(&[
            user(UserMessageContent::Text("q".into()), 1),
            AgentMessage::Other(json!({"role":"custom","content":"x"})),
            assistant_with_blocks(
                vec![AssistantContentBlock::Text(TextContent {
                    text: "a".into(),
                    text_signature: None,
                })],
                2,
            ),
        ]);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 2);
    }
}
