//! Serde mirror of pi-mono's coding-agent RPC wire protocol.
//!
//! Source of truth:
//! - `pi-mono/packages/coding-agent/src/modes/rpc/rpc-types.ts` (commands,
//!   responses, extension UI envelope).
//! - `pi-mono/packages/coding-agent/src/modes/rpc/rpc-mode.ts` (actual emission
//!   site — confirms session events are written to stdout verbatim with no
//!   wrapper, just `{type: ..., ...}` lines).
//! - `pi-mono/packages/agent/src/types.ts` (`AgentEvent` union, lines 326-341).
//! - `pi-mono/packages/coding-agent/src/core/agent-session.ts:111-129`
//!   (`AgentSessionEvent` extends `AgentEvent` with queue/compaction/retry).
//! - `pi-mono/packages/ai/src/types.ts` (`AssistantMessageEvent`,
//!   `ImageContent`, `Message`, `Model`, `Usage`).
//!
//! All shapes use `#[serde(tag = "type", rename_all = "snake_case")]` because
//! pi tags every union with `type` in snake_case (`new_session`,
//! `set_thinking_level`, `agent_start`, `tool_execution_start`, etc.).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Pi RPC Commands (bridge → pi, on stdin)
// ============================================================================

/// Top-level command envelope written to pi's stdin as one JSON line.
///
/// Every variant carries an optional correlation `id`; pi echoes it back on
/// the matching [`RpcResponse`]. The bridge always supplies an id so the pool
/// can demux concurrent in-flight commands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RpcCommand {
    // Prompting
    Prompt(PromptCmd),
    Steer(SteerCmd),
    FollowUp(FollowUpCmd),
    Abort(BareCmd),
    NewSession(NewSessionCmd),

    // State
    GetState(BareCmd),

    // Model
    SetModel(SetModelCmd),
    CycleModel(BareCmd),
    GetAvailableModels(BareCmd),

    // Thinking
    SetThinkingLevel(SetThinkingLevelCmd),
    CycleThinkingLevel(BareCmd),

    // Queue modes
    SetSteeringMode(SetQueueModeCmd),
    SetFollowUpMode(SetQueueModeCmd),

    // Compaction
    Compact(CompactCmd),
    SetAutoCompaction(SetEnabledCmd),

    // Retry
    SetAutoRetry(SetEnabledCmd),
    AbortRetry(BareCmd),

    // Bash
    Bash(BashCmd),
    AbortBash(BareCmd),

    // Session
    GetSessionStats(BareCmd),
    ExportHtml(ExportHtmlCmd),
    SwitchSession(SwitchSessionCmd),
    Fork(ForkCmd),
    GetForkMessages(BareCmd),
    GetLastAssistantText(BareCmd),
    SetSessionName(SetSessionNameCmd),
    ListSessions(ListSessionsCmd),

    // Messages
    GetMessages(BareCmd),

    // Commands (slash command discovery)
    GetCommands(BareCmd),

    // Extension UI response (bridge → pi, paired with `extension_ui_request`)
    ExtensionUiResponse(ExtensionUiResponse),
}

/// Command with no payload other than optional correlation id.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BareCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromptCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ImageContent>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "streamingBehavior"
    )]
    pub streaming_behavior: Option<StreamingBehavior>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SteerCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ImageContent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FollowUpCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ImageContent>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewSessionCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "parentSession"
    )]
    pub parent_session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetModelCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub provider: String,
    #[serde(rename = "modelId")]
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetThinkingLevelCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub level: ThinkingLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetQueueModeCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub mode: QueueMode,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "customInstructions"
    )]
    pub custom_instructions: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetEnabledCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BashCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub command: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportHtmlCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "outputPath"
    )]
    pub output_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SwitchSessionCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "sessionPath")]
    pub session_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForkCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "entryId")]
    pub entry_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetSessionNameCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListSessionsCmd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false", rename = "allProjects")]
    pub all_projects: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Streaming behavior for `prompt` when an existing turn is in flight.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StreamingBehavior {
    Steer,
    FollowUp,
}

/// Pi's reasoning level vocabulary (`pi-mono/packages/agent/src/types.ts:220`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ThinkingLevel {
    Off,
    Minimal,
    Low,
    Medium,
    High,
    /// Only valid for select OpenAI gpt-5.x models.
    Xhigh,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum QueueMode {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "one-at-a-time")]
    OneAtATime,
}

// ============================================================================
// Pi RPC Responses (pi → bridge, on stdout)
// ============================================================================

/// Pi response envelope. Always shaped as
/// `{type:"response", command:<name>, success:bool, [id], [data?], [error?]}`.
///
/// `data` is left as `serde_json::Value` because the schema depends on
/// `command` + `success`. Callers downcast to a typed payload using
/// [`response_data`] helpers below or via `serde_json::from_value` against the
/// per-command Data structs in this module.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcResponse {
    /// Always `"response"` — present for round-tripping but not enforced here.
    #[serde(rename = "type")]
    pub kind: ResponseKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub command: String,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResponseKind {
    Response,
}

// ----- Per-command response data payloads -----

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewSessionData {
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<PiModel>,
    #[serde(rename = "thinkingLevel")]
    pub thinking_level: ThinkingLevel,
    #[serde(rename = "isStreaming")]
    pub is_streaming: bool,
    #[serde(rename = "isCompacting")]
    pub is_compacting: bool,
    #[serde(rename = "steeringMode")]
    pub steering_mode: QueueMode,
    #[serde(rename = "followUpMode")]
    pub follow_up_mode: QueueMode,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sessionFile"
    )]
    pub session_file: Option<String>,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sessionName"
    )]
    pub session_name: Option<String>,
    #[serde(rename = "autoCompactionEnabled")]
    pub auto_compaction_enabled: bool,
    #[serde(rename = "messageCount")]
    pub message_count: u64,
    #[serde(rename = "pendingMessageCount")]
    pub pending_message_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CycleModelData {
    pub model: PiModel,
    #[serde(rename = "thinkingLevel")]
    pub thinking_level: ThinkingLevel,
    #[serde(rename = "isScoped")]
    pub is_scoped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AvailableModelsData {
    pub models: Vec<PiModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CycleThinkingLevelData {
    pub level: ThinkingLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompactionResultData {
    pub summary: String,
    #[serde(rename = "firstKeptEntryId")]
    pub first_kept_entry_id: String,
    #[serde(rename = "tokensBefore")]
    pub tokens_before: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BashResultData {
    pub output: String,
    #[serde(rename = "exitCode", default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub cancelled: bool,
    pub truncated: bool,
    #[serde(
        rename = "fullOutputPath",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub full_output_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionStatsData {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sessionFile"
    )]
    pub session_file: Option<String>,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "userMessages")]
    pub user_messages: u64,
    #[serde(rename = "assistantMessages")]
    pub assistant_messages: u64,
    #[serde(rename = "toolCalls")]
    pub tool_calls: u64,
    #[serde(rename = "toolResults")]
    pub tool_results: u64,
    #[serde(rename = "totalMessages")]
    pub total_messages: u64,
    pub tokens: SessionStatsTokens,
    pub cost: f64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "contextUsage"
    )]
    pub context_usage: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionStatsTokens {
    pub input: u64,
    pub output: u64,
    #[serde(rename = "cacheRead")]
    pub cache_read: u64,
    #[serde(rename = "cacheWrite")]
    pub cache_write: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportHtmlData {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SwitchSessionData {
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForkData {
    pub text: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForkMessagesData {
    pub messages: Vec<ForkMessageEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForkMessageEntry {
    #[serde(rename = "entryId")]
    pub entry_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LastAssistantTextData {
    /// `null` when no assistant text exists yet.
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListSessionsData {
    pub sessions: Vec<SessionInfoData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInfoData {
    pub path: String,
    pub id: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "parentSessionPath"
    )]
    pub parent_session_path: Option<String>,
    pub created: String,
    pub modified: String,
    #[serde(rename = "messageCount")]
    pub message_count: usize,
    #[serde(rename = "firstMessage")]
    pub first_message: String,
    #[serde(default, rename = "allMessagesText")]
    pub all_messages_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GetMessagesData {
    pub messages: Vec<AgentMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GetCommandsData {
    pub commands: Vec<RpcSlashCommand>,
}

// ============================================================================
// Slash command (response payload for `get_commands`)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcSlashCommand {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub source: SlashCommandSource,
    #[serde(rename = "sourceInfo")]
    pub source_info: SourceInfo,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SlashCommandSource {
    Extension,
    Prompt,
    Skill,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceInfo {
    pub path: String,
    pub source: String,
    pub scope: SourceScope,
    pub origin: SourceOrigin,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "baseDir")]
    pub base_dir: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SourceScope {
    User,
    Project,
    Temporary,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum SourceOrigin {
    Package,
    TopLevel,
}

// ============================================================================
// Pi events (pi → bridge, multiplexed with responses on stdout)
// ============================================================================

/// Anything pi writes to stdout that is not an `RpcResponse`. Tagged on `type`.
///
/// Combines the `AgentEvent` union (`pi-mono/packages/agent/src/types.ts:326`)
/// with `AgentSessionEvent` extras (`agent-session.ts:111`) and the
/// `extension_ui_request` / `extension_error` envelopes pi emits from RPC mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PiEvent {
    // Agent lifecycle
    AgentStart,
    AgentEnd {
        messages: Vec<AgentMessage>,
    },

    // Turn lifecycle (one assistant response + tool calls/results)
    TurnStart,
    TurnEnd {
        message: AgentMessage,
        #[serde(rename = "toolResults")]
        tool_results: Vec<ToolResultMessage>,
    },
    ThinkingLevelChanged {
        level: ThinkingLevel,
    },

    // Message lifecycle
    MessageStart {
        message: AgentMessage,
    },
    MessageUpdate {
        message: AgentMessage,
        #[serde(rename = "assistantMessageEvent")]
        assistant_message_event: Box<AssistantMessageEvent>,
    },
    MessageEnd {
        message: AgentMessage,
    },

    // Tool execution lifecycle
    ToolExecutionStart {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: Value,
    },
    ToolExecutionUpdate {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        args: Value,
        #[serde(rename = "partialResult")]
        partial_result: Value,
    },
    ToolExecutionEnd {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolName")]
        tool_name: String,
        result: Value,
        #[serde(rename = "isError")]
        is_error: bool,
    },

    // Session-level extensions (AgentSessionEvent)
    QueueUpdate {
        steering: Vec<String>,
        #[serde(rename = "followUp")]
        follow_up: Vec<String>,
    },
    CompactionStart {
        reason: CompactionReason,
    },
    CompactionEnd {
        reason: CompactionReason,
        result: Option<CompactionResultData>,
        aborted: bool,
        #[serde(rename = "willRetry")]
        will_retry: bool,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            rename = "errorMessage"
        )]
        error_message: Option<String>,
    },
    AutoRetryStart {
        attempt: u32,
        #[serde(rename = "maxAttempts")]
        max_attempts: u32,
        #[serde(rename = "delayMs")]
        delay_ms: u64,
        #[serde(rename = "errorMessage")]
        error_message: String,
    },
    AutoRetryEnd {
        success: bool,
        attempt: u32,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            rename = "finalError"
        )]
        final_error: Option<String>,
    },

    // Extension UI (pi → bridge → codex client)
    ExtensionUiRequest(ExtensionUiRequest),

    /// Pi emits this when an extension throws while handling an event
    /// (`rpc-mode.ts:332`). Bridge logs and surfaces as a codex `error`.
    ExtensionError {
        #[serde(rename = "extensionPath")]
        extension_path: String,
        event: Value,
        error: Value,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}

// ============================================================================
// Extension UI envelopes
// ============================================================================

/// `extension_ui_request` body without the outer `type` discriminator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum ExtensionUiRequest {
    #[serde(alias = "select")]
    Select {
        id: String,
        title: String,
        options: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    #[serde(alias = "confirm")]
    Confirm {
        id: String,
        title: String,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    #[serde(alias = "input")]
    Input {
        id: String,
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        placeholder: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout: Option<u64>,
    },
    #[serde(alias = "editor")]
    Editor {
        id: String,
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefill: Option<String>,
    },
    #[serde(alias = "notify")]
    Notify {
        id: String,
        message: String,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            rename = "notifyType"
        )]
        notify_type: Option<NotifyType>,
    },
    #[serde(alias = "setStatus")]
    SetStatus {
        id: String,
        #[serde(rename = "statusKey")]
        status_key: String,
        #[serde(rename = "statusText")]
        status_text: Option<String>,
    },
    #[serde(alias = "setWidget")]
    SetWidget {
        id: String,
        #[serde(rename = "widgetKey")]
        widget_key: String,
        #[serde(rename = "widgetLines")]
        widget_lines: Option<Vec<String>>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            rename = "widgetPlacement"
        )]
        widget_placement: Option<WidgetPlacement>,
    },
    #[serde(alias = "setTitle")]
    SetTitle { id: String, title: String },
    #[serde(alias = "setEditorText")]
    SetEditorText { id: String, text: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NotifyType {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum WidgetPlacement {
    AboveEditor,
    BelowEditor,
}

/// Bridge → pi reply to an `extension_ui_request`. Emitted as the
/// `ExtensionUiResponse` variant of [`RpcCommand`] (untagged inside, since pi
/// dispatches on field presence — `value`, `confirmed`, or `cancelled:true`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ExtensionUiResponse {
    Value {
        id: String,
        value: String,
    },
    Confirmed {
        id: String,
        confirmed: bool,
    },
    Cancelled {
        id: String,
        /// Always `true` per pi's discriminator.
        cancelled: bool,
    },
}

// ============================================================================
// AssistantMessageEvent (streaming sub-events on `message_update`)
// ============================================================================

/// Sub-event nested inside `message_update.assistantMessageEvent`.
/// Source: `pi-mono/packages/ai/src/types.ts:237-249`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantMessageEvent {
    Start {
        partial: AssistantMessage,
    },
    TextStart {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        partial: AssistantMessage,
    },
    TextDelta {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        delta: String,
        partial: AssistantMessage,
    },
    TextEnd {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        content: String,
        partial: AssistantMessage,
    },
    ThinkingStart {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        partial: AssistantMessage,
    },
    ThinkingDelta {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        delta: String,
        partial: AssistantMessage,
    },
    ThinkingEnd {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        content: String,
        partial: AssistantMessage,
    },
    ToolcallStart {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        partial: AssistantMessage,
    },
    ToolcallDelta {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        delta: String,
        partial: AssistantMessage,
    },
    ToolcallEnd {
        #[serde(rename = "contentIndex")]
        content_index: u32,
        #[serde(rename = "toolCall")]
        tool_call: ToolCall,
        partial: AssistantMessage,
    },
    Done {
        reason: StopReason,
        message: AssistantMessage,
    },
    Error {
        reason: StopReason,
        error: AssistantMessage,
    },
}

// ============================================================================
// Pi message types (`pi-mono/packages/ai/src/types.ts:184-213`)
// ============================================================================

/// Top-level pi conversation message, surfaced via `agent_end.messages`,
/// `turn_end.message`, `message_*.message`, and `get_messages`.
///
/// Pi tags variants with the `role` field but the variant payloads *also*
/// carry a literal `role` (TS lines 184-211 hard-code each `role`). To keep
/// standalone usage (e.g. `AssistantMessageEvent::Done.message`) lossless,
/// each variant struct keeps its own `role` field and we use `untagged` here
/// — serde dispatches on field presence (e.g. `toolCallId` for ToolResult,
/// `provider`/`api` for Assistant, otherwise User).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum AgentMessage {
    /// Order matters for `untagged` dispatch — match the most specific shapes
    /// first so the catch-all `Other` does not swallow well-formed messages.
    ToolResult(ToolResultMessage),
    Assistant(AssistantMessage),
    User(UserMessage),

    /// Catch-all for app-defined custom messages (`CustomAgentMessages`).
    /// Pi extensions can declare additional roles via declaration merging
    /// (`pi-mono/packages/agent/src/types.ts:236`); we don't decode them but
    /// preserve the wire payload so `get_messages` round-trips losslessly.
    Other(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UserMessage {
    pub role: UserRole,
    pub content: UserMessageContent,
    /// Unix milliseconds.
    pub timestamp: i64,
}

/// Literal `"user"` discriminator. Defaults to [`UserRole::User`] so
/// constructing a [`UserMessage`] in code stays ergonomic.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UserRole {
    #[default]
    User,
}

/// Pi's `UserMessage.content` is `string | (TextContent|ImageContent)[]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum UserMessageContent {
    Text(String),
    Blocks(Vec<UserContentBlock>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum UserContentBlock {
    Text(TextContent),
    Image(ImageContent),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AssistantMessage {
    pub role: AssistantRole,
    pub content: Vec<AssistantContentBlock>,
    pub api: String,
    pub provider: String,
    pub model: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "responseId"
    )]
    pub response_id: Option<String>,
    pub usage: Usage,
    #[serde(rename = "stopReason")]
    pub stop_reason: StopReason,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "errorMessage"
    )]
    pub error_message: Option<String>,
    /// Unix milliseconds.
    pub timestamp: i64,
}

/// Literal `"assistant"` discriminator.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AssistantRole {
    #[default]
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AssistantContentBlock {
    Text(TextContent),
    Thinking(ThinkingContent),
    ToolCall(ToolCall),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TextContent {
    pub text: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "textSignature"
    )]
    pub text_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThinkingContent {
    pub thinking: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "thinkingSignature"
    )]
    pub thinking_signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "thoughtSignature"
    )]
    pub thought_signature: Option<String>,
}

/// Pi `image` content block, base64-encoded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageContent {
    pub data: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResultMessage {
    pub role: ToolResultRole,
    #[serde(rename = "toolCallId")]
    pub tool_call_id: String,
    #[serde(rename = "toolName")]
    pub tool_name: String,
    pub content: Vec<ToolResultContentBlock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(rename = "isError")]
    pub is_error: bool,
    pub timestamp: i64,
}

/// Literal `"toolResult"` discriminator.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ToolResultRole {
    #[default]
    ToolResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ToolResultContentBlock {
    Text(TextContent),
    Image(ImageContent),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum StopReason {
    Stop,
    Length,
    ToolUse,
    Error,
    Aborted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    #[serde(rename = "cacheRead")]
    pub cache_read: u64,
    #[serde(rename = "cacheWrite")]
    pub cache_write: u64,
    #[serde(rename = "totalTokens")]
    pub total_tokens: u64,
    pub cost: UsageCost,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct UsageCost {
    pub input: f64,
    pub output: f64,
    #[serde(rename = "cacheRead")]
    pub cache_read: f64,
    #[serde(rename = "cacheWrite")]
    pub cache_write: f64,
    pub total: f64,
}

// ============================================================================
// Pi Model (`pi-mono/packages/ai/src/types.ts:378`)
// ============================================================================

/// Pi `Model<TApi>` minus the API-specific `compat` shape, which we leave as
/// raw JSON. Unknown fields are preserved via `extra` so we forward
/// future additions to the codex `model/list` translator without recompiling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PiModel {
    pub id: String,
    pub name: String,
    pub api: String,
    pub provider: String,
    #[serde(rename = "baseUrl")]
    pub base_url: String,
    pub reasoning: bool,
    pub input: Vec<ModelInputModality>,
    pub cost: ModelCost,
    #[serde(rename = "contextWindow")]
    pub context_window: u64,
    #[serde(rename = "maxTokens")]
    pub max_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compat: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ModelInputModality {
    Text,
    Image,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ModelCost {
    pub input: f64,
    pub output: f64,
    #[serde(rename = "cacheRead")]
    pub cache_read: f64,
    #[serde(rename = "cacheWrite")]
    pub cache_write: f64,
}

// ============================================================================
// Frame demuxer
// ============================================================================

/// Each line pi writes to stdout is one of: a response, an event, or a stray
/// extension UI request (which we represent as a [`PiEvent::ExtensionUiRequest`]
/// for routing parity).
///
/// Used by `pool/process.rs` to dispatch each line to either the
/// per-id response oneshot or the broadcast event channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PiOutboundMessage {
    /// `{type:"response", command, success, ...}` — must come first because
    /// `PiEvent` would otherwise match the `type:"response"` literal as an
    /// unknown variant on permissive deserializers.
    Response(RpcResponse),
    /// Any tagged session event.
    Event(PiEvent),
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    /// Round-trip a value: serialize, parse back into a `serde_json::Value`,
    /// and compare against an expected JSON. This catches both field-renaming
    /// regressions and extra/missing fields.
    fn round_trip<T: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug + PartialEq>(
        value: &T,
        expected_json: Value,
    ) {
        let serialized = serde_json::to_value(value).expect("serialize");
        assert_eq!(serialized, expected_json, "serialized form mismatch");
        let parsed: T = serde_json::from_value(expected_json).expect("deserialize");
        assert_eq!(&parsed, value, "round-trip identity mismatch");
    }

    #[test]
    fn prompt_command_serializes_with_streaming_behavior() {
        let cmd = RpcCommand::Prompt(PromptCmd {
            id: Some("c1".into()),
            message: "hello".into(),
            images: vec![ImageContent {
                data: "AAA".into(),
                mime_type: "image/png".into(),
            }],
            streaming_behavior: Some(StreamingBehavior::Steer),
        });
        round_trip(
            &cmd,
            json!({
                "type": "prompt",
                "id": "c1",
                "message": "hello",
                "images": [{"data":"AAA","mimeType":"image/png"}],
                "streamingBehavior": "steer"
            }),
        );
    }

    #[test]
    fn bare_command_omits_optional_id() {
        let cmd = RpcCommand::Abort(BareCmd { id: None });
        let v = serde_json::to_value(&cmd).unwrap();
        assert_eq!(v, json!({"type":"abort"}));
    }

    #[test]
    fn list_sessions_command_uses_pi_wire_name() {
        let cmd = RpcCommand::ListSessions(ListSessionsCmd {
            id: Some("list-1".into()),
            all_projects: true,
        });
        round_trip(
            &cmd,
            json!({
                "type": "list_sessions",
                "id": "list-1",
                "allProjects": true
            }),
        );
    }

    #[test]
    fn new_session_with_parent() {
        let cmd = RpcCommand::NewSession(NewSessionCmd {
            id: Some("c2".into()),
            parent_session: Some("/path/to/session.jsonl".into()),
        });
        round_trip(
            &cmd,
            json!({
                "type": "new_session",
                "id": "c2",
                "parentSession": "/path/to/session.jsonl"
            }),
        );
    }

    #[test]
    fn set_thinking_level_xhigh() {
        let cmd = RpcCommand::SetThinkingLevel(SetThinkingLevelCmd {
            id: None,
            level: ThinkingLevel::Xhigh,
        });
        round_trip(&cmd, json!({"type":"set_thinking_level","level":"xhigh"}));
    }

    #[test]
    fn set_steering_mode_one_at_a_time() {
        let cmd = RpcCommand::SetSteeringMode(SetQueueModeCmd {
            id: None,
            mode: QueueMode::OneAtATime,
        });
        round_trip(
            &cmd,
            json!({"type":"set_steering_mode","mode":"one-at-a-time"}),
        );
    }

    #[test]
    fn extension_ui_response_value() {
        let cmd = RpcCommand::ExtensionUiResponse(ExtensionUiResponse::Value {
            id: "u1".into(),
            value: "yes".into(),
        });
        // Outer command type wraps the untagged inner.
        let v = serde_json::to_value(&cmd).unwrap();
        assert_eq!(
            v,
            json!({"type":"extension_ui_response","id":"u1","value":"yes"})
        );
        let back: RpcCommand = serde_json::from_value(v).unwrap();
        assert_eq!(back, cmd);
    }

    #[test]
    fn extension_ui_response_cancelled() {
        let cmd = RpcCommand::ExtensionUiResponse(ExtensionUiResponse::Cancelled {
            id: "u2".into(),
            cancelled: true,
        });
        round_trip(
            &cmd,
            json!({"type":"extension_ui_response","id":"u2","cancelled":true}),
        );
    }

    #[test]
    fn rpc_response_success_with_data() {
        let body = json!({
            "type":"response",
            "id":"c1",
            "command":"new_session",
            "success":true,
            "data":{"cancelled":false}
        });
        let r: RpcResponse = serde_json::from_value(body.clone()).unwrap();
        assert!(r.success);
        assert_eq!(r.command, "new_session");
        let data: NewSessionData = serde_json::from_value(r.data.clone().unwrap()).unwrap();
        assert!(!data.cancelled);
        assert_eq!(serde_json::to_value(&r).unwrap(), body);
    }

    #[test]
    fn rpc_response_error() {
        let body = json!({
            "type":"response",
            "command":"set_model",
            "success":false,
            "error":"Model not found"
        });
        let r: RpcResponse = serde_json::from_value(body.clone()).unwrap();
        assert!(!r.success);
        assert_eq!(r.error.as_deref(), Some("Model not found"));
        assert!(r.id.is_none());
        assert_eq!(serde_json::to_value(&r).unwrap(), body);
    }

    fn sample_assistant_message_json() -> Value {
        json!({
            "role":"assistant",
            "content":[{"type":"text","text":"hi"}],
            "api":"openai-responses",
            "provider":"openai",
            "model":"gpt-5",
            "usage":{
                "input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,
                "cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0,"total":0.0}
            },
            "stopReason":"toolUse",
            "timestamp": 1700000000000_i64
        })
    }

    #[test]
    fn pi_event_message_update_text_delta() {
        let body = json!({
            "type": "message_update",
            "message": sample_assistant_message_json(),
            "assistantMessageEvent": {
                "type":"text_delta",
                "contentIndex":0,
                "delta":"hi",
                "partial": sample_assistant_message_json()
            }
        });
        let event: PiEvent = serde_json::from_value(body.clone()).unwrap();
        match &event {
            PiEvent::MessageUpdate {
                assistant_message_event,
                ..
            } => match &**assistant_message_event {
                AssistantMessageEvent::TextDelta { delta, .. } => assert_eq!(delta, "hi"),
                _ => panic!("expected text_delta"),
            },
            _ => panic!("expected message_update"),
        }
        assert_eq!(serde_json::to_value(&event).unwrap(), body);
    }

    #[test]
    fn pi_event_tool_execution_start() {
        let body = json!({
            "type": "tool_execution_start",
            "toolCallId": "tc1",
            "toolName": "bash",
            "args": {"command":"ls"}
        });
        let event: PiEvent = serde_json::from_value(body.clone()).unwrap();
        match &event {
            PiEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => {
                assert_eq!(tool_call_id, "tc1");
                assert_eq!(tool_name, "bash");
                assert_eq!(args["command"], "ls");
            }
            _ => panic!("expected tool_execution_start"),
        }
        assert_eq!(serde_json::to_value(&event).unwrap(), body);
    }

    #[test]
    fn pi_event_compaction_end_with_result() {
        let body = json!({
            "type":"compaction_end",
            "reason":"threshold",
            "result":{
                "summary":"S",
                "firstKeptEntryId":"e1",
                "tokensBefore":12345,
            },
            "aborted":false,
            "willRetry":false
        });
        let event: PiEvent = serde_json::from_value(body.clone()).unwrap();
        match &event {
            PiEvent::CompactionEnd {
                reason,
                result,
                aborted,
                will_retry,
                error_message,
            } => {
                assert_eq!(*reason, CompactionReason::Threshold);
                assert!(!*aborted);
                assert!(!*will_retry);
                assert!(error_message.is_none());
                assert_eq!(result.as_ref().unwrap().summary, "S");
            }
            _ => panic!("expected compaction_end"),
        }
        // Re-serialize and compare structurally — `result.details` is None and
        // `errorMessage` is None, so both should be omitted.
        assert_eq!(serde_json::to_value(&event).unwrap(), body);
    }

    #[test]
    fn pi_event_extension_ui_request_select() {
        let body = json!({
            "type":"extension_ui_request",
            "method":"select",
            "id":"u1",
            "title":"Pick one",
            "options":["a","b"]
        });
        let event: PiEvent = serde_json::from_value(body.clone()).unwrap();
        match &event {
            PiEvent::ExtensionUiRequest(ExtensionUiRequest::Select {
                id,
                title,
                options,
                timeout,
            }) => {
                assert_eq!(id, "u1");
                assert_eq!(title, "Pick one");
                assert_eq!(options, &vec!["a".to_string(), "b".to_string()]);
                assert!(timeout.is_none());
            }
            _ => panic!("expected extension_ui_request select"),
        }
        assert_eq!(serde_json::to_value(&event).unwrap(), body);
    }

    #[test]
    fn pi_event_extension_ui_request_accepts_camel_case_status_and_widget() {
        let status = json!({
            "type":"extension_ui_request",
            "method":"setStatus",
            "id":"s1",
            "statusKey":"speed",
            "statusText":"idle"
        });
        match serde_json::from_value::<PiOutboundMessage>(status).unwrap() {
            PiOutboundMessage::Event(PiEvent::ExtensionUiRequest(
                ExtensionUiRequest::SetStatus {
                    id,
                    status_key,
                    status_text,
                },
            )) => {
                assert_eq!(id, "s1");
                assert_eq!(status_key, "speed");
                assert_eq!(status_text.as_deref(), Some("idle"));
            }
            other => panic!("expected setStatus extension UI request, got {other:?}"),
        }

        let widget = json!({
            "type":"extension_ui_request",
            "method":"setWidget",
            "id":"w1",
            "widgetKey":"git-status",
            "widgetPlacement":"belowEditor"
        });
        match serde_json::from_value::<PiOutboundMessage>(widget).unwrap() {
            PiOutboundMessage::Event(PiEvent::ExtensionUiRequest(
                ExtensionUiRequest::SetWidget {
                    id,
                    widget_key,
                    widget_lines,
                    widget_placement,
                },
            )) => {
                assert_eq!(id, "w1");
                assert_eq!(widget_key, "git-status");
                assert!(widget_lines.is_none());
                assert_eq!(widget_placement, Some(WidgetPlacement::BelowEditor));
            }
            other => panic!("expected setWidget extension UI request, got {other:?}"),
        }
    }

    #[test]
    fn outbound_demuxer_picks_response_over_event() {
        let resp = json!({"type":"response","command":"abort","success":true});
        match serde_json::from_value::<PiOutboundMessage>(resp).unwrap() {
            PiOutboundMessage::Response(_) => {}
            PiOutboundMessage::Event(_) => panic!("response misclassified as event"),
        }
        let evt = json!({"type":"agent_start"});
        match serde_json::from_value::<PiOutboundMessage>(evt).unwrap() {
            PiOutboundMessage::Event(PiEvent::AgentStart) => {}
            _ => panic!("agent_start misclassified"),
        }
    }

    #[test]
    fn thinking_level_changed_event_round_trips() {
        for level in [
            ThinkingLevel::Off,
            ThinkingLevel::Minimal,
            ThinkingLevel::Low,
            ThinkingLevel::Medium,
            ThinkingLevel::High,
            ThinkingLevel::Xhigh,
        ] {
            let event = PiEvent::ThinkingLevelChanged { level };
            let body = serde_json::to_value(&event).unwrap();
            assert_eq!(
                body.get("type").and_then(Value::as_str),
                Some("thinking_level_changed")
            );

            match serde_json::from_value::<PiOutboundMessage>(body).unwrap() {
                PiOutboundMessage::Event(PiEvent::ThinkingLevelChanged { level: parsed }) => {
                    assert_eq!(parsed, level);
                }
                _ => panic!("thinking_level_changed misclassified"),
            }
        }
    }

    #[test]
    fn agent_message_user_string_content() {
        let body = json!({
            "role":"user",
            "content":"hello",
            "timestamp": 1700000000000_i64
        });
        let m: AgentMessage = serde_json::from_value(body.clone()).unwrap();
        match &m {
            AgentMessage::User(u) => match &u.content {
                UserMessageContent::Text(s) => assert_eq!(s, "hello"),
                _ => panic!("expected text content"),
            },
            _ => panic!("expected user role"),
        }
        assert_eq!(serde_json::to_value(&m).unwrap(), body);
    }

    #[test]
    fn agent_message_user_blocks_content() {
        let body = json!({
            "role":"user",
            "content":[
                {"type":"text","text":"see this"},
                {"type":"image","data":"AAA","mimeType":"image/png"}
            ],
            "timestamp": 1700000000000_i64
        });
        let m: AgentMessage = serde_json::from_value(body.clone()).unwrap();
        match &m {
            AgentMessage::User(u) => match &u.content {
                UserMessageContent::Blocks(blocks) => assert_eq!(blocks.len(), 2),
                _ => panic!("expected blocks content"),
            },
            _ => panic!("expected user role"),
        }
        assert_eq!(serde_json::to_value(&m).unwrap(), body);
    }

    #[test]
    fn agent_message_tool_result_round_trip() {
        let body = json!({
            "role":"toolResult",
            "toolCallId":"tc1",
            "toolName":"bash",
            "content":[{"type":"text","text":"ok"}],
            "isError":false,
            "timestamp": 1700000000000_i64
        });
        let m: AgentMessage = serde_json::from_value(body.clone()).unwrap();
        match &m {
            AgentMessage::ToolResult(t) => {
                assert_eq!(t.tool_call_id, "tc1");
                assert!(!t.is_error);
            }
            _ => panic!("expected toolResult"),
        }
        assert_eq!(serde_json::to_value(&m).unwrap(), body);
    }

    #[test]
    fn rpc_slash_command_round_trip() {
        let cmd = RpcSlashCommand {
            name: "review".into(),
            description: Some("Review changes".into()),
            source: SlashCommandSource::Skill,
            source_info: SourceInfo {
                path: "/repo/.claude/skills/review".into(),
                source: "user-config".into(),
                scope: SourceScope::Project,
                origin: SourceOrigin::TopLevel,
                base_dir: Some("/repo".into()),
            },
        };
        round_trip(
            &cmd,
            json!({
                "name": "review",
                "description": "Review changes",
                "source": "skill",
                "sourceInfo": {
                    "path": "/repo/.claude/skills/review",
                    "source": "user-config",
                    "scope": "project",
                    "origin": "top-level",
                    "baseDir": "/repo"
                }
            }),
        );
    }

    #[test]
    fn pi_model_preserves_unknown_fields() {
        let body = json!({
            "id": "gpt-5",
            "name": "GPT-5",
            "api": "openai-responses",
            "provider": "openai",
            "baseUrl": "https://api.openai.com/v1",
            "reasoning": true,
            "input": ["text", "image"],
            "cost": {"input":1.0,"output":2.0,"cacheRead":0.5,"cacheWrite":0.5},
            "contextWindow": 200000,
            "maxTokens": 128000,
            "futureField": {"experimental": true}
        });
        let m: PiModel = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(m.id, "gpt-5");
        assert!(m.extra.contains_key("futureField"));
        assert_eq!(serde_json::to_value(&m).unwrap(), body);
    }

    #[test]
    fn agent_event_agent_end_with_messages() {
        let body = json!({
            "type":"agent_end",
            "messages":[
                {"role":"user","content":"hi","timestamp":1_i64}
            ]
        });
        let event: PiEvent = serde_json::from_value(body.clone()).unwrap();
        match &event {
            PiEvent::AgentEnd { messages } => assert_eq!(messages.len(), 1),
            _ => panic!("expected agent_end"),
        }
        assert_eq!(serde_json::to_value(&event).unwrap(), body);
    }
}
