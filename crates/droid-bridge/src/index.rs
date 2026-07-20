use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_codex_proto as p;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};

pub use alleycat_bridge_core::{
    IndexEntry as CoreIndexEntry, ListFilter, ListPage, ListSort, ThreadIndex as CoreThreadIndex,
};

pub const CLI_VERSION: &str = concat!("alleycat-droid-bridge/", env!("CARGO_PKG_VERSION"));

const MAX_SESSION_SUMMARY_SCAN_BYTES: u64 = 128 * 1024;
const MAX_SESSION_SUMMARY_TEXT_BYTES: usize = 8 * 1024;

#[derive(Debug, Default)]
struct SessionScanDiagnostics {
    bytes_read: u64,
    truncated: bool,
    malformed_lines: usize,
    boundary_fragments: usize,
    text_truncations: usize,
}

#[derive(Debug)]
struct SessionScanOutcome {
    info: DroidSessionInfo,
    diagnostics: SessionScanDiagnostics,
}

#[derive(Debug, Default)]
struct HydrationDiagnostics {
    scanned_sessions: usize,
    truncated_sessions: usize,
    malformed_lines: usize,
    boundary_fragments: usize,
    text_truncations: usize,
    unreadable_sessions: usize,
    bytes_read: u64,
}

impl HydrationDiagnostics {
    fn record(&mut self, diagnostics: &SessionScanDiagnostics) {
        self.scanned_sessions += 1;
        self.truncated_sessions += usize::from(diagnostics.truncated);
        self.malformed_lines += diagnostics.malformed_lines;
        self.boundary_fragments += diagnostics.boundary_fragments;
        self.text_truncations += diagnostics.text_truncations;
        self.bytes_read += diagnostics.bytes_read;
    }

    fn record_unreadable(&mut self) {
        self.unreadable_sessions += 1;
    }

    fn log_if_degraded(&self, root: &Path) {
        if self.truncated_sessions == 0
            && self.malformed_lines == 0
            && self.boundary_fragments == 0
            && self.text_truncations == 0
            && self.unreadable_sessions == 0
        {
            return;
        }
        tracing::warn!(
            root = %root.display(),
            scanned_sessions = self.scanned_sessions,
            truncated_sessions = self.truncated_sessions,
            malformed_lines = self.malformed_lines,
            boundary_fragments = self.boundary_fragments,
            text_truncations = self.text_truncations,
            unreadable_sessions = self.unreadable_sessions,
            bytes_read = self.bytes_read,
            scan_limit_bytes = MAX_SESSION_SUMMARY_SCAN_BYTES,
            "droid hydration retained bounded best-effort summaries"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanSegment {
    Full,
    Head,
    Tail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DroidSessionRef {
    pub droid_session_path: PathBuf,
    pub droid_session_id: String,
}

pub type IndexEntry = CoreIndexEntry<DroidSessionRef>;
pub type ThreadIndex = CoreThreadIndex<DroidSessionRef>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DroidSessionInfo {
    pub path: PathBuf,
    pub session_id: String,
    pub cwd: String,
    pub title: Option<String>,
    pub session_title: Option<String>,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub first_message: String,
    pub latest_message: String,
}

pub struct DroidHydrator {
    pub override_sessions_dir: Option<PathBuf>,
}

impl DroidHydrator {
    pub fn new() -> Self {
        Self {
            override_sessions_dir: None,
        }
    }

    pub fn with_sessions_dir(dir: PathBuf) -> Self {
        Self {
            override_sessions_dir: Some(dir),
        }
    }

    pub async fn scan_sessions(&self) -> Vec<DroidSessionInfo> {
        match self.override_sessions_dir.as_deref() {
            Some(dir) => list_all_from_root(dir).await,
            None => list_all().await,
        }
    }
}

impl Default for DroidHydrator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl alleycat_bridge_core::Hydrator<DroidSessionRef> for DroidHydrator {
    async fn scan(&self) -> Result<Vec<IndexEntry>> {
        Ok(self
            .scan_sessions()
            .await
            .iter()
            .map(entry_from_droid)
            .collect())
    }
}

pub async fn open_and_hydrate(
    codex_home: &Path,
    sessions_dir: Option<PathBuf>,
) -> Result<Arc<ThreadIndex>> {
    let hydrator = match sessions_dir {
        Some(dir) => DroidHydrator::with_sessions_dir(dir),
        None => DroidHydrator::new(),
    };
    ThreadIndex::open_and_hydrate(codex_home.join("threads.json"), &hydrator).await
}

pub fn factory_home() -> Option<PathBuf> {
    if let Ok(dir) =
        std::env::var("DROID_BRIDGE_FACTORY_HOME").or_else(|_| std::env::var("FACTORY_HOME"))
    {
        return Some(expand_tilde(&dir));
    }
    Some(directories::UserDirs::new()?.home_dir().join(".factory"))
}

pub fn factory_sessions_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("DROID_BRIDGE_FACTORY_SESSIONS_DIR") {
        return Some(expand_tilde(&dir));
    }
    factory_home().map(|home| home.join("sessions"))
}

pub async fn list_all() -> Vec<DroidSessionInfo> {
    let Some(root) = factory_sessions_dir() else {
        return Vec::new();
    };
    list_all_from_root(&root).await
}

pub async fn list_all_from_root(root: &Path) -> Vec<DroidSessionInfo> {
    let mut diagnostics = HydrationDiagnostics::default();
    let mut sessions = list_sessions_from_dir_with_diagnostics(root, &mut diagnostics).await;
    let mut read_dir = match fs::read_dir(root).await {
        Ok(rd) => rd,
        Err(_) => {
            diagnostics.log_if_degraded(root);
            return sessions;
        }
    };
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        if entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
        {
            sessions.extend(
                list_sessions_from_dir_with_diagnostics(&entry.path(), &mut diagnostics).await,
            );
        }
    }
    sessions.sort_by(|a, b| b.modified.cmp(&a.modified));
    diagnostics.log_if_degraded(root);
    sessions
}

pub async fn list_sessions_from_dir(dir: &Path) -> Vec<DroidSessionInfo> {
    let mut diagnostics = HydrationDiagnostics::default();
    let sessions = list_sessions_from_dir_with_diagnostics(dir, &mut diagnostics).await;
    diagnostics.log_if_degraded(dir);
    sessions
}

async fn list_sessions_from_dir_with_diagnostics(
    dir: &Path,
    diagnostics: &mut HydrationDiagnostics,
) -> Vec<DroidSessionInfo> {
    let mut sessions = Vec::new();
    let mut read_dir = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return sessions,
    };
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        match build_session_info_bounded(&path, MAX_SESSION_SUMMARY_SCAN_BYTES).await {
            Ok(outcome) => {
                diagnostics.record(&outcome.diagnostics);
                sessions.push(outcome.info);
            }
            Err(_) => {
                diagnostics.record_unreadable();
            }
        }
    }
    sessions
}

pub async fn build_session_info(path: &Path) -> Option<DroidSessionInfo> {
    match build_session_info_bounded(path, MAX_SESSION_SUMMARY_SCAN_BYTES).await {
        Ok(outcome) => {
            let diagnostics = &outcome.diagnostics;
            if diagnostics.truncated
                || diagnostics.malformed_lines > 0
                || diagnostics.boundary_fragments > 0
                || diagnostics.text_truncations > 0
            {
                tracing::warn!(
                    path = %path.display(),
                    truncated = diagnostics.truncated,
                    malformed_lines = diagnostics.malformed_lines,
                    boundary_fragments = diagnostics.boundary_fragments,
                    text_truncations = diagnostics.text_truncations,
                    bytes_read = diagnostics.bytes_read,
                    scan_limit_bytes = MAX_SESSION_SUMMARY_SCAN_BYTES,
                    "droid hydration retained a bounded best-effort summary"
                );
            }
            Some(outcome.info)
        }
        Err(err) => {
            tracing::warn!(path = %path.display(), %err, "failed to summarize droid session");
            None
        }
    }
}

pub fn entry_from_droid(info: &DroidSessionInfo) -> IndexEntry {
    let name = display_name(info);
    let preview = first_non_empty([
        Some(info.first_message.as_str()),
        Some(info.latest_message.as_str()),
        name.as_deref(),
    ])
    .unwrap_or("(no messages)")
    .to_string();

    IndexEntry {
        thread_id: info.session_id.clone(),
        cwd: info.cwd.clone(),
        created_at: info.created.timestamp_millis(),
        updated_at: info.modified.timestamp_millis(),
        archived: false,
        name,
        preview,
        forked_from_id: None,
        model_provider: "droid".to_string(),
        source: p::ThreadSourceKind::AppServer,
        metadata: DroidSessionRef {
            droid_session_path: info.path.clone(),
            droid_session_id: info.session_id.clone(),
        },
    }
}

pub fn thread_from_entry(entry: &IndexEntry) -> p::Thread {
    p::Thread {
        id: entry.thread_id.clone(),
        session_id: entry.metadata.droid_session_id.clone(),
        forked_from_id: entry.forked_from_id.clone(),
        preview: entry.preview.clone(),
        ephemeral: false,
        model_provider: entry.model_provider.clone(),
        created_at: entry.created_at,
        updated_at: entry.updated_at,
        status: p::ThreadStatus::NotLoaded,
        path: Some(
            entry
                .metadata
                .droid_session_path
                .to_string_lossy()
                .into_owned(),
        ),
        cwd: entry.cwd.clone(),
        cli_version: CLI_VERSION.to_string(),
        source: source_kind_to_session_source(entry.source),
        thread_source: None,
        agent_nickname: None,
        agent_role: None,
        git_info: alleycat_bridge_core::git_info_for_cwd(&entry.cwd),
        name: entry.name.clone(),
        turns: Vec::new(),
    }
}

pub async fn transcript_turns(path: &Path) -> Result<Vec<p::Turn>> {
    let text = match fs::read_to_string(path).await {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    Ok(transcript_text_to_turns(&text))
}

pub fn transcript_text_to_turns(text: &str) -> Vec<p::Turn> {
    let mut turns = Vec::new();
    let mut current_items = Vec::new();
    let mut current_started_at: Option<i64> = None;
    let mut current_completed_at: Option<i64> = None;
    let mut cwd = String::new();
    let mut pending_tools: HashMap<String, PendingTool> = HashMap::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) == Some("session_start") {
            if cwd.is_empty()
                && let Some(value) = value.get("cwd").and_then(Value::as_str)
            {
                cwd = value.to_string();
            }
            continue;
        }
        if value.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        let Some(message) = value.get("message") else {
            continue;
        };
        if cwd.is_empty()
            && let Some(value) = value.get("cwd").and_then(Value::as_str)
        {
            cwd = value.to_string();
        }
        let role = message.get("role").and_then(Value::as_str).unwrap_or("");
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_iso8601)
            .map(|dt| dt.timestamp_millis());
        let id = value
            .get("id")
            .or_else(|| message.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("item_{}", turns.len() + current_items.len()));

        match role {
            "user" => {
                let content = message.get("content").unwrap_or(&Value::Null);
                for result in tool_results_from_content(content) {
                    if current_items.is_empty() {
                        current_started_at = timestamp;
                    }
                    current_completed_at = timestamp.or(current_completed_at);
                    complete_pending_tool(
                        &mut current_items,
                        &mut pending_tools,
                        &result.tool_use_id,
                        &cwd,
                        &result.content,
                        result.is_error,
                    );
                }

                let inputs = content_to_user_inputs(content);
                if inputs.is_empty() || inputs_are_system_reminders(&inputs) {
                    continue;
                }
                push_turn(
                    &mut turns,
                    &mut current_items,
                    &mut current_started_at,
                    &mut current_completed_at,
                );
                pending_tools.clear();
                current_started_at = timestamp;
                current_completed_at = timestamp;
                current_items.push(p::ThreadItem::UserMessage {
                    id,
                    content: inputs,
                });
            }
            "assistant" => {
                let added = append_assistant_content(
                    &mut current_items,
                    &mut pending_tools,
                    message.get("content").unwrap_or(&Value::Null),
                    &id,
                    &cwd,
                );
                if !added {
                    continue;
                }
                if current_started_at.is_none() {
                    current_started_at = timestamp;
                }
                current_completed_at = timestamp.or(current_completed_at);
            }
            _ => {}
        }
    }
    push_turn(
        &mut turns,
        &mut current_items,
        &mut current_started_at,
        &mut current_completed_at,
    );
    turns
}

#[derive(Debug, Clone)]
struct PendingTool {
    name: String,
    input: Value,
    item_index: usize,
}

#[derive(Debug, Clone)]
struct PersistedToolResult {
    tool_use_id: String,
    content: String,
    is_error: bool,
}

fn append_assistant_content(
    current_items: &mut Vec<p::ThreadItem>,
    pending_tools: &mut HashMap<String, PendingTool>,
    content: &Value,
    base_id: &str,
    cwd: &str,
) -> bool {
    if let Some(text) = content.as_str().filter(|text| !text.is_empty()) {
        current_items.push(p::ThreadItem::AgentMessage {
            id: base_id.to_string(),
            text: text.to_string(),
            phase: None,
            memory_citation: None,
        });
        return true;
    }

    let Some(parts) = content.as_array() else {
        return false;
    };
    let simple_text = parts.len() == 1
        && parts[0].get("type").and_then(Value::as_str) == Some("text")
        && parts[0].get("text").and_then(Value::as_str).is_some();
    let mut added = false;

    for (index, part) in parts.iter().enumerate() {
        match part.get("type").and_then(Value::as_str) {
            Some("text") => {
                let Some(text) = part.get("text").and_then(Value::as_str) else {
                    continue;
                };
                if text.is_empty() {
                    continue;
                }
                current_items.push(p::ThreadItem::AgentMessage {
                    id: if simple_text {
                        base_id.to_string()
                    } else {
                        format!("{base_id}:text:{index}")
                    },
                    text: text.to_string(),
                    phase: None,
                    memory_citation: None,
                });
                added = true;
            }
            Some("thinking") | Some("reasoning") => {
                let Some(text) = part
                    .get("thinking")
                    .or_else(|| part.get("text"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if text.is_empty() {
                    continue;
                }
                current_items.push(p::ThreadItem::Reasoning {
                    id: format!("{base_id}:reasoning:{index}"),
                    summary: Vec::new(),
                    content: vec![text.to_string()],
                });
                added = true;
            }
            Some("tool_use") => {
                let id = part
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("{base_id}:tool:{index}"));
                let name = part
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let input = part.get("input").cloned().unwrap_or(Value::Null);
                let tool = PendingTool {
                    name,
                    input,
                    item_index: current_items.len(),
                };
                current_items.push(tool_item_from_use(&id, &tool, cwd, false));
                pending_tools.insert(id, tool);
                added = true;
            }
            _ => {}
        }
    }

    added
}

fn complete_pending_tool(
    current_items: &mut [p::ThreadItem],
    pending_tools: &mut HashMap<String, PendingTool>,
    id: &str,
    cwd: &str,
    content: &str,
    is_error: bool,
) {
    let Some(tool) = pending_tools.remove(id) else {
        return;
    };
    if let Some(slot) = current_items.get_mut(tool.item_index) {
        *slot = completed_tool_item_from_use(id, &tool, cwd, content, is_error);
    }
}

fn tool_item_from_use(id: &str, tool: &PendingTool, cwd: &str, completed: bool) -> p::ThreadItem {
    match tool.name.as_str() {
        "Execute" => p::ThreadItem::CommandExecution {
            id: id.to_string(),
            command: tool
                .input
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            cwd: cwd.to_string(),
            process_id: None,
            source: p::CommandExecutionSource::Agent,
            status: if completed {
                p::CommandExecutionStatus::Completed
            } else {
                p::CommandExecutionStatus::InProgress
            },
            command_actions: Vec::new(),
            aggregated_output: None,
            exit_code: None,
            duration_ms: None,
        },
        "Read" | "LS" | "Grep" | "Glob" => {
            let command = factory_tool_command(&tool.name, &tool.input);
            p::ThreadItem::CommandExecution {
                id: id.to_string(),
                command: command.clone(),
                cwd: cwd.to_string(),
                process_id: None,
                source: p::CommandExecutionSource::Agent,
                status: if completed {
                    p::CommandExecutionStatus::Completed
                } else {
                    p::CommandExecutionStatus::InProgress
                },
                command_actions: factory_command_actions(&tool.name, &tool.input, &command),
                aggregated_output: None,
                exit_code: None,
                duration_ms: None,
            }
        }
        "Edit" | "Create" | "ApplyPatch" => p::ThreadItem::FileChange {
            id: id.to_string(),
            changes: Vec::new(),
            status: if completed {
                p::PatchApplyStatus::Completed
            } else {
                p::PatchApplyStatus::InProgress
            },
        },
        _ => p::ThreadItem::DynamicToolCall {
            id: id.to_string(),
            namespace: None,
            tool: tool.name.clone(),
            arguments: tool.input.clone(),
            status: if completed {
                p::DynamicToolCallStatus::Completed
            } else {
                p::DynamicToolCallStatus::InProgress
            },
            content_items: None,
            success: None,
            duration_ms: None,
        },
    }
}

fn completed_tool_item_from_use(
    id: &str,
    tool: &PendingTool,
    cwd: &str,
    content: &str,
    is_error: bool,
) -> p::ThreadItem {
    match tool.name.as_str() {
        "Execute" => p::ThreadItem::CommandExecution {
            id: id.to_string(),
            command: tool
                .input
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            cwd: cwd.to_string(),
            process_id: None,
            source: p::CommandExecutionSource::Agent,
            status: if is_error {
                p::CommandExecutionStatus::Failed
            } else {
                p::CommandExecutionStatus::Completed
            },
            command_actions: Vec::new(),
            aggregated_output: Some(content.to_string()),
            exit_code: parse_exit_code(content),
            duration_ms: None,
        },
        "Read" | "LS" | "Grep" | "Glob" => {
            let command = factory_tool_command(&tool.name, &tool.input);
            p::ThreadItem::CommandExecution {
                id: id.to_string(),
                command: command.clone(),
                cwd: cwd.to_string(),
                process_id: None,
                source: p::CommandExecutionSource::Agent,
                status: if is_error {
                    p::CommandExecutionStatus::Failed
                } else {
                    p::CommandExecutionStatus::Completed
                },
                command_actions: factory_command_actions(&tool.name, &tool.input, &command),
                aggregated_output: Some(content.to_string()),
                exit_code: None,
                duration_ms: None,
            }
        }
        "Edit" | "Create" | "ApplyPatch" => p::ThreadItem::FileChange {
            id: id.to_string(),
            changes: Vec::new(),
            status: if is_error {
                p::PatchApplyStatus::Failed
            } else {
                p::PatchApplyStatus::Completed
            },
        },
        _ => p::ThreadItem::DynamicToolCall {
            id: id.to_string(),
            namespace: None,
            tool: tool.name.clone(),
            arguments: tool.input.clone(),
            status: if is_error {
                p::DynamicToolCallStatus::Failed
            } else {
                p::DynamicToolCallStatus::Completed
            },
            content_items: Some(vec![json!({"type": "text", "text": content})]),
            success: Some(!is_error),
            duration_ms: None,
        },
    }
}

fn factory_tool_command(name: &str, input: &Value) -> String {
    match name {
        "Read" => input
            .get("file_path")
            .or_else(|| input.get("path"))
            .and_then(Value::as_str)
            .map(|path| format!("read {path}"))
            .unwrap_or_else(|| "read".to_string()),
        "LS" => input
            .get("directory_path")
            .or_else(|| input.get("path"))
            .and_then(Value::as_str)
            .map(|path| format!("ls {path}"))
            .unwrap_or_else(|| "ls".to_string()),
        "Grep" => {
            let pattern = input.get("pattern").and_then(Value::as_str);
            let path = input.get("path").and_then(Value::as_str);
            match (pattern, path) {
                (Some(pattern), Some(path)) => format!("grep {pattern} {path}"),
                (Some(pattern), None) => format!("grep {pattern}"),
                (None, Some(path)) => format!("grep {path}"),
                (None, None) => "grep".to_string(),
            }
        }
        "Glob" => {
            let pattern = input
                .get("pattern")
                .or_else(|| input.get("patterns"))
                .and_then(factory_pattern_text);
            let path = input
                .get("folder")
                .or_else(|| input.get("path"))
                .and_then(Value::as_str);
            match (pattern, path) {
                (Some(pattern), Some(path)) => format!("glob {pattern} {path}"),
                (Some(pattern), None) => format!("glob {pattern}"),
                (None, Some(path)) => format!("glob {path}"),
                (None, None) => "glob".to_string(),
            }
        }
        _ => name.to_string(),
    }
}

fn factory_command_actions(name: &str, input: &Value, command: &str) -> Vec<Value> {
    match name {
        "Read" => input
            .get("file_path")
            .or_else(|| input.get("path"))
            .and_then(Value::as_str)
            .map(|path| {
                json!({
                    "type": "read",
                    "command": command,
                    "name": file_name(path),
                    "path": path
                })
            })
            .into_iter()
            .collect(),
        "LS" => input
            .get("directory_path")
            .or_else(|| input.get("path"))
            .and_then(Value::as_str)
            .map(|path| json!({"type": "listFiles", "command": command, "path": path}))
            .into_iter()
            .collect(),
        "Grep" => vec![json!({
            "type": "search",
            "command": command,
            "query": input.get("pattern").and_then(Value::as_str),
            "path": input.get("path").and_then(Value::as_str)
        })],
        "Glob" => vec![json!({
            "type": "search",
            "command": command,
            "query": input
                .get("pattern")
                .or_else(|| input.get("patterns"))
                .and_then(factory_pattern_text),
            "path": input
                .get("folder")
                .or_else(|| input.get("path"))
                .and_then(Value::as_str)
        })],
        _ => Vec::new(),
    }
}

fn factory_pattern_text(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    value.as_array().map(|items| {
        items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    })
}

fn file_name(path: &str) -> String {
    path.rsplit(['/', '\\'])
        .find(|part| !part.is_empty())
        .unwrap_or("file")
        .to_string()
}

pub async fn session_model(path: &Path) -> Option<String> {
    let settings_path = path.with_extension("settings.json");
    let text = fs::read_to_string(settings_path).await.ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub fn factory_session_path_for(cwd: &Path, session_id: &str) -> Option<PathBuf> {
    let mut path = factory_sessions_dir()?;
    path.push(encode_cwd(cwd));
    path.push(format!("{session_id}.jsonl"));
    Some(path)
}

async fn find_session_path(session_id: &str, sessions_dir: Option<&Path>) -> Option<PathBuf> {
    let root = match sessions_dir {
        Some(dir) => dir.to_path_buf(),
        None => factory_sessions_dir()?,
    };
    let mut read_dir = fs::read_dir(root).await.ok()?;
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.is_dir() {
            let candidate = path.join(format!("{session_id}.jsonl"));
            if fs::metadata(&candidate).await.is_ok() {
                return Some(candidate);
            }
        } else if path.file_stem().and_then(|s| s.to_str()) == Some(session_id) {
            return Some(path);
        }
    }
    None
}

pub async fn session_path_for(
    cwd: &Path,
    session_id: &str,
    sessions_dir: Option<&Path>,
) -> Option<PathBuf> {
    find_session_path(session_id, sessions_dir)
        .await
        .or_else(|| match sessions_dir {
            Some(dir) => {
                let mut path = dir.to_path_buf();
                path.push(encode_cwd(cwd));
                path.push(format!("{session_id}.jsonl"));
                Some(path)
            }
            None => factory_session_path_for(cwd, session_id),
        })
}

#[derive(Debug)]
struct DroidSessionSummary {
    path: PathBuf,
    session_id: String,
    cwd: String,
    title: Option<String>,
    session_title: Option<String>,
    first_message: String,
    latest_message: String,
    first_timestamp: Option<DateTime<Utc>>,
    last_timestamp: Option<DateTime<Utc>>,
    tail_timestamp: Option<DateTime<Utc>>,
}

impl DroidSessionSummary {
    fn new(path: &Path) -> io::Result<Self> {
        let session_id = path
            .file_stem()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing session id"))?
            .to_string_lossy()
            .to_string();
        Ok(Self {
            path: path.to_path_buf(),
            session_id,
            cwd: String::new(),
            title: None,
            session_title: None,
            first_message: String::new(),
            latest_message: String::new(),
            first_timestamp: None,
            last_timestamp: None,
            tail_timestamp: None,
        })
    }

    fn observe(
        &mut self,
        value: &Value,
        segment: ScanSegment,
        diagnostics: &mut SessionScanDiagnostics,
    ) {
        match value.get("type").and_then(Value::as_str) {
            Some("session_start") if segment != ScanSegment::Tail => {
                if let Some(id) = value
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                {
                    self.session_id = id.to_string();
                }
                if self.cwd.is_empty()
                    && let Some(cwd) = value.get("cwd").and_then(Value::as_str)
                {
                    self.cwd = cwd.to_string();
                }
                self.title = clean_title(value.get("title").and_then(Value::as_str))
                    .or_else(|| self.title.take());
                self.session_title = clean_title(value.get("sessionTitle").and_then(Value::as_str))
                    .or_else(|| self.session_title.take());
            }
            Some("message") => {
                let Some(message) = value.get("message") else {
                    return;
                };
                if self.cwd.is_empty()
                    && let Some(cwd) = value.get("cwd").and_then(Value::as_str)
                {
                    self.cwd = cwd.to_string();
                }
                let role = message.get("role").and_then(Value::as_str).unwrap_or("");
                if role != "user" && role != "assistant" {
                    return;
                }
                let (text, text_truncated) =
                    extract_summary_text_content(message.get("content").unwrap_or(&Value::Null));
                diagnostics.text_truncations += usize::from(text_truncated);
                if text.is_empty() {
                    return;
                }
                let is_displayable_user = role == "user" && !is_system_reminder_text(&text);
                if segment != ScanSegment::Tail
                    && self.first_message.is_empty()
                    && is_displayable_user
                {
                    self.first_message = text.clone();
                }
                if role == "assistant" || is_displayable_user {
                    self.latest_message = text;
                }
                if let Some(timestamp) = value
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .and_then(parse_iso8601)
                {
                    if segment != ScanSegment::Tail && self.first_timestamp.is_none() {
                        self.first_timestamp = Some(timestamp);
                    }
                    self.last_timestamp = Some(
                        self.last_timestamp
                            .map_or(timestamp, |last| last.max(timestamp)),
                    );
                    if segment == ScanSegment::Tail {
                        self.tail_timestamp = Some(
                            self.tail_timestamp
                                .map_or(timestamp, |last| last.max(timestamp)),
                        );
                    }
                }
            }
            _ => {}
        }
    }

    fn finish(self, metadata: &std::fs::Metadata, truncated: bool) -> DroidSessionInfo {
        let metadata_modified = system_time_modified(metadata);
        let modified = if truncated {
            self.tail_timestamp
                .or(metadata_modified)
                .or(self.last_timestamp)
        } else {
            self.last_timestamp.or(metadata_modified)
        }
        .unwrap_or_else(Utc::now);
        let created = self
            .first_timestamp
            .or_else(|| system_time_created(metadata))
            .unwrap_or(modified);

        DroidSessionInfo {
            path: self.path,
            session_id: self.session_id,
            cwd: self.cwd,
            title: self.title,
            session_title: self.session_title,
            created,
            modified,
            first_message: self.first_message,
            latest_message: self.latest_message,
        }
    }
}

async fn build_session_info_bounded(
    path: &Path,
    scan_limit_bytes: u64,
) -> io::Result<SessionScanOutcome> {
    let mut file = fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session path is not a regular file",
        ));
    }

    let file_len = metadata.len();
    let scan_limit_bytes = scan_limit_bytes.max(2);
    let truncated = file_len > scan_limit_bytes;
    let mut diagnostics = SessionScanDiagnostics {
        truncated,
        ..SessionScanDiagnostics::default()
    };
    let mut summary = DroidSessionSummary::new(path)?;

    if truncated {
        let head_bytes = scan_limit_bytes / 2;
        let tail_bytes = scan_limit_bytes - head_bytes;
        scan_summary_segment(
            &mut file,
            0,
            head_bytes,
            false,
            ScanSegment::Head,
            &mut summary,
            &mut diagnostics,
        )
        .await?;
        scan_summary_segment(
            &mut file,
            file_len - tail_bytes,
            tail_bytes,
            true,
            ScanSegment::Tail,
            &mut summary,
            &mut diagnostics,
        )
        .await?;
    } else {
        scan_summary_segment(
            &mut file,
            0,
            file_len,
            true,
            ScanSegment::Full,
            &mut summary,
            &mut diagnostics,
        )
        .await?;
    }

    Ok(SessionScanOutcome {
        info: summary.finish(&metadata, truncated),
        diagnostics,
    })
}

async fn scan_summary_segment(
    file: &mut fs::File,
    start: u64,
    length: u64,
    reaches_eof: bool,
    segment: ScanSegment,
    summary: &mut DroidSessionSummary,
    diagnostics: &mut SessionScanDiagnostics,
) -> io::Result<()> {
    if length == 0 {
        return Ok(());
    }

    let starts_at_line_boundary = if start == 0 {
        true
    } else {
        file.seek(SeekFrom::Start(start - 1)).await?;
        let mut previous = [0u8; 1];
        file.read_exact(&mut previous).await?;
        previous[0] == b'\n'
    };
    file.seek(SeekFrom::Start(start)).await?;
    let mut reader = BufReader::new(file.take(length));
    let mut line = Vec::new();

    if !starts_at_line_boundary {
        let bytes = reader.read_until(b'\n', &mut line).await?;
        diagnostics.bytes_read += bytes as u64;
        diagnostics.boundary_fragments += usize::from(bytes > 0);
        line.clear();
    }

    loop {
        let bytes = reader.read_until(b'\n', &mut line).await?;
        if bytes == 0 {
            break;
        }
        diagnostics.bytes_read += bytes as u64;
        let complete_line = line.last() == Some(&b'\n') || reaches_eof;
        if !complete_line {
            diagnostics.boundary_fragments += 1;
            break;
        }
        let trimmed = trim_ascii_whitespace(&line);
        if !trimmed.is_empty() {
            match std::str::from_utf8(trimmed)
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
            {
                Some(value) => summary.observe(&value, segment, diagnostics),
                None => diagnostics.malformed_lines += 1,
            }
        }
        line.clear();
    }
    Ok(())
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn extract_summary_text_content(content: &Value) -> (String, bool) {
    let mut output = String::new();
    let mut truncated = false;
    match content {
        Value::String(text) => {
            append_summary_text(&mut output, text, &mut truncated);
        }
        Value::Array(parts) => {
            for text in parts.iter().filter_map(|part| {
                (part.get("type").and_then(Value::as_str) == Some("text"))
                    .then(|| part.get("text").and_then(Value::as_str))
                    .flatten()
            }) {
                if !output.is_empty() {
                    append_summary_text(&mut output, "\n", &mut truncated);
                }
                append_summary_text(&mut output, text, &mut truncated);
                if truncated {
                    break;
                }
            }
        }
        _ => {}
    }
    (output, truncated)
}

fn append_summary_text(output: &mut String, text: &str, truncated: &mut bool) {
    let remaining = MAX_SESSION_SUMMARY_TEXT_BYTES.saturating_sub(output.len());
    if remaining == 0 {
        *truncated |= !text.is_empty();
        return;
    }
    if text.len() <= remaining {
        output.push_str(text);
        return;
    }
    let mut end = remaining;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    output.push_str(&text[..end]);
    *truncated = true;
}

fn push_turn(
    turns: &mut Vec<p::Turn>,
    current_items: &mut Vec<p::ThreadItem>,
    current_started_at: &mut Option<i64>,
    current_completed_at: &mut Option<i64>,
) {
    if current_items.is_empty() {
        return;
    }
    turns.push(p::Turn {
        id: format!("turn_{}", turns.len()),
        items: std::mem::take(current_items),
        items_view: alleycat_codex_proto::default_items_view(),
        status: p::TurnStatus::Completed,
        error: None,
        started_at: current_started_at.take(),
        completed_at: current_completed_at.take(),
        duration_ms: None,
    });
}

fn content_to_user_inputs(content: &Value) -> Vec<p::UserInput> {
    match content {
        Value::String(text) if !text.is_empty() && !is_system_reminder_text(text) => {
            vec![p::UserInput::Text {
                text: text.clone(),
                text_elements: Vec::new(),
            }]
        }
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| match part.get("type").and_then(Value::as_str) {
                Some("text") => part.get("text").and_then(Value::as_str).and_then(|text| {
                    if text.is_empty() || is_system_reminder_text(text) {
                        None
                    } else {
                        Some(p::UserInput::Text {
                            text: text.to_string(),
                            text_elements: Vec::new(),
                        })
                    }
                }),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn tool_results_from_content(content: &Value) -> Vec<PersistedToolResult> {
    let Some(parts) = content.as_array() else {
        return Vec::new();
    };
    parts
        .iter()
        .filter_map(|part| {
            if part.get("type").and_then(Value::as_str) != Some("tool_result") {
                return None;
            }
            let tool_use_id = part
                .get("tool_use_id")
                .or_else(|| part.get("toolUseId"))
                .and_then(Value::as_str)?
                .to_string();
            Some(PersistedToolResult {
                tool_use_id,
                content: tool_result_content(part.get("content").unwrap_or(&Value::Null)),
                is_error: part
                    .get("is_error")
                    .or_else(|| part.get("isError"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect()
}

fn tool_result_content(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                if let Some(text) = part.as_str() {
                    Some(text)
                } else {
                    part.get("text")
                        .or_else(|| part.get("content"))
                        .and_then(Value::as_str)
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn inputs_are_system_reminders(inputs: &[p::UserInput]) -> bool {
    inputs.iter().all(|input| match input {
        p::UserInput::Text { text, .. } => is_system_reminder_text(text),
        _ => false,
    })
}

fn is_system_reminder_text(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("<system-reminder>")
}

fn display_name(info: &DroidSessionInfo) -> Option<String> {
    clean_title(info.session_title.as_deref())
        .filter(|title| !title.eq_ignore_ascii_case("new session"))
        .or_else(|| clean_title(info.title.as_deref()))
}

fn clean_title(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn first_non_empty<const N: usize>(values: [Option<&str>; N]) -> Option<&str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn parse_iso8601(input: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn parse_exit_code(content: &str) -> Option<i32> {
    let marker = "[Process exited with code ";
    let start = content.find(marker)? + marker.len();
    let rest = &content[start..];
    let end = rest.find(']')?;
    rest[..end].trim().parse().ok()
}

fn system_time_modified(metadata: &std::fs::Metadata) -> Option<DateTime<Utc>> {
    metadata.modified().ok().and_then(system_time_to_datetime)
}

fn system_time_created(metadata: &std::fs::Metadata) -> Option<DateTime<Utc>> {
    metadata.created().ok().and_then(system_time_to_datetime)
}

fn system_time_to_datetime(time: std::time::SystemTime) -> Option<DateTime<Utc>> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| DateTime::<Utc>::from_timestamp_millis(duration.as_millis() as i64))
}

fn source_kind_to_session_source(kind: p::ThreadSourceKind) -> p::SessionSource {
    match kind {
        p::ThreadSourceKind::Cli => p::SessionSource::Cli,
        p::ThreadSourceKind::VsCode => p::SessionSource::VsCode,
        p::ThreadSourceKind::Exec => p::SessionSource::Exec,
        p::ThreadSourceKind::AppServer => p::SessionSource::AppServer,
        _ => p::SessionSource::AppServer,
    }
}

fn expand_tilde(input: &str) -> PathBuf {
    if input == "~" {
        if let Some(home) = directories::UserDirs::new() {
            return home.home_dir().to_path_buf();
        }
    }
    if let Some(rest) = input.strip_prefix("~/")
        && let Some(home) = directories::UserDirs::new()
    {
        return home.home_dir().join(rest);
    }
    PathBuf::from(input)
}

fn encode_cwd(cwd: &Path) -> String {
    let canonical = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
    canonical
        .to_string_lossy()
        .chars()
        .map(|ch| if ch == '/' || ch == '\\' { '-' } else { ch })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn lists_factory_session_from_project_dir() {
        let dir = TempDir::new().unwrap();
        let project_dir = dir.path().join("-Users-test-work");
        std::fs::create_dir_all(&project_dir).unwrap();
        let session_path = project_dir.join("abc.jsonl");
        let mut file = std::fs::File::create(&session_path).unwrap();
        writeln!(
            file,
            "{}",
            r#"{"type":"session_start","id":"abc","title":"hi","sessionTitle":"Greeting","cwd":"/Users/test/work"}"#
        )
        .unwrap();
        writeln!(
            file,
            "{}",
            r#"{"type":"message","id":"u1","timestamp":"2026-05-09T22:52:02.922Z","message":{"role":"user","content":[{"type":"text","text":"hello droid"}]}}"#
        )
        .unwrap();
        writeln!(
            file,
            "{}",
            r#"{"type":"message","id":"a1","timestamp":"2026-05-09T22:52:05.000Z","message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}"#
        )
        .unwrap();

        let sessions = list_all_from_root(dir.path()).await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "abc");
        assert_eq!(sessions[0].cwd, "/Users/test/work");
        assert_eq!(sessions[0].first_message, "hello droid");

        let entry = entry_from_droid(&sessions[0]);
        assert_eq!(entry.thread_id, "abc");
        assert_eq!(entry.name.as_deref(), Some("Greeting"));
        assert_eq!(entry.preview, "hello droid");
    }

    #[tokio::test]
    async fn bounded_scan_preserves_header_first_latest_and_timestamps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("large.jsonl");
        let scan_limit = 1024u64;
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            r#"{{"type":"session_start","id":"canonical-id","title":"Header title","sessionTitle":"Session title","cwd":"/work"}}"#
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"type":"message","timestamp":"2026-01-01T00:00:01Z","message":{{"role":"user","content":"first request"}}}}"#
        )
        .unwrap();
        file.write_all(&vec![b'x'; scan_limit as usize * 4])
            .unwrap();
        file.write_all(b"\n").unwrap();
        writeln!(
            file,
            r#"{{"type":"message","timestamp":"2026-01-01T00:00:09Z","message":{{"role":"assistant","content":"latest response"}}}}"#
        )
        .unwrap();
        drop(file);

        let outcome = build_session_info_bounded(&path, scan_limit).await.unwrap();
        assert!(outcome.diagnostics.truncated);
        assert!(outcome.diagnostics.bytes_read <= scan_limit);
        assert!(outcome.diagnostics.boundary_fragments > 0);
        assert_eq!(outcome.info.session_id, "canonical-id");
        assert_eq!(outcome.info.cwd, "/work");
        assert_eq!(outcome.info.title.as_deref(), Some("Header title"));
        assert_eq!(outcome.info.session_title.as_deref(), Some("Session title"));
        assert_eq!(outcome.info.first_message, "first request");
        assert_eq!(outcome.info.latest_message, "latest response");
        assert_eq!(
            outcome.info.created,
            parse_iso8601("2026-01-01T00:00:01Z").unwrap()
        );
        assert_eq!(
            outcome.info.modified,
            parse_iso8601("2026-01-01T00:00:09Z").unwrap()
        );
    }

    #[tokio::test]
    async fn bounded_scan_falls_back_to_file_mtime_when_tail_has_no_timestamp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mtime.jsonl");
        let scan_limit = 512u64;
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(
            file,
            r#"{{"type":"session_start","id":"mtime","cwd":"/work"}}"#
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"type":"message","timestamp":"2020-01-01T00:00:01Z","message":{{"role":"user","content":"first"}}}}"#
        )
        .unwrap();
        file.write_all(&vec![b'x'; scan_limit as usize * 4])
            .unwrap();
        drop(file);
        let expected_mtime = system_time_modified(&std::fs::metadata(&path).unwrap()).unwrap();

        let outcome = build_session_info_bounded(&path, scan_limit).await.unwrap();
        assert!(outcome.diagnostics.truncated);
        assert_eq!(outcome.info.first_message, "first");
        assert_eq!(outcome.info.latest_message, "first");
        assert_eq!(outcome.info.modified, expected_mtime);
    }

    #[tokio::test]
    async fn bounded_scan_handles_one_huge_malformed_line_without_amplification() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fallback-id.jsonl");
        let scan_limit = 512u64;
        std::fs::write(&path, vec![b'x'; 8 * 1024 * 1024]).unwrap();

        let outcome = build_session_info_bounded(&path, scan_limit).await.unwrap();
        assert!(outcome.diagnostics.truncated);
        assert!(outcome.diagnostics.bytes_read <= scan_limit);
        assert!(outcome.diagnostics.boundary_fragments > 0);
        assert_eq!(outcome.info.session_id, "fallback-id");
        assert_eq!(outcome.info.cwd, "");
        assert_eq!(outcome.info.first_message, "");
        assert_eq!(outcome.info.latest_message, "");
    }

    #[tokio::test]
    async fn malformed_line_is_reported_without_poisoning_valid_summary() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("malformed.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "not-json").unwrap();
        writeln!(
            file,
            r#"{{"type":"session_start","id":"valid","cwd":"/work"}}"#
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"type":"message","timestamp":"2026-01-01T00:00:01Z","message":{{"role":"user","content":"survives"}}}}"#
        )
        .unwrap();
        drop(file);

        let outcome = build_session_info_bounded(&path, 4096).await.unwrap();
        assert!(!outcome.diagnostics.truncated);
        assert_eq!(outcome.diagnostics.malformed_lines, 1);
        assert_eq!(outcome.info.session_id, "valid");
        assert_eq!(outcome.info.first_message, "survives");
        assert_eq!(outcome.info.latest_message, "survives");
    }

    #[test]
    fn transcript_groups_user_and_assistant_into_turn() {
        let text = [
            r#"{"type":"message","id":"ctx","timestamp":"2026-05-09T22:52:01.000Z","message":{"role":"user","content":[{"type":"text","text":"<system-reminder>context</system-reminder>"}]}}"#,
            r#"{"type":"message","id":"u1","timestamp":"2026-05-09T22:52:02.922Z","message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            r#"{"type":"message","id":"a1","timestamp":"2026-05-09T22:52:05.000Z","message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}"#,
        ]
        .join("\n");
        let turns = transcript_text_to_turns(&text);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 2);
        match &turns[0].items[0] {
            p::ThreadItem::UserMessage { content, .. } => match &content[0] {
                p::UserInput::Text { text, .. } => assert_eq!(text, "hello"),
                other => panic!("expected user text, got {other:?}"),
            },
            other => panic!("expected user message, got {other:?}"),
        }
        match &turns[0].items[1] {
            p::ThreadItem::AgentMessage { text, .. } => assert_eq!(text, "hi"),
            other => panic!("expected agent message, got {other:?}"),
        }
    }

    #[test]
    fn transcript_maps_persisted_tool_calls_and_results() {
        let text = [
            r#"{"type":"session_start","id":"abc","cwd":"/tmp/work"}"#,
            r#"{"type":"message","id":"u1","timestamp":"2026-05-09T22:52:02.000Z","message":{"role":"user","content":[{"type":"text","text":"inspect this"}]}}"#,
            r#"{"type":"message","id":"a1","timestamp":"2026-05-09T22:52:03.000Z","message":{"role":"assistant","content":[{"type":"text","text":"I'll check."},{"type":"tool_use","id":"tool_read","name":"Read","input":{"file_path":"/tmp/work/main.rs"}},{"type":"tool_use","id":"tool_exec","name":"Execute","input":{"command":"cargo test"}}]}}"#,
            r#"{"type":"message","id":"u2","timestamp":"2026-05-09T22:52:04.000Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_read","content":"fn main() {}"},{"type":"tool_result","tool_use_id":"tool_exec","content":"ok\n[Process exited with code 0]"}]}}"#,
            r#"{"type":"message","id":"a2","timestamp":"2026-05-09T22:52:05.000Z","message":{"role":"assistant","content":[{"type":"text","text":"Done."}]}}"#,
        ]
        .join("\n");

        let turns = transcript_text_to_turns(&text);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 5);

        match &turns[0].items[2] {
            p::ThreadItem::CommandExecution {
                id,
                command,
                cwd,
                status,
                command_actions,
                aggregated_output,
                ..
            } => {
                assert_eq!(id, "tool_read");
                assert_eq!(command, "read /tmp/work/main.rs");
                assert_eq!(cwd, "/tmp/work");
                assert_eq!(*status, p::CommandExecutionStatus::Completed);
                assert_eq!(aggregated_output.as_deref(), Some("fn main() {}"));
                assert_eq!(command_actions.len(), 1);
                assert_eq!(command_actions[0]["type"], "read");
                assert_eq!(command_actions[0]["name"], "main.rs");
                assert_eq!(command_actions[0]["path"], "/tmp/work/main.rs");
            }
            other => panic!("expected read command execution, got {other:?}"),
        }

        match &turns[0].items[3] {
            p::ThreadItem::CommandExecution {
                id,
                command,
                cwd,
                status,
                aggregated_output,
                exit_code,
                ..
            } => {
                assert_eq!(id, "tool_exec");
                assert_eq!(command, "cargo test");
                assert_eq!(cwd, "/tmp/work");
                assert_eq!(*status, p::CommandExecutionStatus::Completed);
                assert_eq!(
                    aggregated_output.as_deref(),
                    Some("ok\n[Process exited with code 0]")
                );
                assert_eq!(*exit_code, Some(0));
            }
            other => panic!("expected command execution, got {other:?}"),
        }

        match &turns[0].items[4] {
            p::ThreadItem::AgentMessage { text, .. } => assert_eq!(text, "Done."),
            other => panic!("expected final agent message, got {other:?}"),
        }
    }

    #[test]
    fn transcript_maps_factory_exploration_tools_to_command_actions() {
        let text = [
            r#"{"type":"session_start","id":"abc","cwd":"/tmp/work"}"#,
            r#"{"type":"message","id":"u1","timestamp":"2026-05-09T22:52:02.000Z","message":{"role":"user","content":[{"type":"text","text":"look around"}]}}"#,
            r#"{"type":"message","id":"a1","timestamp":"2026-05-09T22:52:03.000Z","message":{"role":"assistant","content":[{"type":"tool_use","id":"tool_ls","name":"LS","input":{"directory_path":"/tmp/work/src"}},{"type":"tool_use","id":"tool_grep","name":"Grep","input":{"pattern":"Droid","path":"/tmp/work/src"}},{"type":"tool_use","id":"tool_glob","name":"Glob","input":{"pattern":"*.rs","folder":"/tmp/work/src"}}]}}"#,
            r#"{"type":"message","id":"u2","timestamp":"2026-05-09T22:52:04.000Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_ls","content":"main.rs"},{"type":"tool_result","tool_use_id":"tool_grep","content":"src/main.rs: Droid"},{"type":"tool_result","tool_use_id":"tool_glob","content":"src/main.rs"}]}}"#,
        ]
        .join("\n");

        let turns = transcript_text_to_turns(&text);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].items.len(), 4);

        let expected = [
            (
                "tool_ls",
                "ls /tmp/work/src",
                "listFiles",
                None,
                Some("/tmp/work/src"),
            ),
            (
                "tool_grep",
                "grep Droid /tmp/work/src",
                "search",
                Some("Droid"),
                Some("/tmp/work/src"),
            ),
            (
                "tool_glob",
                "glob *.rs /tmp/work/src",
                "search",
                Some("*.rs"),
                Some("/tmp/work/src"),
            ),
        ];
        for (
            offset,
            (expected_id, expected_command, expected_type, expected_query, expected_path),
        ) in expected.into_iter().enumerate()
        {
            match &turns[0].items[offset + 1] {
                p::ThreadItem::CommandExecution {
                    id,
                    command,
                    command_actions,
                    status,
                    ..
                } => {
                    assert_eq!(id, expected_id);
                    assert_eq!(command, expected_command);
                    assert_eq!(*status, p::CommandExecutionStatus::Completed);
                    assert_eq!(command_actions.len(), 1);
                    assert_eq!(command_actions[0]["type"], expected_type);
                    assert_eq!(
                        command_actions[0].get("query").and_then(Value::as_str),
                        expected_query
                    );
                    assert_eq!(
                        command_actions[0].get("path").and_then(Value::as_str),
                        expected_path
                    );
                }
                other => panic!("expected exploration command execution, got {other:?}"),
            }
        }
    }
}
