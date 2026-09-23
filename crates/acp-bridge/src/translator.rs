//! Translate ACP `session/update` notifications into codex `ThreadItem` JSON.
//!
//! ACP's prompt-turn protocol (https://agentclientprotocol.com/protocol/prompt-turn)
//! streams the agent's work as `session/update` notifications. The
//! variants we care about:
//!
//! * `user_message_chunk` / `agent_message_chunk` / `agent_thought_chunk`
//!   — a stream of text chunks. Adjacent chunks of the same kind belong
//!   to one logical item; a different kind (or a tool_call interruption)
//!   ends the run.
//! * `tool_call` — announces a new tool call. Optional `content`,
//!   `locations`, `rawInput`, `rawOutput` fields are added later via
//!   `tool_call_update`.
//! * `tool_call_update` — merges optional fields into an existing
//!   `tool_call` by `toolCallId`. The spec says *all fields except
//!   toolCallId are optional in updates*, and a call MAY go straight
//!   `pending → completed` without an intermediate `in_progress`.
//! * `plan` — replaces the agent's plan; we track the latest one and
//!   surface it as a `turn/plan/updated` notification alongside the
//!   item stream.
//!
//! The translator coalesces text chunks into one `agentMessage` /
//! `reasoning` / `userMessage` ThreadItem per contiguous run, and
//! maintains an in-flight map of tool calls so updates merge into the
//! same item id (`acp-tool-{toolCallId}`). Items are emitted in stream
//! order so the UI renders them as they appeared in the conversation.

use std::collections::HashMap;

use serde_json::{Value, json};

/// One unit of work the iOS UI should render.
#[derive(Debug, Clone)]
pub struct TranslatedStream {
    /// `ThreadItem` JSON values in chronological order.
    pub items: Vec<Value>,
    /// Most-recent ACP `plan` entries (raw — caller maps to codex
    /// `TurnPlanStep` shape). `None` if the agent didn't emit a plan.
    pub plan_entries: Option<Vec<Value>>,
    /// Most-recent ACP `available_commands_update` payload. `None`
    /// if the agent didn't emit one during this segment.
    pub available_commands: Option<Vec<Value>>,
}

/// Stateful translator. Build with `SessionUpdateTranslator::new()`,
/// feed every drained `session/update` JSON via [`ingest`], then call
/// [`finish`] to recover the ordered item list and any captured plan.
pub struct SessionUpdateTranslator {
    items: Vec<Value>,
    /// Map item id → position in `items`, used for in-place mutation
    /// when a `tool_call_update` lands.
    item_index: HashMap<String, usize>,
    tool_calls: HashMap<String, ToolCallState>,
    text: TextChunkAccumulator,
    plan_entries: Option<Vec<Value>>,
    available_commands: Option<Vec<Value>>,
    /// Monotonic counter used to mint unique ids for items that don't
    /// otherwise have one (text chunks, fabricated user messages).
    seq: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextKind {
    AgentMessage,
    Reasoning,
}

#[derive(Default)]
struct TextChunkAccumulator {
    kind: Option<TextKind>,
    buf: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ToolCallState {
    pub item_id: String,
    /// ACP kind (`read`, `edit`, `delete`, `move`, `search`, `execute`,
    /// `think`, `fetch`, `other`). Defaults to `other` if absent.
    pub kind: String,
    pub title: String,
    pub status: String,
    pub content: Vec<Value>,
    pub locations: Vec<Value>,
    pub raw_input: Value,
    pub raw_output: Option<Value>,
    /// Wall-clock timestamps so we can compute `durationMs` once the
    /// call reaches a terminal status.
    pub started_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

impl ToolCallState {
    /// Build the in-flight state from an ACP `tool_call` announcement.
    pub(crate) fn from_announce(update: &Value) -> Self {
        let id = update
            .get("toolCallId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let now_ms = chrono::Utc::now().timestamp_millis();
        Self {
            item_id: format!("acp-tool-{id}"),
            kind: update
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("other")
                .to_string(),
            title: update
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            status: update
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("pending")
                .to_string(),
            content: update
                .get("content")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            locations: update
                .get("locations")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            raw_input: update.get("rawInput").cloned().unwrap_or(Value::Null),
            raw_output: update.get("rawOutput").cloned(),
            started_at_ms: now_ms,
            completed_at_ms: None,
        }
    }

    /// Apply an ACP `tool_call_update`, merging only set fields. Per
    /// ACP spec, all fields except toolCallId are optional in updates.
    pub(crate) fn merge_update(&mut self, update: &Value) {
        if let Some(kind) = update.get("kind").and_then(|v| v.as_str()) {
            self.kind = kind.to_string();
        }
        if let Some(title) = update.get("title").and_then(|v| v.as_str()) {
            self.title = title.to_string();
        }
        if let Some(status) = update.get("status").and_then(|v| v.as_str()) {
            self.status = status.to_string();
            if matches!(status, "completed" | "failed") && self.completed_at_ms.is_none() {
                self.completed_at_ms = Some(chrono::Utc::now().timestamp_millis());
            }
        }
        if let Some(arr) = update.get("content").and_then(|v| v.as_array()) {
            self.content.extend(arr.iter().cloned());
        }
        if let Some(arr) = update.get("locations").and_then(|v| v.as_array()) {
            self.locations.extend(arr.iter().cloned());
        }
        if let Some(raw_input) = update.get("rawInput") {
            self.raw_input = raw_input.clone();
        }
        if let Some(raw_output) = update.get("rawOutput") {
            self.raw_output = Some(raw_output.clone());
        }
    }
}

/// Alias kept for the streaming module's import.
pub(crate) type ToolCallStatePublic = ToolCallState;
pub(crate) fn render_tool_call_public(state: &ToolCallState) -> Value {
    render_tool_call(state)
}

impl Default for SessionUpdateTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionUpdateTranslator {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            item_index: HashMap::new(),
            tool_calls: HashMap::new(),
            text: TextChunkAccumulator::default(),
            plan_entries: None,
            available_commands: None,
            seq: 0,
        }
    }

    /// Process one drained `session/update` notification frame.
    pub fn ingest(&mut self, note: &Value) {
        // We expect `{method: "session/update", params: {sessionId, update: {sessionUpdate: ...}}}`.
        // Anything else (e.g. devin's _cognition.ai/* notifications) is dropped.
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
            "user_message_chunk" => {
                self.flush_text();
                // For user messages we keep things simple: concatenate
                // any text/resource/resource_link to a single text item.
                // Embedded images on the user side are rare and codex's
                // UserInput::Image already covers iOS-originated input
                // (see `handle_turn_start`), so we don't bother emitting
                // a separate imageView here.
                let text = match classify_content_block(update.get("content")) {
                    ChunkContribution::Text(t) => t,
                    _ => String::new(),
                };
                if !text.is_empty() {
                    let id = self.next_id("acp-user");
                    let item = json!({
                        "id": id,
                        "type": "userMessage",
                        "content": [{"type": "text", "text": text}],
                    });
                    self.push_item(item);
                }
            }
            "agent_message_chunk" => {
                self.append_text(TextKind::AgentMessage, update.get("content"));
            }
            "agent_thought_chunk" => {
                self.append_text(TextKind::Reasoning, update.get("content"));
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
                // ACP broadcasts the agent's slash-command list per
                // session. Capture the latest snapshot so the caller can
                // stash it for `skills/list` to serve.
                if let Some(cmds) = update.get("availableCommands").and_then(|v| v.as_array()) {
                    self.available_commands = Some(cmds.clone());
                }
            }
            // current_mode_update, usage_update, and other vendor-specific
            // kinds: drop silently. Anything not on the codex side gets
            // lost rather than crashing the turn.
            _ => {}
        }
    }

    /// Drain any pending text chunk and return the final item list.
    pub fn finish(mut self) -> TranslatedStream {
        self.flush_text();
        TranslatedStream {
            items: self.items,
            plan_entries: self.plan_entries,
            available_commands: self.available_commands,
        }
    }

    fn append_text(&mut self, kind: TextKind, content: Option<&Value>) {
        match classify_content_block(content) {
            ChunkContribution::None => {}
            ChunkContribution::Text(text) => {
                if self.text.kind != Some(kind) {
                    self.flush_text();
                    self.text.kind = Some(kind);
                }
                if !text.is_empty() {
                    self.text.buf.push_str(&text);
                }
            }
            ChunkContribution::Item(item) => {
                // Non-text content (image, etc.) — break the current
                // text run so the item sits in stream order, then push.
                self.flush_text();
                self.push_item(item);
            }
        }
    }

    fn flush_text(&mut self) {
        let kind = match self.text.kind.take() {
            Some(k) => k,
            None => return,
        };
        let buf = std::mem::take(&mut self.text.buf);
        if buf.is_empty() {
            return;
        }
        match kind {
            TextKind::AgentMessage => {
                let id = self.next_id("acp-agent");
                let item = json!({
                    "id": id,
                    "type": "agentMessage",
                    "text": buf,
                    "phase": null,
                    "memoryCitation": null,
                });
                self.push_item(item);
            }
            TextKind::Reasoning => {
                let id = self.next_id("acp-reasoning");
                let item = json!({
                    "id": id,
                    "type": "reasoning",
                    "summary": [],
                    "content": [buf],
                });
                self.push_item(item);
            }
        }
    }

    fn handle_tool_call(&mut self, update: &Value) {
        let id = match update.get("toolCallId").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };
        let state = ToolCallState::from_announce(update);
        let rendered = render_tool_call(&state);
        self.push_item(rendered);
        self.tool_calls.insert(id, state);
    }

    fn handle_tool_call_update(&mut self, update: &Value) {
        let id = match update.get("toolCallId").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => return,
        };
        if !self.tool_calls.contains_key(&id) {
            self.handle_tool_call(update);
        }
        let state = match self.tool_calls.get_mut(&id) {
            Some(s) => s,
            None => return,
        };
        state.merge_update(update);
        let rendered = render_tool_call(state);
        let item_id = state.item_id.clone();
        self.replace_item(&item_id, rendered);
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
            // Wasn't found — append. Defensive, shouldn't happen because
            // we always insert during tool_call announce.
            self.push_item(new_item);
        }
    }

    fn next_id(&mut self, prefix: &str) -> String {
        let id = format!("{prefix}-{}", self.seq);
        self.seq += 1;
        id
    }
}

/// How a single ACP `ContentBlock` should fold into the translated
/// stream: as text (coalesce into the current agent/reasoning run), as a
/// standalone item (e.g. an image), or skipped.
enum ChunkContribution {
    None,
    Text(String),
    Item(Value),
}

/// Classify an ACP ContentBlock (`text` | `image` | `audio` | `resource`
/// | `resource_link`) into a ChunkContribution.
///
/// * `text` → text to coalesce.
/// * `image` → write base64 payload to a temp file and emit a codex
///   `ImageView` item. Skipped if no `data` or `uri` is present.
/// * `audio` → no codex equivalent; surface a placeholder text marker so
///   the user at least sees that audio was returned.
/// * `resource` → inline embedded text if present; otherwise URI.
/// * `resource_link` → URI as plain text.
fn classify_content_block(content: Option<&Value>) -> ChunkContribution {
    let v = match content {
        Some(v) => v,
        None => return ChunkContribution::None,
    };
    if let Some(s) = v.as_str() {
        return ChunkContribution::Text(s.to_string());
    }

    let block_type = v.get("type").and_then(|v| v.as_str()).unwrap_or("text");
    match block_type {
        "text" => v
            .get("text")
            .and_then(|t| t.as_str())
            .map(|t| ChunkContribution::Text(t.to_string()))
            .unwrap_or(ChunkContribution::None),
        "image" => {
            // Prefer a URI-backed image; otherwise decode base64 data to a
            // temp file so codex `ImageView` (which only takes `path`) has
            // something to load.
            if let Some(uri) = v.get("uri").and_then(|u| u.as_str()) {
                return ChunkContribution::Item(json!({
                    "id": new_item_id("acp-image"),
                    "type": "imageView",
                    "path": uri,
                }));
            }
            let data = v.get("data").and_then(|d| d.as_str()).unwrap_or("");
            let mime = v
                .get("mimeType")
                .and_then(|m| m.as_str())
                .unwrap_or("image/png");
            if data.is_empty() {
                return ChunkContribution::None;
            }
            match write_image_data(data, mime) {
                Ok(path) => ChunkContribution::Item(json!({
                    "id": new_item_id("acp-image"),
                    "type": "imageView",
                    "path": path,
                })),
                Err(err) => {
                    tracing::warn!(?err, "failed to materialize ACP image content; dropping");
                    ChunkContribution::None
                }
            }
        }
        "audio" => {
            // codex has no audio ThreadItem variant. Surface a textual
            // marker so the data isn't completely silent.
            let mime = v
                .get("mimeType")
                .and_then(|m| m.as_str())
                .unwrap_or("audio");
            ChunkContribution::Text(format!("[audio: {mime}]"))
        }
        "resource" => {
            if let Some(t) = v
                .get("resource")
                .and_then(|r| r.get("text"))
                .and_then(|t| t.as_str())
            {
                return ChunkContribution::Text(t.to_string());
            }
            if let Some(uri) = v
                .get("resource")
                .and_then(|r| r.get("uri"))
                .and_then(|u| u.as_str())
            {
                return ChunkContribution::Text(uri.to_string());
            }
            ChunkContribution::None
        }
        "resource_link" => v
            .get("uri")
            .and_then(|u| u.as_str())
            .map(|u| ChunkContribution::Text(u.to_string()))
            .unwrap_or(ChunkContribution::None),
        _ => {
            // Unknown type — try text fields as a fallback.
            if let Some(t) = v.get("text").and_then(|t| t.as_str()) {
                return ChunkContribution::Text(t.to_string());
            }
            ChunkContribution::None
        }
    }
}

/// Generate a UUIDv7-based item id with the given prefix. Used for
/// inline content-block items (images) where we don't have an upstream
/// id to anchor to.
fn new_item_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

/// Decode a base64-encoded image payload to a temp file under
/// `<tmp>/alleycat-acp-images/` and return the path. The file persists
/// for the lifetime of the daemon (no cleanup) — adequate for a v1
/// since iOS only needs to load it while the conversation is open.
fn write_image_data(data_b64: &str, mime: &str) -> std::io::Result<String> {
    use base64::Engine;
    use std::io::Write;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data_b64.as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let dir = std::env::temp_dir().join("alleycat-acp-images");
    std::fs::create_dir_all(&dir)?;
    let ext = match mime.split('/').nth(1).unwrap_or("png") {
        "jpeg" => "jpg",
        other => other,
    };
    let name = format!("{}.{}", uuid::Uuid::now_v7(), ext);
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path)?;
    f.write_all(&bytes)?;
    Ok(path.to_string_lossy().into_owned())
}

/// Map an ACP tool call (current state) into a codex `ThreadItem` JSON.
///
/// `kind` mapping:
/// * `execute` → `commandExecution` (the most useful one for terminal-tool agents)
/// * any update with at least one `{type: "diff"}` content block → `fileChange`
/// * anything else → `dynamicToolCall` (a generic catch-all the codex
///   schema accepts)
fn render_tool_call(state: &ToolCallState) -> Value {
    let codex_status = map_status(&state.status);
    let duration_ms = state
        .completed_at_ms
        .map(|end| end.saturating_sub(state.started_at_ms));

    // Detect diff blocks for fileChange routing.
    let diff_blocks: Vec<&Value> = state
        .content
        .iter()
        .filter(|block| block.get("type").and_then(|v| v.as_str()) == Some("diff"))
        .collect();

    if state.kind == "edit" || !diff_blocks.is_empty() {
        let changes: Vec<Value> = diff_blocks
            .iter()
            .map(|block| {
                let path = block.get("path").and_then(|v| v.as_str()).unwrap_or("");
                let old_text = block.get("oldText").and_then(|v| v.as_str()).unwrap_or("");
                let new_text = block.get("newText").and_then(|v| v.as_str()).unwrap_or("");
                let kind = if old_text.is_empty() {
                    json!({"type": "add"})
                } else if new_text.is_empty() {
                    json!({"type": "delete"})
                } else {
                    json!({"type": "update"})
                };
                let diff = build_unified_diff(path, old_text, new_text);
                json!({"path": path, "kind": kind, "diff": diff})
            })
            .collect();
        let patch_status = match codex_status {
            "completed" => "completed",
            "failed" => "failed",
            _ => "inProgress",
        };
        return json!({
            "id": state.item_id,
            "type": "fileChange",
            "changes": changes,
            "status": patch_status,
        });
    }

    if state.kind == "execute" {
        let command = state
            .raw_input
            .get("command")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| extract_command_from_content(&state.content))
            .unwrap_or_else(|| state.title.clone());
        let cwd = state
            .raw_input
            .get("cwd")
            .and_then(|v| v.as_str())
            .or_else(|| {
                state
                    .locations
                    .first()
                    .and_then(|loc| loc.get("path").and_then(|v| v.as_str()))
            })
            .unwrap_or("")
            .to_string();
        let aggregated_output = aggregate_text_output(&state.content);
        let exit_code = state
            .raw_output
            .as_ref()
            .and_then(|out| out.get("exitCode").or_else(|| out.get("exit_code")))
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let exec_status = match codex_status {
            "completed" => "completed",
            "failed" => "failed",
            _ => "inProgress",
        };
        let mut item = json!({
            "id": state.item_id,
            "type": "commandExecution",
            "command": command,
            "cwd": cwd,
            "source": "agent",
            "status": exec_status,
            "commandActions": [],
        });
        if let Some(out) = aggregated_output {
            item["aggregatedOutput"] = Value::String(out);
        }
        if let Some(code) = exit_code {
            item["exitCode"] = json!(code);
        }
        if let Some(ms) = duration_ms {
            item["durationMs"] = json!(ms);
        }
        return item;
    }

    // dynamicToolCall — generic fallback.
    let dyn_status = match codex_status {
        "completed" => "completed",
        "failed" => "failed",
        _ => "inProgress",
    };
    let tool = if !state.title.is_empty() {
        state.title.clone()
    } else {
        state.kind.clone()
    };
    let success = match dyn_status {
        "completed" => Some(true),
        "failed" => Some(false),
        _ => None,
    };
    let mut item = json!({
        "id": state.item_id,
        "type": "dynamicToolCall",
        "tool": tool,
        "arguments": state.raw_input,
        "status": dyn_status,
        "contentItems": acp_content_to_codex_items(&state.content),
    });
    if let Some(s) = success {
        item["success"] = Value::Bool(s);
    }
    if let Some(ms) = duration_ms {
        item["durationMs"] = json!(ms);
    }
    item
}

/// codex `CommandExecutionStatus` is `inProgress | completed | failed | declined`
/// — ACP also has `pending`. Collapse pending into inProgress.
fn map_status(acp_status: &str) -> &'static str {
    match acp_status {
        "completed" => "completed",
        "failed" => "failed",
        _ => "inProgress",
    }
}

fn extract_command_from_content(content: &[Value]) -> Option<String> {
    for block in content {
        if let Some(inner) = block.get("content") {
            if let Some(text) = inner.get("text").and_then(|v| v.as_str())
                && !text.is_empty()
            {
                return Some(text.to_string());
            }
            if let Some(resource) = inner.get("resource")
                && let Some(text) = resource.get("text").and_then(|v| v.as_str())
                && !text.is_empty()
            {
                return Some(text.to_string());
            }
        }
    }
    None
}

/// Translate ACP tool-call content blocks (`{"type":"content","content":{...}}`,
/// `{"type":"diff",...}`, `{"type":"terminal",...}`) into codex
/// `DynamicToolCallOutputContentItem`s. The codex schema only accepts
/// `inputText` and `inputImage` variants, so anything else collapses to
/// `inputText` with a best-effort textual rendering. Sending the raw ACP
/// shape through made iOS fail thread/resume deserialization with
/// `unknown variant 'content', expected 'inputText' or 'inputImage'`.
fn acp_content_to_codex_items(content: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for block in content {
        let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match block_type {
            "content" => {
                let Some(inner) = block.get("content") else {
                    continue;
                };
                let inner_type = inner.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match inner_type {
                    "text" => {
                        if let Some(text) = inner.get("text").and_then(|v| v.as_str()) {
                            out.push(json!({"type": "inputText", "text": text}));
                        }
                    }
                    "image" => {
                        if let Some(url) = inner.get("uri").and_then(|v| v.as_str()) {
                            out.push(json!({"type": "inputImage", "imageUrl": url}));
                        } else if let Some(data) = inner.get("data").and_then(|v| v.as_str()) {
                            let mime = inner
                                .get("mimeType")
                                .and_then(|v| v.as_str())
                                .unwrap_or("image/png");
                            out.push(json!({
                                "type": "inputImage",
                                "imageUrl": format!("data:{mime};base64,{data}"),
                            }));
                        }
                    }
                    "resource" => {
                        if let Some(text) = inner
                            .get("resource")
                            .and_then(|r| r.get("text"))
                            .and_then(|v| v.as_str())
                        {
                            out.push(json!({"type": "inputText", "text": text}));
                        }
                    }
                    _ => {}
                }
            }
            "terminal" => {
                if let Some(t) = block.get("terminalId").and_then(|v| v.as_str()) {
                    out.push(json!({"type": "inputText", "text": format!("[terminal:{t}]")}));
                }
            }
            _ => {}
        }
    }
    out
}

fn aggregate_text_output(content: &[Value]) -> Option<String> {
    let mut buf = String::new();
    for block in content {
        if block.get("type").and_then(|v| v.as_str()) == Some("content") {
            if let Some(inner) = block.get("content")
                && let Some(text) = inner.get("text").and_then(|v| v.as_str())
            {
                buf.push_str(text);
            }
        } else if block.get("type").and_then(|v| v.as_str()) == Some("terminal")
            && let Some(t) = block.get("terminalId").and_then(|v| v.as_str())
        {
            buf.push_str(&format!("[terminal:{t}]"));
        }
    }
    if buf.is_empty() { None } else { Some(buf) }
}

/// Build a minimal unified diff. We don't try to be fancy — this is for
/// display only, not for re-application. Single-hunk, no headers.
fn build_unified_diff(path: &str, old_text: &str, new_text: &str) -> String {
    let mut out = String::new();
    out.push_str(&format!("--- a/{path}\n+++ b/{path}\n"));
    for line in old_text.lines() {
        out.push_str(&format!("-{line}\n"));
    }
    for line in new_text.lines() {
        out.push_str(&format!("+{line}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(kind: &str, content_text: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s1",
                "update": {
                    "sessionUpdate": kind,
                    "content": {"type": "text", "text": content_text},
                }
            }
        })
    }

    fn tool_call_note(id: &str, kind: &str, title: &str, command: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s1",
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": id,
                    "kind": kind,
                    "title": title,
                    "status": "pending",
                    "rawInput": {"command": command},
                    "content": []
                }
            }
        })
    }

    fn tool_update_note(id: &str, status: &str, output_text: &str, exit_code: i32) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s1",
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": id,
                    "status": status,
                    "content": [{"type": "content", "content": {"type": "text", "text": output_text}}],
                    "rawOutput": {"exitCode": exit_code}
                }
            }
        })
    }

    #[test]
    fn coalesces_agent_message_chunks() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&note("agent_message_chunk", "Hel"));
        t.ingest(&note("agent_message_chunk", "lo"));
        t.ingest(&note("agent_message_chunk", " world"));
        let out = t.finish();
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.items[0]["type"], "agentMessage");
        assert_eq!(out.items[0]["text"], "Hello world");
    }

    #[test]
    fn separates_reasoning_from_agent_message() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&note("agent_thought_chunk", "thinking..."));
        t.ingest(&note("agent_message_chunk", "Hello"));
        t.ingest(&note("agent_thought_chunk", "more thinking"));
        t.ingest(&note("agent_message_chunk", " there"));
        let out = t.finish();
        assert_eq!(out.items.len(), 4);
        assert_eq!(out.items[0]["type"], "reasoning");
        assert_eq!(out.items[1]["type"], "agentMessage");
        assert_eq!(out.items[2]["type"], "reasoning");
        assert_eq!(out.items[3]["type"], "agentMessage");
    }

    #[test]
    fn tool_call_then_update_merges_into_one_item() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&tool_call_note("call_001", "execute", "Ran command", "pwd"));
        t.ingest(&tool_update_note("call_001", "completed", "/Users/x\n", 0));
        let out = t.finish();
        assert_eq!(out.items.len(), 1);
        let item = &out.items[0];
        assert_eq!(item["type"], "commandExecution");
        assert_eq!(item["id"], "acp-tool-call_001");
        assert_eq!(item["command"], "pwd");
        assert_eq!(item["status"], "completed");
        assert_eq!(item["aggregatedOutput"], "/Users/x\n");
        assert_eq!(item["exitCode"], 0);
    }

    #[test]
    fn tool_call_kind_search_falls_back_to_dynamic() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": "search_1",
                "kind": "search",
                "title": "Find files matching `*`",
                "status": "completed",
                "rawInput": {"query": "*"},
            }}
        }));
        let out = t.finish();
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.items[0]["type"], "dynamicToolCall");
        assert_eq!(out.items[0]["tool"], "Find files matching `*`");
        assert_eq!(out.items[0]["status"], "completed");
    }

    #[test]
    fn user_message_chunk_emits_user_item() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&note("user_message_chunk", "hello"));
        t.ingest(&note("agent_message_chunk", "hi"));
        let out = t.finish();
        assert_eq!(out.items.len(), 2);
        assert_eq!(out.items[0]["type"], "userMessage");
        assert_eq!(out.items[0]["content"][0]["text"], "hello");
        assert_eq!(out.items[1]["type"], "agentMessage");
    }

    #[test]
    fn plan_captured_separately() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&json!({
            "method": "session/update",
            "params": {"sessionId": "s", "update": {
                "sessionUpdate": "plan",
                "entries": [
                    {"content": "Read the file", "status": "completed", "priority": "high"},
                    {"content": "Run tests", "status": "pending", "priority": "high"}
                ]
            }}
        }));
        let out = t.finish();
        assert_eq!(out.items.len(), 0);
        let entries = out.plan_entries.expect("plan entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["content"], "Read the file");
    }

    #[test]
    fn unknown_session_update_is_dropped() {
        let mut t = SessionUpdateTranslator::new();
        t.ingest(&note("usage_update", "ignored"));
        t.ingest(&note("current_mode_update", "ignored"));
        t.ingest(&note("agent_message_chunk", "kept"));
        let out = t.finish();
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.items[0]["type"], "agentMessage");
        assert_eq!(out.items[0]["text"], "kept");
    }
}
