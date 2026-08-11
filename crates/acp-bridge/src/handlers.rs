//! Handlers for Codex protocol methods.

use std::sync::Arc;

use alleycat_bridge_core::{JsonRpcError, error_codes};
use alleycat_codex_proto as p;
use serde_json::{Value, json};
use tracing::{info, instrument};

use crate::acp_client::AcpClient;
use crate::translate;

/// ACP `session/new` and `session/load` require `cwd` to be an absolute
/// path. Grok enforces this strictly and rejects empty or relative paths
/// with `-32602 Invalid params: Path is not absolute`. When the caller
/// didn't provide one we fall back to `/` so the agent at least accepts
/// the request.
fn coerce_absolute_cwd(cwd: Option<&str>) -> &str {
    match cwd {
        Some(p) if p.starts_with('/') => p,
        _ => "/",
    }
}

/// Handle initialize request.
pub async fn handle_initialize(
    client: &Arc<AcpClient>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let acp_request = translate::codex_to_acp_initialize(&params).map_err(|e| JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!("Failed to translate initialize params: {}", e),
        data: None,
    })?;

    let acp_response = client
        .send_request("initialize", acp_request)
        .await
        .map_err(|e| JsonRpcError {
            code: error_codes::INTERNAL_ERROR,
            message: format!("Failed to send initialize to ACP agent: {}", e),
            data: None,
        })?;

    translate::acp_to_codex_initialize_result(&acp_response).map_err(|e| JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: format!("Failed to translate initialize response: {}", e),
        data: None,
    })
}

/// Handle account/read request.
pub fn handle_account_read(_params: p::GetAccountParams) -> p::GetAccountResponse {
    p::GetAccountResponse {
        account: Some(p::Account::ApiKey {}),
        requires_openai_auth: false,
    }
}

/// Handle account/rateLimits/read request.
pub fn handle_account_rate_limits_read() -> p::GetAccountRateLimitsResponse {
    p::GetAccountRateLimitsResponse::default()
}

/// Handle config/read request.
pub fn handle_config_read(_params: p::ConfigReadParams) -> p::ConfigReadResponse {
    p::ConfigReadResponse {
        config: json!({}),
        origins: Default::default(),
        layers: None,
    }
}

/// Handle configRequirements/read request.
pub fn handle_config_requirements_read() -> p::ConfigRequirementsReadResponse {
    p::ConfigRequirementsReadResponse { requirements: None }
}

/// Handle model/list request. `agent_id` is the wire name of the agent
/// this connection is bound to (e.g. `"devin"`), pulled from the iroh
/// session — without it every ACP-backed agent would advertise the same
/// `"acp-default"` model and the iOS picker would have no way to match
/// the thread's `model` field against a real selection. Using the agent
/// id keeps every ACP agent self-identifying while staying generic.
pub fn handle_model_list(
    bridge: &crate::bridge::AcpBridge,
    agent_id: &str,
    _params: p::ModelListParams,
) -> p::ModelListResponse {
    let cached = bridge.all_models();
    if !cached.is_empty() {
        let data: Vec<p::Model> = cached.iter().map(acp_model_to_codex).collect();
        return p::ModelListResponse {
            data,
            next_cursor: None,
        };
    }
    // Fallback: agent hasn't yet started a session so we have no
    // catalog. Return a single placeholder so the iOS picker has at
    // least one entry it can pin the active thread to.
    let id = if agent_id.is_empty() {
        "acp-default".to_string()
    } else {
        agent_id.to_string()
    };
    let display_name = title_case(&id);
    let data = vec![p::Model {
        id: id.clone(),
        model: id,
        upgrade: None,
        upgrade_info: None,
        availability_nux: None,
        display_name: display_name.clone(),
        description: format!("Default model for {display_name}"),
        hidden: false,
        supported_reasoning_efforts: vec![p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Medium,
            description: "Default".to_string(),
        }],
        default_reasoning_effort: p::ReasoningEffort::Medium,
        input_modalities: vec![json!("text")],
        supports_personality: false,
        additional_speed_tiers: vec![],
        service_tiers: vec![p::ModelServiceTier {
            id: "standard".to_string(),
            name: "Standard".to_string(),
            description: "Standard service tier".to_string(),
        }],
        is_default: true,
    }];
    p::ModelListResponse {
        data,
        next_cursor: None,
    }
}

/// Translate an ACP `configOptions[id=model].options[]` entry into
/// codex `Model`. ACP exposes only `value` (the model id) + `name` (the
/// display label); reasoning effort is implied by the model id (e.g.
/// `claude-opus-4-7-high` vs `-low`) so we don't try to infer it.
fn acp_model_to_codex(entry: &Value) -> p::Model {
    let id = entry
        .get("value")
        .or_else(|| entry.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let display_name = entry
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or(id.as_str())
        .to_string();
    let description = entry
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    p::Model {
        id: id.clone(),
        model: id,
        upgrade: None,
        upgrade_info: None,
        availability_nux: None,
        display_name,
        description,
        hidden: false,
        supported_reasoning_efforts: vec![p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Medium,
            description: "Default".to_string(),
        }],
        default_reasoning_effort: p::ReasoningEffort::Medium,
        input_modalities: vec![json!("text")],
        supports_personality: false,
        additional_speed_tiers: vec![],
        service_tiers: vec![p::ModelServiceTier {
            id: "standard".to_string(),
            name: "Standard".to_string(),
            description: "Standard service tier".to_string(),
        }],
        is_default: false,
    }
}

/// Pull the `options` array out of `session/new`'s `configOptions[id=model]`.
/// Returns the raw ACP entries so the bridge can dedupe and we keep
/// translation in one place.
pub(crate) fn extract_models_from_config_options(session_new: &Value) -> Vec<Value> {
    let options = session_new
        .get("configOptions")
        .and_then(|v| v.as_array())
        .map(|v| v.iter())
        .into_iter()
        .flatten();
    for opt in options {
        if opt.get("id").and_then(|v| v.as_str()) == Some("model")
            && let Some(arr) = opt.get("options").and_then(|v| v.as_array())
        {
            return arr.clone();
        }
    }
    Vec::new()
}

/// Pull `modes.currentModeId` and `modes.availableModes` out of
/// `session/new`. ACP wraps both under a top-level `modes` object.
pub(crate) fn extract_modes_from_session_new(session_new: &Value) -> crate::bridge::ModesSnapshot {
    let modes = match session_new.get("modes") {
        Some(m) => m,
        None => return crate::bridge::ModesSnapshot::default(),
    };
    let current = modes
        .get("currentModeId")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let available = modes
        .get("availableModes")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    crate::bridge::ModesSnapshot { current, available }
}

fn title_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, ch) in s.chars().enumerate() {
        if i == 0 {
            out.extend(ch.to_uppercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Handle experimentalFeature/list request.
pub fn handle_experimental_feature_list() -> p::ExperimentalFeatureListResponse {
    p::ExperimentalFeatureListResponse {
        data: vec![],
        next_cursor: None,
    }
}

/// Handle collaborationMode/list request.
///
/// codex's response is `{data: Vec<JsonValue>}` (opaque), so we can
/// pass the raw ACP mode entries through unchanged — each is
/// `{id, name, description?}` which already matches what iOS expects.
pub fn handle_collaboration_mode_list(
    bridge: &crate::bridge::AcpBridge,
) -> p::CollaborationModeListResponse {
    p::CollaborationModeListResponse {
        data: bridge.all_modes(),
    }
}

/// Handle mcpServerStatus/list request.
pub fn handle_mcp_server_status_list(
    _params: p::ListMcpServerStatusParams,
) -> p::ListMcpServerStatusResponse {
    p::ListMcpServerStatusResponse {
        data: vec![],
        next_cursor: None,
    }
}

/// Handle skills/list request.
pub fn handle_skills_list(
    bridge: &crate::bridge::AcpBridge,
    params: p::SkillsListParams,
) -> p::SkillsListResponse {
    // Map ACP `availableCommands` entries to codex `SkillMetadata` and
    // pack them under each requested cwd. If the caller didn't supply a
    // cwd, return one bundle keyed on "" so iOS still gets the list.
    let commands = bridge.all_available_commands();
    let skills: Vec<p::SkillMetadata> = commands
        .into_iter()
        .filter_map(|cmd| {
            let name = cmd.get("name").and_then(|v| v.as_str())?.to_string();
            let description = cmd
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Some(p::SkillMetadata {
                name,
                description,
                short_description: None,
                interface: None,
                dependencies: None,
                // ACP commands have no on-disk path; codex `SkillMetadata`
                // requires the field, so we feed an empty string.
                path: String::new(),
                scope: p::SkillScope::User,
                enabled: true,
            })
        })
        .collect();

    let cwds = if params.cwds.is_empty() {
        vec![std::path::PathBuf::new()]
    } else {
        params.cwds
    };
    let data = cwds
        .into_iter()
        .map(|cwd| p::SkillsListEntry {
            cwd,
            skills: skills.clone(),
            errors: vec![],
        })
        .collect();
    p::SkillsListResponse { data }
}

/// Handle thread/start request.
pub async fn handle_thread_start(
    ctx: &alleycat_bridge_core::Conn,
    bridge: &crate::bridge::AcpBridge,
    client: &Arc<AcpClient>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let agent_id = ctx.session().agent.to_string();
    let typed: p::ThreadStartParams = serde_json::from_value(params).map_err(|e| JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!("Invalid ThreadStartParams: {}", e),
        data: None,
    })?;

    let typed_value = serde_json::to_value(&typed).map_err(|e| JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: format!("Failed to serialize ThreadStartParams: {}", e),
        data: None,
    })?;

    let acp_request =
        translate::codex_to_acp_new_session(&typed_value).map_err(|e| JsonRpcError {
            code: error_codes::INVALID_PARAMS,
            message: format!("Failed to translate thread start params: {}", e),
            data: None,
        })?;

    let acp_response = client
        .send_request("session/new", acp_request)
        .await
        .map_err(|e| JsonRpcError {
            code: error_codes::INTERNAL_ERROR,
            message: e.to_string(),
            data: None,
        })?;

    let session_id = acp_response
        .get("sessionId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| JsonRpcError {
            code: error_codes::INTERNAL_ERROR,
            message: "ACP session/new response missing sessionId".to_string(),
            data: None,
        })?
        .to_string();

    // Capture models + modes advertised by `session/new`. ACP doesn't
    // have dedicated model_list / mode_list methods — both arrive as
    // entries in `configOptions[]`. We stash the parsed shapes per
    // session so `model/list` and `collaborationMode/list` can serve
    // real data instead of placeholder rows.
    let models = extract_models_from_config_options(&acp_response);
    if !models.is_empty() {
        bridge.set_models(&session_id, models);
    }
    let modes = extract_modes_from_session_new(&acp_response);
    if modes.current.is_some() || !modes.available.is_empty() {
        bridge.set_modes(&session_id, modes);
    }

    // codex `ThreadStartResponse` mirrors `ThreadResumeResponse` — full
    // Thread plus model/modelProvider/cwd/approvalPolicy/approvalsReviewer/
    // sandbox. Building the same shape we use in resume keeps the iOS
    // deserializer happy.
    let cwd = typed.cwd.clone().unwrap_or_default();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let cached_title = bridge.get_thread_title(&session_id);
    Ok(json!({
        "thread": {
            "id": session_id,
            "sessionId": session_id,
            "name": cached_title,
            "preview": "",
            "ephemeral": false,
            "modelProvider": &agent_id,
            "createdAt": now_ms,
            "updatedAt": now_ms,
            "status": { "type": "idle" },
            "cwd": &cwd,
            "cliVersion": "",
            "source": "appServer",
            "agentNickname": null,
            "agentRole": null,
            "turns": [],
        },
        "model": &agent_id,
        "modelProvider": &agent_id,
        "cwd": cwd,
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": { "type": "workspaceWrite" },
    }))
}

/// Handle thread/list request.
pub async fn handle_thread_list(
    client: &Arc<AcpClient>,
    _params: p::ThreadListParams,
) -> Result<Value, JsonRpcError> {
    // Try to use ACP's session/list if available
    match client.send_request("session/list", json!({})).await {
        Ok(acp_response) => {
            // Parse ACP session list response
            let empty_sessions = vec![];
            let sessions = acp_response
                .get("sessions")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty_sessions);

            let threads: Vec<Value> = sessions
                .iter()
                .filter_map(|session| {
                    let session_id = session.get("sessionId").and_then(|v| v.as_str())?;
                    let name = session
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&format!("Session {}", session_id))
                        .to_string();
                    let created_at = timestamp_ms(session.get("createdAt"));
                    let updated_at = timestamp_ms(session.get("updatedAt"));

                    Some(json!({
                        "id": session_id,
                        "sessionId": session_id,
                        "forkedFromId": null,
                        "preview": name,
                        "ephemeral": false,
                        "modelProvider": "acp",
                        "createdAt": created_at,
                        "updatedAt": updated_at,
                        "status": { "type": "idle" },
                        "path": "",
                        "cwd": "",
                        "cliVersion": "",
                        "source": "appServer",
                        "threadSource": null,
                        "agentNickname": null,
                        "agentRole": null,
                        "gitInfo": null,
                        "name": name,
                        "turns": [],
                    }))
                })
                .collect();

            Ok(json!({
                "data": threads,
                "nextCursor": null,
                "backwardsCursor": null,
            }))
        }
        Err(_) => {
            // ACP doesn't support session/list, return empty
            Ok(json!({
                "data": [],
                "nextCursor": null,
                "backwardsCursor": null,
            }))
        }
    }
}

fn timestamp_ms(value: Option<&Value>) -> i64 {
    if let Some(ms) = value.and_then(Value::as_i64) {
        return ms;
    }
    if let Some(text) = value.and_then(Value::as_str)
        && let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text)
    {
        return parsed.timestamp_millis();
    }
    chrono::Utc::now().timestamp_millis()
}

/// Handle thread/resume request.
///
/// Sends ACP `session/load` and translates the replayed history
/// (streamed as `session/update` notifications BEFORE the response) into
/// codex `Turn` objects. The replay is split at every `user_message_chunk`
/// so iOS sees multiple turns instead of one giant one. Locally-captured
/// turns (from earlier `turn/start` calls in this daemon run) take
/// priority over the agent's replay so we don't lose work-in-progress
/// state to a stale agent-side snapshot.
pub async fn handle_thread_resume(
    ctx: &alleycat_bridge_core::Conn,
    bridge: &crate::bridge::AcpBridge,
    client: &Arc<AcpClient>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let agent_id = ctx.session().agent.to_string();
    let typed: p::ThreadResumeParams =
        serde_json::from_value(params).map_err(|e| JsonRpcError {
            code: error_codes::INVALID_PARAMS,
            message: format!("Invalid ThreadResumeParams: {}", e),
            data: None,
        })?;

    // ACP spec method is `session/load`, not `session/resume`. The agent
    // advertises this via `agentCapabilities.loadSession: true`. `mcpServers`
    // is required by ACP even when empty. `cwd` is also required as a
    // string; mobile clients often call thread/resume without knowing the
    // session's original cwd. Devin's serde tolerates `""`, but grok
    // rejects relative paths with `-32602 Invalid params: Path is not
    // absolute: `, so fall back to `/` when the client didn't supply one.
    let acp_request = json!({
        "sessionId": typed.thread_id,
        "cwd": coerce_absolute_cwd(typed.cwd.as_deref()),
        "mcpServers": [],
    });

    let _acp_response = client
        .send_request("session/load", acp_request)
        .await
        .map_err(|e| JsonRpcError {
            code: error_codes::INTERNAL_ERROR,
            // Propagate the agent's own message unchanged — the iOS error
            // toast shows this verbatim, so prefixes like "Failed to send
            // ... to ACP agent:" turn a useful sentence ("Session 'X' is
            // already open in another process") into noise.
            message: e.to_string(),
            data: None,
        })?;

    // session/load also streams `available_commands_update` ahead of the
    // response — cache whatever the agent sends so `skills/list` returns
    // a real list even before the first turn is sent.
    let acp_notifications = client.take_pending_notifications().await;
    if let Some(cmds) = extract_available_commands(&acp_notifications) {
        bridge.set_available_commands(&typed.thread_id, cmds);
    }
    if let Some(mode) = extract_current_mode(&acp_notifications) {
        bridge.set_current_mode(&typed.thread_id, mode);
    }

    // Local turns (captured by handle_turn_start in this daemon run) take
    // priority if present; otherwise rebuild from the ACP replay stream.
    let local_turns = bridge.get_turns(&typed.thread_id);
    let stored_turns: Vec<crate::bridge::StoredTurn> = if !local_turns.is_empty() {
        local_turns
    } else {
        let rebuilt = build_turns_from_replay(&acp_notifications);
        if !rebuilt.is_empty() {
            bridge.set_turns(&typed.thread_id, rebuilt.clone());
        }
        rebuilt
    };

    let turns_json: Vec<Value> = stored_turns.iter().map(stored_turn_to_json).collect();

    let (created_at_ms, updated_at_ms) = thread_timestamps(&stored_turns);

    // Codex `ThreadResumeResponse` has hard requirements: a fully-shaped
    // `Thread` (id, sessionId, preview, ephemeral, modelProvider, createdAt,
    // updatedAt, status, cwd, cliVersion, source) plus top-level model /
    // modelProvider / cwd / approvalPolicy / approvalsReviewer / sandbox.
    // Missing any one of these makes the iOS deserializer reject the whole
    // resume with `missing field <foo>`.
    let cwd = typed.cwd.clone().unwrap_or_default();
    let model = typed.model.clone().unwrap_or_else(|| agent_id.clone());
    let model_provider = typed
        .model_provider
        .clone()
        .unwrap_or_else(|| agent_id.clone());
    let cached_title = bridge.get_thread_title(&typed.thread_id);
    Ok(json!({
        "thread": {
            "id": typed.thread_id,
            "sessionId": typed.thread_id,
            "name": cached_title,
            "preview": "",
            "ephemeral": false,
            "modelProvider": &model_provider,
            "createdAt": created_at_ms,
            "updatedAt": updated_at_ms,
            "status": { "type": "idle" },
            "cwd": &cwd,
            "cliVersion": "",
            "source": "appServer",
            "agentNickname": null,
            "agentRole": null,
            "turns": turns_json,
        },
        "model": model,
        "modelProvider": model_provider,
        "cwd": cwd,
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": { "type": "workspaceWrite" },
    }))
}

/// Convert a `StoredTurn` into a codex `Turn` JSON object.
///
/// Timestamps for the Turn schema are SECONDS (per the `started_at` /
/// `completed_at` docstrings in `codex-proto/src/protocol/v2/thread_data.rs`),
/// but our storage keeps millis for precision — divide here.
fn stored_turn_to_json(turn: &crate::bridge::StoredTurn) -> Value {
    let duration_ms = turn
        .completed_at_ms
        .map(|end| end.saturating_sub(turn.started_at_ms));
    json!({
        "id": turn.id,
        "items": turn.items,
        "itemsView": "full",
        "status": turn.status,
        "error": turn.error,
        "startedAt": turn.started_at_ms / 1000,
        "completedAt": turn.completed_at_ms.map(|ms| ms / 1000),
        "durationMs": duration_ms,
    })
}

/// Pick createdAt = earliest turn started, updatedAt = latest turn
/// completed (falling back to now() when there are no turns).
fn thread_timestamps(turns: &[crate::bridge::StoredTurn]) -> (i64, i64) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    if turns.is_empty() {
        return (now_ms, now_ms);
    }
    let created = turns
        .iter()
        .map(|t| t.started_at_ms)
        .min()
        .unwrap_or(now_ms);
    let updated = turns
        .iter()
        .map(|t| t.completed_at_ms.unwrap_or(t.started_at_ms))
        .max()
        .unwrap_or(now_ms);
    (created, updated)
}

/// Translate codex `UserInput[]` into ACP `ContentBlock[]` for `session/prompt`.
///
/// Mapping (per https://agentclientprotocol.com/protocol/content):
/// * `Text` → `{type: "text", text}`
/// * `Image{url}` → `{type: "image", uri: url}` (preferred over the
///   base64 form when we already have a URI)
/// * `LocalImage{path}` → read the file, base64-encode, emit
///   `{type: "image", mimeType, data}`. If the file can't be read, fall
///   back to a `resource_link` so the reference isn't lost entirely.
/// * `Skill{name}` → `{type: "text", text: "/{name}"}` so the agent sees
///   it as a slash command. ACP has no first-class skill block.
/// * `Mention{name, path}` → text `@{name}` plus a `resource_link` to
///   the path so the agent can resolve the reference.
fn user_input_to_acp_prompt(input: &[p::UserInput]) -> Vec<Value> {
    let mut blocks = Vec::new();
    for item in input {
        match item {
            p::UserInput::Text { text, .. } => {
                blocks.push(json!({"type": "text", "text": text}));
            }
            p::UserInput::Image { url } => {
                blocks.push(json!({"type": "image", "uri": url}));
            }
            p::UserInput::LocalImage { path } => match encode_local_image(path) {
                Ok((mime, data)) => {
                    blocks.push(json!({
                        "type": "image",
                        "mimeType": mime,
                        "data": data,
                    }));
                }
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        ?path,
                        "failed to read local image; sending resource_link"
                    );
                    let uri = format!("file://{}", path.display());
                    blocks.push(json!({"type": "resource_link", "uri": uri}));
                }
            },
            p::UserInput::Skill { name, .. } => {
                blocks.push(json!({"type": "text", "text": format!("/{name}")}));
            }
            p::UserInput::Mention { name, path } => {
                blocks.push(json!({"type": "text", "text": format!("@{name}")}));
                let uri = if path.starts_with("file://") || path.starts_with("http") {
                    path.clone()
                } else {
                    format!("file://{path}")
                };
                blocks.push(json!({"type": "resource_link", "uri": uri}));
            }
        }
    }
    if blocks.is_empty() {
        blocks.push(json!({"type": "text", "text": ""}));
    }
    blocks
}

/// One-line text summary of UserInput[] used for the codex
/// `userMessage` ThreadItem we persist locally (since codex represents
/// user inputs as a sequence too, but we want a stable readable preview
/// for the bridge's own conversation_history mirror).
fn user_input_text_summary(input: &[p::UserInput]) -> String {
    input
        .iter()
        .map(|item| match item {
            p::UserInput::Text { text, .. } => text.clone(),
            p::UserInput::Skill { name, .. } => format!("/{name}"),
            p::UserInput::Mention { name, .. } => format!("@{name}"),
            p::UserInput::Image { url } => format!("[image: {url}]"),
            p::UserInput::LocalImage { path } => format!("[image: {}]", path.display()),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Read a local image file and base64-encode it for ACP `image` blocks.
/// Returns `(mimeType, base64Data)`.
fn encode_local_image(path: &std::path::Path) -> std::io::Result<(String, String)> {
    use base64::Engine;
    let bytes = std::fs::read(path)?;
    let mime = mime_for_extension(path.extension().and_then(|e| e.to_str()).unwrap_or(""));
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok((mime, encoded))
}

fn mime_for_extension(ext: &str) -> String {
    match ext.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "heic" => "image/heic",
        "heif" => "image/heif",
        "tiff" | "tif" => "image/tiff",
        _ => "image/png",
    }
    .to_string()
}

/// Pull the latest `currentModeId` from `current_mode_update` frames.
fn extract_current_mode(notifications: &[Value]) -> Option<String> {
    let mut latest: Option<String> = None;
    for note in notifications {
        if note.get("method").and_then(|v| v.as_str()) != Some("session/update") {
            continue;
        }
        let update = match note.get("params").and_then(|p| p.get("update")) {
            Some(u) => u,
            None => continue,
        };
        if update.get("sessionUpdate").and_then(|v| v.as_str()) != Some("current_mode_update") {
            continue;
        }
        if let Some(id) = update.get("currentModeId").and_then(|v| v.as_str()) {
            latest = Some(id.to_string());
        }
    }
    latest
}

/// Pull the latest `availableCommands` array out of a notification
/// stream so it can be cached without re-running the full translator.
fn extract_available_commands(notifications: &[Value]) -> Option<Vec<Value>> {
    let mut latest: Option<Vec<Value>> = None;
    for note in notifications {
        if note.get("method").and_then(|v| v.as_str()) != Some("session/update") {
            continue;
        }
        let update = match note.get("params").and_then(|p| p.get("update")) {
            Some(u) => u,
            None => continue,
        };
        if update.get("sessionUpdate").and_then(|v| v.as_str()) != Some("available_commands_update")
        {
            continue;
        }
        if let Some(arr) = update.get("availableCommands").and_then(|v| v.as_array()) {
            latest = Some(arr.clone());
        }
    }
    latest
}

/// Split a drained `session/load` notification stream into per-turn
/// `StoredTurn` records.
///
/// ACP `session/load` replays the entire conversation as
/// `session/update` notifications (https://agentclientprotocol.com/protocol/session-setup#loading-sessions),
/// but doesn't surface explicit turn boundaries. We segment at every
/// `user_message_chunk` — each user message starts a new turn.
/// Notifications before the first user message (preface from the agent)
/// land in a "turn-acp-pre" bucket so they're not lost.
fn build_turns_from_replay(notifications: &[Value]) -> Vec<crate::bridge::StoredTurn> {
    // Find indices of user_message_chunk frames so we know where to slice.
    let user_boundaries: Vec<usize> = notifications
        .iter()
        .enumerate()
        .filter_map(|(idx, note)| {
            let kind = note
                .get("params")
                .and_then(|p| p.get("update"))
                .and_then(|u| u.get("sessionUpdate"))
                .and_then(|v| v.as_str());
            if kind == Some("user_message_chunk") {
                Some(idx)
            } else {
                None
            }
        })
        .collect();

    if user_boundaries.is_empty() {
        // No clear boundaries — wrap the whole replay in one turn.
        let mut translator = crate::translator::SessionUpdateTranslator::new();
        for note in notifications {
            translator.ingest(note);
        }
        let translated = translator.finish();
        if translated.items.is_empty() {
            return Vec::new();
        }
        return vec![crate::bridge::StoredTurn {
            id: "turn-acp-0".to_string(),
            items: translated.items,
            status: "completed".to_string(),
            started_at_ms: 0,
            completed_at_ms: Some(0),
            error: None,
        }];
    }

    let mut turns = Vec::new();

    // Any notifications before the first user_message_chunk form an
    // implicit "preface" turn (rare — usually empty).
    if user_boundaries[0] > 0 {
        let mut translator = crate::translator::SessionUpdateTranslator::new();
        for note in &notifications[..user_boundaries[0]] {
            translator.ingest(note);
        }
        let translated = translator.finish();
        if !translated.items.is_empty() {
            turns.push(crate::bridge::StoredTurn {
                id: format!("turn-acp-pre-{}", turns.len()),
                items: translated.items,
                status: "completed".to_string(),
                started_at_ms: 0,
                completed_at_ms: Some(0),
                error: None,
            });
        }
    }

    // Slice [user_boundaries[i]..user_boundaries[i+1]] into turn i.
    for (turn_idx, win) in user_boundaries.windows(2).enumerate() {
        let start = win[0];
        let end = win[1];
        let mut translator = crate::translator::SessionUpdateTranslator::new();
        for note in &notifications[start..end] {
            translator.ingest(note);
        }
        let translated = translator.finish();
        if translated.items.is_empty() {
            continue;
        }
        turns.push(crate::bridge::StoredTurn {
            id: format!("turn-acp-{turn_idx}"),
            items: translated.items,
            status: "completed".to_string(),
            started_at_ms: 0,
            completed_at_ms: Some(0),
            error: None,
        });
    }

    // The last segment (from last user_message_chunk to end of stream).
    let last_start = *user_boundaries.last().unwrap();
    let mut translator = crate::translator::SessionUpdateTranslator::new();
    for note in &notifications[last_start..] {
        translator.ingest(note);
    }
    let translated = translator.finish();
    if !translated.items.is_empty() {
        turns.push(crate::bridge::StoredTurn {
            id: format!("turn-acp-{}", turns.len()),
            items: translated.items,
            status: "completed".to_string(),
            started_at_ms: 0,
            completed_at_ms: Some(0),
            error: None,
        });
    }

    turns
}

/// Handle thread/read request.
///
/// Serializes the bridge's stored `Vec<StoredTurn>` for this thread
/// directly into codex `Turn` JSON. No more `chunks(2)` heuristics — the
/// stored turns already carry tool calls, reasoning, and plans in their
/// original order with the same item ids that `turn/start` broadcast live,
/// so iOS reconciliation between cached + refreshed views is a no-op.
pub async fn handle_thread_read(
    ctx: &alleycat_bridge_core::Conn,
    bridge: &crate::bridge::AcpBridge,
    _client: &Arc<AcpClient>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let agent_id = ctx.session().agent.to_string();
    let typed: p::ThreadReadParams = serde_json::from_value(params).map_err(|e| JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!("Invalid ThreadReadParams: {}", e),
        data: None,
    })?;

    let stored_turns = bridge.get_turns(&typed.thread_id);
    let turns_json: Vec<Value> = if typed.include_turns {
        stored_turns.iter().map(stored_turn_to_json).collect()
    } else {
        Vec::new()
    };

    let (created_at_ms, updated_at_ms) = thread_timestamps(&stored_turns);

    // ThreadReadResponse is `{thread: Thread}` — turns live inside the
    // Thread, not at the top level. Thread requires the same battery of
    // fields the resume/start responses do; missing any one of them
    // makes iOS reject the entire deserialization.
    let cached_title = bridge.get_thread_title(&typed.thread_id);
    Ok(json!({
        "thread": {
            "id": typed.thread_id,
            "sessionId": typed.thread_id,
            "name": cached_title,
            "preview": "",
            "ephemeral": false,
            "modelProvider": &agent_id,
            "createdAt": created_at_ms,
            "updatedAt": updated_at_ms,
            "status": { "type": "idle" },
            "cwd": "",
            "cliVersion": "",
            "source": "appServer",
            "agentNickname": null,
            "agentRole": null,
            "turns": turns_json,
        }
    }))
}

/// Handle thread/name/set request.
pub fn handle_thread_name_set(
    ctx: &alleycat_bridge_core::Conn,
    bridge: &crate::bridge::AcpBridge,
    params: p::ThreadSetNameParams,
) -> p::ThreadSetNameResponse {
    // Emit thread/name/updated notification
    bridge.emit_thread_name_updated(ctx, &params.thread_id, &params.name);
    p::ThreadSetNameResponse {}
}

/// Handle turn/start request.
///
/// Lifecycle:
///   1. Emit `turn/started` (status `inProgress`) with a provisional turn id.
///   2. Send `session/prompt` to the ACP agent. Notifications stream
///      ahead of the response and are buffered in
///      `client.pending_notifications`.
///   3. Drain notifications and feed them to `SessionUpdateTranslator`
///      to produce codex `ThreadItem`s (user_message_chunk →
///      userMessage, agent_message_chunk → agentMessage,
///      agent_thought_chunk → reasoning, tool_call+tool_call_update →
///      commandExecution / fileChange / dynamicToolCall, plan → captured
///      separately so we can fire `turn/plan/updated`).
///   4. Prepend a userMessage we built from the request's text input —
///      the source of truth for what the user typed. Drop any
///      duplicate userMessage the agent might have echoed.
///   5. Fan items out as `item/started` + `item/completed` (with the
///      `startedAtMs`/`completedAtMs` fields codex strict-decodes).
///   6. Map `stopReason` from the session/prompt response to a
///      `TurnStatus` (refusal/cancelled → failed). Emit `turn/completed`
///      with the full Turn shape and persist the turn via
///      `bridge.append_turn` so `thread/read`/`thread/resume` can round-trip
///      the same items + ids.
#[instrument(skip(ctx, bridge, client, params), fields(thread_id))]
pub async fn handle_turn_start(
    ctx: &alleycat_bridge_core::Conn,
    bridge: &crate::bridge::AcpBridge,
    client: &Arc<AcpClient>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let typed: p::TurnStartParams = serde_json::from_value(params).map_err(|e| JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!("Invalid TurnStartParams: {}", e),
        data: None,
    })?;

    tracing::Span::current().record("thread_id", &typed.thread_id);
    info!("Starting turn for thread: {}", typed.thread_id);

    // Build ACP ContentBlock array from codex UserInput[]. Honors Text,
    // Image url, LocalImage on-disk path, Skill (translated to a slash
    // command), and Mention (translated to inline text + resource_link).
    let prompt_blocks = user_input_to_acp_prompt(&typed.input);
    let text_content = user_input_text_summary(&typed.input);

    let provisional_user_uuid = uuid::Uuid::now_v7().to_string();

    bridge.set_session_status(ctx, &typed.thread_id, crate::bridge::SessionStatus::Active);
    let turn_start_ms = chrono::Utc::now().timestamp_millis();
    // codex Turn.started_at is documented as seconds.
    let turn_start_secs = turn_start_ms / 1000;
    let provisional_turn_id = format!("turn-{}", provisional_user_uuid);

    if ctx.should_emit("turn/started") {
        let _ = ctx.notifier().send_notification(
            "turn/started",
            json!({
                "threadId": typed.thread_id,
                "turn": {
                    "id": provisional_turn_id,
                    "items": [],
                    "itemsView": "full",
                    "status": "inProgress",
                    "error": null,
                    "startedAt": turn_start_secs,
                    "completedAt": null,
                    "durationMs": null,
                },
            }),
        );
    }

    // Per ACP spec (https://agentclientprotocol.com/protocol/prompting)
    // session/prompt params are `{sessionId, prompt: ContentBlock[]}`.
    let acp_request = json!({
        "sessionId": typed.thread_id,
        "prompt": prompt_blocks,
    });

    // We use `provisional_turn_id` (derived from a UUIDv7 we minted
    // up-front) for the whole lifecycle. The previous implementation
    // tried to swap to a devin-supplied `userMessageId`-derived turn id
    // after the response, but that broke any live notifications already
    // emitted under the provisional id. With a single id across stream
    // and refresh, iOS reconciliation is straightforward.
    let stable_turn_id = provisional_turn_id.clone();
    let user_item_id = format!("acp-user-{provisional_user_uuid}");

    // Build and emit the user-message item BEFORE issuing `session/prompt`
    // so iOS sees it ordered ahead of the assistant's streamed items. If
    // we emitted it after the streaming call, the agent's reasoning /
    // agentMessage notifications would arrive first on the wire and the
    // user bubble would render under the response. We preserve the full
    // codex `UserInput[]` shape (text + image + mention + skill) by
    // round-tripping `typed.input` directly.
    let user_content = serde_json::to_value(&typed.input)
        .unwrap_or_else(|_| json!([{"type": "text", "text": text_content.clone()}]));
    let user_item = json!({
        "id": user_item_id,
        "type": "userMessage",
        "content": user_content,
    });
    let user_ts_ms = chrono::Utc::now().timestamp_millis();
    let _ = ctx.notifier().send_notification(
        "item/started",
        json!({
            "threadId": typed.thread_id,
            "turnId": stable_turn_id,
            "item": user_item,
            "startedAtMs": user_ts_ms,
        }),
    );
    let _ = ctx.notifier().send_notification(
        "item/completed",
        json!({
            "threadId": typed.thread_id,
            "turnId": stable_turn_id,
            "item": user_item,
            "completedAtMs": user_ts_ms,
        }),
    );

    // Stream notifications live as they arrive. The emitter renders
    // codex item/* notifications on the fly and also accumulates the
    // final item list for StoredTurn.
    let notifier = ctx.notifier().clone();
    let emitter = std::sync::Arc::new(std::sync::Mutex::new(
        crate::streaming::TurnStreamEmitter::new(
            move |method, params| {
                let _ = notifier.send_notification(method, params.clone());
            },
            typed.thread_id.clone(),
            stable_turn_id.clone(),
        ),
    ));
    let emitter_cb = std::sync::Arc::clone(&emitter);

    let acp_response = client
        .send_request_streaming("session/prompt", acp_request, move |note| {
            if let Ok(mut e) = emitter_cb.lock() {
                e.ingest(&note);
            }
        })
        .await
        .map_err(|e| {
            bridge.emit_thread_warning(
                ctx,
                &typed.thread_id,
                &format!("Failed to send session/prompt to ACP agent: {}", e),
            );
            JsonRpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("Failed to send session/prompt to ACP agent: {}", e),
                data: None,
            }
        })?;

    // Discard any notifications still in the fallback buffer — the
    // streaming subscriber already processed them.
    let _ = client.take_pending_notifications().await;

    let stream = std::sync::Arc::try_unwrap(emitter)
        .ok()
        .expect("emitter Arc has one strong ref after streaming")
        .into_inner()
        .expect("emitter mutex not poisoned")
        .finish();
    tracing::info!(
        thread_id = %typed.thread_id,
        item_count = stream.items.len(),
        "consumed live ACP stream"
    );

    // The streaming emitter skips user_message_chunk echoes, so the
    // assembled stream.items doesn't include the user message — we
    // prepend it below for the canonical turn payload.
    let mut canonical_items: Vec<Value> = Vec::with_capacity(stream.items.len() + 1);
    canonical_items.push(user_item);
    canonical_items.extend(stream.items);

    // Map ACP `stopReason` to codex `TurnStatus`. ACP values:
    // end_turn | max_tokens | max_turn_requests | refusal | cancelled.
    let stop_reason = acp_response
        .get("stopReason")
        .and_then(|v| v.as_str())
        .unwrap_or("end_turn");
    let (turn_status, turn_error) = match stop_reason {
        "refusal" => (
            "failed",
            Some(json!({
                "type": "agentRefused",
                "message": "Agent refused to complete the turn",
            })),
        ),
        "cancelled" => (
            "failed",
            Some(json!({
                "type": "cancelled",
                "message": "Turn was cancelled",
            })),
        ),
        "max_tokens" | "max_turn_requests" => {
            bridge.emit_thread_warning(
                ctx,
                &typed.thread_id,
                &format!("Turn ended due to {stop_reason}"),
            );
            ("completed", None)
        }
        _ => ("completed", None),
    };

    tracing::info!(
        thread_id = %typed.thread_id,
        item_count = canonical_items.len(),
        stop_reason,
        turn_status,
        "finalized turn"
    );

    // Cache the agent's most-recent slash-command list so iOS's
    // `skills/list` returns something useful instead of [].
    if let Some(cmds) = stream.available_commands.clone() {
        bridge.set_available_commands(&typed.thread_id, cmds);
    }
    // Mode the agent switched into mid-turn (if any) — codex has no
    // dedicated notification but the up-to-date snapshot will surface
    // on the next `collaborationMode/list` call.
    if let Some(mode) = stream.current_mode.clone() {
        bridge.set_current_mode(&typed.thread_id, mode);
    }

    // If the agent emitted a plan, surface it as turn/plan/updated.
    // We map ACP plan entries → codex TurnPlanStep (status: pending|inProgress|completed),
    // dropping the ACP `priority` field which has no codex equivalent.
    if let Some(entries) = stream.plan_entries.as_ref() {
        let plan: Vec<Value> = entries
            .iter()
            .map(|entry| {
                let step = entry
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let status = match entry.get("status").and_then(|v| v.as_str()) {
                    Some("completed") => "completed",
                    Some("in_progress") => "inProgress",
                    _ => "pending",
                };
                json!({"step": step, "status": status})
            })
            .collect();
        if ctx.should_emit("turn/plan/updated") {
            let _ = ctx.notifier().send_notification(
                "turn/plan/updated",
                json!({
                    "threadId": typed.thread_id,
                    "turnId": stable_turn_id,
                    "explanation": null,
                    "plan": plan,
                }),
            );
        }
    }

    let turn_end_ms = chrono::Utc::now().timestamp_millis();
    let turn_end_secs = turn_end_ms / 1000;
    let duration_ms = turn_end_ms - turn_start_ms;

    let completed_turn = json!({
        "id": stable_turn_id,
        "items": canonical_items.clone(),
        "itemsView": "full",
        "status": turn_status,
        "error": turn_error,
        "startedAt": turn_start_secs,
        "completedAt": turn_end_secs,
        "durationMs": duration_ms,
    });
    if ctx.should_emit("turn/completed") {
        let _ = ctx.notifier().send_notification(
            "turn/completed",
            json!({
                "threadId": typed.thread_id,
                "turn": completed_turn.clone(),
            }),
        );
    }
    bridge.set_session_status(ctx, &typed.thread_id, crate::bridge::SessionStatus::Idle);

    // Persist the turn so thread/read returns the same items+ids on refresh.
    bridge.append_turn(
        &typed.thread_id,
        crate::bridge::StoredTurn {
            id: stable_turn_id.clone(),
            items: canonical_items.clone(),
            status: turn_status.to_string(),
            started_at_ms: turn_start_ms,
            completed_at_ms: Some(turn_end_ms),
            error: turn_error.clone(),
        },
    );

    Ok(json!({
        "turn": completed_turn,
    }))
}

/// Handle command/exec request.
///
/// codex `CommandExecParams` doesn't include a `threadId` field, so the
/// bridge has no way to know which ACP session a shell command should
/// run against (and ACP `terminal/*` methods require a session id). The
/// previous implementation hard-coded `"default"`, hit "Session not
/// found" on every call, then mapped that to a confusing
/// METHOD_NOT_FOUND with a misleading "agent doesn't support terminal
/// operations" message.
///
/// Surface a clear METHOD_NOT_FOUND up front. iOS shouldn't be calling
/// command/exec on ACP threads anyway — tool execution from the agent
/// flows through `session/update` → `tool_call`, which the translator
/// already renders as `commandExecution` ThreadItems.
pub async fn handle_command_exec(
    _ctx: &alleycat_bridge_core::Conn,
    _bridge: &crate::bridge::AcpBridge,
    _client: &Arc<AcpClient>,
    _params: Value,
) -> Result<Value, JsonRpcError> {
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message:
            "command/exec is not supported by ACP bridges (no threadId in CommandExecParams). \
                  Agent-initiated commands flow through session/update tool_call events instead."
                .to_string(),
        data: None,
    })
}

/// Handle thread/fork request.
pub async fn handle_thread_fork(
    ctx: &alleycat_bridge_core::Conn,
    client: &Arc<AcpClient>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    let typed: p::ThreadForkParams = serde_json::from_value(params).map_err(|e| JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: format!("Invalid ThreadForkParams: {}", e),
        data: None,
    })?;
    let agent_id = ctx.session().agent.to_string();

    // ACP `session/new` requires both `cwd: string` and `mcpServers: array`
    // (even when empty). Without `mcpServers` devin returns "missing field
    // mcpServers". Grok additionally rejects empty/relative cwd with
    // `-32602 Invalid params: Path is not absolute`, so coerce to `/`.
    let cwd = coerce_absolute_cwd(typed.cwd.as_deref()).to_string();
    let acp_request = json!({
        "cwd": &cwd,
        "mcpServers": [],
    });

    let acp_response = client
        .send_request("session/new", acp_request)
        .await
        .map_err(|e| JsonRpcError {
            code: error_codes::INTERNAL_ERROR,
            message: format!("Failed to send session/new for fork: {}", e),
            data: None,
        })?;

    let new_session_id = acp_response
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let now_ms = chrono::Utc::now().timestamp_millis();
    let model = typed.model.clone().unwrap_or_else(|| agent_id.clone());
    let model_provider = typed
        .model_provider
        .clone()
        .unwrap_or_else(|| agent_id.clone());

    // Codex `ThreadForkResponse` has the same hard Thread+top-level
    // requirements as resume/read — sessionId, preview, ephemeral,
    // modelProvider, createdAt/updatedAt as i64 millis, status, cwd,
    // cliVersion, source, plus top-level cwd/approvalPolicy/
    // approvalsReviewer/sandbox. Previously this returned a bespoke
    // shape (`title`, RFC3339 strings, `forkedFromId`) that iOS rejected
    // on first deserialize.
    Ok(json!({
        "thread": {
            "id": new_session_id,
            "sessionId": new_session_id,
            "name": null,
            "preview": "",
            "ephemeral": false,
            "modelProvider": &model_provider,
            "createdAt": now_ms,
            "updatedAt": now_ms,
            "status": { "type": "idle" },
            "cwd": &cwd,
            "cliVersion": "",
            "source": "appServer",
            "agentNickname": null,
            "agentRole": null,
            "turns": [],
        },
        "model": model,
        "modelProvider": model_provider,
        "cwd": cwd,
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": { "type": "workspaceWrite" },
    }))
}

/// Handle thread/rollback request.
pub fn handle_thread_rollback(_params: p::ThreadRollbackParams) -> Result<Value, JsonRpcError> {
    // ACP doesn't support rollback, return an error
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "thread/rollback is not supported by ACP agents".to_string(),
        data: None,
    })
}

/// Handle thread/archive request.
pub fn handle_thread_archive(_params: p::ThreadArchiveParams) -> Result<Value, JsonRpcError> {
    // ACP doesn't support archive, return an error
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "thread/archive is not supported by ACP agents".to_string(),
        data: None,
    })
}

/// Handle thread/unarchive request.
pub fn handle_thread_unarchive(_params: p::ThreadUnarchiveParams) -> Result<Value, JsonRpcError> {
    // ACP doesn't support unarchive, return an error
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "thread/unarchive is not supported by ACP agents".to_string(),
        data: None,
    })
}

/// Handle review/start request.
pub fn handle_review_start(_params: p::ReviewStartParams) -> Result<Value, JsonRpcError> {
    // ACP doesn't support review, return an error
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "review/start is not supported by ACP agents".to_string(),
        data: None,
    })
}

/// Handle command/exec/terminate request.
pub async fn handle_command_exec_terminate(
    _client: &Arc<AcpClient>,
    _params: p::CommandExecTerminateParams,
) -> Result<p::CommandExecTerminateResponse, JsonRpcError> {
    // CommandExecTerminateParams has a process id but no thread/session id.
    // ACP terminal/kill requires both. The old implementation sent every kill
    // to a fabricated "default" session and reported success even when the
    // agent rejected it, leaving the real process running.
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message:
            "command/exec/terminate is not supported by ACP bridges (no session id in request)"
                .to_string(),
        data: None,
    })
}

/// Handle command/exec/write request.
pub fn handle_command_exec_write(
    _params: p::CommandExecWriteParams,
) -> Result<Value, JsonRpcError> {
    // ACP doesn't support streaming stdin to terminals
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "command/exec/write is not supported by ACP agents (no streaming stdin support)"
            .to_string(),
        data: None,
    })
}

/// Handle command/exec/resize request.
pub fn handle_command_exec_resize(
    _params: p::CommandExecResizeParams,
) -> Result<Value, JsonRpcError> {
    // ACP doesn't support PTY resizing
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "command/exec/resize is not supported by ACP agents (no PTY support)".to_string(),
        data: None,
    })
}

/// Handle turn/steer request.
///
/// ACP has no native "steer" — agents don't expose a way to redirect an
/// in-flight turn. Returning a fake success would lie to iOS (the UI
/// shows the user's steering message as accepted while the agent ignores
/// it). Surface METHOD_NOT_FOUND so iOS can disable the steer UI.
pub async fn handle_turn_steer(
    _client: &Arc<AcpClient>,
    _params: p::TurnSteerParams,
) -> Result<p::TurnSteerResponse, JsonRpcError> {
    Err(JsonRpcError {
        code: error_codes::METHOD_NOT_FOUND,
        message: "turn/steer is not supported by ACP agents".to_string(),
        data: None,
    })
}

/// Handle turn/interrupt request.
///
/// ACP `session/cancel` is the cancellation primitive
/// (https://agentclientprotocol.com/protocol/prompt-turn — cancellation):
/// a JSON-RPC notification (no response) the client sends to interrupt
/// the agent's current `session/prompt`. We send it and synthesize an
/// empty `TurnInterruptResponse` since the cancel itself doesn't return
/// a value.
pub async fn handle_turn_interrupt(
    client: &Arc<AcpClient>,
    params: p::TurnInterruptParams,
) -> Result<p::TurnInterruptResponse, JsonRpcError> {
    let req = json!({ "sessionId": params.thread_id });
    client
        .send_notification("session/cancel", req)
        .await
        .map_err(|e| JsonRpcError {
            code: error_codes::INTERNAL_ERROR,
            message: format!("Failed to send session/cancel: {}", e),
            data: None,
        })?;
    Ok(p::TurnInterruptResponse {})
}

// Note: ACP `session/update` translation lives in `crate::translator`.
// `handle_turn_start` and `build_turns_from_replay` are the only callers.
