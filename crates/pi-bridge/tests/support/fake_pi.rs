//! Test-only stand-in for `pi-coding-agent --mode rpc`.
//!
//! Reads `RpcCommand` JSON lines on stdin, emits `RpcResponse` and `PiEvent`
//! JSON lines on stdout. Used by the bridge verification matrix to exercise
//! `PiPool` / `PiProcessHandle` without needing a real pi build at test time.
//!
//! ## Behavior
//!
//! - Every command receives a `{"type":"response","command":<type>,"success":true,...}`
//!   with the same `id`. Commands that pi answers with `data` get a sensible
//!   canned `data` payload (model lists, session ids, etc.).
//! - `prompt` / `steer` / `follow_up` additionally emit a scripted sequence of
//!   events on stdout *before* the response, terminating with `agent_end` so
//!   the bridge sees a complete turn.
//! - Scripts are loaded from the path in `FAKE_PI_SCRIPT`. Each non-empty,
//!   non-`#` line is one JSON event written verbatim. If the env var is unset
//!   or the file is missing, a minimal default script (`agent_start` →
//!   `agent_end`) is used. The script can also include `{"type":"sleep",
//!   "ms":N}` directives to insert delays — these are stripped from the wire.
//! - The fake exits cleanly when stdin EOFs, mirroring the real pi shutdown
//!   path the bridge relies on (closing stdin = drain pending work + exit).
//!
//! Anything not in this surface is intentionally minimal — extend on demand.

use std::env;
use std::fs;
use std::io::{self, BufRead, Write};
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};

fn main() -> ExitCode {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();

    let script = load_script();
    let mut session_id = mint_session_id();
    let mut session_path: Option<String> = None;

    let mut lines = stdin.lock().lines();
    while let Some(Ok(line)) = lines.next() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let cmd: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(err) => {
                emit(
                    &mut out,
                    &json!({
                        "type": "response",
                        "command": "unknown",
                        "success": false,
                        "error": format!("parse error: {err}"),
                    }),
                );
                continue;
            }
        };

        let id = cmd.get("id").and_then(|v| v.as_str()).map(str::to_owned);
        let cmd_type = cmd.get("type").and_then(|v| v.as_str()).unwrap_or("");

        // Optional command-log side-channel for verification tests that need
        // to assert "the bridge sent pi command X". When `FAKE_PI_COMMAND_LOG`
        // is set, append the inbound command's `type` to that file (one per
        // line). Best-effort — write errors are ignored.
        if let Ok(log_path) = env::var("FAKE_PI_COMMAND_LOG")
            && !log_path.is_empty()
            && let Ok(mut f) = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
        {
            let _ = writeln!(f, "{cmd_type}");
        }

        match cmd_type {
            "prompt" | "steer" | "follow_up" => {
                run_script(&mut out, &script);
                emit(&mut out, &response(id.as_deref(), cmd_type, true, None));
            }
            "abort" | "abort_bash" | "abort_retry" => {
                emit(&mut out, &response(id.as_deref(), cmd_type, true, None));
            }
            "new_session" => {
                session_id = mint_session_id();
                session_path = Some(format!("/tmp/fake-pi-sessions/{session_id}.jsonl"));
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "cancelled": false })),
                    ),
                );
            }
            "switch_session" => {
                if let Some(path) = cmd.get("sessionPath").and_then(|v| v.as_str()) {
                    session_path = Some(path.to_string());
                }
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "cancelled": false })),
                    ),
                );
            }
            "fork" => {
                let new_id = mint_session_id();
                session_id = new_id.clone();
                session_path = Some(format!("/tmp/fake-pi-sessions/{new_id}.jsonl"));
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "text": "forked", "cancelled": false })),
                    ),
                );
            }
            "get_state" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({
                            "thinkingLevel": "off",
                            "isStreaming": false,
                            "isCompacting": false,
                            "steeringMode": "all",
                            "followUpMode": "all",
                            "sessionFile": session_path,
                            "sessionId": session_id,
                            "autoCompactionEnabled": true,
                            "messageCount": 0,
                            "pendingMessageCount": 0,
                        })),
                    ),
                );
            }
            "get_available_models" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({
                            "models": [{
                                "id": "fake-model",
                                "modelId": "fake-model",
                                "provider": "fake",
                                "displayName": "Fake Model",
                                "supportsThinking": false,
                                "inputModalities": ["text"],
                                "outputModalities": ["text"],
                            }]
                        })),
                    ),
                );
            }
            "list_sessions" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "sessions": [] })),
                    ),
                );
            }
            "set_model" => {
                let provider = cmd
                    .get("provider")
                    .and_then(|v| v.as_str())
                    .unwrap_or("fake")
                    .to_string();
                let model_id = cmd
                    .get("modelId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("fake-model")
                    .to_string();
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({
                            "id": format!("{provider}/{model_id}"),
                            "modelId": model_id,
                            "provider": provider,
                            "displayName": "Fake Model",
                        })),
                    ),
                );
            }
            "cycle_model" => {
                emit(
                    &mut out,
                    &response(id.as_deref(), cmd_type, true, Some(Value::Null)),
                );
            }
            "set_thinking_level"
            | "cycle_thinking_level"
            | "set_steering_mode"
            | "set_follow_up_mode"
            | "set_auto_compaction"
            | "set_auto_retry"
            | "set_session_name" => {
                emit(&mut out, &response(id.as_deref(), cmd_type, true, None));
            }
            "compact" => {
                // Pi emits `compaction_start` and `compaction_end` events
                // around the compaction work, then returns the command's
                // success response. We mirror that here so V5 (compaction
                // flow) can observe the full event sequence.
                let result = json!({
                    "summary": "compacted (fake)",
                    "firstKeptEntryId": "entry-1",
                    "tokensBefore": 1234,
                });
                emit(
                    &mut out,
                    &json!({"type": "compaction_start", "reason": "manual"}),
                );
                emit(
                    &mut out,
                    &json!({
                        "type": "compaction_end",
                        "reason": "manual",
                        "result": result,
                        "aborted": false,
                        "willRetry": false,
                    }),
                );
                emit(
                    &mut out,
                    &response(id.as_deref(), cmd_type, true, Some(result)),
                );
            }
            "bash" => {
                let command = cmd.get("command").and_then(|v| v.as_str()).unwrap_or("");
                // Special-case `pwd` so verification tests (V2 cross-cwd) can
                // confirm pi was spawned in the right directory without
                // relying on /proc or lsof. Real pi runs the command for
                // real; the fake just reports its `current_dir`.
                let output = if command.trim() == "pwd" {
                    let cwd = std::env::current_dir()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| "?".to_string());
                    format!("{cwd}\n")
                } else {
                    format!("[fake bash] {command}\n")
                };
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({
                            "output": output,
                            "exitCode": 0,
                            "cancelled": false,
                            "truncated": false,
                            "fullOutputPath": null,
                        })),
                    ),
                );
            }
            "get_session_stats" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({
                            "sessionId": session_id,
                            "tokens": {
                                "total": 0,
                                "input": 0,
                                "output": 0,
                                "cachedInput": 0,
                            },
                            "messageCount": 0,
                        })),
                    ),
                );
            }
            "export_html" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "path": "/tmp/fake.html" })),
                    ),
                );
            }
            "get_fork_messages" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "messages": [] })),
                    ),
                );
            }
            "get_last_assistant_text" => {
                emit(
                    &mut out,
                    &response(id.as_deref(), cmd_type, true, Some(json!({ "text": null }))),
                );
            }
            "get_messages" => {
                // Real pi reconstructs `messages` from the active session's
                // JSONL on disk. We mirror that: if the current `session_path`
                // points at a readable JSONL, replay every `entry.message`
                // (in file order). Lets V3-style tests that seed a session
                // ahead of time observe rehydration without bespoke env vars.
                let messages = session_messages(session_path.as_deref());
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "messages": messages })),
                    ),
                );
            }
            "get_commands" => {
                emit(
                    &mut out,
                    &response(
                        id.as_deref(),
                        cmd_type,
                        true,
                        Some(json!({ "commands": [] })),
                    ),
                );
            }
            "" => {
                emit(
                    &mut out,
                    &response(
                        None,
                        "unknown",
                        false,
                        Some(json!({ "error": "missing command type" })),
                    ),
                );
            }
            other => {
                emit(
                    &mut out,
                    &json!({
                        "type": "response",
                        "command": other,
                        "success": false,
                        "error": format!("fake-pi: unknown command {other}"),
                        "id": id,
                    }),
                );
            }
        }
    }

    ExitCode::SUCCESS
}

fn emit<W: Write>(out: &mut W, v: &Value) {
    let _ = serde_json::to_writer(&mut *out, v);
    let _ = out.write_all(b"\n");
    let _ = out.flush();
}

/// Build a `{"type":"response", ...}` payload with optional `data` and the
/// caller's `id` echoed back. Pi only includes `id` when it was present on the
/// inbound command, so we mirror that.
fn response(id: Option<&str>, command: &str, success: bool, data: Option<Value>) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("type".into(), json!("response"));
    obj.insert("command".into(), json!(command));
    obj.insert("success".into(), json!(success));
    if let Some(id) = id {
        obj.insert("id".into(), json!(id));
    }
    if success {
        if let Some(data) = data {
            if !data.is_null() {
                obj.insert("data".into(), data);
            } else {
                obj.insert("data".into(), Value::Null);
            }
        }
    } else if let Some(err) = data
        && let Some(msg) = err.get("error").and_then(|v| v.as_str())
    {
        obj.insert("error".into(), json!(msg));
    }
    Value::Object(obj)
}

/// One scripted directive. Either a passthrough event (written verbatim to
/// stdout) or a `sleep` directive that delays the next event.
enum ScriptStep {
    Event(Value),
    Sleep(Duration),
}

fn load_script() -> Vec<ScriptStep> {
    let path = match env::var("FAKE_PI_SCRIPT") {
        Ok(p) if !p.is_empty() => p,
        _ => return default_script(),
    };
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(_) => return default_script(),
    };
    let mut steps = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let value: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if value.get("type").and_then(|v| v.as_str()) == Some("sleep") {
            let ms = value.get("ms").and_then(|v| v.as_u64()).unwrap_or(0);
            steps.push(ScriptStep::Sleep(Duration::from_millis(ms)));
        } else {
            steps.push(ScriptStep::Event(value));
        }
    }
    if steps.is_empty() {
        default_script()
    } else {
        steps
    }
}

fn default_script() -> Vec<ScriptStep> {
    vec![
        ScriptStep::Event(json!({"type":"agent_start"})),
        ScriptStep::Event(json!({
            "type": "agent_end",
            "messages": []
        })),
    ]
}

fn run_script<W: Write>(out: &mut W, steps: &[ScriptStep]) {
    for step in steps {
        match step {
            ScriptStep::Event(v) => emit(out, v),
            ScriptStep::Sleep(d) => std::thread::sleep(*d),
        }
    }
}

/// Read a pi session JSONL file and extract every entry's `message` field, in
/// file order. Returns an empty list on any error or missing file. Mirrors
/// what real pi does inside `get_messages` (which walks its in-memory entry
/// tree from leaf to root) but simpler: we just project `message` out of
/// every `{"type":"message", ..., "message": {...}}` entry. Skips malformed
/// lines silently — tests that need strict parsing should assert
/// pre-conditions on the seeded JSONL themselves.
fn session_messages(path: Option<&str>) -> Vec<Value> {
    let Some(path) = path else { return Vec::new() };
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if entry.get("type").and_then(|v| v.as_str()) != Some("message") {
            continue;
        }
        if let Some(message) = entry.get("message") {
            out.push(message.clone());
        }
    }
    out
}

fn mint_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("fake-{n}")
}
