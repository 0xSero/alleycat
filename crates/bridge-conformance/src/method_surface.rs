//! Runtime method-surface probing.
//!
//! The old integration test checked bridge source files with `source.contains`.
//! This module keeps the standard Codex request-method inventory in one place
//! and evaluates support from real JSON-RPC responses instead.

use serde_json::{Value, json};

use crate::diff::{Finding, KnownDivergence};
use crate::{Frame, FrameKind, TargetId};

pub const STANDARD_REQUEST_METHODS: &[&str] = &[
    "account/read",
    "account/rateLimits/read",
    "account/login/start",
    "account/login/cancel",
    "account/logout",
    "feedback/upload",
    "config/read",
    "config/value/write",
    "config/batchWrite",
    "configRequirements/read",
    "mcpServerStatus/list",
    "config/mcpServer/reload",
    "mcpServer/oauth/login",
    "mock/experimentalMethod",
    "experimentalFeature/list",
    "collaborationMode/list",
    "model/list",
    "skills/list",
    "skills/remote/list",
    "skills/remote/export",
    "skills/config/write",
    "command/exec",
    "command/exec/terminate",
    "command/exec/write",
    "command/exec/resize",
    "thread/start",
    "thread/resume",
    "thread/fork",
    "thread/archive",
    "thread/unarchive",
    "thread/name/set",
    "thread/compact/start",
    "thread/rollback",
    "thread/list",
    "thread/loaded/list",
    "thread/read",
    "thread/turns/list",
    "thread/backgroundTerminals/clean",
    "turn/start",
    "turn/steer",
    "turn/interrupt",
    "review/start",
];

#[derive(Debug, Clone, Default)]
pub struct ProbeContext {
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub cwd: Option<String>,
    pub process_id: Option<String>,
}

pub fn min_params_for(method: &str) -> Value {
    min_params_for_with(method, &ProbeContext::default())
}

pub fn min_params_for_with(method: &str, ctx: &ProbeContext) -> Value {
    let thread_id = ctx
        .thread_id
        .as_deref()
        .unwrap_or("bridge-conformance-thread");
    let turn_id = ctx.turn_id.as_deref().unwrap_or("bridge-conformance-turn");
    let process_id = ctx
        .process_id
        .as_deref()
        .unwrap_or("bridge-conformance-process");
    let cwd = ctx.cwd.as_deref().unwrap_or(".");

    match method {
        "config/value/write" => json!({
            "keyPath": "bridge_conformance.probe",
            "value": true,
            "mergeStrategy": "replace",
        }),
        "config/batchWrite" => json!({
            "edits": [{
                "keyPath": "bridge_conformance.probe",
                "value": true,
                "mergeStrategy": "replace",
            }],
        }),
        "account/login/start" => json!({
            "type": "apiKey",
            "apiKey": "bridge-conformance-probe",
        }),
        "account/login/cancel" => json!({"loginId": "bridge-conformance-login"}),
        "feedback/upload" => json!({"classification": "other"}),
        "mcpServer/oauth/login" => json!({"name": "bridge-conformance-mcp"}),
        "skills/config/write" => json!({"enabled": true}),
        "command/exec" => json!({"command": ["sh", "-c", "printf probe"]}),
        "command/exec/terminate" => json!({"processId": process_id}),
        "command/exec/write" => json!({
            "processId": process_id,
            "deltaBase64": "cHJvYmU=",
            "closeStdin": false,
        }),
        "command/exec/resize" => json!({
            "processId": process_id,
            "size": {"rows": 24, "cols": 80},
        }),
        "thread/start" => json!({
            "cwd": cwd,
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }),
        "thread/resume" => json!({"threadId": thread_id}),
        "thread/fork" => json!({"threadId": thread_id, "cwd": cwd}),
        "thread/archive" | "thread/unarchive" | "thread/compact/start" => {
            json!({"threadId": thread_id})
        }
        "thread/name/set" => json!({"threadId": thread_id, "name": "conformance-probe"}),
        "thread/rollback" => json!({"threadId": thread_id, "numTurns": 1}),
        "thread/list" => json!({"archived": false, "cwd": cwd}),
        "thread/read" => json!({"threadId": thread_id, "includeTurns": true}),
        "thread/turns/list" => json!({"threadId": thread_id, "limit": 1}),
        "thread/backgroundTerminals/clean" => json!({"threadId": thread_id}),
        "turn/start" => json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "Reply OK."}],
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }),
        "turn/steer" => json!({
            "threadId": thread_id,
            "expectedTurnId": turn_id,
            "input": [{"type": "text", "text": "continue"}],
        }),
        "turn/interrupt" => json!({"threadId": thread_id, "turnId": turn_id}),
        "review/start" => json!({
            "threadId": thread_id,
            "target": {"type": "uncommittedChanges"},
        }),
        _ => json!({}),
    }
}

pub fn assert_method_response(frame: &Frame, target: TargetId) -> Option<Finding> {
    let div = KnownDivergence::for_target(target);
    let error = frame.raw.get("error")?;
    let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("(no message)")
        .to_string();
    if div.skipped_methods.contains(&frame.method.as_str()) {
        return None;
    }
    Some(Finding::UnexpectedError {
        step: frame.step.clone(),
        method: frame.method.clone(),
        code,
        message,
    })
}

pub fn response_frame(step: &str, method: &str, raw: Value) -> Frame {
    Frame {
        step: step.to_string(),
        method: method.to_string(),
        kind: FrameKind::Response,
        raw,
    }
}
