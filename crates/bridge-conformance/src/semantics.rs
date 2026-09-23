//! Semantic content checks for the canonical conformance scenario.
//!
//! The schema and shape-diff layers can prove a response is well formed, but
//! not that it contains useful scenario state. This module checks the parts of
//! the transcript where "empty but valid" is still a protocol regression.

use std::collections::HashMap;

use serde_json::Value;

use crate::diff::{Finding, KnownDivergence};
use crate::{Frame, FrameKind, Transcript};

#[derive(Debug, Clone)]
pub struct SemanticContext {
    pub thread_id: Option<String>,
    pub disposable_thread_id: Option<String>,
    pub forked_thread_id: Option<String>,
    pub interrupted_turn_id: Option<String>,
    pub rollback_before_turns: Option<usize>,
    pub marker_token: String,
    pub simple_prompt: String,
    pub expected_simple_reply: String,
    pub expected_command_stdout: String,
    pub streaming_process_id: Option<String>,
}

impl SemanticContext {
    pub fn new(marker_token: String, simple_prompt: String) -> Self {
        Self {
            thread_id: None,
            disposable_thread_id: None,
            forked_thread_id: None,
            interrupted_turn_id: None,
            rollback_before_turns: None,
            marker_token,
            simple_prompt,
            expected_simple_reply: "OK".to_string(),
            expected_command_stdout: "hello".to_string(),
            streaming_process_id: None,
        }
    }
}

pub fn check_all(transcript: &Transcript) -> Vec<Finding> {
    let Some(ctx) = transcript.semantic_ctx.as_ref() else {
        return Vec::new();
    };
    let mut findings = Vec::new();

    for frame in transcript.responses() {
        if response_error(frame) {
            continue;
        }
        match frame.step.as_str() {
            "initialize" => check_initialize(frame, &mut findings),
            "model/list" => check_model_list(frame, &mut findings),
            "thread/list" => check_thread_list_array(frame, &mut findings),
            "thread/list.disposable" => check_thread_list_contains(
                frame,
                ctx.disposable_thread_id.as_deref(),
                &mut findings,
            ),
            "thread/list.archived" => check_thread_list_contains_as(
                frame,
                "thread/archive",
                ctx.disposable_thread_id.as_deref(),
                &mut findings,
            ),
            "thread/list.unarchived" => check_thread_list_contains_as(
                frame,
                "thread/unarchive",
                ctx.disposable_thread_id.as_deref(),
                &mut findings,
            ),
            "thread/start" | "thread/start.disposable" => {
                check_thread_attaches(frame, &mut findings)
            }
            "thread/resume" => check_thread_id_equals(
                frame,
                "thread/resume.echoes_thread_id",
                ctx.thread_id.as_deref(),
                &mut findings,
            ),
            "thread/read" => check_thread_read_simple(frame, ctx, &mut findings),
            "thread/read.afterTool" => check_thread_read_after_tool(frame, ctx, &mut findings),
            "thread/fork" => check_thread_fork(frame, ctx, &mut findings),
            "thread/rollback" => check_thread_rollback(frame, ctx, &mut findings),
            "thread/turns/list" => {
                check_data_len_at_least(frame, "thread/turns/list.paginates", 1, &mut findings)
            }
            "thread/loaded/list" => {
                check_data_is_array(frame, "thread/loaded/list.array", &mut findings)
            }
            "command/exec" => check_command_exec(frame, ctx, &mut findings),
            "command/exec.streaming" => check_command_exec_terminated(frame, &mut findings),
            _ => {}
        }
    }

    check_turn_start_notifications(transcript, ctx, &mut findings);
    check_tool_turn_notifications(transcript, ctx, &mut findings);
    check_turn_interrupt_notifications(transcript, &mut findings);

    let div = KnownDivergence::for_target(transcript.target);
    findings
        .into_iter()
        .filter(|finding| !div.skipped_methods.contains(&finding.method()))
        .filter(|finding| !div.skips_step(finding.step()))
        .filter(|finding| !div.relaxes_semantic(finding))
        .collect()
}

fn check_initialize(frame: &Frame, findings: &mut Vec<Finding>) {
    for field in ["userAgent", "codexHome", "platformOs"] {
        if !non_empty_string(result(frame).and_then(|r| r.get(field))) {
            push(
                findings,
                frame,
                "initialize.non_empty_server_info",
                format!("result.{field} must be a non-empty string"),
            );
        }
    }
}

fn check_model_list(frame: &Frame, findings: &mut Vec<Finding>) {
    let Some(data) = result(frame)
        .and_then(|r| r.get("data"))
        .and_then(Value::as_array)
    else {
        push(
            findings,
            frame,
            "model/list.populated_catalog",
            "result.data must be an array",
        );
        return;
    };
    if data.is_empty() {
        push(
            findings,
            frame,
            "model/list.populated_catalog",
            "result.data must contain at least one model",
        );
        return;
    }
    for (idx, item) in data.iter().enumerate() {
        if !non_empty_string(item.get("id")) || !non_empty_string(item.get("displayName")) {
            push(
                findings,
                frame,
                "model/list.populated_catalog",
                format!("result.data[{idx}] must have non-empty id and displayName"),
            );
        }
    }
}

fn check_thread_list_array(frame: &Frame, findings: &mut Vec<Finding>) {
    if result(frame)
        .and_then(|r| r.get("data"))
        .and_then(Value::as_array)
        .is_none()
    {
        push(
            findings,
            frame,
            "thread/list.array_shape",
            "result.data must be an array",
        );
    }
}

fn check_thread_list_contains(frame: &Frame, wanted: Option<&str>, findings: &mut Vec<Finding>) {
    check_thread_list_contains_as(frame, frame.method.as_str(), wanted, findings);
}

fn check_thread_list_contains_as(
    frame: &Frame,
    method: &str,
    wanted: Option<&str>,
    findings: &mut Vec<Finding>,
) {
    let Some(wanted) = wanted else {
        return;
    };
    let contains = result(frame)
        .and_then(|r| r.get("data"))
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("id").and_then(Value::as_str) == Some(wanted))
        });
    if !contains {
        findings.push(Finding::SemanticViolation {
            step: frame.step.clone(),
            method: method.to_string(),
            contract: "thread/list.contains_thread".to_string(),
            detail: format!("result.data[].id must include {wanted}"),
        });
    }
}

fn check_thread_attaches(frame: &Frame, findings: &mut Vec<Finding>) {
    if thread_id(result(frame)).is_none_or(str::is_empty) {
        push(
            findings,
            frame,
            "thread/start.attaches",
            "result.thread.id must be non-empty",
        );
    }
}

fn check_thread_id_equals(
    frame: &Frame,
    contract: &str,
    wanted: Option<&str>,
    findings: &mut Vec<Finding>,
) {
    let Some(wanted) = wanted else {
        return;
    };
    let got = thread_id(result(frame));
    if got != Some(wanted) {
        push(
            findings,
            frame,
            contract,
            format!("result.thread.id was {:?}, expected {wanted:?}", got),
        );
    }
}

fn check_thread_read_simple(frame: &Frame, ctx: &SemanticContext, findings: &mut Vec<Finding>) {
    let Some(turns) = turns(result(frame)) else {
        push(
            findings,
            frame,
            "thread/read.echoes_turns",
            "result.thread.turns must be an array",
        );
        return;
    };
    if turns.is_empty() {
        push(
            findings,
            frame,
            "thread/read.echoes_turns",
            "result.thread.turns must contain at least one turn",
        );
        return;
    }
    let saw_prompt = turns
        .iter()
        .any(|turn| contains_text(turn, &ctx.simple_prompt));
    let saw_reply = turns.iter().flat_map(turn_items).any(|item| {
        is_type(item, "agentMessage") && contains_text(item, &ctx.expected_simple_reply)
    });
    if !saw_prompt && !saw_reply {
        push(
            findings,
            frame,
            "thread/read.echoes_input_or_reply",
            "thread history must include the simple prompt or an OK agent reply",
        );
    }
}

fn check_thread_read_after_tool(frame: &Frame, ctx: &SemanticContext, findings: &mut Vec<Finding>) {
    let Some(turns) = turns(result(frame)) else {
        push(
            findings,
            frame,
            "thread/read.afterTool.turns",
            "result.thread.turns must be an array",
        );
        return;
    };
    if turns.len() < 2 {
        push(
            findings,
            frame,
            "thread/read.afterTool.turns",
            "result.thread.turns must contain at least two turns",
        );
    }
    let saw_marker = turns
        .iter()
        .flat_map(turn_items)
        .any(|item| contains_text(item, &ctx.marker_token));
    if !saw_marker {
        push(
            findings,
            frame,
            "thread/read.afterTool.marker_visible",
            "thread history must include an item containing the marker token",
        );
    }
}

fn check_thread_fork(frame: &Frame, ctx: &SemanticContext, findings: &mut Vec<Finding>) {
    let got = thread_id(result(frame));
    if got.is_none_or(str::is_empty) {
        push(
            findings,
            frame,
            "thread/fork.new_id",
            "result.thread.id must be non-empty",
        );
    }
    if let (Some(got), Some(disposable)) = (got, ctx.disposable_thread_id.as_deref())
        && got == disposable
    {
        push(
            findings,
            frame,
            "thread/fork.new_id",
            "forked thread id must differ from disposable thread id",
        );
    }
}

fn check_thread_rollback(frame: &Frame, ctx: &SemanticContext, findings: &mut Vec<Finding>) {
    let Some(before) = ctx.rollback_before_turns else {
        return;
    };
    let after = turns(result(frame)).map_or(0, |turns| turns.len());
    if after >= before {
        push(
            findings,
            frame,
            "thread/rollback.drops_turns",
            format!("rollback response has {after} turns; expected fewer than {before}"),
        );
    }
}

fn check_data_len_at_least(frame: &Frame, contract: &str, min: usize, findings: &mut Vec<Finding>) {
    let len = result(frame)
        .and_then(|r| r.get("data"))
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if len < min {
        push(
            findings,
            frame,
            contract,
            format!("result.data length was {len}; expected at least {min}"),
        );
    }
}

fn check_data_is_array(frame: &Frame, contract: &str, findings: &mut Vec<Finding>) {
    if result(frame)
        .and_then(|r| r.get("data"))
        .and_then(Value::as_array)
        .is_none()
    {
        push(findings, frame, contract, "result.data must be an array");
    }
}

fn check_command_exec(frame: &Frame, ctx: &SemanticContext, findings: &mut Vec<Finding>) {
    let Some(result) = result(frame) else {
        push(
            findings,
            frame,
            "command/exec.stdout_matches",
            "missing result",
        );
        return;
    };
    let exit_code = result.get("exitCode").and_then(Value::as_i64);
    let stdout = result.get("stdout").and_then(Value::as_str).unwrap_or("");
    if exit_code != Some(0) || stdout != ctx.expected_command_stdout {
        push(
            findings,
            frame,
            "command/exec.stdout_matches",
            format!(
                "exitCode={exit_code:?}, stdout={stdout:?}; expected exitCode=0 and stdout={:?}",
                ctx.expected_command_stdout
            ),
        );
    }
}

fn check_command_exec_terminated(frame: &Frame, findings: &mut Vec<Finding>) {
    let exit_code = result(frame)
        .and_then(|r| r.get("exitCode"))
        .and_then(Value::as_i64);
    if exit_code.is_none() || exit_code == Some(0) {
        push(
            findings,
            frame,
            "command/exec/terminate.terminates_process",
            format!("terminated companion command exitCode was {exit_code:?}; expected non-zero"),
        );
    }
}

fn check_turn_start_notifications(
    transcript: &Transcript,
    ctx: &SemanticContext,
    findings: &mut Vec<Finding>,
) {
    let mut items: HashMap<String, (bool, bool, bool)> = HashMap::new();
    for frame in transcript
        .notifications()
        .filter(|frame| frame.step == "turn/start")
    {
        match frame.method.as_str() {
            "item/started" if item_type_from_notification(frame) == Some("agentMessage") => {
                if let Some(id) = item_id_from_notification(frame) {
                    items.entry(id.to_string()).or_default().0 = true;
                }
            }
            "item/agentMessage/delta" => {
                if let Some(id) = frame.raw.pointer("/params/itemId").and_then(Value::as_str) {
                    items.entry(id.to_string()).or_default().1 = true;
                }
            }
            "item/completed" if item_type_from_notification(frame) == Some("agentMessage") => {
                if let Some(id) = item_id_from_notification(frame) {
                    let completed_has_ok = frame
                        .raw
                        .pointer("/params/item")
                        .is_some_and(|item| contains_text(item, &ctx.expected_simple_reply));
                    items.entry(id.to_string()).or_default().2 |= completed_has_ok;
                }
            }
            _ => {}
        }
    }
    if !items
        .values()
        .any(|(started, delta, completed)| *started && *delta && *completed)
    {
        findings.push(Finding::SemanticViolation {
            step: "turn/start".to_string(),
            method: "turn/start".to_string(),
            contract: "turn/start.streamed_reply".to_string(),
            detail:
                "expected item/started -> item/agentMessage/delta -> item/completed containing OK"
                    .to_string(),
        });
    }
}

fn check_tool_turn_notifications(
    transcript: &Transcript,
    ctx: &SemanticContext,
    findings: &mut Vec<Finding>,
) {
    let saw_tool = transcript
        .notifications()
        .filter(|frame| frame.step == "turn/start.tool" && frame.method == "item/completed")
        .filter_map(|frame| frame.raw.pointer("/params/item"))
        .any(|item| {
            is_type(item, "commandExecution") && command_output_contains(item, &ctx.marker_token)
        });
    if !saw_tool {
        findings.push(Finding::SemanticViolation {
            step: "turn/start.tool".to_string(),
            method: "turn/start".to_string(),
            contract: "turn/start.tool_invoked".to_string(),
            detail:
                "expected a completed commandExecution item whose output contains the marker token"
                    .to_string(),
        });
    }
}

fn check_turn_interrupt_notifications(transcript: &Transcript, findings: &mut Vec<Finding>) {
    let saw_completed = transcript
        .notifications()
        .any(|frame| frame.step == "turn/interrupt" && frame.method == "turn/completed");
    if transcript
        .responses()
        .any(|frame| frame.step == "turn/interrupt")
        && !saw_completed
    {
        findings.push(Finding::SemanticViolation {
            step: "turn/interrupt".to_string(),
            method: "turn/interrupt".to_string(),
            contract: "turn/interrupt.terminates_turn".to_string(),
            detail: "expected a turn/completed notification after interrupt".to_string(),
        });
    }
}

fn result(frame: &Frame) -> Option<&Value> {
    frame.raw.get("result")
}

fn response_error(frame: &Frame) -> bool {
    frame.kind == FrameKind::Response && frame.raw.get("error").is_some()
}

fn push(findings: &mut Vec<Finding>, frame: &Frame, contract: &str, detail: impl Into<String>) {
    let (method, contract) = contract
        .split_once('.')
        .map(|(method, tail)| (method.to_string(), tail.to_string()))
        .unwrap_or_else(|| (frame.method.clone(), contract.to_string()));
    findings.push(Finding::SemanticViolation {
        step: frame.step.clone(),
        method,
        contract,
        detail: detail.into(),
    });
}

fn non_empty_string(value: Option<&Value>) -> bool {
    value.and_then(Value::as_str).is_some_and(|s| !s.is_empty())
}

fn thread_id(result: Option<&Value>) -> Option<&str> {
    result?.get("thread")?.get("id")?.as_str()
}

fn turns(result: Option<&Value>) -> Option<&Vec<Value>> {
    result?.get("thread")?.get("turns")?.as_array()
}

fn turn_items(turn: &Value) -> impl Iterator<Item = &Value> {
    turn.get("items")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

fn is_type(value: &Value, expected: &str) -> bool {
    value.get("type").and_then(Value::as_str) == Some(expected)
}

fn contains_text(value: &Value, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    match value {
        Value::String(s) => s.contains(needle),
        Value::Array(items) => items.iter().any(|item| contains_text(item, needle)),
        Value::Object(map) => map.values().any(|item| contains_text(item, needle)),
        _ => false,
    }
}

fn command_output_contains(item: &Value, needle: &str) -> bool {
    ["aggregatedOutput", "stdout", "stderr", "output"]
        .iter()
        .any(|key| {
            item.get(*key)
                .is_some_and(|value| contains_text(value, needle))
        })
        || contains_text(item, needle)
}

fn item_type_from_notification(frame: &Frame) -> Option<&str> {
    frame
        .raw
        .pointer("/params/item/type")
        .and_then(Value::as_str)
}

fn item_id_from_notification(frame: &Frame) -> Option<&str> {
    frame.raw.pointer("/params/item/id").and_then(Value::as_str)
}
