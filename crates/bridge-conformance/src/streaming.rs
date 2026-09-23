//! Per-target streaming-lifecycle invariants.
//!
//! Schema and key-fingerprint checks miss bugs like "deltas use a different
//! `itemId` than the bracketing `item/started`/`item/completed`" because the
//! frames individually conform to the schema. This module walks each turn's
//! notification stream and validates the cross-frame lifecycle:
//!
//!   * Every `item/started` for a streamable item kind is followed by zero or
//!     more deltas with `itemId` equal to the started item's `id`, then by an
//!     `item/completed` for that same id.
//!   * Final `item/completed` payload's text equals the concatenation of all
//!     the deltas the bridge emitted for that item (the "final message after
//!     deltas are done" contract).
//!   * No deltas appear with an `itemId` that has no matching `item/started`.
//!
//! Findings are reported through the same [`crate::diff::Finding`] / report
//! mechanism the schema/diff layers use.

use std::collections::HashMap;

use serde_json::Value;

use crate::diff::{Finding, KnownDivergence};
use crate::{Frame, FrameKind, Transcript};

/// Run the streaming-lifecycle check on every notification in `transcript`.
/// Returns one [`Finding`] per anomaly.
pub fn check(transcript: &Transcript) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut state: HashMap<String, ItemState> = HashMap::new();

    for frame in &transcript.frames {
        if frame.kind != FrameKind::Notification {
            continue;
        }
        match frame.method.as_str() {
            "item/started" => on_started(&mut state, frame),
            "item/completed" => on_completed(&mut state, frame, &mut findings),
            "item/agentMessage/delta" => on_delta(
                &mut state,
                frame,
                "agentMessage",
                ItemKindGate::AgentMessage,
                &mut findings,
            ),
            "item/reasoning/textDelta" => on_delta(
                &mut state,
                frame,
                "reasoning.text",
                ItemKindGate::Reasoning,
                &mut findings,
            ),
            _ => {}
        }
    }

    // Anything still open at end-of-stream is an unbracketed item.
    for (item_id, st) in state {
        if !st.completed {
            findings.push(Finding::SchemaError {
                step: st.step.clone(),
                method: "item/started".into(),
                kind: FrameKind::Notification,
                message: format!(
                    "item id={item_id} type={kind:?} started but never `item/completed`",
                    kind = st.kind,
                ),
            });
        }
    }

    let div = KnownDivergence::for_target(transcript.target);
    findings
        .into_iter()
        .filter(|finding| !div.relaxes_streaming(finding))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ItemKind {
    AgentMessage,
    Reasoning,
}

#[derive(Debug, Clone, Copy)]
enum ItemKindGate {
    AgentMessage,
    Reasoning,
}

#[derive(Debug)]
struct ItemState {
    step: String,
    kind: ItemKind,
    accumulated_text: String,
    completed: bool,
}

fn on_started(state: &mut HashMap<String, ItemState>, frame: &Frame) {
    let item = match frame.raw.pointer("/params/item") {
        Some(item) => item,
        None => return,
    };
    let id = match item.get("id").and_then(Value::as_str) {
        Some(id) => id.to_string(),
        None => return,
    };
    let kind = match item.get("type").and_then(Value::as_str) {
        Some("agentMessage") => ItemKind::AgentMessage,
        Some("reasoning") => ItemKind::Reasoning,
        _ => return,
    };
    state.insert(
        id,
        ItemState {
            step: frame.step.clone(),
            kind,
            accumulated_text: String::new(),
            completed: false,
        },
    );
}

fn on_completed(
    state: &mut HashMap<String, ItemState>,
    frame: &Frame,
    findings: &mut Vec<Finding>,
) {
    let item = match frame.raw.pointer("/params/item") {
        Some(item) => item,
        None => return,
    };
    let id = match item.get("id").and_then(Value::as_str) {
        Some(id) => id.to_string(),
        None => return,
    };
    let final_text = item.get("text").and_then(Value::as_str).map(str::to_string);
    let final_content = item.get("content").and_then(Value::as_array).map(|arr| {
        arr.iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("")
    });
    let completed_kind = match item.get("type").and_then(Value::as_str) {
        Some("agentMessage") => Some(ItemKind::AgentMessage),
        Some("reasoning") => Some(ItemKind::Reasoning),
        _ => None,
    };

    match state.get_mut(&id) {
        None if completed_kind.is_some() => findings.push(Finding::SchemaError {
            step: frame.step.clone(),
            method: "item/completed".into(),
            kind: FrameKind::Notification,
            message: format!("item/completed for id={id} with no matching item/started"),
        }),
        None => {}
        Some(st) => {
            st.completed = true;
            // Verify "final message after deltas" contract.
            match (st.kind, final_text.as_deref(), final_content.as_deref()) {
                (ItemKind::AgentMessage, Some(text), _) => {
                    if st.accumulated_text.is_empty() {
                        findings.push(Finding::SchemaError {
                            step: frame.step.clone(),
                            method: "item/completed".into(),
                            kind: FrameKind::Notification,
                            message: format!("agentMessage item={id} produced no deltas"),
                        });
                    }
                    if text != st.accumulated_text {
                        findings.push(Finding::SchemaError {
                            step: frame.step.clone(),
                            method: "item/completed".into(),
                            kind: FrameKind::Notification,
                            message: format!(
                                "agentMessage final text {text:?} does not match concatenated deltas {acc:?}",
                                acc = st.accumulated_text,
                            ),
                        });
                    }
                }
                (ItemKind::Reasoning, _, Some(text)) => {
                    if !st.accumulated_text.is_empty() && text != st.accumulated_text {
                        findings.push(Finding::SchemaError {
                            step: frame.step.clone(),
                            method: "item/completed".into(),
                            kind: FrameKind::Notification,
                            message: format!(
                                "reasoning final content {text:?} does not match concatenated deltas {acc:?}",
                                acc = st.accumulated_text,
                            ),
                        });
                    }
                }
                _ => {}
            }
        }
    }
}

fn on_delta(
    state: &mut HashMap<String, ItemState>,
    frame: &Frame,
    label: &'static str,
    expected: ItemKindGate,
    findings: &mut Vec<Finding>,
) {
    let item_id = match frame.raw.pointer("/params/itemId").and_then(Value::as_str) {
        Some(id) => id.to_string(),
        None => return,
    };
    let delta = frame
        .raw
        .pointer("/params/delta")
        .and_then(Value::as_str)
        .unwrap_or("");
    match state.get_mut(&item_id) {
        None => findings.push(Finding::SchemaError {
            step: frame.step.clone(),
            method: format!("item/{label}/delta"),
            kind: FrameKind::Notification,
            message: format!("delta for itemId={item_id} arrived with no matching item/started"),
        }),
        Some(st) => {
            let kind_ok = matches!(
                (expected, st.kind),
                (ItemKindGate::AgentMessage, ItemKind::AgentMessage)
                    | (ItemKindGate::Reasoning, ItemKind::Reasoning)
            );
            if !kind_ok {
                findings.push(Finding::SchemaError {
                    step: frame.step.clone(),
                    method: format!("item/{label}/delta"),
                    kind: FrameKind::Notification,
                    message: format!(
                        "delta for itemId={item_id} expected kind {expected:?} but item/started recorded {actual:?}",
                        actual = st.kind,
                    ),
                });
            } else {
                st.accumulated_text.push_str(delta);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TargetId;
    use serde_json::json;

    fn frame(step: &str, method: &str, raw: Value) -> Frame {
        Frame {
            step: step.to_string(),
            method: method.to_string(),
            kind: FrameKind::Notification,
            raw,
        }
    }

    fn started(id: &str, kind: &str) -> Frame {
        frame(
            "turn/start",
            "item/started",
            json!({"params":{"item":{"id": id, "type": kind, "text": ""}}}),
        )
    }

    fn delta(method: &str, item_id: &str, delta_text: &str) -> Frame {
        frame(
            "turn/start",
            method,
            json!({"params":{"itemId": item_id, "delta": delta_text}}),
        )
    }

    fn completed(id: &str, kind: &str, body: Value) -> Frame {
        let mut item = body;
        item["id"] = json!(id);
        item["type"] = json!(kind);
        frame(
            "turn/start",
            "item/completed",
            json!({"params":{"item": item}}),
        )
    }

    #[test]
    fn agent_message_clean_lifecycle() {
        let mut t = Transcript::new(TargetId::Pi);
        t.push(started("m1", "agentMessage"));
        t.push(delta("item/agentMessage/delta", "m1", "O"));
        t.push(delta("item/agentMessage/delta", "m1", "K"));
        t.push(completed("m1", "agentMessage", json!({"text":"OK"})));
        let findings = check(&t);
        assert!(findings.is_empty(), "{findings:#?}");
    }

    #[test]
    fn delta_with_wrong_item_id_is_flagged() {
        let mut t = Transcript::new(TargetId::Opencode);
        t.push(started("m1", "agentMessage"));
        t.push(delta("item/agentMessage/delta", "p1", "OK")); // wrong id
        t.push(completed("m1", "agentMessage", json!({"text":"OK"})));
        let findings = check(&t);
        assert!(
            findings.iter().any(|f| matches!(
                f, Finding::SchemaError { message, .. }
                if message.contains("itemId=p1") && message.contains("no matching item/started")
            )),
            "{findings:#?}"
        );
    }

    #[test]
    fn reasoning_without_started_is_flagged() {
        let mut t = Transcript::new(TargetId::Opencode);
        t.push(delta("item/reasoning/textDelta", "p1", "Got it"));
        let findings = check(&t);
        assert!(!findings.is_empty());
    }

    #[test]
    fn final_text_mismatch_is_flagged() {
        let mut t = Transcript::new(TargetId::Pi);
        t.push(started("m1", "agentMessage"));
        t.push(delta("item/agentMessage/delta", "m1", "OK"));
        t.push(completed("m1", "agentMessage", json!({"text":"DIFFERENT"})));
        let findings = check(&t);
        assert!(
            findings.iter().any(|f| matches!(
                f, Finding::SchemaError { message, .. }
                if message.contains("final text")
            )),
            "{findings:#?}"
        );
    }

    #[test]
    fn agent_message_without_delta_is_flagged() {
        let mut t = Transcript::new(TargetId::Pi);
        t.push(started("m1", "agentMessage"));
        t.push(completed("m1", "agentMessage", json!({"text":"OK"})));
        let findings = check(&t);
        assert!(
            findings.iter().any(|f| matches!(
                f, Finding::SchemaError { message, .. }
                if message.contains("produced no deltas")
            )),
            "{findings:#?}"
        );
    }
}
