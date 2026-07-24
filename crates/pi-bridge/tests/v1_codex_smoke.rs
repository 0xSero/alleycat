//! V1 verification matrix: codex-style end-to-end smoke covering
//! `initialize` → `thread/start` → `turn/start` against the fake-pi harness.
//!
//! ## Why in-process?
//!
//! Task #24 originally called for spawning the bridge binary and piping
//! JSON-RPC frames over stdin/stdout. At the time this test was written,
//! `crates/pi-bridge/src/main.rs` only dispatched lifecycle/account/config/
//! mcp/command-exec methods — `thread/*` and `turn/*` returned
//! `MethodNotFound`. The dispatcher wireup is tracked separately as task #32.
//!
//! Until that lands, this test exercises the same handler functions that
//! `main.rs` will dispatch into, against a real `PiPool` spawning the
//! `fake-pi` binary. The transport is the only thing skipped; everything
//! below the dispatcher (handler logic, pi spawn, event pump, codex
//! notification translation) runs end-to-end. When #32 ships, we'll add a
//! companion test that exercises the binary boundary and shares this script.

mod support;

use std::sync::Arc;
use std::time::Duration;

use alleycat_pi_bridge::codex_proto as p;
use alleycat_pi_bridge::handlers;
use alleycat_pi_bridge::pool::PiPool;
use alleycat_pi_bridge::state::{ConnectionState, ThreadDefaults};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};

use support::thread_index_stub::NoopThreadIndex;
use support::{fake_pi_path, write_script};

/// Cap on how long we wait for any single notification before declaring the
/// pipeline broken. Generous so flaky CI doesn't false-positive, but small
/// enough that a real regression surfaces fast.
const NOTIFY_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn initialize_thread_start_turn_start_smoke() {
    // The script fake-pi replays on every `prompt`. Two assistant text
    // deltas plus a clean `agent_end` so the bridge sees a full turn cycle.
    // Each event JSON has to deserialize into the bridge's `PiEvent` enum,
    // so the AssistantMessage payloads carry the strict fields it expects.
    let script_dir = TempDir::new().unwrap();
    let script_path = write_script(
        script_dir.path(),
        &[
            json!({"type": "agent_start"}),
            json!({"type": "turn_start"}),
            json!({
                "type": "message_start",
                "message": assistant_message("hello", 1)
            }),
            json!({
                "type": "message_update",
                "message": assistant_message("hello", 1),
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "hello",
                    "partial": assistant_message("hello", 1)
                }
            }),
            json!({
                "type": "message_update",
                "message": assistant_message("hello world", 1),
                "assistantMessageEvent": {
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": " world",
                    "partial": assistant_message("hello world", 1)
                }
            }),
            json!({
                "type": "message_end",
                "message": assistant_message("hello world", 1)
            }),
            json!({
                "type": "agent_end",
                "messages": [assistant_message("hello world", 1)]
            }),
        ],
    );
    // Safety: cargo serializes integration test functions within a single
    // binary; this is the only test in the file.
    unsafe {
        std::env::set_var("FAKE_PI_SCRIPT", &script_path);
    }

    // Build the bridge state. `NoopThreadIndex` is fine because this test
    // doesn't drive `thread/list` / `thread/read` — those have their own
    // verification matrix entries.
    let pool = Arc::new(PiPool::new(fake_pi_path()));
    let (state, notif_rx) = ConnectionState::for_test(
        Arc::clone(&pool),
        Arc::new(NoopThreadIndex),
        ThreadDefaults::default(),
    );

    // --- initialize -------------------------------------------------------
    let init_resp = handlers::lifecycle::handle_initialize(
        &state,
        p::InitializeParams {
            client_info: p::ClientInfo {
                name: "v1-smoke".to_string(),
                title: None,
                version: "0.0.1".to_string(),
            },
            capabilities: None,
        },
        std::path::Path::new("/tmp"),
    );
    assert!(
        !init_resp.user_agent.is_empty(),
        "initialize should report user_agent"
    );
    handlers::lifecycle::handle_initialized(&state);

    // --- thread/start -----------------------------------------------------
    let cwd = TempDir::new().unwrap();
    let start_resp = handlers::thread::handle_thread_start(
        &state,
        p::ThreadStartParams {
            cwd: Some(cwd.path().to_string_lossy().into_owned()),
            approval_policy: Some(p::AskForApproval::Never),
            ..Default::default()
        },
    )
    .await
    .expect("thread/start should succeed against fake-pi");

    let thread_id = start_resp.thread.id.clone();
    assert!(!thread_id.is_empty(), "thread_id must be non-empty");
    assert_eq!(start_resp.cwd, cwd.path().to_string_lossy());
    assert_eq!(start_resp.approval_policy, p::AskForApproval::Never);
    assert_eq!(
        state.defaults().approval_policy,
        Some(p::AskForApproval::Never),
        "turns must inherit the thread's full-access approval policy"
    );

    // --- turn/start -------------------------------------------------------
    let started_at = Instant::now();
    let turn_resp = handlers::turn::handle_turn_start(
        &state,
        p::TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![p::UserInput::Text {
                text: "hello".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        },
    )
    .await
    .expect("turn/start should succeed against fake-pi");

    assert_eq!(turn_resp.turn.status, p::TurnStatus::InProgress);
    let turn_id = turn_resp.turn.id.clone();
    assert!(!turn_id.is_empty());

    // --- drain notifications ---------------------------------------------
    let observed = drain_notifications(notif_rx, NOTIFY_TIMEOUT).await;

    // (b) `turn/started` arrives within ~100ms of the request boundary.
    let turn_started = observed
        .iter()
        .find(|(method, _)| method == "turn/started")
        .expect("turn/started notification missing");
    let turn_started_payload: p::TurnStartedNotification =
        serde_json::from_value(turn_started.1.clone()).expect("decode turn/started");
    assert_eq!(turn_started_payload.thread_id, thread_id);
    assert_eq!(turn_started_payload.turn.id, turn_id);
    assert!(
        started_at.elapsed() < Duration::from_millis(500),
        "turn/started should fire promptly; took {:?}",
        started_at.elapsed()
    );

    // (c) Two `item/agentMessage/delta` notifications carrying our scripted
    // text in order. Drop any reasoning/text_delta notifications that may
    // sneak in from translator side-effects.
    let deltas: Vec<String> = observed
        .iter()
        .filter(|(method, _)| method == "item/agentMessage/delta")
        .map(|(_, params)| {
            params
                .get("delta")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    assert_eq!(
        deltas,
        vec!["hello".to_string(), " world".to_string()],
        "expected scripted deltas in order; got {deltas:?}"
    );

    // (d) `turn/completed` arrives last (or at least after both deltas) with a
    // non-failed status.
    let completed = observed
        .iter()
        .rev()
        .find(|(method, _)| method == "turn/completed")
        .expect("turn/completed notification missing");
    let turn_completed_payload: p::TurnCompletedNotification =
        serde_json::from_value(completed.1.clone()).expect("decode turn/completed");
    assert_eq!(turn_completed_payload.thread_id, thread_id);
    assert_eq!(turn_completed_payload.turn.id, turn_id);
    assert!(
        !matches!(turn_completed_payload.turn.status, p::TurnStatus::Failed),
        "turn must not be Failed; got {:?}",
        turn_completed_payload.turn.status
    );

    // Sanity: turn/completed comes after both deltas in stream order.
    let last_delta_idx = observed
        .iter()
        .rposition(|(method, _)| method == "item/agentMessage/delta")
        .unwrap();
    let completed_idx = observed
        .iter()
        .rposition(|(method, _)| method == "turn/completed")
        .unwrap();
    assert!(
        completed_idx > last_delta_idx,
        "turn/completed must follow the last delta"
    );

    unsafe {
        std::env::remove_var("FAKE_PI_SCRIPT");
    }
}

/// Build a minimal pi `AssistantMessage` JSON value carrying `text` as a
/// single text content block. `timestamp` differentiates instances when the
/// script reuses the same text at different stream positions.
///
/// Field set must match `pool::pi_protocol::AssistantMessage` exactly — the
/// outer `AgentMessage` enum is `untagged`, so a missing field silently falls
/// to the `Other` variant and the translator drops the event.
fn assistant_message(text: &str, timestamp: i64) -> Value {
    json!({
        "role": "assistant",
        "content": [{ "type": "text", "text": text }],
        "api": "fake",
        "provider": "fake",
        "model": "fake-model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {
                "input": 0.0,
                "output": 0.0,
                "cacheRead": 0.0,
                "cacheWrite": 0.0,
                "total": 0.0
            }
        },
        "stopReason": "stop",
        "timestamp": timestamp
    })
}

/// Drain `notif_rx` until it goes idle. Returns each frame as a
/// `(method, params)` pair. Stops once no new notification has arrived for
/// `quiet_period` (200ms is plenty for the fake — pi events arrive in
/// microseconds once the writer flushes).
async fn drain_notifications(
    mut notif_rx: mpsc::UnboundedReceiver<alleycat_bridge_core::session::Sequenced>,
    overall_timeout: Duration,
) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    let deadline = Instant::now() + overall_timeout;
    let quiet_period = Duration::from_millis(200);

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        let wait = remaining.min(quiet_period);
        match timeout(wait, notif_rx.recv()).await {
            Ok(Some(seq)) => {
                let value = seq.payload;
                // Filter to notifications: requests/responses have an `id`
                // field, notifications have a method and no id.
                if value.get("id").is_some() {
                    continue;
                }
                let Some(method) = value.get("method").and_then(Value::as_str) else {
                    continue;
                };
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                out.push((method.to_string(), params));
            }
            Ok(None) => break, // sender dropped
            Err(_) => {
                // Quiet period elapsed with no new notification — assume
                // the stream has settled and we have everything.
                if !out.is_empty() {
                    break;
                }
                // Nothing yet at all — keep waiting until the overall
                // deadline so a slow first event doesn't false-fail.
            }
        }
    }
    out
}
