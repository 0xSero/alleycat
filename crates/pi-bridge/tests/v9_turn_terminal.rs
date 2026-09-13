//! Regression coverage for terminal turn lifecycle fidelity.

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

#[tokio::test]
async fn rejected_and_aborted_prompts_close_the_emitted_turn() {
    let (state, notifications) = test_state();
    let cwd = TempDir::new().unwrap();
    let thread_id = start_thread(&state, &cwd).await;

    let error = handlers::turn::handle_turn_start(
        &state,
        p::TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![p::UserInput::Text {
                text: "FAKE_PI_REJECT: model is not running".into(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        },
    )
    .await
    .expect_err("fake Pi must reject prompt preflight");
    assert!(error.to_string().contains("model is not running"));

    let observed = drain_notifications(notifications).await;
    let started: p::TurnStartedNotification = decode_last(&observed, "turn/started");
    let completed: p::TurnCompletedNotification = decode_last(&observed, "turn/completed");
    assert_eq!(completed.thread_id, thread_id);
    assert_eq!(completed.turn.id, started.turn.id);
    assert_eq!(completed.turn.status, p::TurnStatus::Failed);
    assert_eq!(
        completed.turn.error.unwrap().message,
        "model is not running"
    );

    let script_dir = TempDir::new().unwrap();
    let script_path = write_script(
        script_dir.path(),
        &[
            json!({"type": "agent_start"}),
            json!({
                "type": "agent_end",
                "messages": [assistant_message("aborted", None)]
            }),
        ],
    );
    // The fake reads its script once at process spawn. This test is the only
    // env-mutating test in its integration-test binary.
    unsafe {
        std::env::set_var("FAKE_PI_SCRIPT", &script_path);
    }
    let (state, notifications) = test_state();
    let cwd = TempDir::new().unwrap();
    let thread_id = start_thread(&state, &cwd).await;
    unsafe {
        std::env::remove_var("FAKE_PI_SCRIPT");
    }

    let response = handlers::turn::handle_turn_start(
        &state,
        p::TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![p::UserInput::Text {
                text: "stop now".into(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        },
    )
    .await
    .expect("aborted Pi turn still passes prompt preflight");

    let observed = drain_notifications(notifications).await;
    let completed: p::TurnCompletedNotification = decode_last(&observed, "turn/completed");
    assert_eq!(completed.thread_id, thread_id);
    assert_eq!(completed.turn.id, response.turn.id);
    assert_eq!(completed.turn.status, p::TurnStatus::Interrupted);
    assert!(completed.turn.error.is_none());
}

fn test_state() -> (
    Arc<ConnectionState>,
    mpsc::UnboundedReceiver<alleycat_bridge_core::session::Sequenced>,
) {
    ConnectionState::for_test(
        Arc::new(PiPool::new(fake_pi_path())),
        Arc::new(NoopThreadIndex),
        ThreadDefaults::default(),
    )
}

async fn start_thread(state: &Arc<ConnectionState>, cwd: &TempDir) -> String {
    handlers::thread::handle_thread_start(
        state,
        p::ThreadStartParams {
            cwd: Some(cwd.path().to_string_lossy().into_owned()),
            approval_policy: Some(p::AskForApproval::Never),
            ..Default::default()
        },
    )
    .await
    .expect("thread/start")
    .thread
    .id
}

fn assistant_message(stop_reason: &str, error_message: Option<&str>) -> Value {
    let mut message = json!({
        "role": "assistant",
        "content": [],
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
        "stopReason": stop_reason,
        "timestamp": 1
    });
    if let Some(error_message) = error_message {
        message["errorMessage"] = json!(error_message);
    }
    message
}

fn decode_last<T: serde::de::DeserializeOwned>(observed: &[(String, Value)], method: &str) -> T {
    let value = observed
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == method)
        .unwrap_or_else(|| panic!("{method} notification missing"))
        .1
        .clone();
    serde_json::from_value(value).unwrap_or_else(|error| panic!("decode {method}: {error}"))
}

async fn drain_notifications(
    mut notifications: mpsc::UnboundedReceiver<alleycat_bridge_core::session::Sequenced>,
) -> Vec<(String, Value)> {
    let mut observed = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(200));
        if wait.is_zero() {
            break;
        }
        match timeout(wait, notifications.recv()).await {
            Ok(Some(frame)) => {
                let value = frame.payload;
                if value.get("id").is_some() {
                    continue;
                }
                if let Some(method) = value.get("method").and_then(Value::as_str) {
                    observed.push((
                        method.to_string(),
                        value.get("params").cloned().unwrap_or(Value::Null),
                    ));
                }
            }
            Ok(None) => break,
            Err(_) if !observed.is_empty() => break,
            Err(_) => {}
        }
    }
    observed
}
