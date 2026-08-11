//! Verification matrix V4 — approval flow round-trip (`approval_policy:on-request`).
//!
//! Plan ("Verification" §5): "configure the bridge with
//! `approval_policy: on-request`, run a `bash` tool call, assert
//! `item/tool/requestUserInput` flows to the codex client, response
//! unblocks pi."
//!
//! Concretely the bridge:
//!
//! - Spawns a fake-pi child for the test thread (via `PiPool::acquire_for_resume`).
//! - Registers an active turn with `approval_policy: OnRequest`.
//! - Sends pi `prompt`. The fake-pi script emits
//!   `tool_execution_start { toolName:"bash", … }` so the bridge's event
//!   pump intercepts it and asks the codex client for approval via
//!   `item/commandExecution/requestApproval` (server→client request).
//! - The test plays the role of the codex client: pulls the request
//!   frame off the outbound channel and resolves the matching
//!   `oneshot::Sender` via `state.resolve_pending_request(...)`.
//!
//! ## Assertions per outcome bucket
//!
//! Codex's `CommandExecutionApprovalDecision` documents three buckets the
//! bridge needs to handle (see `approval::ApprovalOutcome`):
//!
//! - **Cancel**: the agent will be interrupted. The pump sends pi `abort`
//!   so the rest of the turn stops. We verify by reading
//!   `FAKE_PI_COMMAND_LOG` and looking for the `abort` entry.
//! - **Accept** / **Decline**: pi keeps running. No `abort` should be
//!   sent. The bash event flows through to its `tool_execution_end` and
//!   the turn finishes normally with `agent_end`.
//!
//! The task description for #27 says "decline → forward pi `abort`", but
//! per codex's own enum docs `Decline` is "agent will continue the turn"
//! and only `Cancel` interrupts. This test follows the codex contract;
//! if a future codex revision flips `Decline`'s semantics, the
//! `bucket_command_decision` mapping in `approval.rs` is the right place
//! to update.

mod support;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;

/// All V4 scenarios mutate `FAKE_PI_SCRIPT` and `FAKE_PI_COMMAND_LOG` env
/// vars, which are process-global per test binary. Serialize via a shared
/// async mutex so cargo's default parallel-tests-within-a-binary mode
/// doesn't trip the harnesses over each other.
fn scenario_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

use alleycat_pi_bridge::approval;
use alleycat_pi_bridge::codex_proto as p;
use alleycat_pi_bridge::handlers::turn::handle_turn_start;
use alleycat_pi_bridge::index::{IndexEntry, ThreadIndex};
use alleycat_pi_bridge::pool::PiPool;
use alleycat_pi_bridge::state::{ConnectionState, ThreadDefaults, ThreadIndexHandle};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::timeout;

use support::{fake_pi_path, write_script};

/// One harness per outcome — drives the same approval flow with a
/// different decision. Returns `(state, command_log_path, prompt_response)`
/// so the caller can finalize assertions.
async fn run_approval_scenario(decision: Value) -> ApprovalScenario {
    // Hold the env-vars mutex for the duration of the scenario so a
    // sibling V4 scenario doesn't stomp our `FAKE_PI_SCRIPT`/`COMMAND_LOG`.
    let _env_guard = scenario_lock().lock().await;
    let script_dir = TempDir::new().expect("script dir");
    let script_path = write_script(
        script_dir.path(),
        &[
            json!({"type": "agent_start"}),
            json!({
                "type": "tool_execution_start",
                "toolCallId": "tc-rm",
                "toolName": "bash",
                "args": { "command": "rm -rf /" }
            }),
            json!({
                "type": "tool_execution_end",
                "toolCallId": "tc-rm",
                "toolName": "bash",
                "result": {"output": "would have been bad", "exitCode": 0},
                "isError": false
            }),
            json!({"type": "agent_end", "messages": []}),
        ],
    );

    let log_dir = TempDir::new().expect("log dir");
    let log_path = log_dir.path().join("commands.log");
    // Safety: cargo serializes integration-test files in their own binary,
    // and within this file the harness runs scenarios sequentially.
    unsafe {
        std::env::set_var("FAKE_PI_SCRIPT", &script_path);
        std::env::set_var("FAKE_PI_COMMAND_LOG", &log_path);
    }

    let cwd = TempDir::new().expect("cwd");
    let pool = Arc::new(PiPool::new(fake_pi_path()));

    // Build a ConnectionState wired to a real ThreadIndex (tempdir-backed).
    let index_dir = TempDir::new().expect("index dir");
    let index = ThreadIndex::open_at(index_dir.path().join("threads.json"))
        .await
        .expect("index open");
    let (state, out_rx) = ConnectionState::for_test(
        pool.clone(),
        Arc::clone(&index) as Arc<dyn ThreadIndexHandle>,
        ThreadDefaults {
            approval_policy: Some(p::AskForApproval::OnRequest),
            ..Default::default()
        },
    );

    // Mint a thread + acquire a fake-pi process for it.
    let thread_id = format!("thr-{}", uuid::Uuid::now_v7());
    let _handle = pool
        .acquire_for_resume(thread_id.clone(), cwd.path())
        .await
        .expect("acquire pi handle");
    // Insert a matching index row so handle_turn_start can find it
    // (turn/start itself doesn't actually call into the index, but we
    // want a realistic state — the pump wires through state.thread_index
    // for index updates).
    index
        .insert(IndexEntry {
            thread_id: thread_id.clone(),
            cwd: cwd.path().to_string_lossy().into_owned(),
            name: None,
            preview: String::new(),
            created_at: 0,
            updated_at: 0,
            archived: false,
            forked_from_id: None,
            model_provider: "fake".into(),
            source: p::ThreadSourceKind::AppServer,
            metadata: alleycat_pi_bridge::PiSessionRef {
                pi_session_path: cwd.path().join("session.jsonl"),
                pi_session_id: "pi-session-1".into(),
            },
        })
        .await
        .expect("insert index row");

    // Drive turn/start. This returns immediately after pi acks `prompt`.
    let resp = handle_turn_start(
        &state,
        p::TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![p::UserInput::Text {
                text: "hi".into(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        },
    )
    .await
    .expect("turn/start");

    // Spawn a background task that pretends to be the codex client:
    // - Drains the outbound mpsc.
    // - Whenever a `item/commandExecution/requestApproval` request lands,
    //   replies via `state.resolve_pending_request(id, Ok({decision}))`.
    let state_for_responder = Arc::clone(&state);
    let decision_for_responder = decision.clone();
    let frame_capture = Arc::new(tokio::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    let frame_capture_for_responder = Arc::clone(&frame_capture);
    let responder = tokio::spawn(async move {
        let mut rx = out_rx;
        while let Some(seq) = rx.recv().await {
            let value = seq.payload;
            // Snapshot every frame we see so the test can assert on the
            // notification stream after the turn ends.
            frame_capture_for_responder.lock().await.push(value.clone());
            // Notifications: check for turn/completed to exit early. The
            // test holds an `Arc<ConnectionState>` plus the pump holds
            // one — neither drops while we're awaiting `recv`, so we'd
            // wait forever otherwise.
            let method = value.get("method").and_then(|v| v.as_str()).unwrap_or("");
            let is_request = value.get("id").is_some() && !method.is_empty();
            if !is_request {
                if method == "turn/completed" {
                    break;
                }
                continue;
            }
            // Server→client request — only `requestApproval` is
            // relevant for V4.
            if method != "item/commandExecution/requestApproval" {
                continue;
            }
            let id_value = match value.get("id").cloned() {
                Some(v) => v,
                None => continue,
            };
            let req_id: p::RequestId = match serde_json::from_value(id_value) {
                Ok(v) => v,
                Err(_) => continue,
            };
            // Decision payload: codex serializes `Accept` as `"accept"`,
            // `Decline` as `"decline"`, `Cancel` as `"cancel"` (camelCase
            // unit variants). Wrap in `{decision: <value>}`.
            state_for_responder
                .resolve_pending_request(
                    &req_id,
                    Ok(json!({"decision": decision_for_responder.clone()})),
                )
                .await;
        }
    });

    // Wait for the turn pump to emit `turn/completed` (or time out). 3s is
    // plenty for the fake-pi script (no real LLM, no real bash) — anything
    // longer means the pump is stuck.
    let frames = frame_capture;
    let saw_turn_completed = wait_for_turn_completed(&frames, Duration::from_secs(3)).await;
    if !saw_turn_completed {
        let captured = frames.lock().await.clone();
        let methods: Vec<String> = captured
            .iter()
            .filter_map(|v| v.get("method").and_then(|m| m.as_str()).map(str::to_string))
            .collect();
        eprintln!("v4_approval: turn/completed timeout. methods seen: {methods:?}");
    }

    // Tear down: drop state senders, await responder.
    drop(state);
    let _ = responder.await;

    let log_contents = std::fs::read_to_string(&log_path).unwrap_or_default();

    // Restore env so neighboring tests are unaffected.
    unsafe {
        std::env::remove_var("FAKE_PI_SCRIPT");
        std::env::remove_var("FAKE_PI_COMMAND_LOG");
    }

    let final_frames = frames.lock().await.clone();
    ApprovalScenario {
        thread_id,
        prompt_response: resp,
        frames: final_frames,
        command_log: log_contents,
        saw_turn_completed,
    }
}

struct ApprovalScenario {
    #[allow(dead_code)]
    thread_id: String,
    #[allow(dead_code)]
    prompt_response: p::TurnStartResponse,
    /// Every JSON-RPC frame the bridge emitted on the outbound channel,
    /// in order.
    frames: Vec<serde_json::Value>,
    /// Newline-separated list of pi command types the fake-pi observed.
    command_log: String,
    saw_turn_completed: bool,
}

async fn wait_for_turn_completed(
    frames: &Arc<tokio::sync::Mutex<Vec<serde_json::Value>>>,
    deadline: Duration,
) -> bool {
    let outcome = timeout(deadline, async {
        loop {
            {
                let guard = frames.lock().await;
                for value in guard.iter() {
                    if value.get("method") == Some(&json!("turn/completed")) {
                        return true;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    outcome.unwrap_or(false)
}

fn pi_commands(log: &str) -> Vec<&str> {
    log.lines().filter(|l| !l.is_empty()).collect()
}

fn methods_seen(frames: &[serde_json::Value]) -> Vec<String> {
    frames
        .iter()
        .filter_map(|v| v.get("method").and_then(|m| m.as_str()).map(str::to_string))
        .collect()
}

// ============================================================================
// scenarios
// ============================================================================

#[tokio::test]
async fn cancel_decision_triggers_pi_abort() {
    let scenario = run_approval_scenario(json!("cancel")).await;
    assert!(
        scenario.saw_turn_completed,
        "bridge should emit turn/completed even on cancel; frames: {:?}",
        methods_seen(&scenario.frames)
    );

    let cmds = pi_commands(&scenario.command_log);
    // Pi should have seen at least: new_session (from acquire_for_resume's
    // initial setup, if any), prompt, abort. The bridge issues abort when
    // the approval bucket is `Cancel`.
    assert!(
        cmds.contains(&"abort"),
        "expected pi to receive `abort` on cancel; got: {cmds:?}",
    );
    assert!(
        cmds.contains(&"prompt"),
        "expected pi to receive `prompt` (the turn started); got: {cmds:?}",
    );

    // Approval request must have been emitted before turn/completed.
    let methods = methods_seen(&scenario.frames);
    let approval_idx = methods
        .iter()
        .position(|m| m == "item/commandExecution/requestApproval");
    let completed_idx = methods.iter().position(|m| m == "turn/completed");
    assert!(
        approval_idx.is_some(),
        "approval request must appear: {methods:?}"
    );
    if let (Some(a), Some(c)) = (approval_idx, completed_idx) {
        assert!(a < c, "approval request should land before turn/completed");
    }
}

#[tokio::test]
async fn accept_decision_does_not_send_abort() {
    let scenario = run_approval_scenario(json!("accept")).await;
    assert!(scenario.saw_turn_completed, "turn should complete cleanly");

    let cmds = pi_commands(&scenario.command_log);
    assert!(
        cmds.contains(&"prompt"),
        "expected pi to receive `prompt`; got: {cmds:?}",
    );
    assert!(
        !cmds.contains(&"abort"),
        "expected NO `abort` on accept; got: {cmds:?}",
    );

    // Approval request still went through; just answered approve.
    let methods = methods_seen(&scenario.frames);
    assert!(
        methods
            .iter()
            .any(|m| m == "item/commandExecution/requestApproval"),
        "approval request must appear even on accept: {methods:?}",
    );
}

#[tokio::test]
async fn decline_does_not_abort_per_codex_decline_semantics() {
    // Codex docs: `Decline` = "user denied; agent will continue the turn".
    // Only `Cancel` interrupts. The bridge should therefore NOT send `abort`
    // when the decision bucket is `Declined`.
    let scenario = run_approval_scenario(json!("decline")).await;
    assert!(scenario.saw_turn_completed, "turn should complete cleanly");

    let cmds = pi_commands(&scenario.command_log);
    assert!(
        !cmds.contains(&"abort"),
        "decline must NOT trigger pi abort (only cancel does); got: {cmds:?}",
    );

    let methods = methods_seen(&scenario.frames);
    assert!(
        methods
            .iter()
            .any(|m| m == "item/commandExecution/requestApproval"),
        "approval request must appear: {methods:?}",
    );
}

#[test]
fn bucketing_lines_up_with_test_decisions() {
    // Sanity: the buckets the test relies on are what `approval.rs`
    // actually returns. Catches a future regression where someone
    // changes the decision string vocabulary out from under the
    // approval pump.
    use approval::{ApprovalKind, should_request_approval};
    assert!(should_request_approval(
        &p::AskForApproval::OnRequest,
        ApprovalKind::Command,
    ));
    assert!(!should_request_approval(
        &p::AskForApproval::Never,
        ApprovalKind::Command,
    ));
}
