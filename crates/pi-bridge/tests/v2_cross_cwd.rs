//! Verification matrix V2 — cross-cwd concurrent threads.
//!
//! From the bridge plan ("Verification" §3): "open two `thread/start { cwd:/tmp/a }`
//! and `thread/start { cwd:/tmp/b }` concurrently. Verify two pi processes
//! exist, each in its own cwd, both turns stream simultaneously."
//!
//! This test exercises the `PiPool` directly (skipping the codex JSON-RPC
//! frontend) since handlers' `thread/start` is still in flight — the contract
//! we're verifying is the pool layer's "one pi per thread, each pinned to its
//! cwd" guarantee. When `handlers/thread.rs` lands, an end-to-end variant
//! that drives the same scenario via codex JSON-RPC can sit alongside this.
//!
//! Assertions:
//! - **(a) Concurrent streaming**: both handles emit their scripted
//!   `agent_start` events while the *other* handle is still mid-script (i.e.
//!   the second `agent_start` lands before the first `agent_end`). This is
//!   the only way to observe "interleaved item/started boundaries" without
//!   wiring up the events translator + handlers.
//! - **(b) Pool bookkeeping**: after both acquires resolve,
//!   `pool.loaded_thread_ids().len() == 2` and the two ids are distinct.
//! - **(c) cwd binding**: each pi child sees its requested cwd. We exploit
//!   the test-only `bash {command:"pwd"}` shortcut in `fake-pi` which echoes
//!   `std::env::current_dir()` instead of running the literal command.

mod support;

use std::time::{Duration, Instant};

use alleycat_pi_bridge::pool::PiPool;
use alleycat_pi_bridge::pool::pi_protocol::{BashCmd, PiEvent, PromptCmd, RpcCommand};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use support::{fake_pi_path, write_script};

/// Resolve a tempdir to its canonical absolute form. macOS likes to expose
/// `/var/folders/...` which the kernel reports as `/private/var/folders/...`
/// from `getcwd()`. Comparing canonical paths sidesteps the symlink jitter.
fn canonical(p: &std::path::Path) -> std::path::PathBuf {
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

#[tokio::test]
async fn two_threads_in_different_cwds_run_concurrently_and_pin_their_cwd() {
    // Each `thread/start` script: agent_start → 200ms sleep → agent_end.
    // The 200ms gap is wide enough that if the pool serialized acquires (or
    // pi processes ran their `prompt` script back-to-back instead of in
    // parallel), the second `agent_start` would only arrive *after* the
    // first `agent_end`. The interleaving assertion catches that.
    let script_dir = TempDir::new().unwrap();
    let script_path = write_script(
        script_dir.path(),
        &[
            json!({"type": "agent_start"}),
            json!({"type": "sleep", "ms": 200}),
            json!({"type": "agent_end", "messages": []}),
        ],
    );
    // Safety: the test runs in a single integration binary, cargo serializes
    // tests in this file by default, and we restore the env after the spawns.
    unsafe {
        std::env::set_var("FAKE_PI_SCRIPT", &script_path);
    }

    let cwd_a = TempDir::new().unwrap();
    let cwd_b = TempDir::new().unwrap();

    let pool = PiPool::with_limits(fake_pi_path(), 2, Duration::from_secs(60));

    // Spawn both threads in parallel. tokio::join! polls them on the same
    // task, but the underlying spawn is async I/O — both children should be
    // up and bookkept well before either responds to a command.
    let (a, b) = tokio::join!(
        pool.acquire_for_new_thread(cwd_a.path()),
        pool.acquire_for_new_thread(cwd_b.path()),
    );
    let (thread_a, handle_a) = a.expect("acquire A");
    let (thread_b, handle_b) = b.expect("acquire B");
    assert_ne!(thread_a, thread_b, "thread ids must be distinct");

    // Now that the pool has stable handles, env mutation is safe to undo.
    unsafe {
        std::env::remove_var("FAKE_PI_SCRIPT");
    }

    // (b) Pool sees two loaded threads.
    let loaded = pool.loaded_thread_ids().await;
    assert_eq!(loaded.len(), 2, "both threads tracked");
    let mut sorted = loaded.clone();
    sorted.sort();
    let mut expected = vec![thread_a.clone(), thread_b.clone()];
    expected.sort();
    assert_eq!(sorted, expected);

    // by_cwd index must list each thread under its own cwd, and only its own.
    let in_a = pool.threads_for_cwd(cwd_a.path()).await;
    let in_b = pool.threads_for_cwd(cwd_b.path()).await;
    assert_eq!(in_a, vec![thread_a.clone()]);
    assert_eq!(in_b, vec![thread_b.clone()]);

    // (c) cwd binding — ask each fake-pi child to pwd. The fake echoes its
    // own `std::env::current_dir()` for the `pwd` shortcut.
    let pwd_a = handle_a
        .send_request(RpcCommand::Bash(BashCmd {
            id: None,
            command: "pwd".to_string(),
        }))
        .await
        .expect("bash A");
    let pwd_b = handle_b
        .send_request(RpcCommand::Bash(BashCmd {
            id: None,
            command: "pwd".to_string(),
        }))
        .await
        .expect("bash B");
    let reported_a = pwd_a
        .data
        .as_ref()
        .and_then(|d| d.get("output"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("");
    let reported_b = pwd_b
        .data
        .as_ref()
        .and_then(|d| d.get("output"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("");
    assert_eq!(
        canonical(std::path::Path::new(reported_a)),
        canonical(cwd_a.path()),
        "pi A should be pinned to cwd_a"
    );
    assert_eq!(
        canonical(std::path::Path::new(reported_b)),
        canonical(cwd_b.path()),
        "pi B should be pinned to cwd_b"
    );
    assert_ne!(reported_a, reported_b, "the two pis must not share a cwd");

    // (a) Concurrent streaming — drive a `prompt` on each in parallel and
    // record the wall-clock arrival time of every event from each handle's
    // broadcast. Assert that thread B's `agent_start` lands *before* thread
    // A's `agent_end`, which can only happen if the two pi processes are
    // running their scripts concurrently.
    let events_a = handle_a.subscribe_events();
    let events_b = handle_b.subscribe_events();

    let send_prompt = |handle: std::sync::Arc<alleycat_pi_bridge::pool::PiProcessHandle>| async move {
        handle
            .send_request(RpcCommand::Prompt(PromptCmd {
                id: None,
                message: "go".into(),
                images: Vec::new(),
                streaming_behavior: None,
            }))
            .await
            .expect("prompt")
    };

    // Fire both prompts; don't await until we've also armed the listeners.
    let prompt_a_fut = send_prompt(handle_a.clone());
    let prompt_b_fut = send_prompt(handle_b.clone());

    // Arm event collectors on a separate task each so they don't block the
    // prompt sends.
    let collect = |mut rx: tokio::sync::broadcast::Receiver<PiEvent>| {
        tokio::spawn(async move {
            let start = Instant::now();
            let mut start_at: Option<Duration> = None;
            let mut end_at: Option<Duration> = None;
            loop {
                let evt = match timeout(Duration::from_secs(5), rx.recv()).await {
                    Ok(Ok(evt)) => evt,
                    _ => break,
                };
                match evt {
                    PiEvent::AgentStart => {
                        start_at = Some(start.elapsed());
                    }
                    PiEvent::AgentEnd { .. } => {
                        end_at = Some(start.elapsed());
                        break;
                    }
                    _ => {}
                }
            }
            (start_at, end_at)
        })
    };
    let collect_a = collect(events_a.resubscribe());
    let collect_b = collect(events_b.resubscribe());
    // Drop the original receivers — we forwarded via resubscribe so the
    // background tasks own theirs.
    drop(events_a);
    drop(events_b);

    let (resp_a, resp_b) = tokio::join!(prompt_a_fut, prompt_b_fut);
    assert!(resp_a.success, "prompt A succeeded");
    assert!(resp_b.success, "prompt B succeeded");

    let (start_a, end_a) = collect_a.await.expect("collect A");
    let (start_b, end_b) = collect_b.await.expect("collect B");
    let start_a = start_a.expect("A saw agent_start");
    let end_a = end_a.expect("A saw agent_end");
    let start_b = start_b.expect("B saw agent_start");
    let end_b = end_b.expect("B saw agent_end");

    // The interleaving check: the *later* of the two starts must precede the
    // *earlier* of the two ends. If the pool serialized them, late-start
    // would land after early-end and the assertion would fire.
    let later_start = start_a.max(start_b);
    let earlier_end = end_a.min(end_b);
    assert!(
        later_start < earlier_end,
        "expected concurrent streaming: later_start={later_start:?} should precede earlier_end={earlier_end:?} (a: {start_a:?}..{end_a:?}, b: {start_b:?}..{end_b:?})"
    );

    // Clean up — release both threads, verify pool is empty.
    pool.release(&thread_a).await;
    pool.release(&thread_b).await;
    assert!(pool.is_empty().await, "pool drained after release");
}
