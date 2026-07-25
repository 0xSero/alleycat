//! V3 verification matrix: `thread/resume` rehydrates from disk after the
//! bridge's `ConnectionState` is torn down and rebuilt.
//!
//! ## In-process precursor
//!
//! Spec called for spawning the bridge binary twice (kill → respawn → resume).
//! Until #32's dispatcher rewireup landed and was followed by a respawn-friendly
//! transport, this test exercises the equivalent in-process path: build a
//! `ConnectionState`, list to confirm hydration sees the seeded session, drop
//! the state (releases the pool which terminates any spawned fake-pi), then
//! build a *fresh* `ConnectionState` against the same `codex_home` +
//! `PI_CODING_AGENT_DIR` and call `thread/resume`. The OS-level handoff is
//! the only thing skipped; everything else (threads.json reload, pi pool
//! respawn, `switch_session`, `get_messages`, `translate_messages`) runs as
//! production.

mod support;

use std::sync::Arc;

use alleycat_pi_bridge::codex_proto as p;
use alleycat_pi_bridge::handlers;
use alleycat_pi_bridge::index::ThreadIndex;
use alleycat_pi_bridge::pool::PiPool;
use alleycat_pi_bridge::state::{ConnectionState, ThreadDefaults};
use serde_json::{Value, json};
use tempfile::TempDir;

use support::{PiHomeFixture, fake_pi_path};

#[tokio::test]
async fn resume_after_state_drop_repopulates_turns_from_jsonl() {
    let home = PiHomeFixture::new();

    // Use a real tempdir as the session's cwd. `acquire_for_resume` will
    // spawn fake-pi with `current_dir(cwd)`, which fails fast if the dir
    // doesn't exist on disk.
    let cwd_dir = TempDir::new().unwrap();
    let cwd_path = cwd_dir.path().to_string_lossy().into_owned();

    // Hand-craft a pi session JSONL with a user/assistant exchange. The
    // fake-pi binary's `get_messages` handler reads this file back when the
    // bridge's resume handler asks for the session's history (see the
    // `session_messages` helper in `support/fake_pi.rs`).
    let session_path = home.seed_session(
        "encoded-cwd",
        "sess-1",
        &[
            json!({
                "type": "session",
                "version": 3,
                "id": "pi-1",
                "timestamp": "2026-04-27T10:00:00Z",
                "cwd": cwd_path
            }),
            json!({
                "type": "message",
                "id": "m1",
                "parentId": null,
                "timestamp": "2026-04-27T10:00:05Z",
                "message": user_message("hello bridge", 1_745_751_605_000)
            }),
            json!({
                "type": "message",
                "id": "m2",
                "parentId": "m1",
                "timestamp": "2026-04-27T10:00:10Z",
                "message": assistant_message("hi back", 1_745_751_610_000)
            }),
        ],
    );

    // codex_home is shared across the two ConnectionStates so the second
    // one's ThreadIndex picks up threads.json written by the first.
    let codex_home = TempDir::new().unwrap();

    // ----- bridge boot #1: discover the seeded session via thread/list ----
    let index1 = ThreadIndex::open(codex_home.path()).await.unwrap();
    index1
        .hydrate_from_pi_dir(Some(&home.sessions_dir()))
        .await
        .unwrap();
    let pool1 = Arc::new(PiPool::new(fake_pi_path()));
    let (state1, _rx1) = ConnectionState::for_test(
        pool1,
        Arc::clone(&index1) as Arc<dyn alleycat_pi_bridge::state::ThreadIndexHandle>,
        ThreadDefaults::default(),
    );

    let list_resp = handlers::thread::handle_thread_list(&state1, p::ThreadListParams::default())
        .await
        .expect("thread/list should succeed against the seeded fixture");
    assert_eq!(
        list_resp.data.len(),
        1,
        "expected one rehydrated thread; got {:?}",
        list_resp.data
    );
    let thread_id = list_resp.data[0].id.clone();
    assert_eq!(list_resp.data[0].cwd, cwd_path);
    assert_eq!(
        list_resp.data[0].path.as_deref(),
        Some(session_path.to_string_lossy().as_ref()),
        "Thread.path should be the seeded JSONL"
    );

    // ----- tear down: drop everything from boot #1 ------------------------
    drop(state1);
    drop(index1);
    // Pool was held only by state1; dropping it terminates any spawned
    // fake-pi children. We didn't spawn any here, but the future tests in
    // this matrix will rely on this contract.

    // ----- bridge boot #2: a fresh state pointed at the same dirs --------
    let index2 = ThreadIndex::open(codex_home.path()).await.unwrap();
    // No hydration this round — the index should restore from threads.json
    // alone. Asserting that here would tighten the contract: the same row
    // survives a process restart without re-walking the disk.
    let pool2 = Arc::new(PiPool::new(fake_pi_path()));
    let (state2, _rx2) = ConnectionState::for_test(
        pool2,
        Arc::clone(&index2) as Arc<dyn alleycat_pi_bridge::state::ThreadIndexHandle>,
        ThreadDefaults::default(),
    );

    // Sanity: the row survived the threads.json round-trip.
    let restored = state2.thread_index().lookup(&thread_id).await;
    assert!(
        restored.is_some(),
        "thread row {thread_id} should reload from threads.json"
    );

    // ----- thread/resume: the actual rehydration check -------------------
    let resume_resp = handlers::thread::handle_thread_resume(
        &state2,
        p::ThreadResumeParams {
            thread_id: thread_id.clone(),
            exclude_turns: false,
            ..Default::default()
        },
    )
    .await
    .expect("thread/resume should succeed");

    assert_eq!(resume_resp.thread.id, thread_id);
    assert_eq!(resume_resp.cwd, cwd_path);

    // The seeded JSONL had a user message and an assistant message — pi
    // (fake-pi here) collapses those into one Turn (user-message → assistant
    // exchange = one turn). At minimum the turns list must be non-empty.
    assert!(
        !resume_resp.thread.turns.is_empty(),
        "ThreadResumeResponse.thread.turns should rehydrate from JSONL; got {:?}",
        resume_resp.thread.turns
    );

    // Sniff: the user message's text should land in some `UserMessage`
    // ThreadItem inside the rehydrated turns. We don't assert exact item
    // shape because translate/items.rs's mapping is exercised by its own
    // unit tests; we just want a smoke that the text round-tripped.
    let serialized = serde_json::to_string(&resume_resp.thread.turns).unwrap();
    assert!(
        serialized.contains("hello bridge"),
        "turns should reference seeded user text; got {serialized}"
    );
    assert!(
        serialized.contains("hi back"),
        "turns should reference seeded assistant text; got {serialized}"
    );
}

/// Regression for the reported "follow-up message crashes after backgrounding"
/// bug: a `turn/start` on a thread whose pi process is gone (idle-reaped, or —
/// as here — cleared by a fresh `ConnectionState` standing in for a daemon
/// restart) must transparently resume the persisted session instead of failing
/// with `ThreadNotLoaded`. This is what gives Local Studio parity with the
/// generic pi runtime.
#[tokio::test]
async fn turn_start_resumes_a_thread_whose_process_was_reaped() {
    let home = PiHomeFixture::new();
    let cwd_dir = TempDir::new().unwrap();
    let cwd_path = cwd_dir.path().to_string_lossy().into_owned();

    let session_path = home.seed_session(
        "encoded-cwd",
        "sess-followup",
        &[
            json!({
                "type": "session",
                "version": 3,
                "id": "pi-followup",
                "timestamp": "2026-04-27T10:00:00Z",
                "cwd": cwd_path
            }),
            json!({
                "type": "message",
                "id": "m1",
                "parentId": null,
                "timestamp": "2026-04-27T10:00:05Z",
                "message": user_message("first turn", 1_745_751_605_000)
            }),
            json!({
                "type": "message",
                "id": "m2",
                "parentId": "m1",
                "timestamp": "2026-04-27T10:00:10Z",
                "message": assistant_message("first reply", 1_745_751_610_000)
            }),
        ],
    );
    let _ = &session_path;

    let codex_home = TempDir::new().unwrap();

    // Boot #1: hydrate the index so threads.json records the session.
    let index1 = ThreadIndex::open(codex_home.path()).await.unwrap();
    index1
        .hydrate_from_pi_dir(Some(&home.sessions_dir()))
        .await
        .unwrap();
    let pool1 = Arc::new(PiPool::new(fake_pi_path()));
    let (state1, _rx1) = ConnectionState::for_test(
        pool1,
        Arc::clone(&index1) as Arc<dyn alleycat_pi_bridge::state::ThreadIndexHandle>,
        ThreadDefaults::default(),
    );
    let list_resp = handlers::thread::handle_thread_list(&state1, p::ThreadListParams::default())
        .await
        .expect("thread/list");
    let thread_id = list_resp.data[0].id.clone();

    // Tear down boot #1: this drops the pool, terminating the fake-pi child —
    // exactly the cold-pool state a daemon restart or idle reap leaves behind.
    drop(state1);
    drop(index1);

    // Boot #2: a fresh state with an empty pool. The thread exists in the
    // index but has no live process.
    let index2 = ThreadIndex::open(codex_home.path()).await.unwrap();
    let pool2 = Arc::new(PiPool::new(fake_pi_path()));
    let (state2, _rx2) = ConnectionState::for_test(
        pool2,
        Arc::clone(&index2) as Arc<dyn alleycat_pi_bridge::state::ThreadIndexHandle>,
        ThreadDefaults::default(),
    );
    assert!(
        state2.pi_pool().get(&thread_id).await.is_none(),
        "precondition: the thread's process must be absent from the fresh pool"
    );

    // The follow-up turn. Before the fix this returned `ThreadNotLoaded`;
    // it must now succeed by resuming the persisted session.
    let resp = handlers::turn::handle_turn_start(
        &state2,
        p::TurnStartParams {
            thread_id: thread_id.clone(),
            input: vec![p::UserInput::Text {
                text: "follow up after backgrounding".into(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        },
    )
    .await
    .expect("turn/start on a reaped thread must resume, not fail ThreadNotLoaded");

    assert_eq!(resp.turn.status, p::TurnStatus::InProgress);
    // The process is now live in the pool, so a second follow-up needs no resume.
    assert!(
        state2.pi_pool().get(&thread_id).await.is_some(),
        "the thread's process should be loaded after the resuming turn/start"
    );
}

/// Build a pi `UserMessage` JSON. Fields must satisfy strict deserialization
/// (`UserMessage.role` / `content` / `timestamp` are required in
/// `pool::pi_protocol::UserMessage`).
fn user_message(text: &str, timestamp: i64) -> Value {
    json!({
        "role": "user",
        "content": text,
        "timestamp": timestamp
    })
}

/// Build a pi `AssistantMessage` JSON. Same strict-shape note as in V1's
/// helper — see `tests/v1_codex_smoke.rs` for the full footgun explanation.
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
