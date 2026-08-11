//! Session resilience: the producer-side state survives an iroh stream
//! detach, and a reattach replays buffered frames + outstanding server
//! requests so the client picks up mid-turn without loss.
//!
//! These tests drive [`Session`] + [`SessionRegistry`] directly rather than
//! spawning a real bridge binary; the same code paths are exercised by
//! `serve_stream_with_session` in production.

use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::session::{AttachKind, Session, SessionRegistry, SessionRegistryConfig};
use alleycat_bridge_core::state::ServerRequestError;
use serde_json::{Value, json};
use tokio::sync::oneshot;

fn notif(method: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": {},
    })
}

#[tokio::test]
async fn detach_then_reattach_replays_missed_frames() {
    let session = Arc::new(Session::new("pi", "node-A".into(), 64, 1 << 20));

    // First attach: drainer reads frames in real time.
    let mut a = session.install_attachment(None);
    session.enqueue(notif("turn/started"));
    session.enqueue(notif("item/started"));
    let f1 = a.live_rx.recv().await.unwrap();
    let f2 = a.live_rx.recv().await.unwrap();
    assert_eq!(f1.seq, 1);
    assert_eq!(f2.seq, 2);
    assert_eq!(f1.payload["method"], "turn/started");

    // Client disconnects.
    session.drop_attachment();
    drop(a);

    // Producer keeps running while detached.
    session.enqueue(notif("item/completed"));
    session.enqueue(notif("turn/completed"));

    // Reattach with last_seen = 2 — must receive the frames missed during
    // the detach window.
    let b = session.install_attachment(Some(2));
    let backlog: Vec<u64> = b.backlog.iter().map(|f| f.seq).collect();
    assert_eq!(backlog, vec![3, 4]);
    let methods: Vec<&str> = b
        .backlog
        .iter()
        .filter_map(|f| f.payload["method"].as_str())
        .collect();
    assert_eq!(methods, vec!["item/completed", "turn/completed"]);
    assert!(
        b.replay_redelivery.is_none(),
        "no outstanding server requests"
    );
}

#[tokio::test]
async fn fresh_attach_after_drop_when_resume_cursor_omitted() {
    let session = Arc::new(Session::new("pi", "node-A".into(), 64, 1 << 20));
    session.enqueue(notif("event-1"));
    let _a = session.install_attachment(None);
    session.drop_attachment();

    // Reattach without resume cursor — caller treats it as a fresh client,
    // backlog is empty even though the ring still has the frame.
    let b = session.install_attachment(None);
    assert!(b.backlog.is_empty());
}

#[tokio::test]
async fn drift_when_cursor_predates_ring_floor() {
    // Ring of 2 messages forces eviction past cursor 0.
    let session = Arc::new(Session::new("pi", "node-A".into(), 2, 1 << 20));
    let _a = session.install_attachment(None);
    session.enqueue(notif("a"));
    session.enqueue(notif("b"));
    session.enqueue(notif("c"));
    session.drop_attachment();

    let b = session.install_attachment(Some(0));
    assert!(matches!(
        alleycat_bridge_core::session::AttachOutcome::DriftReload,
        _outcome if matches!(b.outcome, alleycat_bridge_core::session::AttachOutcome::DriftReload)
    ));
    assert!(b.backlog.is_empty());
}

#[tokio::test]
async fn outstanding_server_request_redelivered_on_reattach() {
    let session = Arc::new(Session::new("pi", "node-A".into(), 64, 1 << 20));
    let _a = session.install_attachment(None);

    // A handler issues a server→client request. The pending oneshot waits
    // for the client; the outstanding-requests table records params for
    // replay.
    let (tx, mut rx) = oneshot::channel::<Result<Value, ServerRequestError>>();
    let req_id = session.next_request_id();
    session.register_pending(
        req_id.clone(),
        "command/approve".into(),
        json!({"command": "rm -rf /", "thread_id": "t1"}),
        tx,
    );
    session.enqueue(json!({
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "command/approve",
        "params": {"command": "rm -rf /", "thread_id": "t1"},
    }));
    session.enqueue(notif("turn/progress"));

    // Client disconnects mid-prompt, before answering.
    session.drop_attachment();

    // Reattach within the grace window. After backlog replay, the drainer
    // emits a `serverRequest/replay` notification listing the still-
    // outstanding request so the new client can re-render the approval UI.
    let b = session.install_attachment(Some(0));
    let replay = b
        .replay_redelivery
        .as_ref()
        .expect("expected serverRequest/replay frame");
    assert_eq!(replay["method"], "serverRequest/replay");
    let outstanding = replay["params"]["outstanding"]
        .as_array()
        .expect("outstanding array");
    assert_eq!(outstanding.len(), 1);
    assert_eq!(outstanding[0]["id"].as_str().unwrap(), req_id);
    assert_eq!(outstanding[0]["method"], "command/approve");
    assert_eq!(outstanding[0]["params"]["command"], "rm -rf /");

    // The original pending oneshot is still alive — the new client answers
    // with the original id and the handler that was awaiting `rx` resumes.
    assert!(session.resolve_pending(&req_id, Ok(json!({"decision": "decline"}))));
    let resolved = rx.try_recv().expect("resolved");
    assert!(resolved.is_ok());
}

#[tokio::test]
async fn pending_grace_expiry_cancels_outstanding_requests() {
    let cfg = SessionRegistryConfig {
        ring_max_msgs: 64,
        ring_max_bytes: 1 << 20,
        idle_ttl: Duration::from_secs(3600),
        pending_grace: Duration::from_millis(0),
    };
    let registry = SessionRegistry::new(cfg.clone());
    let session = registry.get_or_create("node-A".into(), "pi");

    let _a = session.install_attachment(None);
    let (tx, rx) = oneshot::channel::<Result<Value, ServerRequestError>>();
    let req_id = session.next_request_id();
    session.register_pending(req_id, "command/approve".into(), json!({}), tx);
    session.drop_attachment();

    // Manual reaper tick: pending_grace=0 so we cancel immediately;
    // idle_ttl is large so the session itself sticks around.
    registry.tick(cfg.pending_grace, cfg.idle_ttl);

    // Reaper cancelled the pending oneshot — the original handler that was
    // awaiting the response receives ConnectionClosed.
    match rx.await {
        Ok(Err(ServerRequestError::ConnectionClosed)) => {}
        other => panic!("expected ConnectionClosed, got {other:?}"),
    }
    // Session itself survives — a fresh attach lands as Resumed.
    assert!(registry.get("node-A", "pi").is_some());
}

#[tokio::test]
async fn registry_resolve_attach_marks_existing_resumed() {
    let registry = SessionRegistry::new(SessionRegistryConfig::default());
    let s1 = registry.get_or_create("node-A".into(), "pi");
    s1.enqueue(notif("first"));

    let resolved = registry.resolve_attach("node-A".into(), "pi", Some(0));
    assert!(Arc::ptr_eq(&resolved.session, &s1));
    assert_eq!(resolved.kind, AttachKind::Resumed);
    assert!(resolved.current_seq >= 1);
}

#[tokio::test]
async fn registry_resolve_attach_minted_session_is_fresh_even_with_resume() {
    let registry = SessionRegistry::new(SessionRegistryConfig::default());
    // Client claims to have a cursor but no prior session exists for this
    // (node, agent) — server treats it as Fresh; the cursor is meaningless
    // against an empty ring.
    let resolved = registry.resolve_attach("node-Z".into(), "claude", Some(42));
    assert_eq!(resolved.kind, AttachKind::Fresh);
}

#[tokio::test]
async fn auto_resume_uses_server_tracked_cursor_when_no_resume_field() {
    // The litter client today sends `Connect { v, token, agent }` with no
    // resume cursor. After an iroh disconnect + reconnect the server should
    // *still* replay anything its previous drainer didn't get to write,
    // by treating no-cursor + existing-session as auto-resume from
    // `last_attempted_seq`. Zero client-side work needed.
    let registry = SessionRegistry::new(SessionRegistryConfig::default());
    let session = registry.get_or_create("node-A".into(), "pi");

    // Install a drainer (mimicking the real attachment). Manually mark
    // each frame as attempted, then drop the drainer — that's the
    // "previous stream died after writing seq 2" state.
    let mut a = session.install_attachment(None);
    session.enqueue(notif("turn/started"));
    session.enqueue(notif("item/started"));
    let f1 = a.live_rx.recv().await.unwrap();
    session.note_drainer_attempt(f1.seq);
    let f2 = a.live_rx.recv().await.unwrap();
    session.note_drainer_attempt(f2.seq);

    // Stream dies before the drainer gets to seq 3 / 4.
    session.drop_attachment();
    drop(a);
    session.enqueue(notif("item/completed"));
    session.enqueue(notif("turn/completed"));

    // Client reconnects with the existing protocol — no `resume` field.
    let resolved = registry.resolve_attach("node-A".into(), "pi", None);
    assert_eq!(resolved.kind, AttachKind::Resumed);
    // Server-tracked cursor is `last_attempted - 1` = 1, so backlog
    // should include seqs 2..=4 (seq 2 is the conservative duplicate
    // for the uncertain frame, 3 and 4 are the missed ones).
    let backlog: Vec<u64> = resolved
        .session
        .install_attachment(resolved.effective_last_seen)
        .backlog
        .iter()
        .map(|f| f.seq)
        .collect();
    assert_eq!(backlog, vec![2, 3, 4]);
}

#[tokio::test]
async fn auto_resume_returns_fresh_for_brand_new_session() {
    // First-ever connect from this `(node_id, agent)`: no prior session,
    // so even if `last_seen=None` we report Fresh, not auto-resume.
    let registry = SessionRegistry::new(SessionRegistryConfig::default());
    let resolved = registry.resolve_attach("node-NEW".into(), "pi", None);
    assert_eq!(resolved.kind, AttachKind::Fresh);
    assert!(resolved.effective_last_seen.is_none());
}

#[tokio::test]
async fn auto_resume_picks_drift_when_buffer_overflowed() {
    // Tiny ring forces the drainer's high-water mark out of the replay
    // window. Auto-resume must report DriftReload so the host's reply
    // tells the client to reload state.
    let cfg = SessionRegistryConfig {
        ring_max_msgs: 2,
        ..Default::default()
    };
    let registry = SessionRegistry::new(cfg);
    let session = registry.get_or_create("node-A".into(), "pi");
    let _a = session.install_attachment(None);
    session.note_drainer_attempt(session.enqueue(notif("a")));
    session.note_drainer_attempt(session.enqueue(notif("b")));
    session.drop_attachment();
    // After detach, the gap continues to fill — pushes seqs that evict
    // the previously-attempted ones from the ring.
    session.enqueue(notif("c"));
    session.enqueue(notif("d"));
    session.enqueue(notif("e"));

    let resolved = registry.resolve_attach("node-A".into(), "pi", None);
    assert_eq!(resolved.kind, AttachKind::DriftReload);
}

#[tokio::test]
async fn enqueue_stamps_alleycat_seq_on_object_payloads() {
    let session = Arc::new(Session::new("pi", "node-A".into(), 16, 1 << 20));
    let mut handle = session.install_attachment(None);
    let seq = session.enqueue(notif("turn/started"));
    let received = handle.live_rx.recv().await.unwrap();
    assert_eq!(received.payload["_alleycat_seq"], seq);
    assert_eq!(received.payload["method"], "turn/started");
}

#[tokio::test]
async fn enqueue_does_not_stamp_non_object_payloads() {
    // Defensive: a stray non-object enqueue (e.g. a bare null) should not
    // panic and should pass through untouched.
    let session = Arc::new(Session::new("pi", "node-A".into(), 16, 1 << 20));
    let mut handle = session.install_attachment(None);
    let seq = session.enqueue(json!(null));
    let received = handle.live_rx.recv().await.unwrap();
    assert_eq!(received.payload, Value::Null);
    let _ = seq;
}

#[tokio::test]
async fn second_attach_preempts_first() {
    let session = Arc::new(Session::new("pi", "node-A".into(), 64, 1 << 20));
    let mut first = session.install_attachment(None);
    let mut second = session.install_attachment(None);
    session.enqueue(notif("post-preempt"));

    // First's live_rx closes — its tx was dropped on replace.
    assert!(first.live_rx.recv().await.is_none());
    // Second sees the new frame.
    let frame = second.live_rx.recv().await.unwrap();
    assert_eq!(frame.payload["method"], "post-preempt");
}
