//! Session layer that decouples bridge state from a single iroh stream.
//!
//! A `Session` is keyed by `(client_node_id, agent)` and outlives any one
//! attached stream. When a client disconnects, the underlying agent process,
//! the writer-bound replay ring, and the pending server-request table all
//! survive — so a reattaching client can resume mid-turn without losing
//! events or in-flight approval prompts.

pub mod registry;
pub mod ring;

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::state::{Capabilities, PendingServerRequest, ServerRequestError};

pub use registry::{AttachKind, ResolvedAttach, SessionRegistry, SessionRegistryConfig};
pub use ring::{ReplayError, ReplayRing, Sequenced};

/// Identifier for a coding-agent backend. Stored as a static string slice
/// throughout to keep keying cheap.
pub type AgentId = &'static str;

/// Cryptographic node id for a paired client. Stored as a hex string so the
/// session module is independent of `iroh` types.
pub type NodeId = String;

/// Outstanding server→client request that has been delivered but not yet
/// answered. Distinct from [`PendingServerRequest`]: that one owns the
/// oneshot responder; this one carries the params we replay on reattach so
/// the client can re-render its approval UI.
#[derive(Debug, Clone)]
pub struct OutstandingRequest {
    pub method: String,
    pub params: serde_json::Value,
}

/// What an `attach` call discovered about prior session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachOutcome {
    /// Session was minted by this attach; ring is empty.
    Fresh,
    /// Session existed and the cursor is within the replay window.
    Resumed,
    /// Session existed but the cursor predates the floor — the client must
    /// reload state from authoritative storage before treating attach as
    /// successful.
    DriftReload,
}

/// Handed back from `Session::install_attachment` to the caller, who is
/// responsible for spawning the drainer task that flushes `backlog` and then
/// `live_rx` to the iroh sink.
pub struct AttachHandle {
    /// Identifies this attachment when its stream finishes or is cancelled.
    pub generation: u64,
    pub outcome: AttachOutcome,
    pub current_seq: u64,
    pub floor_seq: u64,
    /// Frames the client missed while detached, in seq order.
    pub backlog: Vec<Sequenced>,
    /// Synthetic `serverRequest/replay` frame to emit after the backlog, if
    /// there are any outstanding requests at attach time. None for fresh
    /// attaches or reattaches with no outstanding requests.
    pub replay_redelivery: Option<Value>,
    /// New live channel; producers `enqueue` after this point will push here.
    pub live_rx: mpsc::UnboundedReceiver<Sequenced>,
}

#[derive(Debug)]
struct Attachment {
    live_tx: mpsc::UnboundedSender<Sequenced>,
    /// Monotonic counter incremented on every fresh `install_attachment`.
    /// Prevents an old stream's teardown from detaching its replacement.
    generation: u64,
}

#[derive(Debug)]
struct DetachState {
    /// When the session became unattached. None while attached.
    detached_at: Option<Instant>,
}

pub struct Session {
    pub agent: AgentId,
    pub node_id: NodeId,
    /// Short stable disambiguator used in server-side request ids.
    session_short: String,
    ring: Mutex<ReplayRing>,
    attachment: Mutex<Option<Attachment>>,
    pending: Mutex<HashMap<String, PendingServerRequest>>,
    outstanding: Mutex<HashMap<String, OutstandingRequest>>,
    capabilities: Mutex<Capabilities>,
    request_counter: AtomicU64,
    attachment_generation: AtomicU64,
    detach: Mutex<DetachState>,
    /// Highest `seq` the drainer has *attempted* to write to the wire,
    /// updated via `fetch_max` immediately before each `write_json_line`.
    ///
    /// The drainer is dead by the time we read this on reattach — but the
    /// counter persists on the session. It lets the server auto-resume
    /// from "what the previous drainer last got to" when a reconnecting
    /// client doesn't send an explicit resume cursor.
    ///
    /// "Attempted" rather than "delivered": a write that returned `Ok` may
    /// still have been buffered in the kernel/QUIC layer and lost on a
    /// hard disconnect, while one that returned `Err` may have partially
    /// reached the peer. Both cases collapse to "uncertain"; the auto-
    /// resume policy replays from `last_attempted_seq.saturating_sub(1)`,
    /// so the most recent uncertain frame is re-sent — duplicates over
    /// missing data.
    last_attempted_seq: AtomicU64,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("agent", &self.agent)
            .field("node_id", &self.node_id)
            .field("session_short", &self.session_short)
            .finish()
    }
}

impl Session {
    pub fn new(
        agent: AgentId,
        node_id: NodeId,
        ring_max_msgs: usize,
        ring_max_bytes: usize,
    ) -> Self {
        let session_short = format!("{:08x}", short_hash(&node_id, agent));
        Self {
            agent,
            node_id,
            session_short,
            ring: Mutex::new(ReplayRing::new(ring_max_msgs, ring_max_bytes)),
            attachment: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            outstanding: Mutex::new(HashMap::new()),
            capabilities: Mutex::new(Capabilities::default()),
            request_counter: AtomicU64::new(0),
            attachment_generation: AtomicU64::new(0),
            detach: Mutex::new(DetachState { detached_at: None }),
            last_attempted_seq: AtomicU64::new(0),
        }
    }

    /// Record that the drainer is about to attempt writing `seq` to the
    /// wire. Idempotent under concurrent calls (uses `fetch_max`). Called
    /// from the drainer in bridge-core / pi-bridge / claude-bridge.
    pub fn note_drainer_attempt(&self, seq: u64) {
        self.last_attempted_seq.fetch_max(seq, Ordering::Relaxed);
    }

    /// Snapshot of the highest seq the drainer has tried to write. Used
    /// by `SessionRegistry::resolve_attach` to compute the server-side
    /// resume cursor when the client didn't supply one.
    pub fn last_attempted_seq(&self) -> u64 {
        self.last_attempted_seq.load(Ordering::Relaxed)
    }

    /// Push a frame into the replay ring and forward to the live drainer if
    /// one is attached. Returns the assigned seq.
    ///
    /// The payload is stamped with `_alleycat_seq: <seq>` as a top-level
    /// field on JSON object payloads so a future cursor-aware client can
    /// read it off the wire. Codex's JSON-RPC envelopes use serde without
    /// `deny_unknown_fields`, so existing litter-side parsers ignore it.
    /// Non-object payloads are passed through unstamped.
    pub fn enqueue(&self, mut payload: Value) -> u64 {
        let seq = {
            let mut ring = self.ring.lock().unwrap();
            let next = ring.next_seq_peek();
            stamp_alleycat_seq(&mut payload, next);
            let assigned = ring.push(payload.clone());
            debug_assert_eq!(assigned, next, "ring assigned a different seq than peeked");
            assigned
        };
        let attachment = self.attachment.lock().unwrap();
        if let Some(attachment) = attachment.as_ref() {
            // Best-effort: if the drainer is gone, the message is still in
            // the ring and a future reattach will replay it.
            let _ = attachment.live_tx.send(Sequenced {
                seq,
                payload,
                bytes: 0,
            });
        }
        seq
    }

    /// Mint a fresh server-side request id (string form), prefixed for
    /// human-readable logs and namespaced to this session.
    pub fn next_request_id(&self) -> String {
        let n = self.request_counter.fetch_add(1, Ordering::Relaxed) + 1;
        format!("bridge-{}-{}", self.session_short, n)
    }

    pub fn capabilities(&self) -> Capabilities {
        self.capabilities.lock().unwrap().clone()
    }

    pub fn set_capabilities(&self, capabilities: Capabilities) {
        *self.capabilities.lock().unwrap() = capabilities;
    }

    pub fn should_emit(&self, method: &str) -> bool {
        !self
            .capabilities
            .lock()
            .unwrap()
            .opt_out_notification_methods
            .contains(method)
    }

    /// Stash a pending server→client request and the params we'd replay on
    /// reattach. Both tables are kept in lockstep. Bridges convert their
    /// envelope-specific request id type to a string at this boundary.
    pub fn register_pending(
        &self,
        id: String,
        method: String,
        params: Value,
        responder: oneshot::Sender<Result<Value, ServerRequestError>>,
    ) {
        self.pending.lock().unwrap().insert(
            id.clone(),
            PendingServerRequest {
                method: method.clone(),
                responder,
            },
        );
        self.outstanding
            .lock()
            .unwrap()
            .insert(id, OutstandingRequest { method, params });
    }

    /// Resolve a pending request; returns `true` if we had it. Always clears
    /// the matching outstanding entry too.
    pub fn resolve_pending(&self, id: &str, result: Result<Value, ServerRequestError>) -> bool {
        let pending = self.pending.lock().unwrap().remove(id);
        self.outstanding.lock().unwrap().remove(id);
        match pending {
            Some(entry) => {
                let _ = entry.responder.send(result);
                true
            }
            None => false,
        }
    }

    /// Drain pending without notifying outstanding — used when the request
    /// failed to enqueue at all, so there's nothing to replay later.
    pub fn forget_pending(&self, id: &str) {
        self.pending.lock().unwrap().remove(id);
        self.outstanding.lock().unwrap().remove(id);
    }

    pub fn cancel_all_pending(&self) {
        let drained: Vec<_> = self.pending.lock().unwrap().drain().collect();
        self.outstanding.lock().unwrap().clear();
        for (_, entry) in drained {
            let _ = entry
                .responder
                .send(Err(ServerRequestError::ConnectionClosed));
        }
    }

    /// Install a new attachment, replacing any prior one. Returns the
    /// replay backlog (frames the reattaching client missed) plus a fresh
    /// `live_rx` the caller drives to deliver subsequent frames.
    ///
    /// Lock order: attachment → ring → outstanding (all released before
    /// returning). `enqueue` takes ring then attachment, never overlapping,
    /// so deadlock is impossible.
    pub fn install_attachment(&self, last_seen: Option<u64>) -> AttachHandle {
        let mut attachment_slot = self.attachment.lock().unwrap();
        let ring_guard = self.ring.lock().unwrap();
        let current_seq = ring_guard.current_seq();
        let floor_seq = ring_guard.floor_seq();

        let (outcome, backlog) = match last_seen {
            None => (AttachOutcome::Fresh, Vec::new()),
            Some(cursor) => match ring_guard.replay_from(cursor) {
                Ok(frames) => (AttachOutcome::Resumed, frames),
                Err(ReplayError::Drift { .. }) => (AttachOutcome::DriftReload, Vec::new()),
            },
        };
        drop(ring_guard);

        let replay_redelivery = if matches!(outcome, AttachOutcome::Resumed) {
            outstanding_replay_message(&self.outstanding.lock().unwrap())
        } else {
            None
        };

        let (live_tx, live_rx) = mpsc::unbounded_channel();
        let generation = self.attachment_generation.fetch_add(1, Ordering::Relaxed) + 1;
        // Replacing drops the previous live_tx; the previous drainer's
        // live_rx closes and that task exits.
        *attachment_slot = Some(Attachment {
            live_tx,
            generation,
        });
        // Clear detach bookkeeping while attached.
        self.detach.lock().unwrap().detached_at = None;

        AttachHandle {
            generation,
            outcome,
            current_seq,
            floor_seq,
            backlog,
            replay_redelivery,
            live_rx,
        }
    }

    /// Clear the attachment slot. Producer enqueues continue to go into the
    /// ring; only the live forwarding stops. If `pending_grace` is configured
    /// and elapses without a reattach, the session reaper will drain pending
    /// requests with `ConnectionClosed` (see [`SessionRegistry`]).
    pub fn drop_attachment(&self, generation: u64) {
        let mut slot = self.attachment.lock().unwrap();
        if !slot
            .as_ref()
            .is_some_and(|attachment| attachment.generation == generation)
        {
            return;
        }
        *slot = None;
        self.detach.lock().unwrap().detached_at = Some(Instant::now());
    }

    pub fn is_attached(&self) -> bool {
        self.attachment.lock().unwrap().is_some()
    }

    /// True when the session has been detached for at least `grace`. While
    /// attached, always returns false.
    pub fn detached_for(&self, grace: Duration) -> bool {
        match self.detach.lock().unwrap().detached_at {
            Some(at) => at.elapsed() >= grace,
            None => false,
        }
    }

    pub fn has_outstanding_requests(&self) -> bool {
        !self.outstanding.lock().unwrap().is_empty()
    }

    /// Read-only snapshot of `(current_seq, floor_seq)`. Useful for probing
    /// the ring before deciding whether a reattach can succeed.
    pub fn peek_seq(&self) -> (u64, u64) {
        let ring = self.ring.lock().unwrap();
        (ring.current_seq(), ring.floor_seq())
    }

    /// Probe the ring for whether `last_seen` is still within the replay
    /// window. Does not change state. `Ok(())` means a future reattach with
    /// the same cursor would be `Resumed`; `Err(Drift)` means it would be
    /// `DriftReload`.
    pub fn peek_replay(&self, last_seen: u64) -> Result<(), ReplayError> {
        self.ring.lock().unwrap().replay_from(last_seen).map(|_| ())
    }
}

/// Stamp a JSON object payload with `_alleycat_seq: <seq>` as a top-level
/// field. No-op for non-object values (arrays, scalars, null) — those don't
/// need a cursor and stamping would change their shape.
fn stamp_alleycat_seq(payload: &mut Value, seq: u64) {
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("_alleycat_seq".to_string(), Value::from(seq));
    }
}

fn outstanding_replay_message(outstanding: &HashMap<String, OutstandingRequest>) -> Option<Value> {
    if outstanding.is_empty() {
        return None;
    }
    let entries: Vec<Value> = outstanding
        .iter()
        .map(|(id, entry)| {
            serde_json::json!({
                "id": id,
                "method": entry.method,
                "params": entry.params,
            })
        })
        .collect();
    Some(serde_json::json!({
        "jsonrpc": "2.0",
        "method": "serverRequest/replay",
        "params": { "outstanding": entries },
    }))
}

fn short_hash(node_id: &str, agent: &str) -> u32 {
    // Cheap non-cryptographic mixer — only used to disambiguate request ids
    // in logs. FNV-1a 32-bit.
    let mut hash: u32 = 0x811c9dc5;
    for byte in node_id
        .as_bytes()
        .iter()
        .chain(b":".iter())
        .chain(agent.as_bytes())
    {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notif(method: &str) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": {},
        })
    }

    #[test]
    fn enqueue_assigns_increasing_seqs() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        assert_eq!(session.enqueue(notif("a")), 1);
        assert_eq!(session.enqueue(notif("b")), 2);
    }

    #[test]
    fn fresh_attach_yields_empty_backlog() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        session.enqueue(notif("a"));
        let handle = session.install_attachment(None);
        assert_eq!(handle.outcome, AttachOutcome::Fresh);
        assert!(handle.backlog.is_empty());
    }

    #[test]
    fn resumed_attach_replays_backlog() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        session.enqueue(notif("a"));
        session.enqueue(notif("b"));
        let handle = session.install_attachment(Some(1));
        assert_eq!(handle.outcome, AttachOutcome::Resumed);
        let seqs: Vec<_> = handle.backlog.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![2]);
    }

    #[test]
    fn drift_attach_returns_no_backlog() {
        // Tiny ring, force eviction past cursor.
        let session = Session::new("pi", "node-abc".into(), 1, 1 << 20);
        session.enqueue(notif("a"));
        session.enqueue(notif("b"));
        session.enqueue(notif("c"));
        let handle = session.install_attachment(Some(0));
        assert_eq!(handle.outcome, AttachOutcome::DriftReload);
        assert!(handle.backlog.is_empty());
    }

    #[tokio::test]
    async fn live_enqueue_after_attach_reaches_drainer() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        let mut handle = session.install_attachment(None);
        session.enqueue(notif("a"));
        let received = handle
            .live_rx
            .recv()
            .await
            .expect("live frame should arrive");
        assert_eq!(received.seq, 1);
    }

    #[tokio::test]
    async fn second_attach_preempts_first() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        let mut first = session.install_attachment(None);
        let mut second = session.install_attachment(None);
        session.enqueue(notif("a"));
        // First's live_rx must close (its tx was dropped on replace).
        assert!(first.live_rx.recv().await.is_none());
        // Second receives the frame.
        let frame = second.live_rx.recv().await.expect("second receives");
        assert_eq!(frame.seq, 1);
    }

    #[test]
    fn next_request_id_is_unique_and_prefixed() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        let a = session.next_request_id();
        let b = session.next_request_id();
        assert_ne!(a, b);
        assert!(a.starts_with("bridge-"));
    }

    #[test]
    fn outstanding_replay_emitted_on_resume_only() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        session.enqueue(notif("first"));
        let (tx, _rx) = oneshot::channel();
        session.register_pending(
            "req-1".into(),
            "command/approve".into(),
            serde_json::json!({"command": "rm -rf /"}),
            tx,
        );
        // Fresh attach: no replay redelivery even with outstanding present.
        let h_fresh = session.install_attachment(None);
        assert!(h_fresh.replay_redelivery.is_none());
        // Re-attach within window: redelivery emitted.
        session.enqueue(notif("second"));
        let h_resume = session.install_attachment(Some(1));
        assert!(h_resume.replay_redelivery.is_some());
    }

    #[test]
    fn cancel_all_pending_clears_outstanding() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        let (tx, rx) = oneshot::channel();
        session.register_pending(
            "req-1".into(),
            "command/approve".into(),
            serde_json::json!({}),
            tx,
        );
        session.cancel_all_pending();
        assert!(!session.has_outstanding_requests());
        // Responder fires with ConnectionClosed.
        match rx.blocking_recv() {
            Ok(Err(ServerRequestError::ConnectionClosed)) => {}
            other => panic!("expected ConnectionClosed, got {other:?}"),
        }
    }

    #[test]
    fn detached_for_tracks_attachment_state() {
        let session = Session::new("pi", "node-abc".into(), 16, 1 << 20);
        assert!(!session.detached_for(Duration::from_millis(0)));
        let _h = session.install_attachment(None);
        assert!(!session.detached_for(Duration::from_millis(0)));
        session.drop_attachment(_h.generation);
        assert!(session.detached_for(Duration::from_millis(0)));
    }
}
