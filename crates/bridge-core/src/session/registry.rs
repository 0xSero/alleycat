//! `SessionRegistry` — the daemon-lifetime owner of all live sessions, keyed
//! by `(node_id, agent)`. Concurrent map operations use a coarse `Mutex`; the
//! contention is fine for our scale (handful of clients × four agents).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::session::{AgentId, AttachOutcome, NodeId, Session};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachKind {
    /// No prior session existed for `(node_id, agent)`; one was minted.
    Fresh,
    /// A live session existed and the cursor was within the replay window.
    Resumed,
    /// A live session existed but the cursor was below the floor — the
    /// caller should treat this as Fresh and the client must reload state.
    DriftReload,
}

impl From<AttachOutcome> for AttachKind {
    fn from(value: AttachOutcome) -> Self {
        match value {
            AttachOutcome::Fresh => Self::Fresh,
            AttachOutcome::Resumed => Self::Resumed,
            AttachOutcome::DriftReload => Self::DriftReload,
        }
    }
}

#[derive(Debug)]
pub struct ResolvedAttach {
    pub session: Arc<Session>,
    pub kind: AttachKind,
    pub current_seq: u64,
    pub floor_seq: u64,
    /// Cursor that should be threaded into `Session::install_attachment`.
    ///
    /// Differs from the client's supplied `last_seen` in one case: when the
    /// client sends no resume cursor but a prior session exists for
    /// `(node_id, agent)`, the registry auto-supplies
    /// `last_attempted_seq.saturating_sub(1)` as the cursor — so a litter
    /// client that calls plain `Connect { v, token, agent }` after an iroh
    /// drop still gets mid-turn replay without knowing about resume.
    pub effective_last_seen: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SessionRegistryConfig {
    pub ring_max_msgs: usize,
    pub ring_max_bytes: usize,
    pub idle_ttl: Duration,
    pub pending_grace: Duration,
}

impl Default for SessionRegistryConfig {
    fn default() -> Self {
        Self {
            ring_max_msgs: 2048,
            ring_max_bytes: 16 << 20,
            idle_ttl: Duration::from_secs(600),
            pending_grace: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
pub struct SessionRegistry {
    inner: Mutex<HashMap<(NodeId, AgentId), Arc<Session>>>,
    config: SessionRegistryConfig,
}

impl SessionRegistry {
    pub fn new(config: SessionRegistryConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            config,
        })
    }

    pub fn config(&self) -> &SessionRegistryConfig {
        &self.config
    }

    /// Lookup an existing session keyed by `(node_id, agent)`. The session
    /// stays in the registry; the caller gets a fresh `Arc`.
    pub fn get(&self, node_id: &str, agent: AgentId) -> Option<Arc<Session>> {
        let inner = self.inner.lock().unwrap();
        inner.get(&(node_id.to_string(), agent)).cloned()
    }

    /// Get-or-create the session for `(node_id, agent)`. Does not install an
    /// attachment — caller does that via `Session::install_attachment` after
    /// resolving the resume cursor.
    pub fn get_or_create(&self, node_id: NodeId, agent: AgentId) -> Arc<Session> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .entry((node_id.clone(), agent))
            .or_insert_with(|| {
                Arc::new(Session::new(
                    agent,
                    node_id,
                    self.config.ring_max_msgs,
                    self.config.ring_max_bytes,
                ))
            })
            .clone()
    }

    /// Resolve a `Connect` attach: get-or-create the session, decide
    /// `Fresh` / `Resumed` / `DriftReload`, and snapshot `(current_seq,
    /// floor_seq)` for the response. Does **not** install the attachment;
    /// the caller threads the same `last_seen` into
    /// `Session::install_attachment` later (the bridge dispatcher does this
    /// inside `serve_stream_with_session`).
    pub fn resolve_attach(
        &self,
        node_id: NodeId,
        agent: AgentId,
        last_seen: Option<u64>,
    ) -> ResolvedAttach {
        // ponytail: the shell agent's ring holds shell/output+shell/exit
        // notifications from a killed pty. Replaying them to a reattaching
        // client makes the client treat a stale exit as its own and drop
        // the terminal (see litter remote_alleycat read_output_loop). Shell
        // has no meaningful cross-connection state, so drop any prior
        // session on reattach and always start Fresh.
        if agent == "shell" {
            self.inner.lock().unwrap().remove(&(node_id.clone(), agent));
        }
        let (session, was_existing) = {
            let mut inner = self.inner.lock().unwrap();
            let key = (node_id.clone(), agent);
            if let Some(existing) = inner.get(&key) {
                (existing.clone(), true)
            } else {
                let fresh = Arc::new(Session::new(
                    agent,
                    node_id,
                    self.config.ring_max_msgs,
                    self.config.ring_max_bytes,
                ));
                inner.insert(key, fresh.clone());
                (fresh, false)
            }
        };
        let (current_seq, floor_seq) = session.peek_seq();

        // Effective cursor used to pick the replay slice. For an existing
        // session where the client didn't carry a resume hint, the server
        // auto-resumes from what its previous drainer last attempted —
        // letting an unmodified litter client get mid-turn replay for free.
        let effective_last_seen: Option<u64> = match (was_existing, last_seen) {
            (false, _) => None,
            (true, Some(cursor)) => Some(cursor),
            (true, None) => Some(session.last_attempted_seq().saturating_sub(1)),
        };

        let kind = match (was_existing, effective_last_seen) {
            (false, _) => AttachKind::Fresh,
            (true, None) => AttachKind::Fresh,
            (true, Some(cursor)) => match session.peek_replay(cursor) {
                Ok(()) => AttachKind::Resumed,
                Err(_) => AttachKind::DriftReload,
            },
        };

        ResolvedAttach {
            session,
            kind,
            current_seq,
            floor_seq,
            effective_last_seen,
        }
    }

    /// Drop the session for `(node_id, agent)` if present.
    pub fn release(&self, node_id: &str, agent: AgentId) -> Option<Arc<Session>> {
        let mut inner = self.inner.lock().unwrap();
        inner.remove(&(node_id.to_string(), agent))
    }

    /// Snapshot live sessions — used by the reaper to scan for idle/grace
    /// expiry without holding the registry lock across awaits.
    pub fn snapshot(&self) -> Vec<Arc<Session>> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    /// Spawn a background task that periodically:
    /// 1. Cancels pending server-requests in sessions detached longer than
    ///    `pending_grace` (caller's outstanding approval prompts time out).
    /// 2. Drops sessions detached longer than `idle_ttl` AND with no
    ///    outstanding requests.
    ///
    /// Returns the task handle so the daemon can join on shutdown. Holds a
    /// `Weak` to the registry so dropping the registry stops the reaper.
    pub fn spawn_reaper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let pending_grace = self.config.pending_grace;
        let idle_ttl = self.config.idle_ttl;
        // Sweep on a coarse interval — the work is cheap and timing precision
        // matters less than not waking up needlessly.
        let interval = std::cmp::min(
            std::cmp::min(pending_grace / 4, idle_ttl / 4),
            Duration::from_secs(30),
        )
        .max(Duration::from_secs(1));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(registry) = weak.upgrade() else {
                    break;
                };
                registry.tick(pending_grace, idle_ttl);
            }
        })
    }

    /// One reaper tick. Public for tests so we can drive deterministic
    /// expiry without sleeping.
    pub fn tick(&self, pending_grace: Duration, idle_ttl: Duration) {
        // Phase 1: cancel pending in sessions past pending_grace. Don't drop
        // them yet — they may still be useful (the agent process is still
        // running and can be reattached for a new turn).
        for session in self.snapshot() {
            if session.detached_for(pending_grace) && session.has_outstanding_requests() {
                session.cancel_all_pending();
            }
        }
        // Phase 2: drop fully-idle sessions.
        let mut to_drop: Vec<(NodeId, AgentId)> = Vec::new();
        {
            let inner = self.inner.lock().unwrap();
            for ((node_id, agent), session) in inner.iter() {
                if session.detached_for(idle_ttl) && !session.has_outstanding_requests() {
                    to_drop.push((node_id.clone(), *agent));
                }
            }
        }
        if !to_drop.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for key in to_drop {
                inner.remove(&key);
            }
        }
    }
}

/// Weak handle, used by callers that want to participate in registry
/// lifecycle without keeping it alive (e.g. logging tasks).
#[allow(dead_code)]
pub type SessionRegistryWeak = Weak<SessionRegistry>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ServerRequestError;
    use serde_json::Value;
    use tokio::sync::oneshot;

    fn notif(method: &str) -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": {},
        })
    }

    #[test]
    fn get_or_create_is_idempotent() {
        let reg = SessionRegistry::new(SessionRegistryConfig::default());
        let a = reg.get_or_create("node-abc".into(), "pi");
        let b = reg.get_or_create("node-abc".into(), "pi");
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn separate_keys_distinct_sessions() {
        let reg = SessionRegistry::new(SessionRegistryConfig::default());
        let a = reg.get_or_create("node-abc".into(), "pi");
        let b = reg.get_or_create("node-abc".into(), "claude");
        let c = reg.get_or_create("node-xyz".into(), "pi");
        assert!(!Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn tick_drops_idle_unattached_sessions() {
        let reg = SessionRegistry::new(SessionRegistryConfig::default());
        let session = reg.get_or_create("node-abc".into(), "pi");
        // Attach + immediately detach so detached_at is set.
        let _h = session.install_attachment(None);
        session.drop_attachment();
        // Use zero grace/ttl so the session expires immediately.
        reg.tick(Duration::from_millis(0), Duration::from_millis(0));
        assert!(reg.get("node-abc", "pi").is_none());
    }

    #[test]
    fn tick_does_not_drop_attached_session() {
        let reg = SessionRegistry::new(SessionRegistryConfig::default());
        let session = reg.get_or_create("node-abc".into(), "pi");
        let _handle = session.install_attachment(None);
        session.enqueue(notif("a"));
        reg.tick(Duration::from_millis(0), Duration::from_millis(0));
        assert!(reg.get("node-abc", "pi").is_some());
    }

    #[test]
    fn tick_cancels_pending_past_grace_but_keeps_session() {
        let reg = SessionRegistry::new(SessionRegistryConfig::default());
        let session = reg.get_or_create("node-abc".into(), "pi");
        let (tx, rx) = oneshot::channel();
        session.register_pending(
            "r-1".into(),
            "command/approve".into(),
            serde_json::json!({}),
            tx,
        );
        let _h = session.install_attachment(None);
        session.drop_attachment();
        // Past pending_grace but well under idle_ttl: cancel pending, keep
        // the session itself for potential reuse on reattach.
        reg.tick(Duration::from_millis(0), Duration::from_secs(3600));
        assert!(reg.get("node-abc", "pi").is_some());
        match rx.blocking_recv() {
            Ok(Err(ServerRequestError::ConnectionClosed)) => {}
            other => panic!("expected ConnectionClosed, got {other:?}"),
        }
    }
}
