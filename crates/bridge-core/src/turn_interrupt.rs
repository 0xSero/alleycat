//! Guarantee a `turn/completed{interrupted}` after a successful
//! `turn/interrupt`.
//!
//! Several bridges only forward the abort to the native agent (pi `abort`,
//! claude SIGINT, opencode `/abort`, ...) and rely on the agent's own
//! turn-end event to emit `turn/completed`. When that event never arrives,
//! or arrives late, clients keep the turn marked active. [`InterruptCompletionBridge`]
//! emits the completion itself as soon as the interrupt succeeds. The
//! session drops any later `turn/completed` for the same thread/turn (and
//! this synthetic one when the bridge already completed the turn), so the
//! client sees exactly one.

use std::sync::Arc;

use alleycat_codex_proto as p;
use async_trait::async_trait;
use serde_json::Value;

use crate::envelope::JsonRpcError;
use crate::server::{Bridge, Conn};

pub const TURN_INTERRUPT_METHOD: &str = "turn/interrupt";

pub struct InterruptCompletionBridge {
    inner: Arc<dyn Bridge>,
}

impl InterruptCompletionBridge {
    pub fn new(inner: Arc<dyn Bridge>) -> Self {
        Self { inner }
    }
}

/// Build and enqueue `turn/completed{status: interrupted}` for `params`
/// (a `turn/interrupt` request). No-op when the ids are missing.
pub fn emit_interrupted_turn_completed(ctx: &Conn, params: &Value) {
    let Ok(params) = serde_json::from_value::<p::TurnInterruptParams>(params.clone()) else {
        return;
    };
    if params.thread_id.is_empty() || params.turn_id.is_empty() {
        return;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default();
    let notification = p::TurnCompletedNotification {
        thread_id: params.thread_id,
        turn: p::Turn {
            id: params.turn_id,
            items: Vec::new(),
            items_view: p::default_items_view(),
            status: p::TurnStatus::Interrupted,
            error: None,
            started_at: None,
            completed_at: Some(now),
            duration_ms: None,
        },
    };
    let _ = ctx.notifier().send_notification("turn/completed", notification);
}

#[async_trait]
impl Bridge for InterruptCompletionBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        self.inner.initialize(ctx, params).await
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        if method != TURN_INTERRUPT_METHOD {
            return self.inner.dispatch(ctx, method, params).await;
        }
        let result = self.inner.dispatch(ctx, method, params.clone()).await;
        if result.is_ok() {
            emit_interrupted_turn_completed(ctx, &params);
        }
        result
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        self.inner.notification(ctx, method, params).await
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Session;
    use serde_json::json;
    use std::sync::Mutex;

    /// Interrupt succeeds unless `fail`; optionally emits its own
    /// `turn/completed` (as a bridge whose turn-end path fires) first.
    struct Fake {
        fail: bool,
        self_complete: Mutex<Option<&'static str>>,
    }

    #[async_trait]
    impl Bridge for Fake {
        async fn initialize(&self, _: &Conn, _: Value) -> Result<Value, JsonRpcError> {
            Ok(json!({}))
        }
        async fn dispatch(&self, ctx: &Conn, method: &str, params: Value) -> Result<Value, JsonRpcError> {
            assert_eq!(method, TURN_INTERRUPT_METHOD);
            if self.fail {
                return Err(JsonRpcError::internal("no active turn"));
            }
            if let Some(status) = *self.self_complete.lock().unwrap() {
                let _ = ctx.notifier().send_notification(
                    "turn/completed",
                    json!({"threadId": params["threadId"], "turn": {"id": params["turnId"], "items": [], "status": status}}),
                );
            }
            Ok(json!({}))
        }
    }

    fn conn() -> (Arc<Session>, Conn) {
        let session = Arc::new(Session::new("pi", "node".into(), 64, 1 << 20));
        (session.clone(), Conn::from_session(session))
    }

    fn completions(session: &Session) -> Vec<Value> {
        session
            .install_attachment(Some(0))
            .backlog
            .into_iter()
            .filter(|item| item.payload["method"] == "turn/completed")
            .map(|item| item.payload["params"].clone())
            .collect()
    }

    #[tokio::test]
    async fn successful_interrupt_emits_interrupted_completion_once() {
        let (session, conn) = conn();
        let bridge = InterruptCompletionBridge::new(Arc::new(Fake { fail: false, self_complete: Mutex::new(None) }));
        let params = json!({"threadId": "th", "turnId": "tu"});
        bridge.dispatch(&conn, TURN_INTERRUPT_METHOD, params.clone()).await.unwrap();
        // The native turn-end path firing afterwards is dropped.
        let _ = conn.notifier().send_notification(
            "turn/completed",
            json!({"threadId": "th", "turn": {"id": "tu", "items": [], "status": "completed"}}),
        );
        let seen = completions(&session);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0]["turn"]["status"], "interrupted");
        assert_eq!(seen[0]["turn"]["id"], "tu");
    }

    #[tokio::test]
    async fn bridge_completion_first_suppresses_synthetic() {
        let (session, conn) = conn();
        let bridge = InterruptCompletionBridge::new(Arc::new(Fake {
            fail: false,
            self_complete: Mutex::new(Some("interrupted")),
        }));
        bridge
            .dispatch(&conn, TURN_INTERRUPT_METHOD, json!({"threadId": "th", "turnId": "tu"}))
            .await
            .unwrap();
        assert_eq!(completions(&session).len(), 1);
    }

    #[tokio::test]
    async fn failed_interrupt_emits_nothing_and_other_turns_unaffected() {
        let (session, conn) = conn();
        let bridge = InterruptCompletionBridge::new(Arc::new(Fake { fail: true, self_complete: Mutex::new(None) }));
        assert!(bridge
            .dispatch(&conn, TURN_INTERRUPT_METHOD, json!({"threadId": "th", "turnId": "tu"}))
            .await
            .is_err());
        let _ = conn.notifier().send_notification(
            "turn/completed",
            json!({"threadId": "th", "turn": {"id": "tu2", "items": [], "status": "completed"}}),
        );
        let seen = completions(&session);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0]["turn"]["id"], "tu2");
    }
}
