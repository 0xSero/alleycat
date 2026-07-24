//! Per-connection state.
//!
//! `ConnectionState` is the per-stream facade that handlers borrow on every
//! request. It pins the negotiated capabilities, the per-connection
//! `ThreadDefaults`, and shared handles to the pi pool, thread index, and
//! `ProcessLauncher`. Handlers never see `PiBridge` directly — `PiBridge`
//! constructs a fresh `ConnectionState` per dispatch and threads it through.
//!
//! What used to be per-stream — the writer mpsc, the pending server-request
//! table — has moved into [`bridge_core::session::Session`] so the
//! producer-side state survives an iroh disconnect. `ConnectionState` now
//! holds an `Arc<Session>` and delegates `send` / `register_pending_request` /
//! `resolve_pending_request` / `cancel_all_pending_requests` to it.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use alleycat_bridge_core::ProcessLauncher;
use alleycat_bridge_core::session::Session;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::codex_proto::{
    ApprovalsReviewer, AskForApproval, InitializeCapabilities, JsonRpcMessage, ReasoningEffort,
    RequestId, SandboxMode,
};
use crate::index::PiSessionRef;
use crate::pool::PiPool;

/// Per-connection bridge state. Cheap to clone: every field is either copy
/// (small enums, atomics in the future) or `Arc`/`Mutex`-protected.
pub struct ConnectionState {
    /// Default config the bridge applies to new threads. Shared via `Arc`
    /// across the handlers in a single connection so writes from
    /// `config/value/write` / `config/batchWrite` are visible to the next
    /// `thread/start` regardless of which handler invocation locked it.
    defaults: Arc<Mutex<ThreadDefaults>>,

    /// Daemon-lifetime session for this `(node_id, agent)` pair. Owns the
    /// writer mpsc, replay ring, and pending server-request table. The
    /// per-stream `ConnectionState` is constructed fresh on every iroh
    /// stream attachment but the session it points at survives.
    session: Arc<Session>,

    /// Pi process pool.
    pi_pool: Arc<PiPool>,

    /// Thread index handle (threads.json).
    thread_index: Arc<dyn ThreadIndexHandle>,

    /// Process launcher used by `command/exec` and the pool. Carries the
    /// daemon's `LocalLauncher` (or, in Litter, an `SshLauncher`) without
    /// the handlers needing to know which.
    launcher: Arc<dyn ProcessLauncher>,

    /// Trust indexed thread cwd values without checking local filesystem
    /// existence. Embedders that run the agent somewhere else, like Litter's
    /// SSH launcher, need the cwd to be validated by that remote process.
    trust_persisted_cwd: bool,

    /// Optional provider scope for this bridge's model catalog.
    model_provider_prefixes: Arc<Vec<String>>,

    /// Optional Pi `models.json` owned by the embedding controller.
    model_catalog_path: Option<PathBuf>,
}

/// Negotiated client capabilities. Defaults to "no opt-outs, no experimental
/// API" so handlers can call `should_emit` even before `initialize` lands.
pub use alleycat_bridge_core::state::Capabilities;

/// Bridge defaults for a new thread. These are seeded on construction and
/// can be overridden per-`thread/start` request via `ThreadStartParams`.
#[derive(Debug, Clone, Default)]
pub struct ThreadDefaults {
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub approval_policy: Option<AskForApproval>,
    pub approvals_reviewer: Option<ApprovalsReviewer>,
    pub sandbox: Option<SandboxMode>,
    /// `service_name` from `thread/start.serviceName`. Persisted so the
    /// bridge can name the underlying pi session consistently.
    pub service_name: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ServerRequestError {
    /// Client reported a JSON-RPC error.
    Rpc { code: i64, message: String },
    /// The connection closed before the client answered.
    ConnectionClosed,
    /// Local timeout fired before the client answered.
    TimedOut,
}

impl From<alleycat_bridge_core::state::ServerRequestError> for ServerRequestError {
    fn from(value: alleycat_bridge_core::state::ServerRequestError) -> Self {
        match value {
            alleycat_bridge_core::state::ServerRequestError::Rpc(err) => Self::Rpc {
                code: err.code,
                message: err.message,
            },
            alleycat_bridge_core::state::ServerRequestError::ConnectionClosed => {
                Self::ConnectionClosed
            }
            alleycat_bridge_core::state::ServerRequestError::TimedOut => Self::TimedOut,
        }
    }
}

// === index handle alias ===================================================

pub use crate::index::{IndexEntry, ListFilter, ListPage, ListSort};

/// Handler-facing surface of the thread index. Marker trait that automatically
/// extends `bridge_core::ThreadIndexHandle<PiSessionRef>` so existing
/// `Arc<dyn ThreadIndexHandle>` callsites keep compiling. The blanket
/// `impl<T: ?Sized + ...>` covers both concrete impls (`ThreadIndex<PiSessionRef>`)
/// and the in-memory test stubs without forcing them to implement two traits.
pub trait ThreadIndexHandle: alleycat_bridge_core::ThreadIndexHandle<PiSessionRef> {}
impl<T: ?Sized + alleycat_bridge_core::ThreadIndexHandle<PiSessionRef>> ThreadIndexHandle for T {}

impl ConnectionState {
    pub fn new(
        session: Arc<Session>,
        pi_pool: Arc<PiPool>,
        thread_index: Arc<dyn ThreadIndexHandle>,
        defaults: Arc<Mutex<ThreadDefaults>>,
        launcher: Arc<dyn ProcessLauncher>,
        trust_persisted_cwd: bool,
        model_provider_prefixes: Arc<Vec<String>>,
        model_catalog_path: Option<PathBuf>,
    ) -> Self {
        Self {
            defaults,
            session,
            pi_pool,
            thread_index,
            launcher,
            trust_persisted_cwd,
            model_provider_prefixes,
            model_catalog_path,
        }
    }

    pub fn model_provider_prefixes(&self) -> &[String] {
        self.model_provider_prefixes.as_slice()
    }

    pub fn model_catalog_path(&self) -> Option<&std::path::Path> {
        self.model_catalog_path.as_deref()
    }

    /// Underlying session — exposed for callers that need session-scoped
    /// request id minting (see `approval::send_server_request`).
    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    /// Replace the negotiated capabilities. Called from `handlers::lifecycle`
    /// when `initialize` lands.
    pub fn set_capabilities(
        &self,
        client_name: Option<String>,
        client_title: Option<String>,
        client_version: Option<String>,
        caps: Option<&InitializeCapabilities>,
    ) {
        let opt_out = caps
            .and_then(|c| c.opt_out_notification_methods.as_ref())
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();
        self.session.set_capabilities(Capabilities {
            experimental_api: caps.is_some_and(|c| c.experimental_api),
            opt_out_notification_methods: opt_out,
            client_name,
            client_title,
            client_version,
        });
    }

    /// Snapshot the current capabilities. Cheap clone of a small struct.
    pub fn capabilities(&self) -> Capabilities {
        self.session.capabilities()
    }

    /// True if the connection has not opted out of `method`.
    pub fn should_emit(&self, method: &str) -> bool {
        self.session.should_emit(method)
    }

    /// Snapshot the bridge thread defaults.
    pub fn defaults(&self) -> ThreadDefaults {
        self.defaults.lock().unwrap().clone()
    }

    /// Mutate the bridge defaults under the lock.
    pub fn update_defaults(&self, f: impl FnOnce(&mut ThreadDefaults)) {
        let mut slot = self.defaults.lock().unwrap();
        f(&mut slot);
    }

    /// Send an outbound JSON-RPC frame (notification, response, or
    /// server→client request) to the client.
    pub fn send(&self, msg: JsonRpcMessage) -> Result<(), SendError> {
        match serde_json::to_value(&msg) {
            Ok(value) => {
                self.session.enqueue(value);
                Ok(())
            }
            Err(_) => Err(SendError::ConnectionClosed),
        }
    }

    /// Register an in-flight server→client request.
    pub async fn register_pending_request(
        &self,
        request_id: RequestId,
        method: String,
        params: Value,
    ) -> oneshot::Receiver<Result<Value, ServerRequestError>> {
        let (tx, rx) = oneshot::channel();
        let key = request_id.to_string();
        let (core_tx, core_rx) =
            oneshot::channel::<Result<Value, alleycat_bridge_core::state::ServerRequestError>>();
        self.session.register_pending(key, method, params, core_tx);
        tokio::spawn(async move {
            let mapped = match core_rx.await {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => Err(e.into()),
                Err(_) => Err(ServerRequestError::ConnectionClosed),
            };
            let _ = tx.send(mapped);
        });
        rx
    }

    /// Resolve an in-flight server→client request with the client's response.
    pub async fn resolve_pending_request(
        &self,
        request_id: &RequestId,
        result: Result<Value, ServerRequestError>,
    ) -> bool {
        let mapped: Result<Value, alleycat_bridge_core::state::ServerRequestError> = match result {
            Ok(v) => Ok(v),
            Err(ServerRequestError::Rpc { code, message }) => {
                Err(alleycat_bridge_core::state::ServerRequestError::Rpc(
                    alleycat_bridge_core::JsonRpcError {
                        code,
                        message,
                        data: None,
                    },
                ))
            }
            Err(ServerRequestError::ConnectionClosed) => {
                Err(alleycat_bridge_core::state::ServerRequestError::ConnectionClosed)
            }
            Err(ServerRequestError::TimedOut) => {
                Err(alleycat_bridge_core::state::ServerRequestError::TimedOut)
            }
        };
        self.session
            .resolve_pending(&request_id.to_string(), mapped)
    }

    /// Cancel every outstanding server→client request, e.g. on connection
    /// shutdown.
    pub async fn cancel_all_pending_requests(&self) {
        self.session.cancel_all_pending();
    }

    pub fn pi_pool(&self) -> &Arc<PiPool> {
        &self.pi_pool
    }

    pub fn thread_index(&self) -> &Arc<dyn ThreadIndexHandle> {
        &self.thread_index
    }

    /// Process launcher used by `command/exec` and the pool.
    pub fn launcher(&self) -> &Arc<dyn ProcessLauncher> {
        &self.launcher
    }

    pub fn trust_persisted_cwd(&self) -> bool {
        self.trust_persisted_cwd
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("connection writer is closed")]
    ConnectionClosed,
}

impl ConnectionState {
    /// Build a `ConnectionState` for tests, backed by an in-memory session
    /// and a `LocalLauncher`.
    pub fn for_test(
        pi_pool: Arc<PiPool>,
        thread_index: Arc<dyn ThreadIndexHandle>,
        defaults: ThreadDefaults,
    ) -> (
        Arc<Self>,
        tokio::sync::mpsc::UnboundedReceiver<alleycat_bridge_core::session::Sequenced>,
    ) {
        let session = Arc::new(Session::new("pi", "test".into(), 64, 1 << 20));
        let attach = session.install_attachment(None);
        let launcher: Arc<dyn ProcessLauncher> = Arc::new(alleycat_bridge_core::LocalLauncher);
        let state = Arc::new(Self::new(
            session,
            pi_pool,
            thread_index,
            Arc::new(Mutex::new(defaults)),
            launcher,
            false,
            Arc::new(Vec::new()),
            None,
        ));
        (state, attach.live_rx)
    }
}
