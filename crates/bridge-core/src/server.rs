use std::collections::HashSet;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixListener;
#[cfg(unix)]
use tracing::debug;
use tracing::warn;

use crate::envelope::{
    InboundMessage, JsonRpcError, JsonRpcMessage, JsonRpcResponse, JsonRpcVersion, error_codes,
};
use crate::framing::{read_json_line, write_json_line};
use crate::notify::NotificationSender;
use crate::session::{AttachHandle, Session, SessionRegistry, SessionRegistryConfig};
use crate::state::Capabilities;

/// Per-stream context handed to bridge handlers. Wraps the session so
/// handlers can emit notifications, issue server→client requests, and read
/// negotiated capabilities — all of which now live on the session and
/// survive the iroh stream lifetime.
#[derive(Clone)]
pub struct Conn {
    session: Arc<Session>,
    notifier: NotificationSender,
}

impl Conn {
    pub fn from_session(session: Arc<Session>) -> Self {
        let notifier = NotificationSender::new(Arc::clone(&session));
        Self { session, notifier }
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    pub fn notifier(&self) -> &NotificationSender {
        &self.notifier
    }

    pub fn capabilities(&self) -> Capabilities {
        self.session.capabilities()
    }

    pub fn should_emit(&self, method: &str) -> bool {
        self.session.should_emit(method)
    }

    pub fn set_initialize_capabilities(&self, params: &Value) {
        let client_info = params.get("clientInfo");
        let capabilities = params.get("capabilities");
        let opt_out = capabilities
            .and_then(|value| value.get("optOutNotificationMethods"))
            .and_then(|value| value.as_array())
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        self.session.set_capabilities(Capabilities {
            experimental_api: capabilities
                .and_then(|value| value.get("experimentalApi"))
                .and_then(|value| value.as_bool())
                .unwrap_or(false),
            opt_out_notification_methods: opt_out,
            client_name: client_info
                .and_then(|value| value.get("name"))
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            client_title: client_info
                .and_then(|value| value.get("title"))
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            client_version: client_info
                .and_then(|value| value.get("version"))
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
        });
    }
}

#[async_trait]
pub trait Bridge: Send + Sync + 'static {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError>;
    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError>;

    async fn notification(&self, _ctx: &Conn, _method: &str, _params: Value) {}

    /// Called once during daemon graceful shutdown. Bridges that spawn
    /// long-lived child processes (ACP agents, claude, opencode, …)
    /// should override this to kill their children synchronously rather
    /// than relying on the tokio runtime's Drop chain — Drop may not
    /// run all the way through during process exit, which leaves
    /// orphaned children behind across restarts.
    async fn shutdown(&self) {}
}

#[derive(Debug, Clone)]
#[cfg(unix)]
pub struct ServerOptions {
    pub socket_path: PathBuf,
    pub unlink_stale: bool,
}

#[cfg(unix)]
pub async fn serve_unix<B>(bridge: Arc<B>, options: ServerOptions) -> anyhow::Result<()>
where
    B: Bridge + ?Sized,
{
    bind_unix_socket(&options.socket_path, options.unlink_stale)?;
    let listener = UnixListener::bind(&options.socket_path)?;
    loop {
        let (stream, _) = listener.accept().await?;
        let bridge = Arc::clone(&bridge);
        tokio::spawn(async move {
            if let Err(error) = serve_stream(bridge, stream).await {
                debug!("bridge connection ended: {error:#}");
            }
        });
    }
}

#[cfg(unix)]
fn bind_unix_socket(path: &Path, unlink_stale: bool) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if unlink_stale {
        match std::fs::remove_file(path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Legacy entry point: anonymous one-shot session that lives for the duration
/// of the stream. Used by callers that don't yet participate in the session
/// registry (Unix-socket bridge servers, conformance tests). Cancels pending
/// server-requests when the stream closes.
pub async fn serve_stream<B, S>(bridge: Arc<B>, stream: S) -> anyhow::Result<()>
where
    B: Bridge + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let registry = SessionRegistry::new(SessionRegistryConfig::default());
    let session = registry.get_or_create("anonymous".into(), "anonymous");
    let result = serve_stream_with_session(bridge, stream, Arc::clone(&session), None).await;
    session.cancel_all_pending();
    result
}

/// Drive a `Bridge` over the process's own `stdin`/`stdout`. Used by bridge
/// binaries when they're launched in stdio mode (no `--socket` flag) — every
/// such `main.rs` used to roll its own `tokio::io::split` + reader/writer
/// plumbing; this helper centralizes it.
///
/// Internally this constructs an `AsyncRead + AsyncWrite` duplex from the two
/// halves and delegates to [`serve_stream`].
pub async fn serve_stdio<B>(bridge: Arc<B>) -> anyhow::Result<()>
where
    B: Bridge + ?Sized,
{
    let stream = StdioStream::new();
    serve_stream(bridge, stream).await
}

/// Combines `tokio::io::stdin()` and `tokio::io::stdout()` into a single
/// `AsyncRead + AsyncWrite` value so the existing `serve_stream` plumbing can
/// drive them. `stdin()` and `stdout()` are themselves `Unpin`, so this
/// wrapper is too.
struct StdioStream {
    stdin: tokio::io::Stdin,
    stdout: tokio::io::Stdout,
}

impl StdioStream {
    fn new() -> Self {
        Self {
            stdin: tokio::io::stdin(),
            stdout: tokio::io::stdout(),
        }
    }
}

impl AsyncRead for StdioStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stdin).poll_read(cx, buf)
    }
}

impl AsyncWrite for StdioStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stdout).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stdout).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stdout).poll_shutdown(cx)
    }
}

/// Drive a stream against a registry-owned session, returning when the
/// reader half closes. The caller is expected to have already validated
/// auth and resolved the resume cursor; on stream close, the caller decides
/// whether to retain the session (for reattach) or drop it.
///
/// `last_seen` is the client's resume cursor. `None` means a fresh attach
/// (no replay).
pub async fn serve_stream_with_session<B, S>(
    bridge: Arc<B>,
    stream: S,
    session: Arc<Session>,
    last_seen: Option<u64>,
) -> anyhow::Result<()>
where
    B: Bridge + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let conn = Conn::from_session(Arc::clone(&session));

    let attach = session.install_attachment(last_seen);
    let generation = attach.generation;
    let writer_task = tokio::spawn(drain_attachment(writer, attach, Arc::clone(&session)));

    let result = run_reader(bridge, &conn, &mut reader).await;
    session.drop_attachment(generation);
    let _ = writer_task.await;
    result
}

/// Background drainer that flushes the attachment's replay backlog and
/// optional `serverRequest/replay` notification, then live-tails the
/// session's mpsc until it closes (because the attachment was preempted or
/// dropped).
///
/// Updates `session.last_attempted_seq` via `fetch_max` immediately before
/// each `write_json_line` — that's the high-water mark the next reattach
/// uses to auto-resume, even if the client didn't send an explicit cursor.
async fn drain_attachment<W>(mut writer: W, attach: AttachHandle, session: Arc<Session>)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let AttachHandle {
        backlog,
        replay_redelivery,
        mut live_rx,
        ..
    } = attach;

    for sequenced in backlog {
        session.note_drainer_attempt(sequenced.seq);
        if write_json_line(&mut writer, &sequenced.payload)
            .await
            .is_err()
        {
            return;
        }
    }
    match replay_redelivery {
        Some(payload) if write_json_line(&mut writer, &payload).await.is_err() => return,
        _ => {}
    }
    while let Some(sequenced) = live_rx.recv().await {
        session.note_drainer_attempt(sequenced.seq);
        if write_json_line(&mut writer, &sequenced.payload)
            .await
            .is_err()
        {
            break;
        }
    }
    let _ = writer.shutdown().await;
}

async fn run_reader<B, R>(
    bridge: Arc<B>,
    conn: &Conn,
    reader: &mut BufReader<R>,
) -> anyhow::Result<()>
where
    B: Bridge + ?Sized,
    R: AsyncRead + Unpin + Send,
{
    while let Some(value) = read_json_line::<Value, _>(reader).await? {
        let inbound = match InboundMessage::from_value(value.clone()) {
            Ok(inbound) => inbound,
            Err(error) => {
                warn!(raw = %value, "discarding malformed json-rpc frame: {error}");
                continue;
            }
        };
        match inbound {
            InboundMessage::Request(request) => {
                tracing::info!(method = %request.method, id = %request.id, "json-rpc request");
                let bridge = Arc::clone(&bridge);
                let conn = conn.clone();
                tokio::spawn(async move {
                    let id = request.id;
                    let method = request.method;
                    let params = request.params.unwrap_or(Value::Null);
                    let result = if method == "initialize" {
                        conn.set_initialize_capabilities(&params);
                        bridge.initialize(&conn, params).await
                    } else {
                        bridge.dispatch(&conn, &method, params).await
                    };
                    let response = match result {
                        Ok(result) => JsonRpcResponse {
                            jsonrpc: JsonRpcVersion,
                            id,
                            result: Some(result),
                            error: None,
                        },
                        Err(error) => JsonRpcResponse {
                            jsonrpc: JsonRpcVersion,
                            id,
                            result: None,
                            error: Some(error),
                        },
                    };
                    let _ = conn
                        .notifier()
                        .send_message(JsonRpcMessage::Response(response));
                });
            }
            InboundMessage::Notification(notification) => {
                bridge
                    .notification(
                        conn,
                        &notification.method,
                        notification.params.unwrap_or(Value::Null),
                    )
                    .await;
            }
            InboundMessage::Response(response) => {
                conn.notifier().resolve_response(response).await;
            }
        }
    }
    Ok(())
}

pub fn json_error_from_anyhow(error: anyhow::Error) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: format!("{error:#}"),
        data: None,
    }
}
