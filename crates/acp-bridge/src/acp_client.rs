//! ACP client for communicating with ACP-compliant agents over stdio.
//!
//! Architecture:
//!
//! * One background reader task owns the agent's stdout for the
//!   lifetime of the client and demuxes every line:
//!   * Frames with an `id` and no `method` are matched to the
//!     currently-outstanding `send_request` and delivered through a
//!     `oneshot::Sender<Value>`.
//!   * Frames with both `id` and `method` are agent→client JSON-RPC
//!     requests (permissions, filesystem, terminal) and are answered by
//!     the reader task.
//!   * Frames without an `id` (JSON-RPC notifications) fire the
//!     currently-registered notification subscriber if any, and are
//!     also pushed onto a fallback buffer so callers using
//!     `take_pending_notifications` see them.
//!
//! * Only one in-flight `send_request` is permitted at a time. We
//!   serialize via the stdin mutex (and a parallel "in-flight slot"
//!   mutex). ACP doesn't tag notifications with which request they
//!   correspond to, so deliberately serializing requests avoids
//!   ambiguity — notifications between request and response are
//!   unambiguously attributable to the in-flight request.
//!
//! * `send_request_streaming` lets the caller observe each notification
//!   as it arrives (instead of waiting for the response and then
//!   draining a buffer). `handle_turn_start` uses this to emit codex
//!   `item/*` notifications live as the ACP agent streams its output,
//!   so iOS sees the assistant text and tool bubbles appear in real
//!   time instead of all at once at the end of the turn.

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alleycat_bridge_core::{ChildProcess, ProcessLauncher, ProcessRole, ProcessSpec, StdioMode};
use anyhow::Result;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::{debug, error, info, warn};

use crate::config::AcpBridgeConfig;

static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared mutable state owned by both the background reader task and
/// public client methods. The reader task holds an `Arc<Inner>` for the
/// lifetime of the agent process.
struct Inner {
    /// Outstanding `send_request` calls keyed by JSON-RPC id. Each entry
    /// is a oneshot the reader fulfills when the matching response
    /// arrives. Multiple entries are allowed in principle, but in
    /// practice we serialize requests via `request_lock`.
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    /// Optional live-notification subscriber installed by
    /// `send_request_streaming`. The reader task forwards every
    /// notification frame here while one is registered.
    notification_tx: Mutex<Option<mpsc::UnboundedSender<Value>>>,
    /// Fallback buffer for `take_pending_notifications` — populated for
    /// every notification regardless of whether a streaming subscriber
    /// is registered. Callers that *only* want streaming should drain
    /// this once after their request to discard the duplicates.
    pending_notifications: Mutex<Vec<Value>>,
    terminals: Mutex<HashMap<String, TerminalRecord>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            notification_tx: Mutex::new(None),
            pending_notifications: Mutex::new(Vec::new()),
            terminals: Mutex::new(HashMap::new()),
        }
    }

    /// Route an inbound JSON frame.
    async fn dispatch(&self, frame: Value) {
        if let Some(id_val) = frame.get("id") {
            let id = match id_val.as_str() {
                Some(s) => s.to_string(),
                None => id_val.to_string(),
            };
            let mut map = self.pending.lock().await;
            if let Some(tx) = map.remove(&id) {
                let _ = tx.send(frame);
            } else {
                // No one is waiting on this id — drop with a warning. ACP
                // agents shouldn't send unsolicited responses.
                warn!(id, "received response for unknown request id; dropping");
            }
            return;
        }
        // Notification: forward to live subscriber + buffer.
        let buffered = frame.clone();
        if let Some(tx) = self.notification_tx.lock().await.as_ref() {
            if tx.send(frame).is_err() {
                // Subscriber went away — clear it so we stop trying.
                *self.notification_tx.lock().await = None;
            }
        }
        self.pending_notifications.lock().await.push(buffered);
    }
}

/// ACP client that communicates with an ACP agent over stdio.
pub struct AcpClient {
    process: Arc<Mutex<Box<dyn ChildProcess>>>,
    stdin: Arc<Mutex<alleycat_bridge_core::ChildStdin>>,
    inner: Arc<Inner>,
    /// Serializes outstanding requests. ACP notifications aren't tagged
    /// with which in-flight request they belong to, so we deliberately
    /// run one request at a time.
    request_lock: Arc<Mutex<()>>,
    /// JoinHandle for the background reader so we can abort it on
    /// `kill()` instead of leaking the task.
    reader_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl AcpClient {
    /// Spawn a new ACP agent process and create a client for it.
    pub async fn spawn(
        config: &AcpBridgeConfig,
        launcher: &Arc<dyn ProcessLauncher>,
    ) -> Result<Self> {
        let args: Vec<OsString> = config
            .agent_args
            .iter()
            .map(|s| OsString::from(s.as_str()))
            .collect();

        // stderr is set to Null on purpose: ACP agents (devin in
        // particular) emit a steady stream of tracing output. A
        // Piped+unread stderr deadlocks the child once the OS pipe
        // buffer fills (~64KB on macOS) — child blocks on stderr write,
        // stops draining stdin, and the bridge then hangs.
        let spec = ProcessSpec {
            program: config.agent_bin.clone(),
            args,
            role: ProcessRole::Agent,
            cwd: None,
            env: vec![],
            env_clear: false,
            stdin: StdioMode::Piped,
            stdout: StdioMode::Piped,
            stderr: StdioMode::Null,
        };

        info!(?spec, "spawning ACP agent process");
        let mut process = launcher.launch(spec).await?;

        let stdin = process
            .take_stdin()
            .ok_or_else(|| anyhow::anyhow!("ACP agent has no stdin pipe"))?;
        let stdout = process
            .take_stdout()
            .ok_or_else(|| anyhow::anyhow!("ACP agent has no stdout pipe"))?;

        let inner = Arc::new(Inner::new());
        let stdin = Arc::new(Mutex::new(stdin));
        let reader_inner = Arc::clone(&inner);
        let reader_stdin = Arc::clone(&stdin);
        let handle = tokio::spawn(async move {
            reader_task(BufReader::new(stdout), reader_inner, reader_stdin).await;
        });

        Ok(Self {
            process: Arc::new(Mutex::new(process)),
            stdin,
            inner,
            request_lock: Arc::new(Mutex::new(())),
            reader_handle: Arc::new(Mutex::new(Some(handle))),
        })
    }

    /// Drain notifications buffered since the last call. Kept for
    /// callers (currently `handle_thread_resume`'s `session/load` drain
    /// path) that prefer the post-response batch model over streaming.
    pub async fn take_pending_notifications(&self) -> Vec<Value> {
        let mut guard = self.inner.pending_notifications.lock().await;
        std::mem::take(&mut *guard)
    }

    /// Send a JSON-RPC request and wait for the response.
    pub async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        self.send_request_inner(method, params, None).await
    }

    /// Send a JSON-RPC request and fire `on_notification(value)` for
    /// every notification frame received between the request being
    /// written and the response arriving. Notifications received after
    /// the response are NOT delivered to `on_notification`; they remain
    /// in `take_pending_notifications`.
    pub async fn send_request_streaming<F>(
        &self,
        method: &str,
        params: Value,
        mut on_notification: F,
    ) -> Result<Value>
    where
        F: FnMut(Value) + Send + 'static,
    {
        let (note_tx, mut note_rx) = mpsc::unbounded_channel::<Value>();
        // Run the notification consumer on its own task so it can keep
        // up while we're blocked on the response oneshot.
        let consumer = tokio::spawn(async move {
            while let Some(v) = note_rx.recv().await {
                on_notification(v);
            }
        });
        let result = self.send_request_inner(method, params, Some(note_tx)).await;
        // Closing the subscriber drops note_tx (unregistered inside
        // send_request_inner), which ends note_rx, which ends `consumer`.
        let _ = consumer.await;
        result
    }

    async fn send_request_inner(
        &self,
        method: &str,
        params: Value,
        notification_tx: Option<mpsc::UnboundedSender<Value>>,
    ) -> Result<Value> {
        // Serialize: only one in-flight request per client.
        let _slot = self.request_lock.lock().await;

        let request_id = REQUEST_ID_COUNTER
            .fetch_add(1, Ordering::SeqCst)
            .to_string();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        });

        // Register oneshot BEFORE writing so a fast response doesn't race us.
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .await
            .insert(request_id.clone(), tx);

        // Install live notification subscriber if requested. Cleared
        // automatically on the way out.
        if let Some(sub) = notification_tx {
            *self.inner.notification_tx.lock().await = Some(sub);
        }

        // Drain any stale buffered notifications from before this
        // request — they belong to whatever happened earlier and would
        // pollute streaming consumers.
        self.inner.pending_notifications.lock().await.clear();

        debug!(method, request_id, "sending ACP request");

        let request_line = serde_json::to_string(&request)?;
        let write_result = async {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(request_line.as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await?;
            Ok::<_, std::io::Error>(())
        }
        .await;
        if let Err(err) = write_result {
            // Clean up the pending registration if the write failed.
            self.inner.pending.lock().await.remove(&request_id);
            *self.inner.notification_tx.lock().await = None;
            return Err(err.into());
        }

        // Wait for the response. The reader task will route the matching
        // frame here once it lands.
        let response = match rx.await {
            Ok(v) => v,
            Err(_) => {
                self.inner.pending.lock().await.remove(&request_id);
                *self.inner.notification_tx.lock().await = None;
                anyhow::bail!("ACP agent connection closed before response");
            }
        };

        // Clear streaming subscriber so it doesn't receive frames from
        // the NEXT request.
        *self.inner.notification_tx.lock().await = None;

        debug!(method, request_id, "received ACP response");

        if let Some(error) = response.get("error") {
            error!(?error, "ACP agent returned error");
            // Surface just the human-readable `message` so callers (and
            // ultimately the iOS error toast) see a clean line.
            let message = error
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("ACP agent returned an error");
            anyhow::bail!("{message}");
        }

        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }

    /// Send a JSON-RPC notification (no response expected).
    pub async fn send_notification(&self, method: &str, params: Value) -> Result<()> {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });

        debug!(method, "sending ACP notification");

        let notification_line = serde_json::to_string(&notification)?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(notification_line.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;

        Ok(())
    }

    /// Kill the underlying agent process and abort the reader task.
    pub async fn kill(&self) -> Result<()> {
        if let Some(handle) = self.reader_handle.lock().await.take() {
            handle.abort();
        }
        let mut process = self.process.lock().await;
        process
            .kill()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to kill process: {}", e))
    }
}

/// Background loop: read newline-delimited JSON frames from the agent
/// and dispatch each one through `Inner`.
async fn reader_task(
    mut reader: BufReader<alleycat_bridge_core::ChildStdout>,
    inner: Arc<Inner>,
    stdin: Arc<Mutex<alleycat_bridge_core::ChildStdin>>,
) {
    loop {
        let mut line = String::new();
        let n = match reader.read_line(&mut line).await {
            Ok(n) => n,
            Err(err) => {
                error!(?err, "error reading from ACP agent stdout");
                break;
            }
        };
        if n == 0 {
            debug!("ACP agent stdout closed");
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(frame) => {
                if frame.get("id").is_some() && frame.get("method").is_some() {
                    // Handle every agent→client request on its own task.
                    // `terminal/wait_for_exit` may legitimately block for
                    // the whole command duration and must not serialize
                    // every other request — including responses that only
                    // this reader task can deliver — behind it.
                    let request_inner = Arc::clone(&inner);
                    let request_stdin = Arc::clone(&stdin);
                    tokio::spawn(respond_to_agent_request(
                        request_inner,
                        request_stdin,
                        frame,
                    ));
                } else {
                    inner.dispatch(frame).await;
                }
            }
            Err(err) => {
                warn!(
                    ?err,
                    line = trimmed,
                    "malformed JSON from ACP agent; dropping"
                );
            }
        }
    }
    // Wake up any outstanding request so callers don't hang forever.
    let mut pending = inner.pending.lock().await;
    for (_id, tx) in pending.drain() {
        // Synthesize an error response so the request fails cleanly.
        let _ = tx.send(serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {"code": -32000, "message": "ACP agent connection closed"},
        }));
    }
}

/// Exit status as reported by `terminal/output` and
/// `terminal/wait_for_exit`. `code` is `None` when the process was
/// terminated by a signal.
#[derive(Debug, Clone)]
struct TerminalExit {
    code: Option<i64>,
    signal: Option<String>,
}

/// Accumulated terminal output. Drains append at the tail; once the
/// configured `outputByteLimit` is exceeded the front is dropped, like a
/// real terminal's scrollback.
#[derive(Debug, Default)]
struct TerminalBuf {
    text: String,
    truncated: bool,
}

/// Live state of one client-side terminal. Clones keep pointing at the
/// same process state, so `terminal/output`, `terminal/wait_for_exit`
/// and `terminal/kill` all observe the same command.
#[derive(Clone)]
struct TerminalRecord {
    buf: Arc<Mutex<TerminalBuf>>,
    exit: watch::Receiver<Option<TerminalExit>>,
    killer: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

async fn respond_to_agent_request(
    inner: Arc<Inner>,
    stdin: Arc<Mutex<alleycat_bridge_core::ChildStdin>>,
    frame: Value,
) {
    let id = frame.get("id").cloned().unwrap_or(Value::Null);
    let method = frame
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let params = frame.get("params").cloned().unwrap_or_else(|| json!({}));
    let response = match handle_agent_request(&inner, method, params).await {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }),
        Err(message) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": message,
            },
        }),
    };
    let line = match serde_json::to_string(&response) {
        Ok(line) => line,
        Err(err) => {
            warn!(?err, "failed to serialize ACP client response");
            return;
        }
    };
    let mut writer = stdin.lock().await;
    if let Err(err) = writer.write_all(line.as_bytes()).await {
        warn!(?err, method, "failed to write ACP client response");
        return;
    }
    if let Err(err) = writer.write_all(b"\n").await {
        warn!(?err, method, "failed to terminate ACP client response");
        return;
    }
    if let Err(err) = writer.flush().await {
        warn!(?err, method, "failed to flush ACP client response");
    }
}

async fn handle_agent_request(
    inner: &Arc<Inner>,
    method: &str,
    params: Value,
) -> std::result::Result<Value, String> {
    match method {
        "session/request_permission" => Ok(handle_permission_request(&params)),
        "fs/read_text_file" => handle_fs_read_text_file(&params).await,
        "fs/write_text_file" => handle_fs_write_text_file(&params).await,
        "fs/file_exists" => Ok(json!(
            path_from_params(&params).is_some_and(|p| p.is_file())
        )),
        "fs/list_directory" => handle_fs_list_directory(&params).await,
        "fs/create_directory" => {
            let path = required_path(&params)?;
            tokio::fs::create_dir_all(&path)
                .await
                .map_err(|err| format!("create_directory {}: {err}", path.display()))?;
            Ok(Value::Null)
        }
        "fs/delete_file" => {
            let path = required_path(&params)?;
            match tokio::fs::remove_file(&path).await {
                Ok(()) => Ok(Value::Null),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Value::Null),
                Err(err) => Err(format!("delete_file {}: {err}", path.display())),
            }
        }
        "terminal/create" => handle_terminal_create(inner, &params).await,
        "terminal/output" => handle_terminal_output(inner, &params).await,
        "terminal/wait_for_exit" => handle_terminal_wait(inner, &params).await,
        "terminal/release" => handle_terminal_release(inner, &params).await,
        "terminal/kill" => handle_terminal_kill(inner, &params).await,
        _ => Err(format!("Unsupported ACP client request method: {method}")),
    }
}

fn handle_permission_request(params: &Value) -> Value {
    let option_id = params
        .get("options")
        .and_then(Value::as_array)
        .and_then(|options| {
            options
                .iter()
                .find(|option| {
                    matches!(
                        option.get("kind").and_then(Value::as_str),
                        Some("allow_once" | "allow_always")
                    )
                })
                .or_else(|| options.first())
        })
        .and_then(|option| option.get("optionId").and_then(Value::as_str));
    match option_id {
        Some(option_id) => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            },
        }),
        None => json!({
            "outcome": {
                "outcome": "cancelled",
            },
        }),
    }
}

async fn handle_fs_read_text_file(params: &Value) -> std::result::Result<Value, String> {
    let path = required_path(params)?;
    let text = tokio::fs::read_to_string(&path)
        .await
        .map_err(|err| format!("read_text_file {}: {err}", path.display()))?;
    let line = params.get("line").and_then(Value::as_u64).unwrap_or(1);
    let limit = params.get("limit").and_then(Value::as_u64);
    let content = if line <= 1 && limit.is_none() {
        text
    } else {
        let start = line.saturating_sub(1) as usize;
        let lines = text.lines().skip(start);
        let selected: Vec<&str> = match limit {
            Some(limit) => lines.take(limit as usize).collect(),
            None => lines.collect(),
        };
        if selected.is_empty() {
            String::new()
        } else {
            let mut joined = selected.join("\n");
            joined.push('\n');
            joined
        }
    };
    Ok(json!({ "content": content }))
}

async fn handle_fs_write_text_file(params: &Value) -> std::result::Result<Value, String> {
    let path = required_path(params)?;
    let content = params
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "write_text_file missing content".to_string())?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| format!("create parent {}: {err}", parent.display()))?;
    }
    tokio::fs::write(&path, content)
        .await
        .map_err(|err| format!("write_text_file {}: {err}", path.display()))?;
    Ok(Value::Null)
}

async fn handle_fs_list_directory(params: &Value) -> std::result::Result<Value, String> {
    let path = required_path(params)?;
    let mut entries = tokio::fs::read_dir(&path)
        .await
        .map_err(|err| format!("list_directory {}: {err}", path.display()))?;
    let mut out = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|err| format!("list_directory {}: {err}", path.display()))?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|err| format!("file_type {}: {err}", entry.path().display()))?;
        out.push(json!({
            "name": entry.file_name().to_string_lossy(),
            "path": entry.path(),
            "isDirectory": file_type.is_dir(),
        }));
    }
    Ok(Value::Array(out))
}

/// Build the process for `terminal/create`.
///
/// Some agents (devin among them) put the entire shell command line into
/// `command` and leave `args` empty. Spawning that string literally
/// looks for an executable named e.g. `echo ok` and fails, so the
/// args-less form goes through the platform shell. When `args` is given
/// the pair is a plain argv and spawns directly.
fn terminal_command(command: &str, args: &[String]) -> Command {
    if args.is_empty() {
        shell_command(command)
    } else {
        let mut cmd = Command::new(command);
        cmd.args(args);
        cmd
    }
}

#[cfg(not(windows))]
fn shell_command(command: &str) -> Command {
    let mut cmd = Command::new("/bin/sh");
    cmd.arg("-c").arg(command);
    cmd
}

#[cfg(windows)]
fn shell_command(command: &str) -> Command {
    let mut cmd = Command::new("cmd");
    cmd.arg("/C").arg(command);
    cmd
}

/// Drain one terminal pipe into the shared scrollback buffer. Both ends
/// drain concurrently so a chatty process can't fill one pipe and
/// deadlock against a read on the other.
async fn drain_terminal_pipe<R>(mut pipe: R, buf: Arc<Mutex<TerminalBuf>>, limit: usize)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0_u8; 8192];
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let mut guard = buf.lock().await;
                guard.text.push_str(&String::from_utf8_lossy(&chunk[..n]));
                if truncate_utf8_tail(&mut guard.text, limit) {
                    guard.truncated = true;
                }
            }
        }
    }
}

fn terminal_exit(res: std::io::Result<std::process::ExitStatus>) -> TerminalExit {
    match res {
        Ok(status) => terminal_exit_status(status),
        Err(err) => TerminalExit {
            code: Some(-1),
            signal: Some(format!("wait failed: {err}")),
        },
    }
}

#[cfg(unix)]
fn terminal_exit_status(status: std::process::ExitStatus) -> TerminalExit {
    use std::os::unix::process::ExitStatusExt;
    TerminalExit {
        code: status.code().map(i64::from),
        signal: status.signal().map(|sig| sig.to_string()),
    }
}

#[cfg(not(unix))]
fn terminal_exit_status(status: std::process::ExitStatus) -> TerminalExit {
    TerminalExit {
        code: status.code().map(i64::from),
        signal: None,
    }
}

/// Wait for the terminal process to exit — or for `terminal/kill` /
/// `terminal/release` to terminate it — and report the final status.
async fn wait_terminal_exit(
    child: &mut tokio::process::Child,
    mut kill_rx: oneshot::Receiver<()>,
) -> TerminalExit {
    enum WaitOutcome {
        Exited(std::io::Result<std::process::ExitStatus>),
        Killed,
    }
    let mut killed = false;
    loop {
        let outcome = tokio::select! {
            res = child.wait() => WaitOutcome::Exited(res),
            _ = &mut kill_rx, if !killed => WaitOutcome::Killed,
        };
        match outcome {
            WaitOutcome::Exited(res) => break terminal_exit(res),
            WaitOutcome::Killed => {
                // Guarded out of future select arms so a spent oneshot
                // can't win every poll forever.
                killed = true;
                let _ = child.start_kill();
            }
        }
    }
}

fn terminal_exit_json(exit: &TerminalExit) -> Value {
    json!({
        "exitCode": exit.code,
        "signal": exit.signal,
    })
}

async fn handle_terminal_create(
    inner: &Arc<Inner>,
    params: &Value,
) -> std::result::Result<Value, String> {
    let command = params
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| "terminal/create missing command".to_string())?;
    let args: Vec<String> = params
        .get("args")
        .and_then(Value::as_array)
        .map(|args| {
            args.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let terminal_id = format!("term_{}", REQUEST_ID_COUNTER.fetch_add(1, Ordering::SeqCst));
    let mut cmd = terminal_command(command, &args);
    if let Some(cwd) = params.get("cwd").and_then(Value::as_str) {
        cmd.current_dir(cwd);
    }
    if let Some(env) = params.get("env").and_then(Value::as_array) {
        for entry in env {
            if let Some(name) = entry.get("name").and_then(Value::as_str) {
                let value = entry.get("value").and_then(Value::as_str).unwrap_or("");
                cmd.env(name, value);
            }
        }
    }
    let limit = params
        .get("outputByteLimit")
        .and_then(Value::as_u64)
        .unwrap_or(1_048_576) as usize;

    // Spawn and hand back the terminal id right away — create must not
    // block on the command itself — then drive the process to exit on a
    // background task. Both pipes drain concurrently so a chatty process
    // can't fill one pipe and deadlock against a read on the other.
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to spawn {command}: {err}"))?;

    let buf = Arc::new(Mutex::new(TerminalBuf::default()));
    let mut drains = Vec::new();
    if let Some(pipe) = child.stdout.take() {
        drains.push(tokio::spawn(drain_terminal_pipe(
            pipe,
            Arc::clone(&buf),
            limit,
        )));
    }
    if let Some(pipe) = child.stderr.take() {
        drains.push(tokio::spawn(drain_terminal_pipe(
            pipe,
            Arc::clone(&buf),
            limit,
        )));
    }

    let (exit_tx, exit_rx) = watch::channel::<Option<TerminalExit>>(None);
    let (kill_tx, kill_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let exit = wait_terminal_exit(&mut child, kill_rx).await;
        // Reap the drains before publishing the exit status so the
        // buffer holds every byte by the time it becomes visible.
        for drain in drains {
            let _ = drain.await;
        }
        let _ = exit_tx.send(Some(exit));
    });

    let record = TerminalRecord {
        buf,
        exit: exit_rx,
        killer: Arc::new(Mutex::new(Some(kill_tx))),
    };
    inner
        .terminals
        .lock()
        .await
        .insert(terminal_id.clone(), record);
    Ok(json!({ "terminalId": terminal_id }))
}

async fn handle_terminal_output(
    inner: &Arc<Inner>,
    params: &Value,
) -> std::result::Result<Value, String> {
    let record = terminal_record(inner, params).await?;
    let exit_status = match record.exit.borrow().clone() {
        Some(exit) => terminal_exit_json(&exit),
        None => Value::Null,
    };
    let buf = record.buf.lock().await;
    Ok(json!({
        "output": buf.text,
        "truncated": buf.truncated,
        "exitStatus": exit_status,
    }))
}

async fn handle_terminal_wait(
    inner: &Arc<Inner>,
    params: &Value,
) -> std::result::Result<Value, String> {
    let mut exit_rx = {
        let record = terminal_record(inner, params).await?;
        record.exit
    };
    let _ = exit_rx
        .wait_for(|exit| exit.is_some())
        .await
        .map_err(|_| "terminal driver stopped without an exit status".to_string())?;
    let exit = exit_rx.borrow().clone().unwrap_or(TerminalExit {
        code: Some(-1),
        signal: None,
    });
    Ok(json!({
        "exitCode": exit.code,
        "signal": exit.signal,
    }))
}

async fn handle_terminal_release(
    inner: &Arc<Inner>,
    params: &Value,
) -> std::result::Result<Value, String> {
    let id = required_terminal_id(params)?;
    if let Some(record) = inner.terminals.lock().await.remove(id) {
        // Release kills a still-running command per spec.
        let mut killer = record.killer.lock().await;
        if let Some(kill) = killer.take() {
            let _ = kill.send(());
        }
    }
    Ok(Value::Null)
}

async fn handle_terminal_kill(
    inner: &Arc<Inner>,
    params: &Value,
) -> std::result::Result<Value, String> {
    let id = required_terminal_id(params)?;
    let map = inner.terminals.lock().await;
    if let Some(record) = map.get(id)
        && record.exit.borrow().is_none()
    {
        let mut killer = record.killer.lock().await;
        if let Some(kill) = killer.take() {
            let _ = kill.send(());
        }
    }
    Ok(Value::Null)
}

async fn terminal_record(
    inner: &Arc<Inner>,
    params: &Value,
) -> std::result::Result<TerminalRecord, String> {
    let id = required_terminal_id(params)?;
    inner
        .terminals
        .lock()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| format!("unknown terminalId: {id}"))
}

fn required_terminal_id(params: &Value) -> std::result::Result<&str, String> {
    params
        .get("terminalId")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing terminalId".to_string())
}

fn required_path(params: &Value) -> std::result::Result<PathBuf, String> {
    path_from_params(params).ok_or_else(|| "missing path".to_string())
}

fn path_from_params(params: &Value) -> Option<PathBuf> {
    params
        .get("path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
}

fn truncate_utf8_tail(text: &mut String, limit: usize) -> bool {
    if text.len() <= limit {
        return false;
    }
    if limit == 0 {
        text.clear();
        return true;
    }
    let mut start = text.len() - limit;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    *text = text[start..].to_string();
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inner() -> Arc<Inner> {
        Arc::new(Inner::new())
    }

    fn params(terminal_id: &str) -> Value {
        json!({ "sessionId": "s", "terminalId": terminal_id })
    }

    async fn create(inner: &Arc<Inner>, create_params: Value) -> String {
        let result = handle_terminal_create(inner, &create_params).await.unwrap();
        result["terminalId"].as_str().unwrap().to_string()
    }

    async fn wait(inner: &Arc<Inner>, id: &str) -> Value {
        handle_terminal_wait(inner, &params(id)).await.unwrap()
    }

    async fn output(inner: &Arc<Inner>, id: &str) -> Value {
        handle_terminal_output(inner, &params(id)).await.unwrap()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_runs_shell_command_line_without_args() {
        // Agents may put the whole shell line in `command` with no args;
        // it must run through the shell rather than spawn as one argv[0].
        let inner = inner();
        let id = create(
            &inner,
            json!({ "sessionId": "s", "command": "echo acp-shell-ok" }),
        )
        .await;
        let exit = wait(&inner, &id).await;
        assert_eq!(exit["exitCode"], json!(0));
        let out = output(&inner, &id).await;
        assert!(out["output"].as_str().unwrap().contains("acp-shell-ok"));
        assert_eq!(out["exitStatus"]["exitCode"], json!(0));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_supports_shell_operators() {
        let inner = inner();
        let id = create(
            &inner,
            json!({ "sessionId": "s", "command": "printf 'x' | wc -c" }),
        )
        .await;
        let exit = wait(&inner, &id).await;
        assert_eq!(exit["exitCode"], json!(0));
        let out = output(&inner, &id).await;
        assert_eq!(out["output"].as_str().unwrap().trim(), "1");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_runs_argv_form_directly() {
        let inner = inner();
        let id = create(
            &inner,
            json!({
                "sessionId": "s",
                "command": "echo",
                "args": ["acp-argv-ok"],
            }),
        )
        .await;
        let exit = wait(&inner, &id).await;
        assert_eq!(exit["exitCode"], json!(0));
        let out = output(&inner, &id).await;
        assert!(out["output"].as_str().unwrap().contains("acp-argv-ok"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_reports_exit_code() {
        let inner = inner();
        let id = create(&inner, json!({ "sessionId": "s", "command": "exit 7" })).await;
        let exit = wait(&inner, &id).await;
        assert_eq!(exit["exitCode"], json!(7));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_honors_cwd_and_env() {
        let inner = inner();
        let id = create(
            &inner,
            json!({
                "sessionId": "s",
                "command": "printf 'VAL=%s' \"$ACP_TEST_ENV\"",
                "cwd": "/tmp",
                "env": [{ "name": "ACP_TEST_ENV", "value": "env-ok" }],
            }),
        )
        .await;
        let exit = wait(&inner, &id).await;
        assert_eq!(exit["exitCode"], json!(0));
        let out = output(&inner, &id).await;
        assert_eq!(out["output"].as_str().unwrap(), "VAL=env-ok");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_with_bad_cwd_fails() {
        let inner = inner();
        let result = handle_terminal_create(
            &inner,
            &json!({
                "sessionId": "s",
                "command": "echo never",
                "cwd": "/nonexistent-dir-for-acp-test",
            }),
        )
        .await;
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_returns_while_command_still_runs_then_kill_terminates() {
        // create must answer promptly — a long-running command can't
        // block the request path, and kill must stop it.
        let inner = inner();
        let started = std::time::Instant::now();
        let id = create(&inner, json!({ "sessionId": "s", "command": "sleep 30" })).await;
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        handle_terminal_kill(&inner, &params(&id)).await.unwrap();
        let exit = wait(&inner, &id).await;
        assert!(exit["exitCode"].is_null());
        assert!(exit["signal"].is_string());
        assert!(started.elapsed() < std::time::Duration::from_secs(15));
    }

    #[tokio::test]
    async fn unknown_terminal_id_errors() {
        let inner = inner();
        assert!(
            handle_terminal_output(&inner, &params("missing"))
                .await
                .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn release_removes_terminal() {
        let inner = inner();
        let id = create(
            &inner,
            json!({ "sessionId": "s", "command": "echo release-ok" }),
        )
        .await;
        handle_terminal_release(&inner, &params(&id)).await.unwrap();
        assert!(handle_terminal_output(&inner, &params(&id)).await.is_err());
    }
}
