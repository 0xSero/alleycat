//! `PiProcessHandle` — spawns a single `pi-coding-agent --mode rpc` subprocess
//! bound to a specific cwd and exposes a request/event channel API on top of
//! its line-delimited JSON stdio.
//!
//! Wire model (from `pi-mono/packages/coding-agent/src/modes/rpc/rpc-mode.ts`):
//!
//! - bridge → pi: one JSON object per stdin line, shaped as [`RpcCommand`].
//! - pi → bridge: one JSON object per stdout line; either an [`RpcResponse`]
//!   correlated by `id`, or a session/agent/extension event (the union covered
//!   by [`PiEvent`]).
//!
//! Two background tasks per process:
//!
//! - **writer**: drains a `mpsc::UnboundedReceiver<String>` and writes each
//!   already-serialized JSON line to pi's stdin, terminating with `\n`.
//! - **reader**: reads pi's stdout line-by-line, deserializes each line as a
//!   [`PiOutboundMessage`], routes responses to a per-id oneshot, and
//!   broadcasts events to all subscribers.
//!
//! Both halves shut down cleanly when stdin is closed (pi exits, reader EOFs)
//! or the [`PiProcessHandle`] is dropped (writer mpsc closes, child receives
//! EOF on stdin and exits per the bridge design doc).

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_bridge_core::{
    ChildProcess, ChildStderr, ChildStdin, ChildStdout, LocalLauncher, ProcessLauncher,
    ProcessRole, ProcessSpec, StdioMode,
};
use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::pi_protocol::{PiEvent, PiOutboundMessage, RpcCommand, RpcResponse};

/// CLI flag pi-coding-agent recognizes for headless RPC mode.
/// (`pi-mono/packages/coding-agent/src/cli/args.ts:74`)
const RPC_MODE_FLAG: (&str, &str) = ("--mode", "rpc");

/// How many events to buffer before slow subscribers start losing events
/// (broadcast::Receiver returns `Lagged(n)` past this watermark). Pi can emit
/// streaming text deltas at sub-millisecond cadence, so this needs headroom.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Errors surfaced from the process I/O layer.
#[derive(Debug, Error)]
pub enum PiProcessError {
    #[error("pi process exited before responding to command id={0}")]
    ProcessClosed(String),

    #[error("response for command id={0} could not be delivered: receiver dropped")]
    ResponseDropped(String),

    #[error("failed to write command to pi stdin: {0}")]
    WriterClosed(String),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Pending in-flight requests keyed by their pi command id.
type ResponseTable = Mutex<HashMap<String, PendingResponse>>;

struct PendingResponse {
    command: String,
    tx: oneshot::Sender<RpcResponse>,
}

impl std::fmt::Debug for PendingResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingResponse")
            .field("command", &self.command)
            .finish_non_exhaustive()
    }
}

/// Handle to a single live pi-coding-agent subprocess. Cloning the handle
/// shares the same channels and reference-counted state, so multiple bridge
/// callers can drive the same pi session through different `Arc<PiProcessHandle>`s.
#[derive(Debug)]
pub struct PiProcessHandle {
    /// Working directory the subprocess was launched in (immutable for the
    /// lifetime of the process — pi's `process.chdir` makes it so).
    cwd: PathBuf,
    /// Path to the `pi-coding-agent` binary that was spawned.
    pi_bin: PathBuf,
    /// Process id of the spawned child, captured at spawn for diagnostics.
    pid: Option<u32>,
    /// Sender end of the writer mpsc — closing this is the signal to the
    /// writer task to drop pi's stdin (which makes pi exit cleanly).
    writer_tx: mpsc::UnboundedSender<String>,
    /// Broadcast end for events. Cloned via `subscribe_events()`.
    events_tx: broadcast::Sender<PiEvent>,
    /// Map of pending request ids → oneshot sender for the matching response.
    pending: Arc<ResponseTable>,
    /// Background tasks. Held so they keep running for the handle's lifetime
    /// and abort cleanly on drop.
    _tasks: Arc<TaskSet>,
}

struct TaskSet {
    writer: Mutex<Option<JoinHandle<()>>>,
    reader: Mutex<Option<JoinHandle<()>>>,
    stderr: Mutex<Option<JoinHandle<()>>>,
    /// The owning Child handle. We hold it so the kernel doesn't reap pi
    /// before our reader sees EOF; explicit shutdown goes through
    /// `shutdown()` which kills the child if needed.
    child: Mutex<Option<Box<dyn ChildProcess>>>,
}

impl std::fmt::Debug for TaskSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskSet").finish_non_exhaustive()
    }
}

impl Drop for TaskSet {
    fn drop(&mut self) {
        // Best-effort cleanup: abort tasks in case they're still parked
        // after a panicked spawn or a forgotten shutdown call.
        if let Some(h) = self.writer.try_lock().ok().and_then(|mut g| g.take()) {
            h.abort();
        }
        if let Some(h) = self.reader.try_lock().ok().and_then(|mut g| g.take()) {
            h.abort();
        }
        if let Some(h) = self.stderr.try_lock().ok().and_then(|mut g| g.take()) {
            h.abort();
        }
        // We can't `await` `child.kill()` in Drop, so we rely on the launcher's
        // own kill-on-drop semantics (LocalLauncher uses `kill_on_drop(true)`)
        // when the boxed handle is dropped here.
        if let Some(mut child) = self.child.try_lock().ok().and_then(|mut g| g.take()) {
            drop(child.take_stdin());
            drop(child.take_stdout());
            drop(child.take_stderr());
        }
    }
}

impl PiProcessHandle {
    /// Spawn `pi-coding-agent --mode rpc` bound to `cwd` via the default
    /// `LocalLauncher`. Compatibility wrapper for callers that don't yet
    /// thread a `ProcessLauncher`; new callers should use `launch_with`.
    pub async fn spawn(cwd: impl AsRef<Path>, pi_bin: impl AsRef<Path>) -> Result<Self> {
        Self::launch_with(&LocalLauncher, cwd, pi_bin).await
    }

    /// Launch `pi-coding-agent --mode rpc` through `launcher`, bound to
    /// `cwd`, and wire up the I/O tasks. `launcher` may be a `LocalLauncher`
    /// (the daemon) or a remote launcher (Litter's SSH variant) — the
    /// reader/writer/stderr pipeline downstream of the launched child is
    /// the same.
    pub async fn launch_with(
        launcher: &dyn ProcessLauncher,
        cwd: impl AsRef<Path>,
        pi_bin: impl AsRef<Path>,
    ) -> Result<Self> {
        let cwd = cwd.as_ref().to_path_buf();
        let pi_bin = pi_bin.as_ref().to_path_buf();

        let spec = ProcessSpec {
            role: ProcessRole::Agent,
            program: pi_bin.clone(),
            args: vec![
                OsString::from(RPC_MODE_FLAG.0),
                OsString::from(RPC_MODE_FLAG.1),
            ],
            cwd: Some(cwd.clone()),
            env: Vec::new(),
            env_clear: false,
            stdin: StdioMode::Piped,
            stdout: StdioMode::Piped,
            stderr: StdioMode::Piped,
        };

        let mut child = launcher.launch(spec).await.with_context(|| {
            // posix_spawn's ENOENT doesn't tell you whether the missing file
            // was the binary, the cwd, or the shebang interpreter. Log all
            // three with their existence states so we can diagnose without
            // attaching a debugger.
            format!(
                "launching {} (cwd={}, cwd_exists={}, pi_bin_exists={})",
                pi_bin.display(),
                cwd.display(),
                cwd.is_dir(),
                pi_bin.exists()
            )
        })?;

        let pid = child.id();
        let stdin: ChildStdin = child
            .take_stdin()
            .ok_or_else(|| anyhow!("pi child has no stdin pipe"))?;
        let stdout: ChildStdout = child
            .take_stdout()
            .ok_or_else(|| anyhow!("pi child has no stdout pipe"))?;
        let stderr: ChildStderr = child
            .take_stderr()
            .ok_or_else(|| anyhow!("pi child has no stderr pipe"))?;

        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<String>();
        let (events_tx, _events_rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let pending: Arc<ResponseTable> = Arc::new(Mutex::new(HashMap::new()));

        let writer = tokio::spawn(writer_task(stdin, writer_rx));
        let reader = tokio::spawn(reader_task(stdout, pending.clone(), events_tx.clone()));
        let stderr_handle = tokio::spawn(stderr_task(stderr, pid));

        let tasks = Arc::new(TaskSet {
            writer: Mutex::new(Some(writer)),
            reader: Mutex::new(Some(reader)),
            stderr: Mutex::new(Some(stderr_handle)),
            child: Mutex::new(Some(child)),
        });

        Ok(Self {
            cwd,
            pi_bin,
            pid,
            writer_tx,
            events_tx,
            pending,
            _tasks: tasks,
        })
    }

    /// Working directory pi was bound to at spawn time.
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    /// Path of the pi binary spawned for this handle.
    pub fn pi_bin(&self) -> &Path {
        &self.pi_bin
    }

    /// OS process id (when the spawn surfaced one).
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Subscribe to the broadcast event channel. New subscribers see only
    /// events emitted *after* they subscribe; replay state on the bridge
    /// translator side, not here.
    pub fn subscribe_events(&self) -> broadcast::Receiver<PiEvent> {
        self.events_tx.subscribe()
    }

    /// Send a command and await the matching response. The pi command's `id`
    /// field is overwritten with a freshly generated UUID so callers don't
    /// need to manage correlation.
    ///
    /// Returns the parsed [`RpcResponse`] regardless of `success` — callers
    /// inspect `response.success` / `response.error` to handle failures.
    pub async fn send_request(
        &self,
        mut command: RpcCommand,
    ) -> Result<RpcResponse, PiProcessError> {
        let id = Uuid::now_v7().to_string();
        set_command_id(&mut command, id.clone());
        let command_value = serde_json::to_value(&command)?;
        let command_name = command_value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let line = serde_json::to_string(&command_value)?;

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(
                id.clone(),
                PendingResponse {
                    command: command_name,
                    tx,
                },
            );
        }

        if self.writer_tx.send(line).is_err() {
            // Writer is gone — pull the slot back so we don't leak.
            let mut pending = self.pending.lock().await;
            pending.remove(&id);
            return Err(PiProcessError::WriterClosed(id));
        }

        match rx.await {
            Ok(response) => Ok(response),
            Err(_) => {
                // Sender dropped before responding — happens when the reader
                // task observes pi's stdout closing.
                Err(PiProcessError::ProcessClosed(id))
            }
        }
    }

    /// Send a fire-and-forget command for which pi does *not* emit a response
    /// — currently only `extension_ui_response`. Caller is responsible for
    /// ensuring the matching `id` is set on the payload.
    pub fn send_notification(&self, command: &RpcCommand) -> Result<(), PiProcessError> {
        let line = serde_json::to_string(command)?;
        self.writer_tx
            .send(line)
            .map_err(|e| PiProcessError::WriterClosed(e.to_string()))
    }

    /// Close stdin to signal a clean shutdown, then wait for pi to exit and
    /// reap the child. Idempotent.
    pub async fn shutdown(&self) {
        if let Some(handle) = self._tasks.writer.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self._tasks.stderr.lock().await.take() {
            handle.abort();
        }
        if let Some(mut child) = self._tasks.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        if let Some(handle) = self._tasks.reader.lock().await.take() {
            handle.abort();
        }
    }
}

impl alleycat_bridge_core::pool::PoolMember for PiProcessHandle {
    async fn shutdown(&self) {
        PiProcessHandle::shutdown(self).await
    }
}

/// Patch the `id` field on whatever variant of [`RpcCommand`] we were given.
/// Centralized here so the table doesn't have to be repeated in every caller.
fn set_command_id(command: &mut RpcCommand, id: String) {
    use super::pi_protocol::RpcCommand::*;
    match command {
        Prompt(c) => c.id = Some(id),
        Steer(c) => c.id = Some(id),
        FollowUp(c) => c.id = Some(id),
        Abort(c) => c.id = Some(id),
        NewSession(c) => c.id = Some(id),
        GetState(c) => c.id = Some(id),
        SetModel(c) => c.id = Some(id),
        CycleModel(c) => c.id = Some(id),
        GetAvailableModels(c) => c.id = Some(id),
        SetThinkingLevel(c) => c.id = Some(id),
        CycleThinkingLevel(c) => c.id = Some(id),
        SetSteeringMode(c) => c.id = Some(id),
        SetFollowUpMode(c) => c.id = Some(id),
        Compact(c) => c.id = Some(id),
        SetAutoCompaction(c) => c.id = Some(id),
        SetAutoRetry(c) => c.id = Some(id),
        AbortRetry(c) => c.id = Some(id),
        Bash(c) => c.id = Some(id),
        AbortBash(c) => c.id = Some(id),
        GetSessionStats(c) => c.id = Some(id),
        ExportHtml(c) => c.id = Some(id),
        SwitchSession(c) => c.id = Some(id),
        Fork(c) => c.id = Some(id),
        GetForkMessages(c) => c.id = Some(id),
        GetLastAssistantText(c) => c.id = Some(id),
        SetSessionName(c) => c.id = Some(id),
        ListSessions(c) => c.id = Some(id),
        GetMessages(c) => c.id = Some(id),
        GetCommands(c) => c.id = Some(id),
        // ExtensionUiResponse uses its own pi-supplied id and never expects
        // a response — `send_notification` is the right path for it.
        ExtensionUiResponse(_) => {}
    }
}

async fn writer_task(mut stdin: ChildStdin, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(mut line) = rx.recv().await {
        line.push('\n');
        if let Err(err) = stdin.write_all(line.as_bytes()).await {
            tracing::warn!(?err, "pi writer task: stdin write failed; exiting");
            break;
        }
        if let Err(err) = stdin.flush().await {
            tracing::warn!(?err, "pi writer task: stdin flush failed; exiting");
            break;
        }
    }
    // Dropping `stdin` here closes pi's input pipe, prompting it to exit.
}

async fn reader_task(
    stdout: ChildStdout,
    pending: Arc<ResponseTable>,
    events_tx: broadcast::Sender<PiEvent>,
) {
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => {
                tracing::debug!("pi reader task: stdout closed");
                break;
            }
            Err(err) => {
                tracing::warn!(?err, "pi reader task: read error; exiting");
                break;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<PiOutboundMessage>(trimmed) {
            Ok(PiOutboundMessage::Response(response)) => {
                deliver_response(&pending, response).await;
            }
            Ok(PiOutboundMessage::Event(event)) => {
                // Send returns Err when there are no subscribers; that's
                // normal early in startup and not a fault.
                let _ = events_tx.send(event);
            }
            Err(err) => {
                let frame_type = serde_json::from_str::<serde_json::Value>(trimmed)
                    .ok()
                    .and_then(|value| value.get("type")?.as_str().map(str::to_owned));
                tracing::warn!(
                    ?err,
                    frame_type,
                    frame_bytes = trimmed.len(),
                    "pi reader task: failed to parse frame"
                );
            }
        }
    }
    // Drain pending: any caller still waiting for a response will see their
    // oneshot dropped, surfacing as PiProcessError::ProcessClosed.
    pending.lock().await.clear();
}

async fn deliver_response(pending: &ResponseTable, mut response: RpcResponse) {
    let id_from_response = response.id.clone();
    let mut guard = pending.lock().await;
    let (id, pending_response) = match id_from_response {
        Some(id) => match guard.remove(&id) {
            Some(pending_response) => (id, pending_response),
            None => {
                tracing::warn!(
                    id,
                    command = %response.command,
                    "pi response had no matching pending request"
                );
                return;
            }
        },
        None => {
            let matches = guard
                .iter()
                .filter_map(|(id, pending)| {
                    (pending.command == response.command).then(|| id.clone())
                })
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [id] => {
                    let id = id.clone();
                    let Some(pending_response) = guard.remove(&id) else {
                        tracing::warn!(
                            command = %response.command,
                            "pi response missing id; matched request disappeared"
                        );
                        return;
                    };
                    tracing::debug!(
                        command = %response.command,
                        fallback_id = %id,
                        "pi response missing id; matched by command"
                    );
                    response.id = Some(id.clone());
                    (id, pending_response)
                }
                [] => {
                    tracing::warn!(
                        command = %response.command,
                        "pi response missing id and no pending request matched command; dropping"
                    );
                    return;
                }
                _ => {
                    tracing::warn!(
                        command = %response.command,
                        matches = matches.len(),
                        "pi response missing id and multiple pending requests matched command; dropping"
                    );
                    return;
                }
            }
        }
    };
    drop(guard);

    match pending_response.tx.send(response) {
        Ok(()) => {}
        Err(_) => {
            tracing::debug!(id, "response receiver dropped before delivery");
        }
    }
}

async fn stderr_task(stderr: ChildStderr, pid: Option<u32>) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        // Pi prints diagnostic chatter to stderr; surface it through tracing
        // so debug builds get it without polluting the codex JSON-RPC channel.
        tracing::debug!(?pid, "pi stderr: {line}");
    }
}

/// Convenience: serialize `T` to a JSON string for callers that want to
/// pre-flight a payload before queuing it for the writer (e.g. tests).
#[allow(dead_code)]
pub(crate) fn serialize_line<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

#[cfg(test)]
impl PiProcessHandle {
    /// Build a [`PiProcessHandle`] that is *not* attached to any subprocess.
    /// Used by `pool` unit tests that exercise pool bookkeeping without
    /// needing a real pi child. Sending requests against the resulting handle
    /// will hang or error; tests must not call `send_request`/`subscribe_events`
    /// on a dangling handle.
    pub(crate) fn __test_dangling(
        writer_tx: mpsc::UnboundedSender<String>,
        events_tx: broadcast::Sender<PiEvent>,
        cwd: PathBuf,
    ) -> Self {
        Self {
            cwd,
            pi_bin: PathBuf::from("/dev/null"),
            pid: None,
            writer_tx,
            events_tx,
            pending: Arc::new(Mutex::new(HashMap::new())),
            _tasks: Arc::new(TaskSet {
                writer: Mutex::new(None),
                reader: Mutex::new(None),
                stderr: Mutex::new(None),
                child: Mutex::new(None),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::pi_protocol::{BareCmd, NewSessionCmd, PromptCmd, ResponseKind};

    #[test]
    fn set_command_id_overwrites_for_each_variant() {
        // Spot-check a representative few — exhaustive coverage is enforced
        // by the match in `set_command_id` itself.
        let mut c = RpcCommand::Prompt(PromptCmd {
            id: Some("old".into()),
            message: "hi".into(),
            images: vec![],
            streaming_behavior: None,
        });
        set_command_id(&mut c, "new".into());
        match c {
            RpcCommand::Prompt(p) => assert_eq!(p.id.as_deref(), Some("new")),
            _ => panic!("variant changed"),
        }

        let mut c = RpcCommand::NewSession(NewSessionCmd {
            id: None,
            parent_session: None,
        });
        set_command_id(&mut c, "abc".into());
        match c {
            RpcCommand::NewSession(n) => assert_eq!(n.id.as_deref(), Some("abc")),
            _ => panic!("variant changed"),
        }

        let mut c = RpcCommand::Abort(BareCmd { id: None });
        set_command_id(&mut c, "xyz".into());
        match c {
            RpcCommand::Abort(b) => assert_eq!(b.id.as_deref(), Some("xyz")),
            _ => panic!("variant changed"),
        }
    }

    #[tokio::test]
    async fn response_missing_id_matches_single_pending_command() {
        let pending = Mutex::new(HashMap::new());
        let (tx, rx) = oneshot::channel();
        pending.lock().await.insert(
            "req-1".to_string(),
            PendingResponse {
                command: "list_sessions".to_string(),
                tx,
            },
        );

        deliver_response(
            &pending,
            RpcResponse {
                kind: ResponseKind::Response,
                id: None,
                command: "list_sessions".to_string(),
                success: true,
                data: None,
                error: None,
            },
        )
        .await;

        let response = rx.await.expect("response delivered");
        assert_eq!(response.id.as_deref(), Some("req-1"));
        assert_eq!(response.command, "list_sessions");
        assert!(response.success);
    }

    /// Drive a fake "pi" process (cat-like binary that echoes our commands as
    /// pre-shaped responses) through the full PiProcessHandle pipeline.
    /// Disabled by default; this test requires `bash` and is purely an
    /// integration smoke test we run by hand.
    #[tokio::test]
    #[ignore]
    async fn round_trip_against_fake_pi() {
        // The fake "pi" reads one line from stdin, echoes a synthetic
        // response, then exits. We assert the handle's send_request resolves.
        let script = r#"
read line
id=$(echo "$line" | python3 -c 'import sys,json; print(json.loads(sys.stdin.read())["id"])')
printf '{"type":"response","id":"%s","command":"abort","success":true}\n' "$id"
"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), script).unwrap();
        let mut perms = std::fs::metadata(tmp.path()).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        std::fs::set_permissions(tmp.path(), perms).unwrap();

        // Use a custom launcher that swaps the program for `bash` and
        // prepends the script path. Avoids reaching into private internals.
        struct BashLauncher {
            script: PathBuf,
        }
        impl ProcessLauncher for BashLauncher {
            fn launch(
                &self,
                _spec: ProcessSpec,
            ) -> futures::future::BoxFuture<'_, std::io::Result<Box<dyn ChildProcess>>>
            {
                let script = self.script.clone();
                Box::pin(async move {
                    let new_spec = ProcessSpec {
                        role: ProcessRole::Agent,
                        program: PathBuf::from("bash"),
                        args: vec![OsString::from(script.as_os_str())],
                        cwd: None,
                        env: Vec::new(),
                        env_clear: false,
                        stdin: StdioMode::Piped,
                        stdout: StdioMode::Piped,
                        stderr: StdioMode::Piped,
                    };
                    LocalLauncher.launch(new_spec).await
                })
            }
        }
        let launcher = BashLauncher {
            script: tmp.path().to_path_buf(),
        };
        let handle =
            PiProcessHandle::launch_with(&launcher, std::env::current_dir().unwrap(), "bash")
                .await
                .unwrap();

        let response = handle
            .send_request(RpcCommand::Abort(BareCmd { id: None }))
            .await
            .expect("response");
        assert!(response.success);
        assert_eq!(response.command, "abort");
    }
}
