use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use alleycat_bridge_core::{
    ChildProcess, ChildStderr, ChildStdin, ChildStdout, ProcessLauncher, ProcessRole, ProcessSpec,
    StdioMode,
};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;

const EVENT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub struct AmpSpawnConfig {
    pub amp_bin: PathBuf,
    pub cwd: PathBuf,
    pub amp_thread_id: Option<String>,
    pub mode: String,
    pub dangerously_allow_all: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum AmpProcessError {
    #[error("failed to write user envelope to amp stdin: {0}")]
    WriterClosed(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub struct AmpProcess {
    pid: Option<u32>,
    events_tx: broadcast::Sender<Value>,
    events_rx: StdMutex<Option<broadcast::Receiver<Value>>>,
    stdin: Mutex<Option<ChildStdin>>,
    tasks: Arc<TaskSet>,
}

impl std::fmt::Debug for AmpProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmpProcess")
            .field("pid", &self.pid)
            .finish_non_exhaustive()
    }
}

struct TaskSet {
    reader: Mutex<Option<JoinHandle<()>>>,
    stderr: Mutex<Option<JoinHandle<()>>>,
    child: Mutex<Option<Box<dyn ChildProcess>>>,
}

impl std::fmt::Debug for TaskSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskSet").finish_non_exhaustive()
    }
}

impl Drop for TaskSet {
    fn drop(&mut self) {
        if let Some(handle) = self.reader.try_lock().ok().and_then(|mut g| g.take()) {
            handle.abort();
        }
        if let Some(handle) = self.stderr.try_lock().ok().and_then(|mut g| g.take()) {
            handle.abort();
        }
        if let Some(_child) = self.child.try_lock().ok().and_then(|mut g| g.take()) {}
    }
}

impl AmpProcess {
    pub async fn launch(
        launcher: Arc<dyn ProcessLauncher>,
        config: AmpSpawnConfig,
    ) -> Result<Arc<Self>> {
        let AmpSpawnConfig {
            amp_bin,
            cwd,
            amp_thread_id,
            mode,
            dangerously_allow_all,
        } = config;

        let mut args: Vec<OsString> = Vec::new();
        if let Some(thread_id) = amp_thread_id {
            args.push("threads".into());
            args.push("continue".into());
            args.push(thread_id.into());
        }
        if dangerously_allow_all {
            args.push("--dangerously-allow-all".into());
        }
        if !mode.trim().is_empty() {
            args.push("--mode".into());
            args.push(mode.into());
        }
        args.push("--execute".into());
        args.push("--stream-json".into());
        args.push("--stream-json-thinking".into());
        args.push("--stream-json-input".into());

        let spec = ProcessSpec {
            role: ProcessRole::Agent,
            program: amp_bin.clone(),
            args,
            cwd: Some(cwd.clone()),
            env: Vec::new(),
            env_clear: false,
            stdin: StdioMode::Piped,
            stdout: StdioMode::Piped,
            stderr: StdioMode::Piped,
        };
        let mut child = launcher.launch(spec).await.with_context(|| {
            format!(
                "spawning {} (cwd={}, cwd_exists={}, amp_bin_exists={})",
                amp_bin.display(),
                cwd.display(),
                cwd.is_dir(),
                amp_bin.exists()
            )
        })?;
        let pid = child.id();
        let stdin = child
            .take_stdin()
            .ok_or_else(|| anyhow!("amp child has no stdin pipe"))?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| anyhow!("amp child has no stdout pipe"))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| anyhow!("amp child has no stderr pipe"))?;

        let (events_tx, events_rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let reader = tokio::spawn(reader_task(stdout, events_tx.clone()));
        let stderr = tokio::spawn(stderr_task(stderr, pid));
        let tasks = Arc::new(TaskSet {
            reader: Mutex::new(Some(reader)),
            stderr: Mutex::new(Some(stderr)),
            child: Mutex::new(Some(child)),
        });

        Ok(Arc::new(Self {
            pid,
            events_tx,
            events_rx: StdMutex::new(Some(events_rx)),
            stdin: Mutex::new(Some(stdin)),
            tasks,
        }))
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events_rx
            .lock()
            .expect("amp process events receiver mutex poisoned")
            .take()
            .unwrap_or_else(|| self.events_tx.subscribe())
    }

    pub async fn send_serialized_and_close<T: serde::Serialize>(
        &self,
        value: &T,
    ) -> Result<(), AmpProcessError> {
        self.send_serialized(value).await?;
        self.close_stdin().await;
        Ok(())
    }

    pub async fn send_serialized<T: serde::Serialize>(
        &self,
        value: &T,
    ) -> Result<(), AmpProcessError> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard
            .as_mut()
            .ok_or_else(|| AmpProcessError::WriterClosed("stdin already closed".to_string()))?;
        let mut line = serde_json::to_string(value)?;
        line.push('\n');
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await?;
        Ok(())
    }

    pub async fn close_stdin(&self) {
        self.stdin.lock().await.take();
    }

    pub async fn shutdown(&self) {
        self.stdin.lock().await.take();
        if let Some(handle) = self.tasks.stderr.lock().await.take() {
            handle.abort();
        }
        if let Some(mut child) = self.tasks.child.lock().await.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        if let Some(handle) = self.tasks.reader.lock().await.take() {
            handle.abort();
        }
    }
}

async fn reader_task(stdout: ChildStdout, events_tx: broadcast::Sender<Value>) {
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut saw_result = false;
    let mut close_reason = "amp stdout closed before result".to_string();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(err) => {
                tracing::warn!(?err, "amp reader task: read error; exiting");
                close_reason = format!("amp stdout read error before result: {err}");
                break;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => {
                if value.get("type").and_then(Value::as_str) == Some("result") {
                    saw_result = true;
                }
                let _ = events_tx.send(value);
            }
            Err(err) => {
                tracing::warn!(?err, line = %trimmed, "amp reader task: failed to parse line");
            }
        }
    }
    if !saw_result {
        let _ = events_tx.send(json!({
            "type": "result",
            "subtype": "bridge_error",
            "is_error": true,
            "error": close_reason,
        }));
    }
}

async fn stderr_task(stderr: ChildStderr, pid: Option<u32>) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(?pid, "amp stderr: {line}");
    }
}

pub fn result_error_message(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("result") {
        return None;
    }
    let is_error = value
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let subtype = value
        .get("subtype")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if !is_error && subtype == "success" {
        return None;
    }
    value
        .get("error")
        .or_else(|| value.get("result"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| Some(format!("amp turn ended with subtype {subtype}")))
}

pub async fn wait_for_exit(process: Arc<AmpProcess>, deadline: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(deadline) => {
            process.shutdown().await;
        }
        _ = async {
            loop {
                if process.tasks.child.lock().await.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } => {}
    }
}
