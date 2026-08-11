use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::{ChildProcess, ProcessLauncher, ProcessRole, ProcessSpec, StdioMode};
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

const FACTORY_API_VERSION: &str = "1.0.0";
const FACTORY_PROTOCOL_VERSION: &str = "1.36.0";
const EVENT_CHANNEL_CAPACITY: usize = 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct DroidSpawnConfig {
    pub thread_id: String,
    pub cwd: PathBuf,
    pub droid_bin: PathBuf,
    pub model: Option<String>,
    pub auto_level: String,
}

#[derive(Debug)]
pub struct DroidProcess {
    thread_id: String,
    cwd: PathBuf,
    writer_tx: mpsc::UnboundedSender<String>,
    events_tx: broadcast::Sender<Value>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    _tasks: Arc<TaskSet>,
}

struct TaskSet {
    writer: Mutex<Option<JoinHandle<()>>>,
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
        if let Some(handle) = self.writer.try_lock().ok().and_then(|mut g| g.take()) {
            handle.abort();
        }
        if let Some(handle) = self.reader.try_lock().ok().and_then(|mut g| g.take()) {
            handle.abort();
        }
        if let Some(handle) = self.stderr.try_lock().ok().and_then(|mut g| g.take()) {
            handle.abort();
        }
        if let Some(_child) = self.child.try_lock().ok().and_then(|mut g| g.take()) {}
    }
}

impl DroidProcess {
    pub async fn launch(
        launcher: Arc<dyn ProcessLauncher>,
        config: DroidSpawnConfig,
    ) -> Result<Arc<Self>> {
        let DroidSpawnConfig {
            thread_id,
            cwd,
            droid_bin,
            model,
            auto_level,
        } = config;

        let mut args: Vec<OsString> = vec![
            "exec".into(),
            "--input-format".into(),
            "stream-jsonrpc".into(),
            "--output-format".into(),
            "stream-jsonrpc".into(),
            "--auto".into(),
            auto_level.into(),
            "--cwd".into(),
            cwd.as_os_str().to_os_string(),
        ];
        if let Some(model) = &model {
            args.push("--model".into());
            args.push(model.into());
        }

        let spec = ProcessSpec {
            role: ProcessRole::Agent,
            program: droid_bin.clone(),
            args,
            cwd: Some(cwd.clone()),
            env: Vec::new(),
            env_clear: false,
            stdin: StdioMode::Piped,
            stdout: StdioMode::Piped,
            stderr: StdioMode::Piped,
        };
        let mut child = launcher
            .launch(spec)
            .await
            .with_context(|| format!("spawning {}", droid_bin.display()))?;
        let stdin = child
            .take_stdin()
            .ok_or_else(|| anyhow!("droid child missing stdin"))?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| anyhow!("droid child missing stdout"))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| anyhow!("droid child missing stderr"))?;

        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<String>();
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let pending = Arc::new(Mutex::new(HashMap::<String, oneshot::Sender<Value>>::new()));

        let writer = tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(line) = writer_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.write_all(b"\n").await.is_err() {
                    break;
                }
                if stdin.flush().await.is_err() {
                    break;
                }
            }
        });

        let reader_pending = Arc::clone(&pending);
        let reader_events = events_tx.clone();
        let reader = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
                    tracing::debug!(line = trimmed, "dropping non-json droid stdout line");
                    continue;
                };
                if value.get("type").and_then(Value::as_str) == Some("response")
                    && let Some(id) = response_id(&value)
                    && let Some(tx) = reader_pending.lock().await.remove(&id)
                {
                    let _ = tx.send(value.clone());
                }
                let _ = reader_events.send(value);
            }
        });

        let stderr_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::warn!(target: "droid", "{line}");
            }
        });

        Ok(Arc::new(Self {
            thread_id,
            cwd,
            writer_tx,
            events_tx,
            pending,
            _tasks: Arc::new(TaskSet {
                writer: Mutex::new(Some(writer)),
                reader: Mutex::new(Some(reader)),
                stderr: Mutex::new(Some(stderr_task)),
                child: Mutex::new(Some(child)),
            }),
        }))
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub fn cwd(&self) -> &std::path::Path {
        &self.cwd
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events_tx.subscribe()
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = Uuid::now_v7().to_string();
        let frame = json!({
            "type": "request",
            "jsonrpc": "2.0",
            "factoryApiVersion": FACTORY_API_VERSION,
            "factoryProtocolVersion": FACTORY_PROTOCOL_VERSION,
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&frame)?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);
        if self.writer_tx.send(line).is_err() {
            self.pending.lock().await.remove(&id);
            return Err(anyhow!("droid writer is closed"));
        }
        let response = match timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                self.pending.lock().await.remove(&id);
                return Err(anyhow!("droid request `{method}` was cancelled"));
            }
            Err(_) => {
                self.pending.lock().await.remove(&id);
                return Err(anyhow!("droid request `{method}` timed out"));
            }
        };
        if let Some(error) = response.get("error") {
            return Err(anyhow!(
                "droid request `{method}` failed: {}",
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn response_id(value: &Value) -> Option<String> {
    let id = value.get("id")?;
    if id.is_null() {
        None
    } else if let Some(s) = id.as_str() {
        Some(s.to_string())
    } else {
        Some(id.to_string())
    }
}
