use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use alleycat_bridge_core::{
    ChildProcess, LocalLauncher, ProcessLauncher, ProcessRole, ProcessSpec, StdioMode,
};
use alleycat_codex_proto as p;
use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::state::ConnectionState;

const DEFAULT_OUTPUT_BYTES_CAP: usize = 256 * 1024;
const DEFAULT_TIMEOUT_MS: i64 = 60_000;

static EXEC_REGISTRY: LazyLock<ExecRegistry> = LazyLock::new(ExecRegistry::default);

#[derive(Default)]
struct ExecRegistry {
    inner: Mutex<HashMap<String, ExecHandle>>,
}

struct ExecHandle {
    terminate_tx: oneshot::Sender<()>,
}

impl ExecRegistry {
    fn insert(&self, id: String, handle: ExecHandle) {
        self.inner.lock().unwrap().insert(id, handle);
    }

    fn take(&self, id: &str) -> Option<ExecHandle> {
        self.inner.lock().unwrap().remove(id)
    }
}

pub async fn handle_command_exec(
    state: &Arc<ConnectionState>,
    params: p::CommandExecParams,
) -> Result<p::CommandExecResponse, ExecError> {
    if params.command.is_empty() {
        return Err(ExecError::InvalidParams("empty command argv".into()));
    }
    if params.tty {
        return Err(ExecError::Unsupported(
            "tty mode is not supported by amp-bridge v1".into(),
        ));
    }
    if params.stream_stdin {
        return Err(ExecError::Unsupported(
            "stream_stdin is not supported by amp-bridge v1".into(),
        ));
    }
    if params.stream_stdout_stderr {
        return Err(ExecError::Unsupported(
            "stream_stdout_stderr is not supported by amp-bridge v1".into(),
        ));
    }
    if params.disable_output_cap && params.output_bytes_cap.is_some() {
        return Err(ExecError::InvalidParams(
            "disable_output_cap cannot be combined with output_bytes_cap".into(),
        ));
    }
    if params.disable_timeout && params.timeout_ms.is_some() {
        return Err(ExecError::InvalidParams(
            "disable_timeout cannot be combined with timeout_ms".into(),
        ));
    }

    let argv = params.command.clone();
    let mut env: Vec<(OsString, OsString)> = Vec::new();
    if let Some(env_map) = &params.env {
        for (k, v) in env_map {
            if let Some(value) = v {
                env.push((k.into(), value.into()));
            }
        }
    }
    let spec = ProcessSpec {
        role: ProcessRole::ToolCommand,
        program: argv[0].clone().into(),
        args: argv[1..].iter().map(|s| s.clone().into()).collect(),
        cwd: params.cwd.clone(),
        env,
        env_clear: false,
        stdin: StdioMode::Null,
        stdout: StdioMode::Piped,
        stderr: StdioMode::Piped,
    };
    let launcher: Arc<dyn ProcessLauncher> = state
        .launcher()
        .cloned()
        .unwrap_or_else(|| Arc::new(LocalLauncher) as Arc<dyn ProcessLauncher>);
    let mut child: Box<dyn ChildProcess> = launcher.launch(spec).await.map_err(ExecError::spawn)?;

    let cap = if params.disable_output_cap {
        usize::MAX
    } else {
        params.output_bytes_cap.unwrap_or(DEFAULT_OUTPUT_BYTES_CAP)
    };
    let timeout_dur = if params.disable_timeout {
        None
    } else {
        let ms = params.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).max(0) as u64;
        Some(Duration::from_millis(ms))
    };

    let terminate_rx = params.process_id.as_ref().map(|pid| {
        let (tx, rx) = oneshot::channel::<()>();
        EXEC_REGISTRY.insert(pid.clone(), ExecHandle { terminate_tx: tx });
        rx
    });

    let stdout = child
        .take_stdout()
        .ok_or_else(|| ExecError::internal("child has no stdout pipe"))?;
    let stderr = child
        .take_stderr()
        .ok_or_else(|| ExecError::internal("child has no stderr pipe"))?;

    let mut stdout = stdout;
    let mut stderr = stderr;
    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = read_capped(&mut stdout, &mut buf, cap).await;
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = read_capped(&mut stderr, &mut buf, cap).await;
        buf
    });

    let exit_status = run_with_supervisor(child, timeout_dur, terminate_rx).await?;

    if let Some(pid) = &params.process_id {
        EXEC_REGISTRY.take(pid);
    }

    let stdout_bytes = stdout_task.await.unwrap_or_default();
    let stderr_bytes = stderr_task.await.unwrap_or_default();

    Ok(p::CommandExecResponse {
        exit_code: exit_status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
        stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
    })
}

pub async fn handle_command_exec_terminate(
    _state: &Arc<ConnectionState>,
    params: p::CommandExecTerminateParams,
) -> p::CommandExecTerminateResponse {
    if let Some(handle) = EXEC_REGISTRY.take(&params.process_id) {
        let _ = handle.terminate_tx.send(());
    }
    p::CommandExecTerminateResponse::default()
}

pub async fn handle_command_exec_write(
    _state: &Arc<ConnectionState>,
    _params: p::CommandExecWriteParams,
) -> Result<p::CommandExecWriteResponse, ExecError> {
    Err(ExecError::Unsupported(
        "command/exec/write is not supported by amp-bridge v1 (no PTY/stdin streaming)".into(),
    ))
}

pub async fn handle_command_exec_resize(
    _state: &Arc<ConnectionState>,
    _params: p::CommandExecResizeParams,
) -> Result<p::CommandExecResizeResponse, ExecError> {
    Err(ExecError::Unsupported(
        "command/exec/resize is not supported by amp-bridge v1 (no PTY)".into(),
    ))
}

async fn run_with_supervisor(
    mut child: Box<dyn ChildProcess>,
    timeout_dur: Option<Duration>,
    terminate_rx: Option<oneshot::Receiver<()>>,
) -> Result<std::process::ExitStatus, ExecError> {
    match (timeout_dur, terminate_rx) {
        (Some(dur), Some(term)) => tokio::select! {
            res = child.wait() => res.map_err(ExecError::wait),
            _ = term => {
                let _ = child.kill().await;
                child.wait().await.map_err(ExecError::wait)
            }
            _ = tokio::time::sleep(dur) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                Err(ExecError::Timeout)
            }
        },
        (Some(dur), None) => match timeout(dur, child.wait()).await {
            Ok(res) => res.map_err(ExecError::wait),
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                Err(ExecError::Timeout)
            }
        },
        (None, Some(term)) => tokio::select! {
            res = child.wait() => res.map_err(ExecError::wait),
            _ = term => {
                let _ = child.kill().await;
                child.wait().await.map_err(ExecError::wait)
            }
        },
        (None, None) => child.wait().await.map_err(ExecError::wait),
    }
}

async fn read_capped<R>(reader: &mut R, dest: &mut Vec<u8>, cap: usize) -> std::io::Result<()>
where
    R: AsyncReadExt + Unpin,
{
    let mut buf = vec![0u8; 8 * 1024];
    while dest.len() < cap {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let remaining = cap - dest.len();
        let take = n.min(remaining);
        dest.extend_from_slice(&buf[..take]);
        if take < n {
            loop {
                let drained = reader.read(&mut buf).await?;
                if drained == 0 {
                    break;
                }
            }
            break;
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("command timed out")]
    Timeout,
    #[error("spawn failed: {0}")]
    Spawn(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl ExecError {
    fn spawn(err: std::io::Error) -> Self {
        Self::Spawn(err.to_string())
    }

    fn wait(err: std::io::Error) -> Self {
        Self::Internal(format!("waiting on child: {err}"))
    }

    fn internal<E: std::fmt::Display>(err: E) -> Self {
        Self::Internal(err.to_string())
    }

    pub fn rpc_code(&self) -> i64 {
        match self {
            ExecError::InvalidParams(_) => p::error_codes::INVALID_PARAMS,
            ExecError::Unsupported(_) => p::error_codes::METHOD_NOT_FOUND,
            ExecError::Timeout | ExecError::Spawn(_) | ExecError::Internal(_) => {
                p::error_codes::INTERNAL_ERROR
            }
        }
    }
}
