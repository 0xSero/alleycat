use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use alleycat_bridge_core::{Bridge, Conn, JsonRpcError, error_codes};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::session::{ShellSession, ShellSize};

const USER_AGENT: &str = concat!("alleycat-shell-bridge/", env!("CARGO_PKG_VERSION"));

#[derive(Default)]
pub struct ShellBridgeBuilder {
    shell_bin: Option<String>,
    default_cwd: Option<PathBuf>,
    allow_env_passthrough: bool,
}

impl ShellBridgeBuilder {
    pub fn shell_bin(mut self, shell_bin: impl Into<String>) -> Self {
        self.shell_bin = Some(shell_bin.into());
        self
    }

    pub fn default_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.default_cwd = Some(cwd.into());
        self
    }

    pub fn allow_env_passthrough(mut self, allow: bool) -> Self {
        self.allow_env_passthrough = allow;
        self
    }

    pub fn from_env(mut self) -> Self {
        if self.shell_bin.is_none()
            && let Ok(shell) = std::env::var("ALLEYCAT_SHELL_BIN")
            && !shell.trim().is_empty()
        {
            self.shell_bin = Some(shell);
        }
        if self.default_cwd.is_none()
            && let Some(cwd) = std::env::var_os("ALLEYCAT_SHELL_CWD")
        {
            self.default_cwd = Some(PathBuf::from(cwd));
        }
        self
    }

    pub fn build(self) -> Arc<ShellBridge> {
        Arc::new(ShellBridge {
            shell_bin: self
                .shell_bin
                .unwrap_or_else(default_shell_from_environment),
            default_cwd: self.default_cwd,
            allow_env_passthrough: self.allow_env_passthrough,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: AtomicU64::new(1),
        })
    }
}

pub struct ShellBridge {
    shell_bin: String,
    default_cwd: Option<PathBuf>,
    allow_env_passthrough: bool,
    sessions: Arc<Mutex<HashMap<String, Arc<ShellSession>>>>,
    next_session_id: AtomicU64,
}

impl ShellBridge {
    pub fn builder() -> ShellBridgeBuilder {
        ShellBridgeBuilder::default()
    }

    fn next_session_id(&self) -> String {
        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        format!("shell-{id}")
    }

    async fn spawn_shell(
        &self,
        ctx: &Conn,
        params: ShellSpawnParams,
    ) -> Result<ShellSpawnResponse, JsonRpcError> {
        let id = self.next_session_id();
        let shell = params
            .shell
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| self.shell_bin.clone());
        let args = params.args.unwrap_or_default();
        let cwd = params
            .cwd
            .map(PathBuf::from)
            .or_else(|| self.default_cwd.clone());
        let env = if self.allow_env_passthrough {
            params.env.unwrap_or_default()
        } else {
            HashMap::new()
        };
        let size = params
            .size
            .validate()
            .map_err(|error| invalid_params(error.to_string()))?;

        let spawned = tokio::task::spawn_blocking({
            let id = id.clone();
            move || ShellSession::spawn(id, shell, args, cwd, env, size)
        })
        .await
        .map_err(|error| internal(format!("joining shell spawn task: {error}")))?
        .map_err(internal)?;

        let session = Arc::clone(&spawned.session);
        self.sessions
            .lock()
            .expect("shell sessions mutex poisoned")
            .insert(id.clone(), Arc::clone(&session));

        spawn_output_thread(
            id.clone(),
            spawned.reader,
            ctx.notifier().clone(),
            Arc::clone(&self.sessions),
        );
        spawn_wait_thread(
            id.clone(),
            spawned.child,
            ctx.notifier().clone(),
            Arc::clone(&self.sessions),
        );

        Ok(ShellSpawnResponse { session_id: id })
    }

    fn get_session(&self, session_id: &str) -> Result<Arc<ShellSession>, JsonRpcError> {
        self.sessions
            .lock()
            .expect("shell sessions mutex poisoned")
            .get(session_id)
            .cloned()
            .ok_or_else(|| invalid_params(format!("unknown shell session `{session_id}`")))
    }
}

#[async_trait]
impl Bridge for ShellBridge {
    async fn initialize(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
        Ok(json!({
            "userAgent": USER_AGENT,
            "capabilities": {
                "methods": ["shell/spawn", "shell/input", "shell/resize", "shell/kill"],
                "outputEncoding": "base64"
            }
        }))
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "shell/spawn" => ok(self.spawn_shell(ctx, decode(params)?).await?),
            "shell/input" => {
                let params: ShellInputParams = decode(params)?;
                let data = STANDARD
                    .decode(params.data_b64.as_bytes())
                    .map_err(|error| invalid_params(format!("invalid base64 input: {error}")))?;
                let session = self.get_session(&params.session_id)?;
                tokio::task::spawn_blocking(move || session.write(&data))
                    .await
                    .map_err(|error| internal(format!("joining shell input task: {error}")))?
                    .map_err(internal)?;
                ok(Ack {})
            }
            "shell/resize" => {
                let params: ShellResizeParams = decode(params)?;
                let session = self.get_session(&params.session_id)?;
                let size = ShellSize {
                    cols: params.cols,
                    rows: params.rows,
                };
                tokio::task::spawn_blocking(move || session.resize(size))
                    .await
                    .map_err(|error| internal(format!("joining shell resize task: {error}")))?
                    .map_err(internal)?;
                ok(Ack {})
            }
            "shell/kill" => {
                let params: ShellKillParams = decode(params)?;
                let session = self.get_session(&params.session_id)?;
                let _signal = params.signal;
                tokio::task::spawn_blocking(move || session.kill())
                    .await
                    .map_err(|error| internal(format!("joining shell kill task: {error}")))?
                    .map_err(internal)?;
                ok(Ack {})
            }
            other => Err(JsonRpcError::method_not_found(other)),
        }
    }

    async fn shutdown(&self) {
        let sessions = self
            .sessions
            .lock()
            .expect("shell sessions mutex poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for session in sessions {
            if let Err(error) = session.kill() {
                debug!(
                    session_id = session.id(),
                    %error,
                    "shell session kill during shutdown failed"
                );
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
struct ShellSpawnParams {
    shell: Option<String>,
    args: Option<Vec<String>>,
    cwd: Option<String>,
    env: Option<HashMap<String, String>>,
    size: ShellSize,
}

impl Default for ShellSpawnParams {
    fn default() -> Self {
        Self {
            shell: None,
            args: None,
            cwd: None,
            env: None,
            size: ShellSize { cols: 80, rows: 24 },
        }
    }
}

#[derive(Debug, Serialize)]
struct ShellSpawnResponse {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct ShellInputParams {
    session_id: String,
    data_b64: String,
}

#[derive(Debug, Deserialize)]
struct ShellResizeParams {
    session_id: String,
    cols: u16,
    rows: u16,
}

#[derive(Debug, Deserialize)]
struct ShellKillParams {
    session_id: String,
    signal: Option<i32>,
}

#[derive(Debug, Serialize)]
struct Ack {}

#[derive(Debug, Serialize)]
struct ShellOutputNotification {
    session_id: String,
    data_b64: String,
}

#[derive(Debug, Serialize)]
struct ShellExitNotification {
    session_id: String,
    code: i32,
}

fn spawn_output_thread(
    session_id: String,
    mut reader: Box<dyn Read + Send>,
    notifier: alleycat_bridge_core::NotificationSender,
    sessions: Arc<Mutex<HashMap<String, Arc<ShellSession>>>>,
) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let payload = ShellOutputNotification {
                        session_id: session_id.clone(),
                        data_b64: STANDARD.encode(&buf[..n]),
                    };
                    if let Err(error) = notifier.send_notification("shell/output", payload) {
                        debug!(%session_id, %error, "failed to send shell/output notification");
                        break;
                    }
                }
                Err(error) => {
                    debug!(%session_id, %error, "PTY reader ended");
                    break;
                }
            }
        }
        let _ = sessions
            .lock()
            .expect("shell sessions mutex poisoned")
            .remove(&session_id);
    });
}

fn spawn_wait_thread(
    session_id: String,
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    notifier: alleycat_bridge_core::NotificationSender,
    sessions: Arc<Mutex<HashMap<String, Arc<ShellSession>>>>,
) {
    std::thread::spawn(move || {
        let code = match child.wait() {
            Ok(status) => status.exit_code() as i32,
            Err(error) => {
                warn!(%session_id, %error, "waiting for shell child failed");
                -1
            }
        };
        let _ = notifier.send_notification(
            "shell/exit",
            ShellExitNotification {
                session_id: session_id.clone(),
                code,
            },
        );
        let _ = sessions
            .lock()
            .expect("shell sessions mutex poisoned")
            .remove(&session_id);
    });
}

fn default_shell_from_environment() -> String {
    std::env::var("SHELL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if cfg!(windows) {
                "powershell.exe".to_string()
            } else {
                "/bin/zsh".to_string()
            }
        })
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, JsonRpcError> {
    serde_json::from_value(value).map_err(|error| invalid_params(error.to_string()))
}

fn ok<T: Serialize>(value: T) -> Result<Value, JsonRpcError> {
    serde_json::to_value(value).map_err(|error| internal(error.to_string()))
}

fn invalid_params(message: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INVALID_PARAMS,
        message: message.into(),
        data: None,
    }
}

fn internal(message: impl std::fmt::Display) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: message.to_string(),
        data: None,
    }
}
