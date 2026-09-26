use std::ffi::OsStr;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::process::Stdio;
use std::time::Duration;

use alleycat_bridge_core::{LaunchEnvironment, LaunchEnvironmentResolver};
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command as TokioCommand};

pub struct OpencodeRuntime {
    pub base_url: String,
    pub auth_token: String,
    _child: Option<Child>,
    _stderr: Option<StderrDrain>,
}

impl OpencodeRuntime {
    pub fn external(base_url: String, auth_token: String) -> Self {
        Self {
            base_url,
            auth_token,
            _child: None,
            _stderr: None,
        }
    }

    pub async fn start_from_env() -> anyhow::Result<Self> {
        let cwd = std::env::current_dir().ok();
        let launch_env = LaunchEnvironmentResolver::default()
            .resolve(cwd.as_deref())
            .await;

        if let Some(base_url) = env_string(&launch_env, "OPENCODE_BRIDGE_BACKEND_URL") {
            let auth_token =
                env_string(&launch_env, "OPENCODE_BRIDGE_AUTH_TOKEN").unwrap_or_default();
            return Ok(Self {
                base_url,
                auth_token,
                _child: None,
                _stderr: None,
            });
        }

        // The daemon writes host-configured opencode.bin into its own process
        // environment just before lazy bridge construction. Keep that explicit
        // config override above shell/mise/direnv ambient values.
        let configured_bin = std::env::var("OPENCODE_BRIDGE_BIN").ok();
        let bin = resolve_opencode_bin(&launch_env, configured_bin.as_deref());
        let port = match env_string(&launch_env, "OPENCODE_BRIDGE_PORT").as_deref() {
            Some("auto") | None => pick_port()?,
            Some(value) => value.parse::<u16>()?,
        };
        // `--auth-token` was removed from `opencode serve` in 1.3.x and
        // passing it makes the binary print usage and exit immediately. Only
        // forward an explicit override; otherwise leave it off and treat the
        // server as unauthenticated (`OpencodeClient` skips the query param
        // when `auth_token` is empty).
        let explicit_auth_token =
            match env_string(&launch_env, "OPENCODE_BRIDGE_AUTH_TOKEN").as_deref() {
                Some("auto") | Some("") | None => None,
                Some(value) => Some(value.to_string()),
            };
        let auth_token = explicit_auth_token.clone().unwrap_or_default();
        let extra_args = env_string(&launch_env, "OPENCODE_BRIDGE_EXTRA_ARGS")
            .map(|raw| {
                raw.split('\u{1f}')
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec!["serve".to_string()]);

        let mut command = TokioCommand::new(bin);
        command
            .env_clear()
            .envs(launch_env.clone().into_pairs())
            .args(extra_args)
            .arg(format!("--port={port}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(token) = explicit_auth_token.as_deref() {
            command.arg(format!("--auth-token={token}"));
        }
        let mut child = command.spawn()?;
        let stderr = child.stderr.take().map(StderrDrain::start);
        let base_url = format!("http://127.0.0.1:{port}");
        // The drain is owned before awaiting readiness, so failed/cancelled
        // startup cannot leave an independent reader task behind.
        if let Err(error) = wait_until_healthy(&base_url, READINESS_TIMEOUT).await {
            let _ = tokio::time::timeout(Duration::from_secs(2), child.kill()).await;
            return Err(error);
        }
        Ok(Self {
            base_url,
            auth_token,
            _child: Some(child),
            _stderr: stderr,
        })
    }
}

const STDERR_CHUNK_BYTES: usize = 4096;

struct StderrDrain(tokio::task::JoinHandle<()>);

impl StderrDrain {
    fn start(reader: impl AsyncRead + Unpin + Send + 'static) -> Self {
        Self(tokio::spawn(async move {
            if let Err(error) = drain_stderr(reader, |chunk| {
                // Keep stderr visible at the default filter while routing it
                // through the daemon's bounded diagnostics, not launchd's file.
                tracing::warn!(stderr = %String::from_utf8_lossy(chunk), "opencode stderr");
            })
            .await
            {
                tracing::warn!(%error, "opencode stderr reader stopped");
            }
        }))
    }
}

impl Drop for StderrDrain {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn drain_stderr(
    mut reader: impl AsyncRead + Unpin,
    mut emit: impl FnMut(&[u8]),
) -> std::io::Result<()> {
    // A child can print arbitrarily long lines (or no newline at all). Never
    // accumulate a line or create another channel; bound every read directly.
    let mut chunk = [0; STDERR_CHUNK_BYTES];
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            return Ok(());
        }
        emit(&chunk[..count]);
    }
}

// `opencode serve` is a JS runtime; on a heavily loaded host (load avg in the
// hundreds) cold start routinely exceeds 10s, which failed the daemon's lazy
// bridge init and surfaced to clients as a dropped `initialize`.
const READINESS_TIMEOUT: Duration = Duration::from_secs(45);
const READINESS_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Poll `GET {base_url}/global/health` until it returns `{healthy:true}` or
/// `timeout` elapses. Replaces the previous fixed 300ms sleep with a
/// race-free readiness gate.
async fn wait_until_healthy(base_url: &str, timeout: Duration) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/global/health", base_url.trim_end_matches('/'));
    // Include connecting and reading the response body in the readiness budget.
    // A backend can accept HTTP before it is ready to answer; checking the
    // deadline only between requests leaves the daemon's lazy OnceCell wedged.
    tokio::time::timeout(timeout, async {
        loop {
            let healthy = tokio::time::timeout(READINESS_REQUEST_TIMEOUT, async {
                if let Ok(resp) = client.get(&url).send().await
                    && resp.status().is_success()
                    && let Ok(body) = resp.json::<serde_json::Value>().await
                {
                    return body.get("healthy").and_then(serde_json::Value::as_bool) == Some(true);
                }
                false
            })
            .await
            .unwrap_or(false);
            if healthy {
                return;
            }
            tokio::time::sleep(READINESS_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("opencode did not report healthy at {url} within {timeout:?}"))
}

fn pick_port() -> anyhow::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

fn resolve_opencode_bin(env: &LaunchEnvironment, configured_bin: Option<&str>) -> PathBuf {
    let env_configured = env_string(env, "OPENCODE_BRIDGE_BIN");
    if let Some(raw) = configured_bin.or(env_configured.as_deref()) {
        let bin = raw.trim();
        if !bin.is_empty() && bin != "opencode" {
            return PathBuf::from(bin);
        }
    }

    if let Some(path) = env.find_on_path("opencode")
        && command_looks_usable(&path, env)
    {
        return path;
    }

    for candidate in fallback_opencode_bins(env) {
        if command_looks_usable(&candidate, env) {
            return candidate;
        }
    }

    PathBuf::from("opencode")
}

fn fallback_opencode_bins(env: &LaunchEnvironment) -> Vec<PathBuf> {
    let mut bins = Vec::new();
    if let Some(home) = env.get("HOME") {
        bins.push(PathBuf::from(home).join(".opencode/bin/opencode"));
    }
    bins.push(PathBuf::from("/opt/homebrew/bin/opencode"));
    bins.push(PathBuf::from("/usr/local/bin/opencode"));
    bins
}

fn command_looks_usable(bin: impl AsRef<OsStr>, env: &LaunchEnvironment) -> bool {
    let mut command = StdCommand::new(bin);
    command
        .env_clear()
        .envs(env.clone().into_pairs())
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn env_string(env: &LaunchEnvironment, key: &str) -> Option<String> {
    env.get(key)
        .and_then(OsStr::to_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[allow(dead_code)]
fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stderr_without_newlines_is_emitted_in_bounded_chunks_until_eof() {
        let payload = vec![b'x'; STDERR_CHUNK_BYTES * 20 + 17];
        let mut observed = Vec::new();
        let mut largest = 0;
        drain_stderr(payload.as_slice(), |chunk| {
            largest = largest.max(chunk.len());
            observed.extend_from_slice(chunk);
        })
        .await
        .unwrap();
        assert_eq!(observed, payload);
        assert_eq!(largest, STDERR_CHUNK_BYTES);
    }

    #[tokio::test]
    async fn dropping_stderr_owner_cancels_pending_read_and_closes_pipe() {
        use tokio::io::AsyncWriteExt;
        let (reader, mut writer) = tokio::io::duplex(16);
        let drain = StderrDrain::start(reader);
        let task = drain.0.abort_handle();
        tokio::task::yield_now().await;
        drop(drain);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned drain must stop without needing child EOF");
        assert!(writer.write_all(b"closed").await.is_err());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn failing_child_stderr_is_preserved_and_reader_exits_at_eof() {
        let mut command = if cfg!(windows) {
            let mut command = TokioCommand::new("cmd.exe");
            command.args(["/D", "/C", "(echo startup failed) 1>&2 & exit /b 7"]);
            command
        } else {
            let mut command = TokioCommand::new("/bin/sh");
            command.args(["-c", "printf 'startup failed' >&2; exit 7"]);
            command
        };
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut observed = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            drain_stderr(child.stderr.take().unwrap(), |chunk| {
                observed.extend_from_slice(chunk)
            }),
        )
        .await
        .expect("failed child must close its diagnostic stream")
        .unwrap();
        assert_eq!(
            String::from_utf8(observed).unwrap().trim(),
            "startup failed"
        );
        assert_eq!(child.wait().await.unwrap().code(), Some(7));
    }

    #[tokio::test]
    async fn readiness_deadline_covers_silent_http_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            wait_until_healthy(&url, Duration::from_millis(50)),
        )
        .await
        .expect("readiness must not hang after HTTP accepts the connection");
        assert!(result.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn readiness_deadline_covers_incomplete_http_body() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            wait_until_healthy(&url, Duration::from_millis(50)),
        )
        .await
        .expect("readiness must not hang reading an incomplete HTTP body");
        assert!(result.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn readiness_retries_after_a_stalled_startup_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_first, _) = listener.accept().await.unwrap();
            let (mut second, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            second.read(&mut request).await.unwrap();
            let body = br#"{"healthy":true}"#;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            second.write_all(header.as_bytes()).await.unwrap();
            second.write_all(body).await.unwrap();
        });
        wait_until_healthy(&url, Duration::from_secs(3))
            .await
            .expect("a fresh health connection must succeed within the original total budget");
        server.await.unwrap();
    }

    #[test]
    fn external_constructor_stores_fields_and_spawns_no_child() {
        let runtime = OpencodeRuntime::external(
            "http://example.test:1234".to_string(),
            "tok-abc".to_string(),
        );
        assert_eq!(runtime.base_url, "http://example.test:1234");
        assert_eq!(runtime.auth_token, "tok-abc");
        assert!(runtime._child.is_none());
    }
}
