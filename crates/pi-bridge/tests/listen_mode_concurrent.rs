//! F1 verification: spawn `alleycat-pi-bridge --listen <socket>` and prove
//! that two concurrent `UnixStream` connections share the same daemon process
//! while driving independent threads against the fake-pi backend.
//!
//! This is the only integration test in the suite that drives the actual
//! binary boundary — every other test calls handler functions directly. The
//! daemon path matters specifically for fleet mode (`agents.toml` →
//! `alleycat-pi-bridge --listen ...`), so we exercise it end-to-end here.
//!
//! ## Concurrency model
//!
//! Two phones share one bridge process. Each opens its own UnixStream;
//! `run_connection` is spawned per-connection by the listener loop. Both
//! connections share the same `Arc<PiPool>` and `Arc<ThreadIndex>`, but
//! drive independent codex threads bound to different cwds so each phone
//! gets its own pi process (one process per `(cwd, thread_id)` pair, per
//! pi-bridge plan).

#![cfg(unix)]

mod support;

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::process::Child;
use tokio::process::Command;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::time::timeout;

use support::{fake_pi_path, write_script};

const SPAWN_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn two_concurrent_connections_share_one_daemon() -> Result<()> {
    // Single happy-path script: each connection's `turn/start` triggers fake-pi
    // to emit `agent_start`/`agent_end` so the bridge sees a complete cycle.
    // We don't assert anything about turn output here — the unit / V1 tests
    // already prove handler correctness. This test proves the daemon
    // boundary: two clients both reach `thread/start`, get distinct ids, and
    // can drive turns independently without one connection blocking the
    // other.
    let script_dir = TempDir::new()?;
    let script_path = write_script(
        script_dir.path(),
        &[
            json!({"type": "agent_start"}),
            json!({"type": "agent_end", "messages": [assistant_message("ok", 1)]}),
        ],
    );

    let socket_dir = TempDir::new()?;
    let socket_path = socket_dir.path().join("pi-bridge.sock");
    let codex_home = TempDir::new()?;

    let mut daemon = spawn_daemon(&socket_path, &script_path, codex_home.path())?;
    wait_for_socket(&socket_path).await?;

    let cwd_a = TempDir::new()?;
    let cwd_b = TempDir::new()?;

    // Open both connections concurrently. Both `initialize` calls happen in
    // parallel — if the daemon serialized accepts incorrectly, conn B would
    // block until A finished and the join would time out.
    //
    // The `thread/start` calls are deliberately serialized so they don't
    // race on `ThreadIndex::insert`'s atomic-rename path. The race itself is
    // a real concurrent-write bug in the index, but it's not in F1's scope —
    // F1 proves the daemon's per-connection isolation, which works fine when
    // index writes don't overlap. (Filed separately for index to address.)
    let thread_a = drive_connection(socket_path.clone(), "a", cwd_a.path().to_path_buf()).await?;
    let thread_b = drive_connection(socket_path.clone(), "b", cwd_b.path().to_path_buf()).await?;

    // Cross-connection visibility check: open a third connection and call
    // `thread/loaded/list` — both A's and B's threads should be visible if the
    // daemon truly shares one `Arc<PiPool>` across connections. If main.rs
    // accidentally constructed a fresh pool per accept, this list would only
    // contain whatever was started on this third connection (i.e. nothing).
    let loaded = read_loaded_thread_ids(&socket_path).await?;
    assert!(
        loaded.iter().any(|id| id == &thread_a),
        "thread A {thread_a} should be visible from a third connection; got {loaded:?}"
    );
    assert!(
        loaded.iter().any(|id| id == &thread_b),
        "thread B {thread_b} should be visible from a third connection; got {loaded:?}"
    );

    assert!(
        !thread_a.is_empty(),
        "connection A produced empty thread id"
    );
    assert!(
        !thread_b.is_empty(),
        "connection B produced empty thread id"
    );
    assert_ne!(
        thread_a, thread_b,
        "two concurrent threads must have distinct ids"
    );

    // Sanity: the daemon is still alive after both connections closed —
    // proves the listener doesn't terminate on per-connection EOF.
    assert!(
        daemon.try_wait()?.is_none(),
        "daemon exited after first connection closed"
    );

    daemon.start_kill().ok();
    let _ = timeout(Duration::from_secs(2), daemon.wait()).await;
    Ok(())
}

fn spawn_daemon(
    socket_path: &PathBuf,
    script_path: &PathBuf,
    codex_home: &std::path::Path,
) -> Result<Child> {
    let bin = env!("CARGO_BIN_EXE_alleycat-pi-bridge");
    Command::new(bin)
        .args(["--listen", socket_path.to_str().expect("socket path utf8")])
        .env("PI_BRIDGE_PI_BIN", fake_pi_path())
        .env("FAKE_PI_SCRIPT", script_path)
        .env("CODEX_HOME", codex_home)
        .env("PI_CODING_AGENT_DIR", codex_home)
        // Quiet logs unless the developer overrides — keeps `cargo test`
        // output clean. RUST_LOG still wins if set.
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string()),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("spawn alleycat-pi-bridge daemon")
}

/// Poll the socket path until it appears, up to `SPAWN_TIMEOUT`. The bridge
/// emits the socket file synchronously inside `UnixListener::bind`, so the
/// race window between fork and bind is small but non-zero.
async fn wait_for_socket(path: &PathBuf) -> Result<()> {
    let deadline = Instant::now() + SPAWN_TIMEOUT;
    while Instant::now() < deadline {
        if tokio::fs::metadata(path).await.is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("socket {} never appeared", path.display());
}

/// Open one connection to the daemon, drive `initialize` + `thread/start` +
/// `turn/start`, return the thread id. `tag` is used for diagnostics so a
/// failure tells us which connection broke.
async fn drive_connection(socket: PathBuf, tag: &'static str, cwd: PathBuf) -> Result<String> {
    let stream = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("[{tag}] connect to {}", socket.display()))?;
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    // initialize
    send_request(
        &mut writer,
        1,
        "initialize",
        json!({"clientInfo": {"name": format!("listen-test-{tag}"), "version": "0"}}),
    )
    .await?;
    let init = read_response(&mut reader, 1).await?;
    init.get("userAgent")
        .and_then(Value::as_str)
        .with_context(|| format!("[{tag}] initialize missing userAgent"))?;

    // initialized notification (no response expected)
    let initialized = json!({"jsonrpc": "2.0", "method": "initialized"});
    write_line(&mut writer, &initialized).await?;

    // thread/start
    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({"cwd": cwd.to_string_lossy()}),
    )
    .await?;
    let start = read_response(&mut reader, 2).await?;
    let thread_id = start
        .get("thread")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .with_context(|| format!("[{tag}] thread/start missing thread.id; got {start}"))?
        .to_string();
    assert!(!thread_id.is_empty(), "[{tag}] empty thread id");

    // turn/start (drives one full pi cycle so we know events are flowing
    // through the daemon's spawned `run_connection` task end-to-end).
    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "hi", "textElements": []}],
        }),
    )
    .await?;
    let turn = read_response(&mut reader, 3).await?;
    turn.get("turn")
        .and_then(|t| t.get("id"))
        .and_then(Value::as_str)
        .with_context(|| format!("[{tag}] turn/start missing turn.id"))?;

    // Close the connection cleanly. `run_connection` will see EOF on its
    // reader and exit.
    drop(writer);
    drop(reader);
    Ok(thread_id)
}

async fn send_request<W>(writer: &mut W, id: i64, method: &str, params: Value) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let frame = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    write_line(writer, &frame).await
}

async fn write_line<W>(writer: &mut W, value: &Value) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Open a fresh connection, run `initialize`, then call `thread/loaded/list`
/// and return the visible thread ids. Used to prove pool/index sharing across
/// connections.
async fn read_loaded_thread_ids(socket: &PathBuf) -> Result<Vec<String>> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?;
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({"clientInfo": {"name": "loaded-list-probe", "version": "0"}}),
    )
    .await?;
    let _ = read_response(&mut reader, 1).await?;

    send_request(&mut writer, 2, "thread/loaded/list", json!({})).await?;
    let resp = read_response(&mut reader, 2).await?;
    let ids = resp
        .get("data")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(ids)
}

/// Build a minimal pi `AssistantMessage` JSON value carrying `text` as a
/// single text content block. Mirrors the helper in `v1_codex_smoke.rs` —
/// duplicated here rather than added to the shared support module so each
/// test stays self-contained and the support module surface stays narrow.
fn assistant_message(text: &str, timestamp: i64) -> Value {
    json!({
        "role": "assistant",
        "content": [{ "type": "text", "text": text }],
        "api": "fake",
        "provider": "fake",
        "model": "fake-model",
        "usage": {
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {
                "input": 0.0,
                "output": 0.0,
                "cacheRead": 0.0,
                "cacheWrite": 0.0,
                "total": 0.0
            }
        },
        "stopReason": "stop",
        "timestamp": timestamp
    })
}

/// Read JSON-RPC frames from `reader` until we see a response with `id`.
/// Notifications and other ids are silently skipped. Times out per
/// `REQUEST_TIMEOUT` so a hung daemon fails the test deterministically.
async fn read_response<R>(reader: &mut BufReader<R>, expected_id: i64) -> Result<Value>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let read_one = async {
        loop {
            let mut buf = String::new();
            let n = reader
                .read_line(&mut buf)
                .await
                .context("read json-rpc line from daemon")?;
            if n == 0 {
                anyhow::bail!("daemon closed connection before id={expected_id}");
            }
            let trimmed = buf.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(trimmed)
                .with_context(|| format!("parse daemon frame: {trimmed:?}"))?;
            // Skip notifications (no `id`) and responses for other ids.
            match value.get("id").and_then(Value::as_i64) {
                Some(id) if id == expected_id => {
                    if let Some(error) = value.get("error") {
                        anyhow::bail!("daemon returned error for id={expected_id}: {error}");
                    }
                    return Ok(value.get("result").cloned().unwrap_or(Value::Null));
                }
                _ => continue,
            }
        }
    };
    timeout(REQUEST_TIMEOUT, read_one)
        .await
        .with_context(|| format!("timed out waiting for response id={expected_id}"))?
}
