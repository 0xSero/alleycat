//! Shared scaffolding for the v2/v4/v5/v6/v7/v9 integration tests.
//!
//! Generalizes the v3/v8 fake-opencode pattern so each test crate can:
//!
//! - bring up an `OpencodeBridge` against a fake HTTP+SSE server,
//! - inject SSE frames at any time after the bridge subscribes,
//! - capture upstream request lines and bodies (keyed by URL prefix),
//! - assert on server→client `JsonRpcRequest` frames the bridge sends and
//!   echo back a typed response by the bridge-generated id,
//! - read codex-side responses and notifications.
//!
//! Cargo integration-test convention: each `tests/*.rs` file is its own
//! crate, so this module is included by `#[path = "support/mod.rs"] mod support;`
//! at the top of every test file (a `mod` declaration with a `#[path]` lets us
//! share via path without depending on `tests/common/mod.rs`'s "every test runs
//! it as its own test binary" wart).
//!
//! All async helpers use `tokio::time::timeout` so a shape mismatch surfaces
//! as a test failure within seconds rather than the bridge's 5-minute approval
//! default.

#![allow(dead_code)] // each test crate uses a different subset of the helpers

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use alleycat_bridge_core::framing::{read_json_line, write_json_line};
use alleycat_bridge_core::server::serve_stream;
use alleycat_bridge_core::{JsonRpcRequest, JsonRpcVersion, RequestId};
use alleycat_opencode_bridge::OpencodeBridge;
use alleycat_opencode_bridge::opencode_proc::OpencodeRuntime;
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};

pub type SseInjector = Arc<Mutex<Option<TcpStream>>>;

/// Shared state for the fake opencode HTTP+SSE server. Tests configure
/// per-route response bodies before bringing up the bridge; the server
/// thread reads from this map and serves them.
#[derive(Default)]
pub struct FakeServerState {
    /// Each entry is `(URL prefix, body Value)`. The longest prefix matching
    /// the request line wins, so a generic collection route can coexist with
    /// a more-specific item route.
    pub routes: Vec<(String, Value)>,
    /// Captured request lines (the literal first line, e.g. `POST /foo HTTP/1.1`).
    pub seen: Vec<String>,
    /// Captured request bodies keyed by URL prefix. Tests assert on these
    /// after the round-trip completes.
    pub bodies: HashMap<String, Value>,
}

impl FakeServerState {
    /// Add or override a route. The body is served as JSON with a 200 status.
    pub fn route(&mut self, prefix: impl Into<String>, body: Value) {
        let prefix = prefix.into();
        if let Some(slot) = self.routes.iter_mut().find(|(p, _)| p == &prefix) {
            slot.1 = body;
        } else {
            self.routes.push((prefix, body));
        }
    }
}

pub type FakeState = Arc<Mutex<FakeServerState>>;

/// Bring up a fake opencode server with the supplied per-route bodies and a
/// fresh `OpencodeBridge` connected to it. Returns the codex-side socket
/// halves, the SSE injector, the temp state dir, and the spawned server task.
///
/// `client_label` is used as the codex `clientInfo.name`.
pub async fn bring_up_bridge(client_label: &str, state: FakeState) -> BridgeFixture {
    let injector: SseInjector = Arc::new(Mutex::new(None));
    let base_url = start_fake_opencode(Arc::clone(&state), Arc::clone(&injector));
    let state_dir = tempfile::TempDir::new().unwrap();
    let bridge = Arc::new(
        OpencodeBridge::new_with_state_dir(
            OpencodeRuntime::external(base_url, "test-token".into()),
            state_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let (client, server) = tokio::io::duplex(64 * 1024);
    let server_task = tokio::spawn(serve_stream(bridge, server));
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);

    send(
        &mut write,
        1,
        "initialize",
        json!({"clientInfo":{"name":client_label,"version":"0"}}),
    )
    .await;
    let _ = read_until_response(&mut read, 1).await;
    send_notification(&mut write, "initialized", json!({})).await;

    // Wait until the SSE consumer attaches its `/event` connection.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if injector.lock().unwrap().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("SSE /event never opened");

    BridgeFixture {
        read,
        write,
        injector,
        state,
        _state_dir: state_dir,
        server_task,
    }
}

pub struct BridgeFixture {
    pub read: BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    pub write: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    pub injector: SseInjector,
    pub state: FakeState,
    pub _state_dir: tempfile::TempDir,
    pub server_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl BridgeFixture {
    /// Send `thread/start` and return the resulting thread id. Most tests
    /// follow `bring_up_bridge` with this immediately.
    pub async fn start_thread(&mut self, cwd: &str) -> String {
        send(
            &mut self.write,
            2,
            "thread/start",
            json!({"cwd":cwd,"model":"openai/gpt-test"}),
        )
        .await;
        let started = read_until_response(&mut self.read, 2).await;
        started["result"]["thread"]["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub fn inject_sse(&self, event: Value) {
        inject_sse(&self.injector, event);
    }

    pub fn captured_body(&self, prefix: &str) -> Option<Value> {
        self.state.lock().unwrap().bodies.get(prefix).cloned()
    }

    pub fn seen(&self) -> Vec<String> {
        self.state.lock().unwrap().seen.clone()
    }

    pub async fn shutdown(self) {
        drop(self.write);
        self.server_task.abort();
    }
}

/// Inject an SSE event into the held-open `/event` connection.
pub fn inject_sse(injector: &SseInjector, event: Value) {
    let payload = format!("data: {}\n\n", serde_json::to_string(&event).unwrap());
    let mut guard = injector.lock().unwrap();
    if let Some(stream) = guard.as_mut() {
        let _ = stream.write_all(payload.as_bytes());
        let _ = stream.flush();
    }
}

pub async fn send<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    id: i64,
    method: &str,
    params: Value,
) {
    write_json_line(
        writer,
        &JsonRpcRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId::Integer(id),
            method: method.to_string(),
            params: Some(params),
        },
    )
    .await
    .unwrap();
}

pub async fn send_notification<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    method: &str,
    params: Value,
) {
    let frame = json!({"jsonrpc":"2.0","method":method,"params":params});
    let line = serde_json::to_vec(&frame).unwrap();
    writer.write_all(&line).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

pub async fn read_until_response<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    id: i64,
) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let value: Value = read_json_line(reader).await.unwrap().unwrap();
            if value.get("id").and_then(Value::as_i64) == Some(id) {
                return value;
            }
        }
    })
    .await
    .unwrap()
}

pub async fn read_until_notification<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    method: &str,
    timeout: Duration,
) -> Value {
    tokio::time::timeout(timeout, async {
        loop {
            let value: Value = read_json_line(reader).await.unwrap().unwrap();
            if value.get("method").and_then(Value::as_str) == Some(method)
                && value.get("id").is_none()
            {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("notification {method} not seen within {timeout:?}"))
}

/// Read frames from the bridge socket until a server→client `JsonRpcRequest`
/// for `method` arrives. Returns the entire frame so the caller can extract
/// `id` and `params`. Notifications and responses are drained silently.
///
/// This is the linchpin for v4_approval / v6_question: the bridge generates a
/// `RequestId::String("bridge-N")` internally, and the test must echo that
/// exact id in its response or the bridge's pending table won't resolve and
/// the round-trip will hang for the full `DEFAULT_APPROVAL_TIMEOUT` (5 min).
pub async fn read_until_server_request<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    method: &str,
    timeout: Duration,
) -> Value {
    tokio::time::timeout(timeout, async {
        loop {
            let value: Value = read_json_line(reader).await.unwrap().unwrap();
            // A request frame has both `id` and `method` (responses have `id`
            // but no `method`; notifications have `method` but no `id`).
            if value.get("method").and_then(Value::as_str) == Some(method)
                && value.get("id").is_some()
            {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("server→client request {method} not seen within {timeout:?}"))
}

/// Write a `JsonRpcResponse` frame back to the bridge with the supplied id
/// (echoed verbatim from the matching `read_until_server_request` frame) and
/// `result` payload. The bridge's `NotificationSender::resolve_response`
/// handler matches on the request id to wake the pending oneshot.
pub async fn send_server_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    id: &Value,
    result: Value,
) {
    let frame = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    });
    let line = serde_json::to_vec(&frame).unwrap();
    writer.write_all(&line).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

/// Block until the fake server has captured a body for `prefix`. Returns the
/// captured body. Polls because the body is recorded by a background blocking
/// thread that races the test's assertions.
pub async fn await_captured_body(state: &FakeState, prefix: &str, timeout: Duration) -> Value {
    let prefix = prefix.to_string();
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(body) = state.lock().unwrap().bodies.get(&prefix).cloned() {
                return body;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("upstream body for {prefix} not captured within {timeout:?}"))
}

fn start_fake_opencode(state: FakeState, injector: SseInjector) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let state = Arc::clone(&state);
            let injector = Arc::clone(&injector);
            thread::spawn(move || handle_conn(stream, state, injector));
        }
    });
    format!("http://{addr}")
}

fn handle_conn(mut stream: TcpStream, state: FakeState, injector: SseInjector) {
    let mut bytes = Vec::new();
    let mut buf = [0u8; 1024];
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let headers_end = loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = find_subsequence(&bytes, b"\r\n\r\n") {
                    break pos + 4;
                }
            }
            Err(_) => return,
        }
    };
    let header_text = String::from_utf8_lossy(&bytes[..headers_end]);
    let content_length = header_text
        .lines()
        .find_map(|line| {
            line.strip_prefix("content-length:")
                .or_else(|| line.strip_prefix("Content-Length:"))
        })
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while bytes.len() < headers_end + content_length {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => bytes.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let req_text = String::from_utf8_lossy(&bytes);
    let request_line = req_text.lines().next().unwrap_or("").to_string();
    state.lock().unwrap().seen.push(request_line.clone());
    let _ = stream.set_read_timeout(None);

    if request_line.starts_with("GET /event") {
        let head = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
        if stream.write_all(head.as_bytes()).is_err() {
            return;
        }
        let _ = stream.flush();
        *injector.lock().unwrap() = Some(stream.try_clone().unwrap());
        thread::sleep(Duration::from_secs(60));
        return;
    }

    // Capture body. URLs in `routes` carry the bare prefix (no trailing query
    // string); strip the query so the prefix `POST /permission/perm_1/reply`
    // matches the actual request line `POST /permission/perm_1/reply?auth_token=…`.
    let body_text = if content_length > 0 && bytes.len() > headers_end {
        let end = (headers_end + content_length).min(bytes.len());
        String::from_utf8_lossy(&bytes[headers_end..end]).to_string()
    } else {
        String::new()
    };
    let body_value = serde_json::from_str::<Value>(&body_text).unwrap_or(Value::Null);

    // Find a matching route. `request_line` is e.g. `POST /foo?auth=… HTTP/1.1`;
    // we match on "<METHOD> <path-with-or-without-query>" prefix.
    let response_body = {
        let st = state.lock().unwrap();
        st.routes
            .iter()
            .filter(|(prefix, _)| request_line.starts_with(prefix.as_str()))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, body)| body.clone())
    };

    // Capture body keyed by the matching prefix (so tests can assert on it).
    {
        let mut st = state.lock().unwrap();
        let matching_prefix = st
            .routes
            .iter()
            .filter(|(prefix, _)| request_line.starts_with(prefix.as_str()))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(prefix, _)| prefix.clone());
        if let Some(prefix) = matching_prefix {
            st.bodies.insert(prefix, body_value);
        }
    }

    // 204 No Content for explicit `__no_content__` sentinel; otherwise JSON.
    let response = match response_body {
        Some(Value::String(ref s)) if s == "__no_content__" => {
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".to_string()
        }
        Some(body) => {
            let body = serde_json::to_string(&body).unwrap();
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
        }
        None => {
            // Default: 200 with empty object — keeps tests forgiving when an
            // upstream call lands that they didn't explicitly register.
            let body = "{}";
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
        }
    };

    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
