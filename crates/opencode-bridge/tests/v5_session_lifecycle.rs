//! T5 acceptance: opencode `session.created` / `session.updated` / `session.diff`
//! SSE events translate into the matching codex thread notifications.

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
use tokio::io::BufReader;

type SseInjector = Arc<Mutex<Option<TcpStream>>>;

#[tokio::test]
async fn session_updated_title_change_emits_thread_name_updated() {
    let (mut read, mut write, sse_injector, _state_dir, server_task) = bring_up_bridge().await;

    // Bind a session via thread/start so the index has a known thread_id.
    let thread_id = start_thread(&mut read, &mut write, "/tmp/opencode-v5").await;

    // session.updated with a new title — bridge should emit thread/name/updated.
    inject_sse(
        &sse_injector,
        json!({
            "type":"session.updated",
            "properties":{
                "sessionID":"ses_1",
                "info":{"title":"Renamed by Peer"}
            }
        }),
    );
    let renamed =
        read_until_notification(&mut read, "thread/name/updated", Duration::from_secs(3)).await;
    assert_eq!(renamed["params"]["threadId"], thread_id);
    assert_eq!(renamed["params"]["threadName"], "Renamed by Peer");

    drop(write);
    server_task.abort();
}

#[tokio::test]
async fn session_updated_archive_transitions_emit_archive_notifications() {
    let (mut read, mut write, sse_injector, _state_dir, server_task) = bring_up_bridge().await;
    let thread_id = start_thread(&mut read, &mut write, "/tmp/opencode-v5-arch").await;

    // Archive: time.archived is set to a number.
    inject_sse(
        &sse_injector,
        json!({
            "type":"session.updated",
            "properties":{
                "sessionID":"ses_1",
                "info":{"time":{"archived":1700000000}}
            }
        }),
    );
    let archived =
        read_until_notification(&mut read, "thread/archived", Duration::from_secs(3)).await;
    assert_eq!(archived["params"]["threadId"], thread_id);

    // Unarchive: time.archived is explicitly null.
    inject_sse(
        &sse_injector,
        json!({
            "type":"session.updated",
            "properties":{
                "sessionID":"ses_1",
                "info":{"time":{"archived":null}}
            }
        }),
    );
    let unarchived =
        read_until_notification(&mut read, "thread/unarchived", Duration::from_secs(3)).await;
    assert_eq!(unarchived["params"]["threadId"], thread_id);

    drop(write);
    server_task.abort();
}

#[tokio::test]
async fn session_diff_emits_turn_diff_updated_with_unified_text() {
    let (mut read, mut write, sse_injector, _state_dir, server_task) = bring_up_bridge().await;
    let thread_id = start_thread(&mut read, &mut write, "/tmp/opencode-v5-diff").await;

    inject_sse(
        &sse_injector,
        json!({
            "type":"session.diff",
            "properties":{
                "sessionID":"ses_1",
                "diff":[
                    {"file":"src/foo.rs","patch":"@@ -1 +1 @@\n-old\n+new\n","additions":1,"deletions":1,"status":"modified"},
                    {"file":"src/bar.rs","patch":"@@ -0,0 +1,2 @@\n+a\n+b\n","additions":2,"deletions":0,"status":"added"}
                ]
            }
        }),
    );
    let diff =
        read_until_notification(&mut read, "turn/diff/updated", Duration::from_secs(3)).await;
    assert_eq!(diff["params"]["threadId"], thread_id);
    let unified = diff["params"]["diff"].as_str().unwrap();
    assert!(unified.contains("diff --git a/src/foo.rs b/src/foo.rs"));
    assert!(unified.contains("diff --git a/src/bar.rs b/src/bar.rs"));
    assert!(unified.contains("+new"));
    assert!(unified.contains("+a\n+b"));

    drop(write);
    server_task.abort();
}

/// Subagent sessions carry a non-empty `parentID` (top-level sessions omit
/// the field). A `session.created` for one must not be bound into the index
/// or announced as `thread/started` — matching the `thread/list` rule that
/// keeps conversation-scoped sessions out of the session list.
#[tokio::test]
async fn session_created_subagent_is_ignored_top_level_is_announced() {
    let (mut read, write, sse_injector, _state_dir, server_task) = bring_up_bridge().await;

    // Subagent session: has parentID → no thread/started.
    inject_sse(
        &sse_injector,
        json!({
            "type":"session.created",
            "properties":{
                "sessionID":"ses_sub_1",
                "info":{
                    "id":"ses_sub_1",
                    "title":"Explore things (@explore subagent)",
                    "directory":"/tmp/opencode-v5",
                    "parentID":"ses_parent_1",
                    "time":{"created":1_000,"updated":1_000}
                }
            }
        }),
    );
    // Top-level session: no parentID → thread/started.
    inject_sse(
        &sse_injector,
        json!({
            "type":"session.created",
            "properties":{
                "sessionID":"ses_top_1",
                "info":{
                    "id":"ses_top_1",
                    "title":"Top-level peer session",
                    "directory":"/tmp/opencode-v5",
                    "time":{"created":1_100,"updated":1_100}
                }
            }
        }),
    );
    let started =
        read_until_notification(&mut read, "thread/started", Duration::from_secs(3)).await;
    // The subagent was injected first; if the guard failed it would be the
    // one announced here. Only the top-level session may pass.
    let announced_name = started["params"]["thread"]["name"].as_str().unwrap();
    assert_eq!(announced_name, "Top-level peer session");

    drop(write);
    server_task.abort();
}

// ---- shared helpers ----

async fn bring_up_bridge() -> (
    BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    tokio::io::WriteHalf<tokio::io::DuplexStream>,
    SseInjector,
    tempfile::TempDir,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let sse_injector: SseInjector = Arc::new(Mutex::new(None));
    let base_url = start_fake_opencode(Arc::clone(&seen), Arc::clone(&sse_injector));
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
        json!({"clientInfo":{"name":"v5","version":"0"}}),
    )
    .await;
    let _ = read_until_response(&mut read, 1).await;
    send_notification(&mut write, "initialized", json!({})).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if sse_injector.lock().unwrap().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("SSE /event never opened");

    (read, write, sse_injector, state_dir, server_task)
}

async fn start_thread<W>(
    read: &mut BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    write: &mut W,
    cwd: &str,
) -> String
where
    W: tokio::io::AsyncWrite + Unpin,
{
    send(
        write,
        2,
        "thread/start",
        json!({"cwd":cwd,"model":"openai/gpt-test"}),
    )
    .await;
    let started = read_until_response(read, 2).await;
    started["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn inject_sse(injector: &SseInjector, event: Value) {
    let payload = format!("data: {}\n\n", serde_json::to_string(&event).unwrap());
    let mut guard = injector.lock().unwrap();
    if let Some(stream) = guard.as_mut() {
        let _ = stream.write_all(payload.as_bytes());
        let _ = stream.flush();
    }
}

async fn send<W: tokio::io::AsyncWrite + Unpin>(
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

async fn send_notification<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    method: &str,
    params: Value,
) {
    let frame = json!({"jsonrpc":"2.0","method":method,"params":params});
    let line = serde_json::to_vec(&frame).unwrap();
    use tokio::io::AsyncWriteExt;
    writer.write_all(&line).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

async fn read_until_response<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R, id: i64) -> Value {
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

async fn read_until_notification<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    method: &str,
    timeout: Duration,
) -> Value {
    tokio::time::timeout(timeout, async {
        loop {
            let value: Value = read_json_line(reader).await.unwrap().unwrap();
            if value.get("method").and_then(Value::as_str) == Some(method) {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("notification {method} not seen within {timeout:?}"))
}

fn start_fake_opencode(seen: Arc<Mutex<Vec<String>>>, injector: SseInjector) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let seen = Arc::clone(&seen);
            let injector = Arc::clone(&injector);
            thread::spawn(move || handle_conn(stream, seen, injector));
        }
    });
    format!("http://{addr}")
}

fn handle_conn(mut stream: TcpStream, seen: Arc<Mutex<Vec<String>>>, injector: SseInjector) {
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
    let req = String::from_utf8_lossy(&bytes);
    let request_line = req.lines().next().unwrap_or("").to_string();
    seen.lock().unwrap().push(request_line.clone());
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

    let body = if request_line.starts_with("POST /session?") {
        json!({
            "id":"ses_1",
            "directory":"/tmp/opencode-v5",
            "title":"V5",
            "time":{"created":1000,"updated":1000}
        })
    } else if request_line.starts_with("GET /provider?") {
        json!({"all":[],"default":[],"connected":[]})
    } else {
        json!({})
    };
    let body = serde_json::to_string(&body).unwrap();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
