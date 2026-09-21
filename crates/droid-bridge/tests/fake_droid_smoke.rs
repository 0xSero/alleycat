use std::time::Duration;

use alleycat_bridge_core::framing::{read_json_line, write_json_line};
use alleycat_bridge_core::server::serve_stream;
use alleycat_bridge_core::{JsonRpcRequest, JsonRpcVersion, RequestId};
use alleycat_droid_bridge::DroidBridge;
use serde_json::{Value, json};
use tokio::io::BufReader;

#[tokio::test]
async fn initialize_thread_start_turn_start_smoke() {
    let bridge = DroidBridge::builder()
        .agent_bin(env!("CARGO_BIN_EXE_fake-droid"))
        .build()
        .await
        .expect("build droid bridge");
    let (client, server) = tokio::io::duplex(64 * 1024);
    let server_task = tokio::spawn(serve_stream(bridge, server));
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);

    send(
        &mut write,
        1,
        "initialize",
        json!({"clientInfo":{"name":"fake-droid-smoke","version":"0"}}),
    )
    .await;
    let init = read_until_response(&mut read, 1).await;
    assert_eq!(init["result"]["userAgent"], "alleycat-droid-bridge/0.1.0");

    send(&mut write, 10, "model/list", json!({})).await;
    let catalog = read_until_response(&mut read, 10).await;
    assert_eq!(catalog["result"]["data"].as_array().unwrap().len(), 4);
    assert!(catalog["result"]["data"].as_array().unwrap().iter().any(|model| model["id"] == "claude-opus-5"));

    let cwd = tempfile::TempDir::new().unwrap();
    send(
        &mut write,
        2,
        "thread/start",
        json!({"cwd": cwd.path().to_string_lossy()}),
    )
    .await;
    let start = read_until_response(&mut read, 2).await;
    let thread_id = start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(start["result"]["modelProvider"], "droid");
    assert_eq!(start["result"]["model"], "gpt-5.6-sol");
    assert_eq!(start["result"]["reasoningEffort"], "medium");

    send(
        &mut write,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "model": "claude-opus-5", "effort": "max",
            "input": [{"type":"text","text":"check-native-model-effort Reply with exactly OK."}]
        }),
    )
    .await;
    let turn = read_until_response(&mut read, 3).await;
    assert_eq!(turn["result"]["turn"]["status"], "inProgress");

    let mut saw_delta = false;
    let mut saw_completed = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !saw_completed {
            let frame: Value = read_json_line(&mut read).await.unwrap().unwrap();
            if frame.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta") {
                saw_delta = frame["params"]["delta"] == "OK";
            }
            if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
                saw_completed = true;
            }
        }
    })
    .await
    .expect("turn should complete");
    assert!(saw_delta, "expected assistant delta");

    drop(write);
    server_task.abort();
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
