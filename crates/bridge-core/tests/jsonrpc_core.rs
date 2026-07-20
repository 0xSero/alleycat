use std::sync::Arc;

use alleycat_bridge_core::{
    Bridge, Conn, InboundMessage, JsonRpcError, JsonRpcMessage, JsonRpcNotification,
    JsonRpcRequest, JsonRpcVersion, RequestId,
    framing::{read_json_line, write_json_line},
    server,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::BufReader;

#[tokio::test]
async fn framing_round_trips_jsonrpc_message() {
    let mut bytes = Vec::new();
    write_json_line(
        &mut bytes,
        &JsonRpcMessage::Notification(JsonRpcNotification {
            jsonrpc: JsonRpcVersion,
            method: "thread/started".to_string(),
            params: Some(json!({"threadId":"t1"})),
        }),
    )
    .await
    .unwrap();

    let mut reader = BufReader::new(bytes.as_slice());
    let got: Value = read_json_line(&mut reader).await.unwrap().unwrap();
    assert_eq!(got["method"], "thread/started");
    assert_eq!(got["params"]["threadId"], "t1");
}

#[tokio::test]
async fn framing_rejects_oversized_jsonl_before_deserialization() {
    use alleycat_bridge_core::framing::{MAX_JSONL_FRAME_BYTES, ReadJsonLineError};

    let oversized = vec![b'x'; MAX_JSONL_FRAME_BYTES + 1];
    let mut reader = BufReader::new(oversized.as_slice());
    let error = read_json_line::<Value, _>(&mut reader).await.unwrap_err();
    assert!(matches!(
        error,
        ReadJsonLineError::Oversized {
            limit: MAX_JSONL_FRAME_BYTES
        }
    ));
}

#[tokio::test]
async fn framing_preserves_blank_line_and_eof_behavior_under_the_limit() {
    let bytes = b"\n  \r\n{\"ok\":true}";
    let mut reader = BufReader::new(bytes.as_slice());
    let value: Value = read_json_line(&mut reader).await.unwrap().unwrap();
    assert_eq!(value, json!({"ok": true}));
    assert!(
        read_json_line::<Value, _>(&mut reader)
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn inbound_routes_request_response_notification() {
    assert!(matches!(
        InboundMessage::from_value(json!({"jsonrpc":"2.0","id":1,"method":"initialize"})).unwrap(),
        InboundMessage::Request(_)
    ));
    assert!(matches!(
        InboundMessage::from_value(json!({"jsonrpc":"2.0","method":"initialized"})).unwrap(),
        InboundMessage::Notification(_)
    ));
    assert!(matches!(
        InboundMessage::from_value(json!({"jsonrpc":"2.0","id":1,"result":{}})).unwrap(),
        InboundMessage::Response(_)
    ));
}

#[tokio::test]
async fn connection_honors_opt_out_notifications() {
    #[derive(Default)]
    struct TestBridge;

    #[async_trait]
    impl Bridge for TestBridge {
        async fn initialize(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
            Ok(
                json!({"userAgent":"test","codexHome":"/tmp","platformFamily":"unix","platformOs":"macos"}),
            )
        }

        async fn dispatch(
            &self,
            ctx: &Conn,
            _method: &str,
            _params: Value,
        ) -> Result<Value, JsonRpcError> {
            ctx.notifier()
                .send_notification("muted/event", json!({"bad":true}))
                .unwrap();
            ctx.notifier()
                .send_notification("visible/event", json!({"ok":true}))
                .unwrap();
            Ok(json!({}))
        }
    }

    let (client, bridge) = tokio::io::duplex(8192);
    let task = tokio::spawn(server::serve_stream(Arc::new(TestBridge), bridge));
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);

    write_json_line(
        &mut write,
        &JsonRpcRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId::Integer(1),
            method: "initialize".to_string(),
            params: Some(json!({
                "clientInfo": {"name":"test","version":"0"},
                "capabilities": {"optOutNotificationMethods": ["muted/event"]}
            })),
        },
    )
    .await
    .unwrap();
    let _init: Value = read_json_line(&mut read).await.unwrap().unwrap();

    write_json_line(
        &mut write,
        &JsonRpcRequest {
            jsonrpc: JsonRpcVersion,
            id: RequestId::Integer(2),
            method: "do".to_string(),
            params: Some(json!({})),
        },
    )
    .await
    .unwrap();

    let first: Value =
        tokio::time::timeout(std::time::Duration::from_secs(2), read_json_line(&mut read))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    let second: Value =
        tokio::time::timeout(std::time::Duration::from_secs(2), read_json_line(&mut read))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    let methods = vec![
        first
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("response"),
        second
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("response"),
    ];
    assert!(methods.contains(&"visible/event"));
    assert!(!methods.contains(&"muted/event"));
    drop(write);
    task.abort();
}
