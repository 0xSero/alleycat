//! V11 — OpenCode catalog and loaded-thread compatibility.
//!
//! Pins two client-visible shapes that the phone relies on:
//!
//! - modern OpenCode serves `models` as an object map, not an array;
//! - `thread/loaded/list` uses the codex `{ data, nextCursor }` list shape.

#[path = "support/mod.rs"]
mod support;

use serde_json::json;
use support::{FakeServerState, bring_up_bridge, read_until_response, send};

#[tokio::test]
async fn model_list_reads_object_catalog_from_config_providers() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    state.lock().unwrap().route(
        "GET /config/providers",
        json!({
            "providers": [{
                "id": "lmstudio",
                "name": "LM Studio",
                "models": {
                    "qwen/qwen3.5-35b-a3b": {
                        "id": "qwen/qwen3.5-35b-a3b",
                        "name": "Qwen 3.5 35B",
                        "description": "Local model",
                        "variants": {"high":{}, "max":{}, "research":{}, "disabled-choice":{"disabled":true}},
                        "capabilities": {
                            "reasoning": true,
                            "input": {
                                "text": true,
                                "image": true
                            }
                        }
                    }
                }
            }],
            "default": {
                "lmstudio": "qwen/qwen3.5-35b-a3b"
            }
        }),
    );
    let mut fx = bring_up_bridge("v11-model-config", state.clone()).await;

    send(&mut fx.write, 2, "model/list", json!({})).await;
    let resp = read_until_response(&mut fx.read, 2).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1, "{data:#?}");
    assert_eq!(data[0]["id"], "lmstudio/qwen/qwen3.5-35b-a3b");
    assert_eq!(data[0]["model"], "lmstudio/qwen/qwen3.5-35b-a3b");
    assert_eq!(data[0]["displayName"], "Qwen 3.5 35B");
    assert_eq!(data[0]["description"], "Local model");
    assert_eq!(data[0]["inputModalities"], json!(["text", "image"]));
    assert_eq!(data[0]["isDefault"], true);
    assert!(
        data[0]["supportedReasoningEfforts"]
            .as_array()
            .expect("reasoning efforts")
            .iter()
            .any(|effort| effort["reasoningEffort"] == "high"),
        "{:#?}",
        data[0]["supportedReasoningEfforts"]
    );

    let efforts = data[0]["supportedReasoningEfforts"].as_array().unwrap();
    assert_eq!(efforts.len(), 3);
    assert!(efforts.iter().any(|e| e["reasoningEffort"] == "research"));
    assert!(!efforts.iter().any(|e| e["reasoningEffort"] == "minimal"));
    // Refresh replaces old models rather than accumulating stale entries.
    state.lock().unwrap().route("GET /config/providers", json!({"providers":[{"id":"lmstudio","models":{"new-model":{"id":"new-model"}}}]}));
    send(&mut fx.write, 3, "model/list", json!({})).await;
    let refreshed = read_until_response(&mut fx.read, 3).await;
    assert_eq!(refreshed["result"]["data"].as_array().unwrap().len(), 1);
    assert_eq!(refreshed["result"]["data"][0]["model"], "lmstudio/new-model");
    assert_eq!(refreshed["result"]["data"][0]["supportedReasoningEfforts"], json!([]));

    fx.shutdown().await;
}

#[tokio::test]
async fn model_list_failure_does_not_replace_catalog_with_fallback() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    state.lock().unwrap().route("GET /config/providers", json!("__server_error__"));
    state.lock().unwrap().route(
        "GET /provider",
        json!({
            "all": [{
                "id": "opencode",
                "name": "OpenCode",
                "models": {
                    "big-pickle": {
                        "id": "big-pickle",
                        "name": "Big Pickle",
                        "capabilities": {
                            "reasoning": false,
                            "input": {
                                "text": true,
                                "image": false
                            }
                        }
                    }
                }
            }]
        }),
    );
    let mut fx = bring_up_bridge("v11-model-provider", state.clone()).await;

    send(&mut fx.write, 2, "model/list", json!({})).await;
    let resp = read_until_response(&mut fx.read, 2).await;
    assert!(resp.get("error").is_some(), "{resp:#?}");
    assert!(resp.get("result").is_none());
    assert!(!resp.to_string().contains("test-token"));
    assert!(!fx.seen().iter().any(|path| path.starts_with("GET /provider")));

    fx.shutdown().await;
}

#[tokio::test]
async fn thread_loaded_list_returns_codex_list_shape() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    let mut fx = bring_up_bridge("v11-loaded-list", state.clone()).await;

    send(&mut fx.write, 2, "thread/loaded/list", json!({})).await;
    let resp = read_until_response(&mut fx.read, 2).await;

    assert_eq!(resp["result"]["data"], json!([]));
    assert!(resp["result"]["nextCursor"].is_null());
    assert!(resp["result"].get("threadIds").is_none(), "{resp:#?}");

    fx.shutdown().await;
}

#[tokio::test]
async fn turn_forwards_provider_model_and_native_variant() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    state.lock().unwrap().route("POST /session", json!({"id":"ses_1"}));
    state.lock().unwrap().route("POST /session/ses_1/prompt_async", json!("__no_content__"));
    let mut fx = bring_up_bridge("variant-routing", state).await;
    let thread_id = fx.start_thread("/tmp/native-variant").await;
    send(&mut fx.write, 3, "turn/start", json!({"threadId":thread_id,"model":"provider/vendor/model","effort":"research","input":[{"type":"text","text":"hello"}]})).await;
    let response = read_until_response(&mut fx.read, 3).await;
    assert!(response.get("error").is_none(), "{response:#?}");
    let body = fx.captured_body("POST /session/ses_1/prompt_async").unwrap();
    assert_eq!(body["model"], json!({"providerID":"provider","modelID":"vendor/model"}));
    assert_eq!(body["variant"], "research");
    for (catalog_id, turn_id, advertised) in [(4, 5, false), (6, 7, true)] {
        let variants = if advertised { json!({"none":{}}) } else { json!({"high":{}}) };
        fx.state.lock().unwrap().route("GET /config/providers", json!({"providers":[{"id":"provider","models":{"vendor/model":{"variants":variants}}}]}));
        send(&mut fx.write, catalog_id, "model/list", json!({})).await;
        assert!(read_until_response(&mut fx.read, catalog_id).await.get("error").is_none());
        send(&mut fx.write, turn_id, "turn/start", json!({"threadId":thread_id,"model":"provider/vendor/model","effort":"none","input":[]})).await;
        assert!(read_until_response(&mut fx.read, turn_id).await.get("error").is_none());
        let body = fx.captured_body("POST /session/ses_1/prompt_async").unwrap();
        assert_eq!(body.get("variant"), advertised.then_some(&json!("none")));
    }
    fx.shutdown().await;
}
