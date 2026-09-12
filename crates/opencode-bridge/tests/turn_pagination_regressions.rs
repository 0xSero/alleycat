#[path = "support/mod.rs"]
mod support;

use serde_json::{Value, json};
use support::{FakeServerState, bring_up_bridge, read_until_response, send};

async fn page_fixture(messages: Value, limit: Value) -> support::BridgeFixture {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    {
        let mut guard = state.lock().unwrap();
        guard.route(
            "GET /session",
            json!([{
                "id":"ses_page", "directory":"/tmp/page", "title":"Page",
                "time":{"created":1000,"updated":1000}
            }]),
        );
        guard.route("GET /session/ses_page/message", messages);
    }
    let mut fx = bring_up_bridge("page-regression", state).await;
    send(&mut fx.write, 2, "thread/list", json!({})).await;
    let response = read_until_response(&mut fx.read, 2).await;
    let id = response["result"]["data"][0]["id"].clone();
    send(
        &mut fx.write,
        3,
        "thread/turns/list",
        json!({"threadId":id,"limit":limit}),
    )
    .await;
    fx
}

#[tokio::test]
async fn repeated_native_page_returns_error_instead_of_looping() {
    // Older servers can accept but ignore `before`. A full assistant-only
    // window must not be appended to itself indefinitely looking for a user.
    let messages: Vec<_> = (1..=4)
        .map(|i| {
            json!({
                "info":{"id":format!("a{i}"),"role":"assistant","time":{"created":i}},
                "parts":[]
            })
        })
        .collect();
    let mut fx = page_fixture(json!(messages), json!(1)).await;
    let response = read_until_response(&mut fx.read, 3).await;
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("pagination did not advance")
    );
    assert_eq!(
        fx.seen()
            .iter()
            .filter(|line| line.contains("/message?"))
            .count(),
        2
    );
    fx.shutdown().await;
}

#[tokio::test]
async fn omitted_limit_fetches_the_user_boundary_of_a_tool_heavy_turn() {
    let messages: Vec<_> = (1..=100)
        .map(|i| {
            json!({
                "info":{"id":format!("a{i}"),"role":"assistant","time":{"created":i}},
                "parts":[]
            })
        })
        .collect();
    // The generic route ignores before; the default limit must still walk
    // the boundary and detect that failure, rather than emit a split turn.
    let mut fx = page_fixture(json!(messages), Value::Null).await;
    let response = read_until_response(&mut fx.read, 3).await;
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("pagination did not advance")
    );
    fx.shutdown().await;
}

#[tokio::test]
async fn default_page_preserves_a_turn_across_native_pages() {
    use base64::Engine;
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    let messages: Vec<_> = (1..=100)
        .map(|i| {
            json!({
                "info":{"id":format!("a{i}"),"role":"assistant","time":{"created":i}},
                "parts":[{"id":format!("p{i}"),"type":"text","text":format!("step {i}")}]
            })
        })
        .collect();
    let before = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&json!({"id":"a1","time":1})).unwrap());
    {
        let mut guard = state.lock().unwrap();
        guard.route(
            "GET /session",
            json!([{
                "id":"ses_page", "directory":"/tmp/page", "title":"Page",
                "time":{"created":1000,"updated":1000}
            }]),
        );
        guard.route("GET /session/ses_page/message", json!(messages));
        guard.route(
            format!("GET /session/ses_page/message?limit=100&before={before}"),
            json!([{
                "info":{"id":"u0","role":"user","time":{"created":0}},
                "parts":[{"id":"p0","type":"text","text":"run all steps"}]
            }]),
        );
    }
    let mut fx = bring_up_bridge("complete-page", state).await;
    send(&mut fx.write, 2, "thread/list", json!({})).await;
    let response = read_until_response(&mut fx.read, 2).await;
    let id = response["result"]["data"][0]["id"].clone();
    send(
        &mut fx.write,
        3,
        "thread/turns/list",
        json!({"threadId":id}),
    )
    .await;
    let response = read_until_response(&mut fx.read, 3).await;
    let turns = response["result"]["data"].as_array().unwrap();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0]["items"][0]["type"], "userMessage");
    assert_eq!(turns[0]["items"].as_array().unwrap().len(), 101);
    assert!(response["result"]["nextCursor"].is_null());
    fx.shutdown().await;
}

#[tokio::test]
async fn oversized_turn_limit_is_clamped_without_integer_wraparound() {
    let mut fx = page_fixture(
        json!([{
            "info":{"id":"u0","role":"user","time":{"created":0}},
            "parts":[{"id":"p0","type":"text","text":"hello"}]
        }]),
        json!(u64::MAX),
    )
    .await;
    let response = read_until_response(&mut fx.read, 3).await;
    assert_eq!(response["result"]["data"].as_array().unwrap().len(), 1);
    assert!(
        fx.seen()
            .iter()
            .any(|line| line.contains("/message?limit=400&"))
    );
    fx.shutdown().await;
}

#[tokio::test]
async fn ignored_cursor_on_a_later_request_does_not_repeat_a_completed_page() {
    let messages: Vec<_> = (1..=2)
        .flat_map(|i| {
            [
                json!({
                    "info":{"id":format!("u{i}"),"role":"user","time":{"created":i * 2}},
                    "parts":[{"id":format!("pu{i}"),"type":"text","text":"hello"}]
                }),
                json!({
                    "info":{"id":format!("a{i}"),"role":"assistant","time":{"created":i * 2 + 1}},
                    "parts":[]
                }),
            ]
        })
        .collect();
    let mut fx = page_fixture(json!(messages), json!(1)).await;
    let first = read_until_response(&mut fx.read, 3).await;
    let cursor = first["result"]["nextCursor"].as_str().unwrap();
    // Resolve the same stable binding again; the fake deliberately ignores
    // the `before` query and returns the newest page for every request.
    send(&mut fx.write, 4, "thread/list", json!({})).await;
    let listed = read_until_response(&mut fx.read, 4).await;
    let thread_id = listed["result"]["data"][0]["id"].clone();
    send(
        &mut fx.write,
        5,
        "thread/turns/list",
        json!({"threadId":thread_id,"limit":1,"cursor":cursor}),
    )
    .await;
    let second = read_until_response(&mut fx.read, 5).await;
    assert!(
        second["error"]["message"]
            .as_str()
            .unwrap()
            .contains("pagination did not advance")
    );
    fx.shutdown().await;
}
