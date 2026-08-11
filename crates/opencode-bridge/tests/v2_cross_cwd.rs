//! T14 / V2 — cross-cwd `thread/start` and `thread/list` separation.
//!
//! Two `thread/start { cwd:/tmp/a }` and `thread/start { cwd:/tmp/b }` round
//! trip cleanly, each binding a distinct opencode session id. A subsequent
//! `thread/list { cwd:/tmp/a }` returns only the threads under `/tmp/a`.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use serde_json::{Value, json};
use support::{FakeServerState, await_captured_body, bring_up_bridge, read_until_response, send};

#[tokio::test]
async fn two_threads_in_different_cwds_remain_separate() {
    // Each `thread/start` POSTs `/session?…`; the fake returns a different id
    // depending on which cwd the request body asks for.
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    {
        let mut guard = state.lock().unwrap();
        // First create_session call gets ses_a; we'll swap to ses_b before the
        // second `thread/start` to give it a distinct binding.
        guard.route(
            "POST /session?",
            json!({
                "id":"ses_a",
                "directory":"/tmp/a",
                "title":"V2-A",
                "time":{"created":1_000,"updated":1_000}
            }),
        );
        guard.route(
            "GET /session",
            json!([
                {
                    "id":"ses_a",
                    "directory":"/tmp/a",
                    "title":"V2-A",
                    "time":{"created":1_000,"updated":1_000}
                },
                {
                    "id":"ses_b",
                    "directory":"/tmp/b",
                    "title":"V2-B",
                    "time":{"created":1_001,"updated":1_001}
                }
            ]),
        );
    }

    let mut fx = bring_up_bridge("v2", state.clone()).await;

    // First thread under /tmp/a.
    send(
        &mut fx.write,
        2,
        "thread/start",
        json!({"cwd":"/tmp/a","model":"openai/gpt-test"}),
    )
    .await;
    let started_a = read_until_response(&mut fx.read, 2).await;
    let thread_a = started_a["result"]["thread"]["id"]
        .as_str()
        .expect("thread a id")
        .to_string();
    assert_eq!(started_a["result"]["cwd"], "/tmp/a");

    // Swap the canned `POST /session` body so the second start binds ses_b.
    state.lock().unwrap().route(
        "POST /session?",
        json!({
            "id":"ses_b",
            "directory":"/tmp/b",
            "title":"V2-B",
            "time":{"created":1_001,"updated":1_001}
        }),
    );

    // Second thread under /tmp/b.
    send(
        &mut fx.write,
        3,
        "thread/start",
        json!({"cwd":"/tmp/b","model":"openai/gpt-test"}),
    )
    .await;
    let started_b = read_until_response(&mut fx.read, 3).await;
    let thread_b = started_b["result"]["thread"]["id"]
        .as_str()
        .expect("thread b id")
        .to_string();
    assert_eq!(started_b["result"]["cwd"], "/tmp/b");
    assert_ne!(
        thread_a, thread_b,
        "distinct cwds must mint distinct threads"
    );

    // thread/list { cwd:/tmp/a } → only ses_a appears.
    send(&mut fx.write, 4, "thread/list", json!({"cwd":"/tmp/a"})).await;
    let list_a = read_until_response(&mut fx.read, 4).await;
    let data_a = list_a["result"]["data"].as_array().expect("data array");
    assert_eq!(data_a.len(), 1, "only one thread under /tmp/a: {data_a:?}");
    assert_eq!(data_a[0]["id"].as_str(), Some(thread_a.as_str()));
    assert_eq!(data_a[0]["cwd"], "/tmp/a");

    // The /tmp/a list filter must not bleed in /tmp/b's session.
    let bodies = data_a
        .iter()
        .filter_map(|t| t["cwd"].as_str())
        .collect::<Vec<_>>();
    assert!(
        bodies.iter().all(|cwd| *cwd == "/tmp/a"),
        "list /tmp/a leaked other cwds: {bodies:?}"
    );

    // /tmp/b list filter likewise narrows.
    send(&mut fx.write, 5, "thread/list", json!({"cwd":"/tmp/b"})).await;
    let list_b = read_until_response(&mut fx.read, 5).await;
    let data_b = list_b["result"]["data"].as_array().expect("data array");
    assert_eq!(data_b.len(), 1);
    assert_eq!(data_b[0]["id"].as_str(), Some(thread_b.as_str()));
    assert_eq!(data_b[0]["cwd"], "/tmp/b");

    // Codex cwd overrides live in the bridge index and may differ from the
    // directory opencode persisted. The bridge must fetch the collection
    // without an upstream directory filter, then apply the filter locally.
    let seen = fx.seen();
    assert!(
        seen.iter()
            .filter(|line| line.starts_with("GET /session?"))
            .count()
            >= 2,
        "expected collection fetches for both thread/list calls: {seen:?}"
    );
    assert!(
        seen.iter().all(|line| !line.contains("directory=")),
        "thread/list must not forward Codex cwd overrides upstream: {seen:?}"
    );

    // Drain any queued response bodies just to keep the mutex's `bodies` map
    // honest in case future assertions rely on its contents — best-effort.
    let _: Value =
        await_captured_body(&fx.state, "POST /session?", Duration::from_millis(200)).await;
    fx.shutdown().await;
}
