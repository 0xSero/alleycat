//! V10 — `thread/list` honors the full codex `ThreadListParams` shape.
//!
//! Seeds three opencode sessions over the fake server and asserts each of the
//! schema-described filters works:
//!
//! - `archived` (omitted/false → only non-archived; true → only archived)
//! - `cwd` (string and array forms)
//! - `searchTerm` (substring match on session title/preview)
//! - `sortKey` + `sortDirection` (defaults: created_at desc)
//! - `cursor` + `limit` (forward pagination)
//! - `modelProviders` (only "opencode" matches)
//! - `sourceKinds` (only "appServer" matches)
//! - `useStateDbOnly` (no-op for opencode; accepted)
//!
//! The fake `GET /session` returns the full canned list regardless of query
//! string; the bridge does the rest of the filter/sort/pagination locally,
//! so the assertions pin the bridge's logic, not opencode's.

#[path = "support/mod.rs"]
mod support;

use serde_json::{Value, json};
use support::{FakeServerState, bring_up_bridge, read_until_response, send};

fn ses(id: &str, dir: &str, title: &str, created: i64, archived: Option<i64>) -> Value {
    let mut time = json!({"created": created, "updated": created});
    if let Some(at) = archived {
        time["archived"] = json!(at);
    }
    json!({
        "id": id,
        "directory": dir,
        "title": title,
        "time": time,
    })
}

/// Seed the fake server with three sessions. Each test reroutes
/// `GET /session` (and `GET /session?...`) to this same array — the bridge
/// post-filters locally.
fn seed_three(state: &std::sync::Arc<std::sync::Mutex<FakeServerState>>) {
    let body = json!([
        ses("ses_a", "/tmp/v10/a", "alpha tutorial", 1_000, None),
        ses("ses_b", "/tmp/v10/b", "beta walkthrough", 2_000, None),
        ses("ses_c", "/tmp/v10/c", "gamma demo", 3_000, Some(9_999)),
    ]);
    let mut guard = state.lock().unwrap();
    guard.route("GET /session?", body.clone());
    guard.route("GET /session ", body.clone());
    guard.route("GET /session\n", body);
}

async fn list(fx: &mut support::BridgeFixture, id: i64, params: Value) -> Value {
    send(&mut fx.write, id, "thread/list", params).await;
    read_until_response(&mut fx.read, id).await
}

#[tokio::test]
async fn archived_default_excludes_archived_sessions() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-arch-default", state.clone()).await;

    // No `archived` field → schema/codex default is false → only
    // non-archived sessions returned.
    let resp = list(&mut fx, 2, json!({})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    let titles: Vec<&str> = data.iter().filter_map(|t| t["name"].as_str()).collect();
    // Only ses_a and ses_b are non-archived. ses_c has time.archived set.
    assert!(
        titles.iter().all(|t| *t != "gamma demo"),
        "leaked archived: {titles:?}"
    );
    assert_eq!(data.len(), 2, "{data:#?}");

    fx.shutdown().await;
}

#[tokio::test]
async fn archived_true_returns_only_archived() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-arch-true", state.clone()).await;

    let resp = list(&mut fx, 2, json!({"archived": true})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["name"], "gamma demo");

    fx.shutdown().await;
}

#[tokio::test]
async fn cwd_array_returns_subset() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-cwd-array", state.clone()).await;

    let resp = list(&mut fx, 2, json!({"cwd": ["/tmp/v10/a", "/tmp/v10/b"]})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    let cwds: Vec<&str> = data.iter().filter_map(|t| t["cwd"].as_str()).collect();
    assert_eq!(data.len(), 2, "got {cwds:?}");
    assert!(cwds.contains(&"/tmp/v10/a"));
    assert!(cwds.contains(&"/tmp/v10/b"));

    fx.shutdown().await;
}

#[tokio::test]
async fn search_term_filters_on_title() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-search", state.clone()).await;

    let resp = list(&mut fx, 2, json!({"searchTerm": "tutorial"})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["name"], "alpha tutorial");

    fx.shutdown().await;
}

#[tokio::test]
async fn sort_default_is_created_at_desc() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-sort-default", state.clone()).await;

    // archived defaults to false so ses_c (archived) drops; the remaining two
    // should come back newest-first by createdAt.
    let resp = list(&mut fx, 2, json!({})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    let names: Vec<&str> = data.iter().filter_map(|t| t["name"].as_str()).collect();
    assert_eq!(names, vec!["beta walkthrough", "alpha tutorial"]);

    fx.shutdown().await;
}

#[tokio::test]
async fn sort_asc_reverses_order() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-sort-asc", state.clone()).await;

    let resp = list(&mut fx, 2, json!({"sortDirection": "asc"})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    let names: Vec<&str> = data.iter().filter_map(|t| t["name"].as_str()).collect();
    assert_eq!(names, vec!["alpha tutorial", "beta walkthrough"]);

    fx.shutdown().await;
}

#[tokio::test]
async fn limit_and_cursor_paginate_forward() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-cursor", state.clone()).await;

    // Page 1: limit=1, default sort (created_at desc) → newest non-archived.
    let page1 = list(&mut fx, 2, json!({"limit": 1})).await;
    let data1 = page1["result"]["data"].as_array().expect("data");
    assert_eq!(data1.len(), 1);
    assert_eq!(data1[0]["name"], "beta walkthrough");
    let next = page1["result"]["nextCursor"]
        .as_str()
        .expect("nextCursor present mid-paging")
        .to_string();
    let backwards = page1["result"]["backwardsCursor"]
        .as_str()
        .expect("backwardsCursor present");
    assert!(!backwards.is_empty(), "backwardsCursor non-empty");

    // Page 2: feed nextCursor → second non-archived entry, no further nextCursor.
    let page2 = list(&mut fx, 3, json!({"limit": 1, "cursor": next})).await;
    let data2 = page2["result"]["data"].as_array().expect("data");
    assert_eq!(data2.len(), 1);
    assert_eq!(data2[0]["name"], "alpha tutorial");
    assert!(
        page2["result"]["nextCursor"].is_null(),
        "should be last page"
    );

    fx.shutdown().await;
}

#[tokio::test]
async fn model_providers_filter_drops_non_opencode() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-providers", state.clone()).await;

    // "openai" is not the bridge's emitted modelProvider — drops everything.
    let resp = list(&mut fx, 2, json!({"modelProviders": ["openai"]})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert!(data.is_empty(), "{data:#?}");

    // "opencode" is the bridge's tag — keeps both non-archived entries.
    let resp = list(&mut fx, 3, json!({"modelProviders": ["opencode"]})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert_eq!(data.len(), 2);

    fx.shutdown().await;
}

#[tokio::test]
async fn source_kinds_filter_excludes_non_app_server() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-sources", state.clone()).await;

    // "cli" is not opencode's emitted source — drops everything.
    let resp = list(&mut fx, 2, json!({"sourceKinds": ["cli"]})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert!(data.is_empty());

    // "appServer" matches.
    let resp = list(&mut fx, 3, json!({"sourceKinds": ["appServer"]})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    assert_eq!(data.len(), 2);

    fx.shutdown().await;
}

#[tokio::test]
async fn use_state_db_only_is_accepted_no_op() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    let mut fx = bring_up_bridge("v10-state-db-only", state.clone()).await;

    // Both true and false should produce identical results (param is a no-op
    // for opencode — the upstream HTTP API is the only state store).
    let resp_t = list(&mut fx, 2, json!({"useStateDbOnly": true})).await;
    let resp_f = list(&mut fx, 3, json!({"useStateDbOnly": false})).await;
    assert_eq!(
        resp_t["result"]["data"], resp_f["result"]["data"],
        "useStateDbOnly should be a no-op",
    );

    fx.shutdown().await;
}

/// Subagent/agent sessions (opencode sessions with a `parentID`) are
/// conversation-scoped, not top-level sessions. The bridge asks opencode for
/// `parentID=null` so newer builds exclude them at the source, and drops any
/// that still come back — stable releases (1.18.x) ignore the query filter
/// but still tag subagent sessions with `parentID`.
#[tokio::test]
async fn session_list_requests_parentid_null_and_drops_subagents() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    let body = json!([
        ses("ses_root_a", "/tmp/v10a", "top-level a", 1_000, None),
        json!({
            "id": "ses_sub_b",
            "directory": "/tmp/v10a",
            "title": "explore things (@explore subagent)",
            "parentID": "ses_root_a",
            "time": {"created": 1_100, "updated": 1_100},
        }),
    ]);
    {
        let mut guard = state.lock().unwrap();
        guard.route("GET /session?", body.clone());
        guard.route("GET /session ", body.clone());
    }
    let mut fx = bring_up_bridge("v10-subagents", state.clone()).await;

    let resp = list(&mut fx, 2, json!({})).await;
    let data = resp["result"]["data"].as_array().expect("data");
    let names: Vec<&str> = data.iter().filter_map(|t| t["name"].as_str()).collect();
    assert_eq!(
        names,
        vec!["top-level a"],
        "subagent session must not appear in the session list: {names:?}"
    );

    // The bridge must also request only top-level sessions upstream so
    // newer opencode builds exclude them at the source.
    let seen = fx.seen();
    assert!(
        seen.iter().any(|line| line.contains("parentID=null")),
        "expected parentID=null in upstream session requests: {seen:?}"
    );

    fx.shutdown().await;
}

#[tokio::test]
async fn failed_worktree_scan_does_not_report_an_authoritative_partial_list() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    seed_three(&state);
    {
        let mut guard = state.lock().unwrap();
        guard.route("GET /project", json!([{"worktree":"/other"}]));
        guard.route("GET /session?directory=", json!("__server_error__"));
    }
    let mut fx = bring_up_bridge("v10-failed-worktree", state).await;
    let response = list(&mut fx, 2, json!({})).await;
    assert!(
        response.get("error").is_some(),
        "partial success would let clients prune missing worktree sessions: {response}"
    );
    fx.shutdown().await;
}
