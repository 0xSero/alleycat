//! Regression coverage for controller-scoped Pi thread configuration.

mod support;

use std::sync::Arc;

use alleycat_pi_bridge::codex_proto as p;
use alleycat_pi_bridge::handlers;
use alleycat_pi_bridge::pool::PiPool;
use alleycat_pi_bridge::state::{ConnectionState, ThreadDefaults};
use serde_json::json;
use tempfile::TempDir;

use support::fake_pi_path;
use support::thread_index_stub::NoopThreadIndex;

#[tokio::test]
async fn thread_start_applies_and_reports_controller_model_and_canonical_effort() {
    let (state, _notifications) = ConnectionState::for_test_with_model_scope(
        Arc::new(PiPool::new(fake_pi_path())),
        Arc::new(NoopThreadIndex),
        ThreadDefaults::default(),
        vec!["local-studio".to_string()],
    );
    let cwd = TempDir::new().unwrap();
    let params: p::ThreadStartParams = serde_json::from_value(json!({
        "cwd": cwd.path(),
        "model": "GLM-5.2",
        "reasoningEffort": "low"
    }))
    .unwrap();

    let response = handlers::thread::handle_thread_start(&state, params)
        .await
        .expect("thread/start");

    assert_eq!(response.model, "GLM-5.2");
    assert_eq!(response.model_provider, "local-studio");
    assert_eq!(response.reasoning_effort, Some(p::ReasoningEffort::Low));
}
