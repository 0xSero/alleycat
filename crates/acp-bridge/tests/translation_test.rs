//! Basic tests for translation functions.

#[test]
fn test_codex_to_acp_initialize() {
    let codex_params = serde_json::json!({
        "clientInfo": {
            "name": "TestClient",
            "version": "1.0.0",
        },
        "capabilities": {
            "experimentalApi": false,
        },
    });

    let acp_request = alleycat_acp_bridge::translate::codex_to_acp_initialize(&codex_params);
    assert!(acp_request.is_ok());

    let acp_request = acp_request.unwrap();
    assert_eq!(acp_request["protocolVersion"], "1.0.0");
    assert_eq!(acp_request["clientInfo"]["name"], "TestClient");
    assert_eq!(acp_request["clientInfo"]["version"], "1.0.0");
}

#[test]
fn test_acp_to_codex_initialize_result() {
    let acp_response = serde_json::json!({
        "protocolVersion": "1.0.0",
        "agentInfo": {
            "name": "TestAgent",
            "version": "2.0.0",
        },
        "agentCapabilities": {
            "promptCapabilities": {
                "audio": false,
                "embeddedContext": false,
                "image": false,
            },
        },
    });

    let codex_result =
        alleycat_acp_bridge::translate::acp_to_codex_initialize_result(&acp_response);
    assert!(codex_result.is_ok());

    let codex_result = codex_result.unwrap();
    assert_eq!(
        codex_result["userAgent"],
        "alleycat-acp-bridge/0.1.0 (TestAgent 2.0.0)"
    );
}

#[test]
fn test_codex_to_acp_new_session() {
    let codex_params = serde_json::json!({
        "cwd": "/home/user/project",
    });

    let acp_request = alleycat_acp_bridge::translate::codex_to_acp_new_session(&codex_params);
    assert!(acp_request.is_ok());

    let acp_request = acp_request.unwrap();
    assert_eq!(acp_request["cwd"], "/home/user/project");
}

#[test]
fn test_acp_to_codex_thread_start() {
    let acp_response = serde_json::json!({
        "sessionId": "test-session-123",
    });

    let codex_response = alleycat_acp_bridge::translate::acp_to_codex_thread_start(&acp_response);
    assert!(codex_response.is_ok());

    let codex_response = codex_response.unwrap();
    assert_eq!(codex_response["threadId"], "test-session-123");
}
