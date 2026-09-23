//! Approval-policy bridging.
//!
//! Codex clients can configure an `approval_policy` (`untrusted`,
//! `on-failure`, `on-request`, `granular`, `never`) per thread or turn.
//! When a policy demands user approval, codex's app-server sends a
//! `item/commandExecution/requestApproval` (or `item/fileChange/...`)
//! request to the client, awaits the user's `decision`, and only then lets
//! the underlying tool call proceed.
//!
//! This module exposes the **request/response half** of that flow as
//! standalone async helpers. The pump that intercepts pi's
//! `tool_execution_start` and decides *whether* to call into these
//! helpers lives in `handlers/turn.rs` (#15) — wiring it there keeps the
//! per-turn bookkeeping (active turn id, EventTranslatorState, item
//! buffering) in one place.
//!
//! ## Known limitation: post-hoc approval
//!
//! Pi's `agent-session` emits `tool_execution_start` *after* the tool
//! dispatcher has already begun running the bash subprocess (or applying
//! the file patch). The bridge cannot prevent the side effect; it can
//! only choose whether to forward the resulting item to the codex client
//! and, on a deny/cancel decision, send pi `abort` to interrupt the
//! pending continuation. True pre-execution approval requires a pi
//! extension that hooks the dispatcher itself; see the plan's
//! "Out of scope" section. Treat the helpers below as "after-the-fact
//! gating" — they keep secrets out of the codex transcript on a deny but
//! cannot un-execute side effects.
//!
//! ## Server→client request envelope
//!
//! Codex's wire shape: every approval request is a JSON-RPC *request*
//! frame the bridge sends on the outbound channel. The matching response
//! is a regular JSON-RPC response with the same `id`. Foundations'
//! `state::ConnectionState::register_pending_request` returns a
//! `oneshot::Receiver` resolving to the client's `result` JSON, which the
//! main dispatch loop already routes via `resolve_pending_request`.
//!
//! ## extension_ui_response back-channel (turn pump only)
//!
//! [`request_user_input`] only handles the **codex** half: codex client's
//! answer comes back as a `Result<HashMap<...>, ApprovalError>`. The pi
//! half — forwarding that answer back to pi via
//! `RpcCommand::ExtensionUiResponse` — is the caller's job. The turn pump
//! (#15) holds an `Arc<PiProcessHandle>` for the active thread anyway, so
//! the pattern there is:
//!
//! ```ignore
//! let answers = approval::request_user_input(state, ..., questions, None).await?;
//! // pi's `id` for the original extension_ui_request is what we
//! // correlate the response against — turn pump remembers it from the
//! // event.
//! handle.send_notification(&pi::RpcCommand::ExtensionUiResponse(
//!     pi::ExtensionUiResponse::Value { id: pi_request_id, value: answers["q1"].answers[0].clone() },
//! ))?;
//! ```
//!
//! This module deliberately does *not* take a pool/handle parameter so it
//! stays a pure codex-side utility; the turn pump owns the back-channel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use thiserror::Error;

use crate::codex_proto::common::AskForApproval;
use crate::codex_proto::jsonrpc::{JsonRpcMessage, JsonRpcRequest, JsonRpcVersion, RequestId};
use crate::codex_proto::notifications::{
    CommandExecutionRequestApprovalParams, FileChangeRequestApprovalParams,
    ToolRequestUserInputAnswer, ToolRequestUserInputParams, ToolRequestUserInputQuestion,
    ToolRequestUserInputResponse,
};
use crate::state::{ConnectionState, ServerRequestError};

/// Default time the bridge waits for a client approval before timing out
/// and treating the prompt as a `Decline`. 5 minutes mirrors the pi
/// extension UI timeout used for the `select` flow (`rpc-mode.ts:107`).
pub const DEFAULT_APPROVAL_TIMEOUT: Duration = Duration::from_secs(5 * 60);

// ============================================================================
// Approval policy gating
// ============================================================================

/// Should the bridge ask the codex client for approval before forwarding a
/// tool-call item with the given `kind`? Driven by the active
/// [`AskForApproval`] policy configured on the thread or turn.
///
/// `untrusted` and `on-request` always prompt; `on-failure` prompts only
/// after a failed run (the bridge never reaches this helper for the
/// success path); `never` never prompts; `granular` is opaque to the
/// bridge — currently treated as "always prompt" (conservative default).
pub fn should_request_approval(policy: &AskForApproval, kind: ApprovalKind) -> bool {
    match (policy, kind) {
        (AskForApproval::Never, _) => false,
        (AskForApproval::OnFailure, ApprovalKind::Command) => false,
        (AskForApproval::OnFailure, ApprovalKind::FileChange) => false,
        (AskForApproval::OnRequest, _) => true,
        (AskForApproval::UnlessTrusted, _) => true,
        // Granular is a tagged-object policy with rules the bridge can't
        // evaluate; default to prompting so we never silently approve.
        (AskForApproval::Granular(_), _) => true,
    }
}

/// What kind of action the user is being asked to approve. Used by
/// [`should_request_approval`] to pick the right policy branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalKind {
    /// `bash`-style command execution.
    Command,
    /// `apply_patch` / `write_file` / `edit_file`.
    FileChange,
}

// ============================================================================
// Bucketed decisions
// ============================================================================

/// Bucketed view of codex's per-decision-type enums. The bridge only needs
/// to know the user's intent: keep going, stop this tool call, or
/// interrupt the whole turn. Variants beyond Approve/Deny/Cancel
/// (`AcceptForSession`, `AcceptWithExecpolicyAmendment`, etc.) bucket as
/// `Approved` because pi has no concept of session-cached approvals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalOutcome {
    /// User approved — the bridge forwards the item and lets pi continue.
    Approved,
    /// User declined — the bridge marks the item failed but lets pi
    /// continue the turn (matches codex `Decline` semantics).
    Declined,
    /// User cancelled — the bridge marks the item failed *and* sends pi
    /// `abort` to interrupt the turn (matches codex `Cancel`).
    Cancelled,
}

impl ApprovalOutcome {
    /// Did the user authorize the underlying tool call to take effect?
    pub fn is_approved(&self) -> bool {
        matches!(self, Self::Approved)
    }

    /// Should the bridge interrupt the rest of the turn after this
    /// decision lands?
    pub fn should_abort_turn(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

/// Bucket a codex `CommandExecutionApprovalDecision` (carried as opaque
/// JSON in our `notifications::CommandExecutionRequestApprovalResponse`)
/// into an [`ApprovalOutcome`].
fn bucket_command_decision(value: &Value) -> ApprovalOutcome {
    bucket_decision_value(value, false)
}

/// Same as [`bucket_command_decision`] but for file-change decisions.
/// Codex's `FileChangeApprovalDecision` enum is a strict subset of the
/// command decision enum, so the bucketing rules collapse to the same
/// match.
fn bucket_file_change_decision(value: &Value) -> ApprovalOutcome {
    bucket_decision_value(value, true)
}

fn bucket_decision_value(value: &Value, _file_change: bool) -> ApprovalOutcome {
    // Codex serializes the decision as either a bare string (`"accept"`,
    // `"decline"`, `"cancel"`, ...) for the unit variants or a tagged
    // object for the carrying variants. Both cases live under camelCase
    // names per codex's `#[serde(rename_all = "camelCase")]`.
    let tag = match value {
        Value::String(s) => s.as_str(),
        Value::Object(m) => {
            // Tagged variant — find the single key that names the variant.
            // Defensive: codex actually uses externally-tagged enums
            // (`{ "acceptWithExecpolicyAmendment": { ... } }`) without a
            // `type` field, so we look for the first object key.
            match m.keys().next() {
                Some(k) => k.as_str(),
                None => return ApprovalOutcome::Declined,
            }
        }
        _ => return ApprovalOutcome::Declined,
    };

    match tag {
        "accept"
        | "acceptForSession"
        | "acceptWithExecpolicyAmendment"
        | "applyNetworkPolicyAmendment" => ApprovalOutcome::Approved,
        "decline" => ApprovalOutcome::Declined,
        "cancel" => ApprovalOutcome::Cancelled,
        // Forward-compat: unknown decision strings bucket as `Declined`
        // (safer default than approving) and we log so future codex
        // additions surface in the bridge's logs.
        other => {
            tracing::warn!(
                decision = %other,
                "unknown codex approval decision tag — treating as decline"
            );
            ApprovalOutcome::Declined
        }
    }
}

// ============================================================================
// Server→client request helpers
// ============================================================================

/// Errors a server→client approval/elicitation request can yield.
#[derive(Debug, Error)]
pub enum ApprovalError {
    /// The local writer task is gone (codex client disconnected).
    #[error("connection to codex client closed before approval landed")]
    ConnectionClosed,
    /// We waited [`DEFAULT_APPROVAL_TIMEOUT`] (or the caller's override)
    /// and the client did not answer.
    #[error("approval request timed out after {0:?}")]
    Timeout(Duration),
    /// The codex client returned a JSON-RPC error frame for our request.
    #[error("codex client returned error {code}: {message}")]
    Rpc { code: i64, message: String },
    /// The client's response shape didn't match what we asked for.
    #[error("malformed approval response: {0}")]
    Malformed(String),
}

impl From<ServerRequestError> for ApprovalError {
    fn from(value: ServerRequestError) -> Self {
        match value {
            ServerRequestError::Rpc { code, message } => Self::Rpc { code, message },
            ServerRequestError::ConnectionClosed => Self::ConnectionClosed,
            ServerRequestError::TimedOut => Self::Timeout(DEFAULT_APPROVAL_TIMEOUT),
        }
    }
}

/// Send `item/commandExecution/requestApproval` to the codex client and
/// await the bucketed decision. Caller picks `timeout` — pass `None` to
/// use [`DEFAULT_APPROVAL_TIMEOUT`].
pub async fn request_command_approval(
    state: &Arc<ConnectionState>,
    params: CommandExecutionRequestApprovalParams,
    timeout: Option<Duration>,
) -> Result<ApprovalOutcome, ApprovalError> {
    let value = send_server_request(
        state,
        "item/commandExecution/requestApproval",
        serde_json::to_value(&params)
            .map_err(|e| ApprovalError::Malformed(format!("encode params: {e}")))?,
        timeout,
    )
    .await?;
    let decision = value
        .get("decision")
        .ok_or_else(|| ApprovalError::Malformed("response missing `decision`".into()))?;
    Ok(bucket_command_decision(decision))
}

/// Send `item/fileChange/requestApproval` to the codex client and await
/// the bucketed decision.
pub async fn request_file_change_approval(
    state: &Arc<ConnectionState>,
    params: FileChangeRequestApprovalParams,
    timeout: Option<Duration>,
) -> Result<ApprovalOutcome, ApprovalError> {
    let value = send_server_request(
        state,
        "item/fileChange/requestApproval",
        serde_json::to_value(&params)
            .map_err(|e| ApprovalError::Malformed(format!("encode params: {e}")))?,
        timeout,
    )
    .await?;
    let decision = value
        .get("decision")
        .ok_or_else(|| ApprovalError::Malformed("response missing `decision`".into()))?;
    Ok(bucket_file_change_decision(decision))
}

/// Translate a pi `extension_ui_request` into codex's
/// `item/tool/requestUserInput` and forward it to the client. The
/// resulting answers are returned as the codex map shape; the caller
/// (handlers/turn.rs) is responsible for translating each `answers[id]`
/// back into a pi `extension_ui_response` payload.
pub async fn request_user_input(
    state: &Arc<ConnectionState>,
    thread_id: String,
    turn_id: String,
    item_id: String,
    questions: Vec<ToolRequestUserInputQuestion>,
    timeout: Option<Duration>,
) -> Result<HashMap<String, ToolRequestUserInputAnswer>, ApprovalError> {
    let params = ToolRequestUserInputParams {
        thread_id,
        turn_id,
        item_id,
        questions,
    };
    let value = send_server_request(
        state,
        "item/tool/requestUserInput",
        serde_json::to_value(&params)
            .map_err(|e| ApprovalError::Malformed(format!("encode params: {e}")))?,
        timeout,
    )
    .await?;
    let resp: ToolRequestUserInputResponse = serde_json::from_value(value)
        .map_err(|e| ApprovalError::Malformed(format!("decode response: {e}")))?;
    Ok(resp.answers)
}

/// Low-level helper: register a pending id, send the request, await the
/// response under a deadline, clean up on timeout. Returns the raw
/// `result` JSON. Callers downcast to a typed payload.
pub async fn send_server_request(
    state: &Arc<ConnectionState>,
    method: &str,
    params: Value,
    timeout: Option<Duration>,
) -> Result<Value, ApprovalError> {
    // Mint a session-scoped id so reattach-replay can rebuild the
    // outstanding-prompt set deterministically. Falls back to a UUID
    // in tests where the session disambiguator is the default empty one.
    let req_id_str = state.session().next_request_id();
    let req_id = RequestId::String(req_id_str);
    let rx = state
        .register_pending_request(req_id.clone(), method.to_string(), params.clone())
        .await;

    let frame = JsonRpcMessage::Request(JsonRpcRequest {
        jsonrpc: JsonRpcVersion,
        id: req_id.clone(),
        method: method.to_string(),
        params: Some(params),
    });
    state
        .send(frame)
        .map_err(|_| ApprovalError::ConnectionClosed)?;

    let dur = timeout.unwrap_or(DEFAULT_APPROVAL_TIMEOUT);
    let outcome = tokio::time::timeout(dur, rx).await;
    match outcome {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(e))) => Err(e.into()),
        Ok(Err(_recv)) => Err(ApprovalError::ConnectionClosed),
        Err(_elapsed) => {
            // Reclaim the slot so a late client response doesn't leak into
            // a future request that reuses the (uuid'd, but be defensive)
            // table entry.
            state
                .resolve_pending_request(&req_id, Err(ServerRequestError::TimedOut))
                .await;
            Err(ApprovalError::Timeout(dur))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::mpsc;

    fn dummy_state() -> (
        Arc<ConnectionState>,
        mpsc::UnboundedReceiver<alleycat_bridge_core::session::Sequenced>,
    ) {
        ConnectionState::for_test(
            Arc::new(crate::pool::PiPool::new("/dev/null")),
            Arc::new(crate::index::testing::NoopThreadIndex),
            Default::default(),
        )
    }

    #[test]
    fn should_request_approval_table() {
        use ApprovalKind::*;
        for kind in [Command, FileChange] {
            assert!(should_request_approval(&AskForApproval::OnRequest, kind));
            assert!(should_request_approval(
                &AskForApproval::UnlessTrusted,
                kind
            ));
            assert!(!should_request_approval(&AskForApproval::Never, kind));
            assert!(!should_request_approval(&AskForApproval::OnFailure, kind));
            assert!(should_request_approval(
                &AskForApproval::Granular(json!({"any": "rules"})),
                kind
            ));
        }
    }

    #[test]
    fn bucket_command_decision_string_variants() {
        assert_eq!(
            bucket_command_decision(&json!("accept")),
            ApprovalOutcome::Approved
        );
        assert_eq!(
            bucket_command_decision(&json!("acceptForSession")),
            ApprovalOutcome::Approved
        );
        assert_eq!(
            bucket_command_decision(&json!("decline")),
            ApprovalOutcome::Declined
        );
        assert_eq!(
            bucket_command_decision(&json!("cancel")),
            ApprovalOutcome::Cancelled
        );
        assert_eq!(
            bucket_command_decision(&json!("inventedFutureVariant")),
            ApprovalOutcome::Declined
        );
    }

    #[test]
    fn bucket_command_decision_object_variants() {
        let amendment = json!({
            "acceptWithExecpolicyAmendment": { "execpolicy_amendment": {} }
        });
        assert_eq!(
            bucket_command_decision(&amendment),
            ApprovalOutcome::Approved
        );
        let net = json!({
            "applyNetworkPolicyAmendment": { "network_policy_amendment": {} }
        });
        assert_eq!(bucket_command_decision(&net), ApprovalOutcome::Approved);
    }

    #[test]
    fn approval_outcome_helpers() {
        assert!(ApprovalOutcome::Approved.is_approved());
        assert!(!ApprovalOutcome::Approved.should_abort_turn());
        assert!(!ApprovalOutcome::Declined.is_approved());
        assert!(!ApprovalOutcome::Declined.should_abort_turn());
        assert!(!ApprovalOutcome::Cancelled.is_approved());
        assert!(ApprovalOutcome::Cancelled.should_abort_turn());
    }

    #[tokio::test]
    async fn send_server_request_routes_through_pending_table() {
        let (state, mut rx) = dummy_state();
        let state2 = Arc::clone(&state);
        let send_task = tokio::spawn(async move {
            send_server_request(
                &state2,
                "item/commandExecution/requestApproval",
                json!({"x": 1}),
                Some(Duration::from_secs(2)),
            )
            .await
        });

        // The request frame should appear on the outbound channel; pick
        // its id off so the test can synthesize a response.
        let msg = rx.recv().await.expect("frame on outbound channel");
        let value = msg.payload;
        assert_eq!(
            value["method"],
            json!("item/commandExecution/requestApproval")
        );
        let id_value = value.get("id").cloned().unwrap();
        let req_id: RequestId = serde_json::from_value(id_value).unwrap();

        // Synthesize the client response.
        state
            .resolve_pending_request(&req_id, Ok(json!({"decision": "accept"})))
            .await;
        let result = send_task.await.unwrap().unwrap();
        assert_eq!(result, json!({"decision": "accept"}));
    }

    #[tokio::test]
    async fn send_server_request_times_out_when_client_silent() {
        let (state, _rx) = dummy_state();
        let err = send_server_request(
            &state,
            "item/tool/requestUserInput",
            json!({}),
            Some(Duration::from_millis(20)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ApprovalError::Timeout(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn request_command_approval_buckets_response() {
        let (state, mut rx) = dummy_state();
        let state2 = Arc::clone(&state);
        let send_task = tokio::spawn(async move {
            request_command_approval(
                &state2,
                CommandExecutionRequestApprovalParams {
                    thread_id: "t1".into(),
                    turn_id: "tu1".into(),
                    item_id: "i1".into(),
                    command: Some("rm -rf /".into()),
                    cwd: Some("/".into()),
                    ..Default::default()
                },
                Some(Duration::from_secs(2)),
            )
            .await
        });
        let msg = rx.recv().await.unwrap();
        let value = msg.payload;
        let id_value = value.get("id").cloned().unwrap();
        let req_id: RequestId = serde_json::from_value(id_value).unwrap();
        state
            .resolve_pending_request(&req_id, Ok(json!({"decision": "cancel"})))
            .await;
        let outcome = send_task.await.unwrap().unwrap();
        assert_eq!(outcome, ApprovalOutcome::Cancelled);
    }

    #[tokio::test]
    async fn request_user_input_decodes_answers_map() {
        let (state, mut rx) = dummy_state();
        let state2 = Arc::clone(&state);
        let q = ToolRequestUserInputQuestion {
            id: "q1".into(),
            header: "h".into(),
            question: "Pick one".into(),
            is_other: false,
            is_secret: false,
            options: None,
        };
        let send_task = tokio::spawn(async move {
            request_user_input(
                &state2,
                "t1".into(),
                "tu1".into(),
                "i1".into(),
                vec![q],
                Some(Duration::from_secs(2)),
            )
            .await
        });
        let msg = rx.recv().await.unwrap();
        let value = msg.payload;
        let id_value = value.get("id").cloned().unwrap();
        let req_id: RequestId = serde_json::from_value(id_value).unwrap();
        state
            .resolve_pending_request(
                &req_id,
                Ok(json!({
                    "answers": {
                        "q1": { "answers": ["yes"] }
                    }
                })),
            )
            .await;
        let answers = send_task.await.unwrap().unwrap();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers["q1"].answers, vec!["yes".to_string()]);
    }
}
