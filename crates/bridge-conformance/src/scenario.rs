//! The canonical conformance scenario.
//!
//! Each target runs *the same* sequence of JSON-RPC ops. We assert on shape
//! (typed deserialize + key-set diff), not LLM-generated content, so the
//! scenario is engineered to keep tool-use and reasoning to a minimum: a
//! one-shot prompt that any minimally-capable agent can answer with a single
//! short assistant message.
//!
//! The scenario records every response and every notification it sees into
//! a [`Transcript`]. It tolerates per-op failures — a `MethodNotFound` reply
//! is still a Frame, and the diff layer's `KnownDivergence` table decides
//! whether that's expected.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::cache;
use crate::semantics::SemanticContext;
use crate::transport::{DrainOutcome, JsonRpcClient};
use crate::{Frame, FrameKind, TargetId, Transcript};

/// Configuration knobs each runner can tune.
#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    /// Working directory passed to `thread/start.cwd` and used as the cwd for
    /// `command/exec`. Should be a fresh tempdir owned by the test.
    pub cwd: PathBuf,
    /// Deadline for "small" rpcs (initialize, list calls, thread/start).
    pub default_deadline: Duration,
    /// Deadline for `turn/start` → `turn/completed` drain.
    pub turn_deadline: Duration,
    /// After each request, wait this long for async notifications to arrive
    /// before moving to the next step. Codex emits things like
    /// `thread/status/changed` and `account/rateLimits/updated` async; pi
    /// and claude emit them synchronously. Without a drain window async
    /// notifications get attributed to the next step's drain.
    pub post_request_idle: Duration,
    /// Prompt sent on the simple-reply `turn/start`. Tuned to keep the
    /// response short and shape-stable across providers.
    pub prompt: String,
    /// Prompt sent on the tool-using `turn/start`. Engineered so each
    /// agent picks its shell tool (no model has a meaningful non-tool
    /// answer to "what's the literal stdout of this exact command").
    pub tool_prompt: String,
    /// Client-info name advertised on `initialize`.
    pub client_name: String,
    /// Client-info version advertised on `initialize`.
    pub client_version: String,
    /// Optional model override threaded into model-bearing methods for
    /// backends whose configured default is not a reliable live target.
    pub model: Option<String>,
    /// Whether the scenario should reuse the per-target cached conformance
    /// thread id. Aggregate shape diffs disable this so every target executes
    /// the same attach method (`thread/start`) instead of comparing a cached
    /// `thread/resume` on one implementation with a fresh start on another.
    pub reuse_thread_cache: bool,
    /// Whether to exercise destructive/mutating APIs against a disposable
    /// thread. Defaults on; set `BRIDGE_CONFORMANCE_SKIP_MUTATIONS=1` to
    /// temporarily narrow a live run.
    pub exercise_mutations: bool,
}

impl ScenarioConfig {
    pub fn for_target(target: TargetId, cwd: PathBuf) -> Self {
        let turn_deadline = match target {
            TargetId::Acp => Duration::from_secs(180),
            _ => Duration::from_secs(60),
        };
        Self {
            cwd,
            default_deadline: Duration::from_secs(20),
            turn_deadline,
            post_request_idle: Duration::from_millis(150),
            prompt: "Reply with exactly the word OK and nothing else.".to_string(),
            tool_prompt: String::new(), // populated per-run by `run` so the
            // marker token is unique each time.
            client_name: format!("alleycat-bridge-conformance/{}", target.label()),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            model: match target {
                TargetId::Opencode => std::env::var("BRIDGE_CONFORMANCE_OPENCODE_MODEL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| Some("xai/grok-4.3".to_string())),
                _ => None,
            },
            reuse_thread_cache: !matches!(target, TargetId::Acp),
            exercise_mutations: std::env::var("BRIDGE_CONFORMANCE_SKIP_MUTATIONS")
                .ok()
                .as_deref()
                != Some("1"),
        }
    }
}

/// Pick a fresh random marker token. Used in the tool-call prompt so
/// the model can't sidestep tool use by memoizing — the marker doesn't
/// exist anywhere except the file we write into the conformance cwd.
fn fresh_marker() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("alleycat-conformance-{nanos:x}")
}

/// Run the full scenario against one client. The transport may fail mid-
/// scenario; transcripts are returned partial in that case via the Err
/// variant containing what we managed to capture.
pub async fn run(
    client: &mut JsonRpcClient,
    cfg: &ScenarioConfig,
    target: TargetId,
) -> Result<Transcript> {
    let mut t = Transcript::new(target);

    // Write a marker file with a unique payload into the conformance cwd.
    // The tool-using turn asks the model to `cat` it; without the marker the
    // smarter models (gpt-5.5) just guess a plausible answer ("conformance")
    // and skip the tool call entirely, which means we never get to compare
    // the CommandExecution wire shape across bridges.
    let marker_token = fresh_marker();
    let marker_path = cfg.cwd.join(format!("{marker_token}.txt"));
    if let Err(err) = std::fs::write(&marker_path, &marker_token) {
        tracing::warn!(?err, path = %marker_path.display(), "failed to write marker file");
    }
    let tool_prompt = if cfg.tool_prompt.is_empty() {
        format!(
            "Use a shell command to read the file `{}` and report the literal contents. The file exists; you must run the command (for example, `cat {}`). Do not guess or skip the tool; the contents are random and unguessable.",
            marker_path.display(),
            marker_path.display(),
        )
    } else {
        cfg.tool_prompt.clone()
    };
    let mut semantic_ctx = SemanticContext::new(marker_token.clone(), cfg.prompt.clone());

    // Step 1: initialize ----------------------------------------------------
    let init = client
        .request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": cfg.client_name,
                    "version": cfg.client_version,
                },
                // Opt into experimental APIs so methods like
                // `collaborationMode/list` are available on codex (which
                // gates them behind this flag).
                "capabilities": { "experimentalApi": true },
            }),
            cfg.default_deadline,
        )
        .await
        .context("initialize")?;
    push_response(&mut t, "initialize", "initialize", &init.response);
    push_notifications(&mut t, "initialize", &init.notifications);

    // Step 2: initialized notification (no response) ------------------------
    client
        .notify("initialized", None)
        .await
        .context("initialized")?;

    // Step 3: read-only fan-out --------------------------------------------
    for (step, method, params) in READ_ONLY_OPS {
        let outcome = client
            .request(method, params(), cfg.default_deadline)
            .await
            .with_context(|| method.to_string())?;
        push_response(&mut t, step, method, &outcome.response);
        let trailing = client.drain_idle(cfg.post_request_idle).await;
        push_notifications(&mut t, step, &outcome.notifications);
        push_notifications(&mut t, step, &trailing);
    }

    // Step 4: thread/list (with explicit archived=false, exercises the param) -
    let listed = client
        .request(
            "thread/list",
            json!({
                "archived": false,
                "cwd": cfg.cwd.to_string_lossy(),
            }),
            cfg.default_deadline,
        )
        .await
        .context("thread/list")?;
    push_response(&mut t, "thread/list", "thread/list", &listed.response);
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(&mut t, "thread/list", &listed.notifications);
    push_notifications(&mut t, "thread/list", &trailing);

    // Step 5: resume the cached test thread, or start a new one ------------
    //
    // Per-target thread persistence: we keep one canonical "conformance"
    // thread per target across runs (cached in `~/.cache/alleycat-bridge-
    // conformance/threads.json`) so the harness doesn't pollute the user's
    // real thread list with a fresh row every time. First run on a clean
    // machine creates the thread; every later run resumes it.
    //
    // approvalPolicy=never + sandbox=danger-full-access so the second turn's
    // shell tool call runs without bouncing a server→client permission
    // prompt the harness has no way to answer.
    let thread_start_params = with_model(
        json!({
            "cwd": cfg.cwd.to_string_lossy(),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }),
        cfg,
    );

    // Label the captured frame with the method we actually call, so the
    // diff layer compares "codex thread/resume" to "bridge thread/resume"
    // (not to "bridge thread/start", which would mismatch shapes).
    let cached_id = if cfg.reuse_thread_cache {
        cache::load_thread_id(target)
    } else {
        None
    };
    let (resumed_id, attach_method, attach_response) = if let Some(id) = cached_id.clone() {
        let resume_params = json!({
            "threadId": id,
            "cwd": cfg.cwd.to_string_lossy(),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        });
        match client
            .request("thread/resume", resume_params, cfg.default_deadline)
            .await
        {
            Ok(out) if frame_is_error(&out.response).is_none() => (Some(id), "thread/resume", out),
            other => {
                if let Ok(out) = &other
                    && let Some((code, msg)) = frame_is_error(&out.response)
                {
                    tracing::info!(
                        cached = %id,
                        code,
                        msg = %msg,
                        "cached thread resume failed; will thread/start fresh"
                    );
                }
                let _ = cache::clear_thread_id(target);
                (
                    None,
                    "thread/start",
                    client
                        .request(
                            "thread/start",
                            thread_start_params.clone(),
                            cfg.default_deadline,
                        )
                        .await
                        .context("thread/start")?,
                )
            }
        }
    } else {
        let out = client
            .request(
                "thread/start",
                thread_start_params.clone(),
                cfg.default_deadline,
            )
            .await
            .context("thread/start")?;
        (None, "thread/start", out)
    };
    push_response(
        &mut t,
        attach_method,
        attach_method,
        &attach_response.response,
    );
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(&mut t, attach_method, &attach_response.notifications);
    push_notifications(&mut t, attach_method, &trailing);

    let thread_id = match resumed_id.or_else(|| extract_thread_id(&attach_response.response)) {
        Some(id) => id,
        None => {
            tracing::warn!(target = %target, "thread attach did not return thread.id; aborting scenario");
            t.semantic_ctx = Some(semantic_ctx);
            return Ok(t);
        }
    };
    semantic_ctx.thread_id = Some(thread_id.clone());
    if cfg.reuse_thread_cache
        && cached_id.as_deref() != Some(thread_id.as_str())
        && let Err(err) = cache::save_thread_id(target, &thread_id)
    {
        tracing::warn!(?err, "failed to persist conformance thread id");
    }

    // Step 6: turn/start + drain until turn/completed -----------------------
    let turn = client
        .request(
            "turn/start",
            with_model(
                json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": cfg.prompt }],
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                }),
                cfg,
            ),
            cfg.turn_deadline,
        )
        .await
        .context("turn/start")?;
    push_response(&mut t, "turn/start", "turn/start", &turn.response);
    push_notifications(&mut t, "turn/start", &turn.notifications);

    let drain = drain_after_prior_notifications(
        client,
        &turn.notifications,
        &["turn/completed"],
        cfg.turn_deadline,
    )
    .await
    .context("draining turn/start")?;
    push_notifications(&mut t, "turn/start", &drain.notifications);

    // Step 7: thread/read ---------------------------------------------------
    let read = client
        .request(
            "thread/read",
            json!({ "threadId": thread_id, "includeTurns": true }),
            cfg.default_deadline,
        )
        .await
        .context("thread/read")?;
    push_response(&mut t, "thread/read", "thread/read", &read.response);
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(&mut t, "thread/read", &read.notifications);
    push_notifications(&mut t, "thread/read", &trailing);

    // Step 7a: turn/start with a tool-using prompt --------------------------
    //
    // Exercises the CommandExecution / DynamicToolCall item shapes that the
    // simple-reply turn doesn't reach. Prompt is engineered so every
    // supported target picks its shell tool without a thinking detour. The bridges'
    // `approvalPolicy: "never"` (set on thread/start) lets the call run
    // without surfacing a server→client permission request.
    let tool_turn = client
        .request(
            "turn/start",
            with_model(
                json!({
                    "threadId": thread_id,
                    "input": [{
                        "type": "text",
                        "text": tool_prompt,
                    }],
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                }),
                cfg,
            ),
            cfg.turn_deadline,
        )
        .await
        .context("turn/start (tool)")?;
    push_response(&mut t, "turn/start.tool", "turn/start", &tool_turn.response);
    push_notifications(&mut t, "turn/start.tool", &tool_turn.notifications);
    let drain = drain_after_prior_notifications(
        client,
        &tool_turn.notifications,
        &["turn/completed"],
        cfg.turn_deadline,
    )
    .await
    .context("draining turn/start (tool)")?;
    push_notifications(&mut t, "turn/start.tool", &drain.notifications);

    // Step 7b: thread/read after the tool-use turn -------------------------
    let read_after_tool = client
        .request(
            "thread/read",
            json!({ "threadId": thread_id, "includeTurns": true }),
            cfg.default_deadline,
        )
        .await
        .context("thread/read (after tool)")?;
    push_response(
        &mut t,
        "thread/read.afterTool",
        "thread/read",
        &read_after_tool.response,
    );
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(
        &mut t,
        "thread/read.afterTool",
        &read_after_tool.notifications,
    );
    push_notifications(&mut t, "thread/read.afterTool", &trailing);

    // Step 7c: thread/resume — re-attach to the same thread. Exercises a
    // separate codex code path (resumes the rollout into a fresh session)
    // and is shape-checked field-by-field against codex.
    let resume = client
        .request(
            "thread/resume",
            json!({ "threadId": thread_id }),
            cfg.default_deadline,
        )
        .await
        .context("thread/resume")?;
    push_response(&mut t, "thread/resume", "thread/resume", &resume.response);
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(&mut t, "thread/resume", &resume.notifications);
    push_notifications(&mut t, "thread/resume", &trailing);

    // Step 8: thread/name/set ----------------------------------------------
    let rename = client
        .request(
            "thread/name/set",
            json!({ "threadId": thread_id, "name": "conformance" }),
            cfg.default_deadline,
        )
        .await
        .context("thread/name/set")?;
    push_response(
        &mut t,
        "thread/name/set",
        "thread/name/set",
        &rename.response,
    );
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(&mut t, "thread/name/set", &rename.notifications);
    push_notifications(&mut t, "thread/name/set", &trailing);

    if cfg.exercise_mutations
        && let Err(err) = exercise_mutations(client, cfg, &mut t, &mut semantic_ctx).await
    {
        if let Some(id) = t.disposable_thread_id.clone() {
            cleanup_disposable_thread(client, &id, cfg.default_deadline, cfg.post_request_idle)
                .await;
        }
        return Err(err);
    }

    // Step 9: command/exec -- a known-deterministic shell command. We
    // deliberately don't set streamStdoutStderr because pi-bridge requires
    // a client-supplied processId in that mode; the buffered shape is the
    // common-denominator path every bridge supports.
    let exec = client
        .request(
            "command/exec",
            json!({
                "command": ["sh", "-c", "printf hello"],
            }),
            cfg.default_deadline,
        )
        .await
        .context("command/exec")?;
    push_response(&mut t, "command/exec", "command/exec", &exec.response);
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(&mut t, "command/exec", &exec.notifications);
    push_notifications(&mut t, "command/exec", &trailing);

    t.semantic_ctx = Some(semantic_ctx);
    Ok(t)
}

async fn exercise_mutations(
    client: &mut JsonRpcClient,
    cfg: &ScenarioConfig,
    t: &mut Transcript,
    semantic_ctx: &mut SemanticContext,
) -> Result<()> {
    let start = request_record(
        client,
        t,
        "thread/start.disposable",
        "thread/start",
        with_model(
            json!({
                "cwd": cfg.cwd.to_string_lossy(),
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
            }),
            cfg,
        ),
        cfg,
    )
    .await
    .context("thread/start disposable")?;
    let Some(disposable_id) = extract_thread_id(&start) else {
        tracing::warn!("thread/start.disposable did not return thread.id; skipping mutation block");
        return Ok(());
    };
    t.disposable_thread_id = Some(disposable_id.clone());
    semantic_ctx.disposable_thread_id = Some(disposable_id.clone());

    let seed_turn = client
        .request(
            "turn/start",
            with_model(
                json!({
                    "threadId": disposable_id,
                    "input": [{ "type": "text", "text": "Reply with exactly OK." }],
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                }),
                cfg,
            ),
            cfg.turn_deadline,
        )
        .await
        .context("turn/start disposable seed")?;
    push_response(
        t,
        "turn/start.disposableSeed",
        "turn/start",
        &seed_turn.response,
    );
    push_notifications(t, "turn/start.disposableSeed", &seed_turn.notifications);
    let seed_drain = drain_after_prior_notifications(
        client,
        &seed_turn.notifications,
        &["turn/completed"],
        cfg.turn_deadline,
    )
    .await
    .context("draining turn/start disposable seed")?;
    push_notifications(t, "turn/start.disposableSeed", &seed_drain.notifications);

    request_record(
        client,
        t,
        "thread/list.disposable",
        "thread/list",
        json!({
            "archived": false,
            "cwd": cfg.cwd.to_string_lossy(),
        }),
        cfg,
    )
    .await
    .context("thread/list disposable")?;

    let fork = request_record(
        client,
        t,
        "thread/fork",
        "thread/fork",
        with_model(
            json!({
                "threadId": disposable_id,
                "cwd": cfg.cwd.to_string_lossy(),
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
            }),
            cfg,
        ),
        cfg,
    )
    .await
    .context("thread/fork")?;
    semantic_ctx.forked_thread_id = extract_thread_id(&fork);

    let turn = client
        .request(
            "turn/start",
            with_model(json!({
                "threadId": disposable_id,
                "input": [{
                    "type": "text",
                    "text": "Count slowly from one to one hundred, one number per sentence. Keep going until interrupted.",
                }],
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
            }), cfg),
            cfg.turn_deadline,
        )
        .await
        .context("turn/start interruptible")?;
    push_response(t, "turn/start.interruptible", "turn/start", &turn.response);
    push_notifications(t, "turn/start.interruptible", &turn.notifications);
    let interrupted_turn_id = extract_turn_id(&turn.response);
    semantic_ctx.interrupted_turn_id = interrupted_turn_id.clone();
    let first_delta_or_done = drain_after_prior_notifications(
        client,
        &turn.notifications,
        &["item/agentMessage/delta", "turn/completed"],
        cfg.turn_deadline,
    )
    .await
    .context("draining interruptible turn until first delta")?;
    push_notifications(
        t,
        "turn/start.interruptible",
        &first_delta_or_done.notifications,
    );
    if first_delta_or_done.terminated_by.as_deref() == Some("item/agentMessage/delta") {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if first_delta_or_done.terminated_by.as_deref() == Some("item/agentMessage/delta")
        && let Some(turn_id) = interrupted_turn_id.as_deref()
    {
        request_record(
            client,
            t,
            "turn/steer",
            "turn/steer",
            with_model(
                json!({
                    "threadId": disposable_id,
                    "expectedTurnId": turn_id,
                    "input": [{ "type": "text", "text": "continue" }],
                }),
                cfg,
            ),
            cfg,
        )
        .await
        .context("turn/steer")?;

        let interrupt = client
            .request(
                "turn/interrupt",
                json!({ "threadId": disposable_id, "turnId": turn_id }),
                cfg.default_deadline,
            )
            .await
            .context("turn/interrupt")?;
        push_response(t, "turn/interrupt", "turn/interrupt", &interrupt.response);
        push_notifications(t, "turn/interrupt", &interrupt.notifications);
        let drain = drain_after_prior_notifications(
            client,
            &interrupt.notifications,
            &["turn/completed"],
            cfg.turn_deadline,
        )
        .await
        .context("draining turn/interrupt")?;
        push_notifications(t, "turn/interrupt", &drain.notifications);
    }

    request_record(
        client,
        t,
        "thread/turns/list",
        "thread/turns/list",
        json!({ "threadId": disposable_id, "limit": 10 }),
        cfg,
    )
    .await
    .context("thread/turns/list")?;

    request_record(
        client,
        t,
        "thread/loaded/list",
        "thread/loaded/list",
        json!({}),
        cfg,
    )
    .await
    .context("thread/loaded/list")?;

    let read_before_rollback = request_record(
        client,
        t,
        "thread/read.beforeRollback",
        "thread/read",
        json!({ "threadId": disposable_id, "includeTurns": true }),
        cfg,
    )
    .await
    .context("thread/read before rollback")?;
    semantic_ctx.rollback_before_turns = count_thread_turns(&read_before_rollback);

    request_record(
        client,
        t,
        "thread/rollback",
        "thread/rollback",
        json!({ "threadId": disposable_id, "numTurns": 1 }),
        cfg,
    )
    .await
    .context("thread/rollback")?;

    request_record(
        client,
        t,
        "thread/compact/start",
        "thread/compact/start",
        with_model(json!({ "threadId": disposable_id }), cfg),
        cfg,
    )
    .await
    .context("thread/compact/start")?;

    exercise_streaming_exec(client, cfg, t, semantic_ctx).await?;

    request_record(
        client,
        t,
        "thread/archive",
        "thread/archive",
        json!({ "threadId": disposable_id }),
        cfg,
    )
    .await
    .context("thread/archive")?;
    request_record(
        client,
        t,
        "thread/list.archived",
        "thread/list",
        json!({
            "archived": true,
            "cwd": cfg.cwd.to_string_lossy(),
        }),
        cfg,
    )
    .await
    .context("thread/list archived")?;

    request_record(
        client,
        t,
        "thread/unarchive",
        "thread/unarchive",
        json!({ "threadId": disposable_id }),
        cfg,
    )
    .await
    .context("thread/unarchive")?;
    request_record(
        client,
        t,
        "thread/list.unarchived",
        "thread/list",
        json!({
            "archived": false,
            "cwd": cfg.cwd.to_string_lossy(),
        }),
        cfg,
    )
    .await
    .context("thread/list unarchived")?;

    cleanup_disposable_thread(
        client,
        &disposable_id,
        cfg.default_deadline,
        cfg.post_request_idle,
    )
    .await;

    Ok(())
}

async fn exercise_streaming_exec(
    client: &mut JsonRpcClient,
    cfg: &ScenarioConfig,
    t: &mut Transcript,
    semantic_ctx: &mut SemanticContext,
) -> Result<()> {
    let process_id = format!("bridge-conformance-{}", fresh_marker());
    semantic_ctx.streaming_process_id = Some(process_id.clone());
    let exec_id = client
        .send_request(
            "command/exec",
            json!({
                "command": ["sh", "-c", "printf ready; sleep 5"],
                "processId": process_id,
                "tty": true,
                "streamStdin": true,
                "streamStdoutStderr": true,
            }),
        )
        .await
        .context("command/exec streaming send")?;
    let startup = client
        .drain_notifications_until(&["command/exec/outputDelta"], Duration::from_secs(2))
        .await
        .context("draining command/exec streaming startup")?;
    push_notifications(t, "command/exec.streaming", &startup.notifications);

    request_record(
        client,
        t,
        "command/exec/write",
        "command/exec/write",
        json!({
            "processId": process_id,
            "deltaBase64": "Cg==",
            "closeStdin": false,
        }),
        cfg,
    )
    .await
    .context("command/exec/write")?;
    request_record(
        client,
        t,
        "command/exec/resize",
        "command/exec/resize",
        json!({
            "processId": process_id,
            "size": { "rows": 24, "cols": 80 },
        }),
        cfg,
    )
    .await
    .context("command/exec/resize")?;
    request_record(
        client,
        t,
        "command/exec/terminate",
        "command/exec/terminate",
        json!({ "processId": process_id }),
        cfg,
    )
    .await
    .context("command/exec/terminate")?;

    let exec = client
        .wait_for_response("command/exec", exec_id, cfg.default_deadline)
        .await
        .context("command/exec streaming response")?;
    push_response(t, "command/exec.streaming", "command/exec", &exec.response);
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(t, "command/exec.streaming", &exec.notifications);
    push_notifications(t, "command/exec.streaming", &trailing);

    Ok(())
}

pub async fn cleanup_disposable_thread(
    client: &mut JsonRpcClient,
    thread_id: &str,
    deadline: Duration,
    post_request_idle: Duration,
) {
    match client
        .request("thread/archive", json!({ "threadId": thread_id }), deadline)
        .await
    {
        Ok(_) => {
            let _ = client.drain_idle(post_request_idle).await;
        }
        Err(err) => {
            tracing::warn!(
                ?err,
                thread_id,
                "failed to cleanup disposable conformance thread"
            );
        }
    }
}

async fn request_record(
    client: &mut JsonRpcClient,
    t: &mut Transcript,
    step: &str,
    method: &str,
    params: Value,
    cfg: &ScenarioConfig,
) -> Result<Value> {
    let out = client
        .request(method, params, cfg.default_deadline)
        .await
        .with_context(|| method.to_string())?;
    push_response(t, step, method, &out.response);
    let trailing = client.drain_idle(cfg.post_request_idle).await;
    push_notifications(t, step, &out.notifications);
    push_notifications(t, step, &trailing);
    Ok(out.response)
}

async fn drain_after_prior_notifications(
    client: &mut JsonRpcClient,
    prior: &[Value],
    stop_methods: &[&str],
    deadline: Duration,
) -> Result<DrainOutcome> {
    if let Some(method) = prior_terminal_method(prior, stop_methods) {
        return Ok(DrainOutcome {
            notifications: Vec::new(),
            terminated_by: Some(method),
        });
    }
    client
        .drain_notifications_until(stop_methods, deadline)
        .await
}

fn prior_terminal_method(prior: &[Value], stop_methods: &[&str]) -> Option<String> {
    // Synchronous bridges may emit the full turn lifecycle before the
    // `turn/start` response. Prefer the terminal event when present so the
    // scenario does not wait for a second completion notification.
    if stop_methods.contains(&"turn/completed")
        && prior
            .iter()
            .any(|frame| frame.get("method").and_then(Value::as_str) == Some("turn/completed"))
    {
        return Some("turn/completed".to_string());
    }
    prior
        .iter()
        .filter_map(|frame| frame.get("method").and_then(Value::as_str))
        .find(|method| stop_methods.contains(method))
        .map(str::to_string)
}

fn with_model(mut params: Value, cfg: &ScenarioConfig) -> Value {
    if let Some(model) = cfg.model.as_deref()
        && let Some(obj) = params.as_object_mut()
    {
        obj.insert("model".to_string(), json!(model));
    }
    params
}

type ParamsBuilder = fn() -> Value;
const READ_ONLY_OPS: &[(&str, &str, ParamsBuilder)] = &[
    ("config/read", "config/read", || json!({})),
    ("model/list", "model/list", || json!({})),
    (
        "experimentalFeature/list",
        "experimentalFeature/list",
        || json!({}),
    ),
    ("collaborationMode/list", "collaborationMode/list", || {
        json!({})
    }),
    ("mcpServerStatus/list", "mcpServerStatus/list", || json!({})),
    ("skills/list", "skills/list", || json!({})),
    ("account/read", "account/read", || json!({})),
];

fn push_response(t: &mut Transcript, step: &str, method: &str, raw: &Value) {
    t.push(Frame {
        step: step.to_string(),
        kind: FrameKind::Response,
        method: method.to_string(),
        raw: raw.clone(),
    });
}

fn push_notifications(t: &mut Transcript, step: &str, notifs: &[Value]) {
    for n in notifs {
        let method = n
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("(missing)")
            .to_string();
        t.push(Frame {
            step: step.to_string(),
            kind: FrameKind::Notification,
            method,
            raw: n.clone(),
        });
    }
}

fn extract_thread_id(response: &Value) -> Option<String> {
    response
        .get("result")?
        .get("thread")?
        .get("id")?
        .as_str()
        .map(str::to_string)
}

fn extract_turn_id(response: &Value) -> Option<String> {
    response
        .get("result")?
        .get("turn")?
        .get("id")?
        .as_str()
        .map(str::to_string)
}

fn count_thread_turns(response: &Value) -> Option<usize> {
    response
        .get("result")?
        .get("thread")?
        .get("turns")?
        .as_array()
        .map(Vec::len)
}

/// `(code, message)` if the response is a JSON-RPC error envelope.
fn frame_is_error(response: &Value) -> Option<(i64, String)> {
    let err = response.get("error")?;
    let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
    let message = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("(no message)")
        .to_string();
    Some((code, message))
}
