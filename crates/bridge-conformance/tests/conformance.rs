//! Conformance test entry points. Each `#[ignore]`d test drives one target
//! through the canonical scenario; the aggregate `conformance_diff` runs all
//! bridge targets and diffs each bridge against the codex reference.
//!
//! Run with:
//!   cargo test -p alleycat-bridge-conformance -- --ignored --nocapture
//!
//! Without prereqs (no codex, no pi/claude/opencode/droid CLIs, no API keys) the
//! suite still passes — each test prints `skipped: <reason>` and exits.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::sync::LazyLock;
use std::time::Duration;

use alleycat_bridge_conformance::{
    TargetId, Transcript, cache,
    diff::{self, ConformanceReport, Finding},
    method_surface::{self, ProbeContext},
    prereq::{self, Prereq, SkipReason},
    scenario, semantics, streaming,
    targets::{self, TargetSpawn},
};
use serde_json::json;
use tokio::sync::Mutex;

static LIVE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[tokio::test]
#[ignore = "live conformance — requires `codex` CLI on PATH"]
async fn conformance_codex() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Codex).await;
}

#[tokio::test]
#[ignore = "live conformance — requires pi-coding-agent on PATH"]
async fn conformance_pi() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Pi).await;
}

#[tokio::test]
#[ignore = "live conformance — requires `amp` CLI"]
async fn conformance_amp() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Amp).await;
}

#[tokio::test]
#[ignore = "live conformance — requires `claude` CLI on PATH"]
async fn conformance_claude() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Claude).await;
}

#[tokio::test]
#[ignore = "live conformance — requires `opencode` CLI"]
async fn conformance_opencode() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Opencode).await;
}

#[tokio::test]
#[ignore = "live conformance — requires `droid` CLI"]
async fn conformance_droid() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Droid).await;
}

#[tokio::test]
#[ignore = "live conformance — requires `hermes` CLI or gateway"]
async fn conformance_hermes() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Hermes).await;
}

#[tokio::test]
#[ignore = "live conformance — requires ACP agent (e.g. `devin` or `grok`) on PATH"]
async fn conformance_acp() {
    let _guard = live_test_guard().await;
    run_target(TargetId::Acp).await;
}

/// Aggregate test: capture transcripts from every available target and
/// diff each non-codex target against codex (the reference). Skips any
/// target whose prereqs aren't met. Skips entirely if codex itself is
/// unavailable.
#[tokio::test]
#[ignore = "live conformance — runs all targets and diffs them"]
async fn conformance_diff_all_against_codex() {
    let _guard = live_test_guard().await;
    let codex_transcript = match drive_fresh(TargetId::Codex).await {
        DriveOutcome::Ran(t) => t,
        DriveOutcome::Skipped(reason) => {
            eprintln!("conformance_diff: codex skipped, no reference; aborting: {reason}");
            return;
        }
        DriveOutcome::Failed(err) => {
            panic!("codex (reference) target failed: {err:#}");
        }
    };
    eprintln!(
        "conformance_diff: codex captured {} frames",
        codex_transcript.frames.len()
    );
    let codex_findings = standalone_findings(&codex_transcript);
    if !codex_findings.is_empty() {
        let report = ConformanceReport {
            target: TargetId::Codex,
            findings: codex_findings,
        };
        panic!(
            "{} method(s) failed: {}\n{report}",
            failed_methods(&report).len(),
            failed_methods_csv(&report)
        );
    }

    let mut had_findings = false;
    let mut matrix: Vec<(TargetId, BTreeSet<String>)> = Vec::new();
    for target in [
        TargetId::Pi,
        TargetId::Amp,
        TargetId::Claude,
        TargetId::Opencode,
        TargetId::Droid,
        TargetId::Hermes,
        TargetId::Acp,
    ] {
        match drive_fresh(target).await {
            DriveOutcome::Ran(t) => {
                eprintln!(
                    "conformance_diff: {target} captured {} frames",
                    t.frames.len()
                );
                let mut report = diff::compare(&codex_transcript, &t);
                report.findings.extend(runtime_findings(&t));
                if !report.is_clean() {
                    had_findings = true;
                    matrix.push((target, failed_methods(&report)));
                    eprintln!("{report}");
                } else {
                    matrix.push((target, BTreeSet::new()));
                    eprintln!("conformance_diff: {target} clean");
                }
            }
            DriveOutcome::Skipped(reason) => {
                eprintln!("conformance_diff: {target} skipped: {reason}");
            }
            DriveOutcome::Failed(err) => {
                eprintln!("conformance_diff: {target} target failed: {err:#}");
                matrix.push((target, BTreeSet::from(["<target failed>".to_string()])));
                had_findings = true;
            }
        }
    }
    eprintln!("conformance_diff method matrix:");
    for (target, methods) in matrix {
        if methods.is_empty() {
            eprintln!("  {target}: PASS");
        } else {
            eprintln!(
                "  {target}: FAIL({}) {}",
                methods.len(),
                methods.into_iter().collect::<Vec<_>>().join(", ")
            );
        }
    }
    assert!(
        !had_findings,
        "one or more bridges diverged from codex (see findings above)"
    );
}

#[tokio::test]
#[ignore = "live conformance — probes every standard method on every bridge"]
async fn method_surface_runtime() {
    let _guard = live_test_guard().await;
    let mut had_findings = false;
    for target in [
        TargetId::Pi,
        TargetId::Amp,
        TargetId::Claude,
        TargetId::Opencode,
        TargetId::Droid,
        TargetId::Hermes,
        TargetId::Acp,
    ] {
        match probe_method_surface(target).await {
            DriveOutcome::Ran(t) => {
                let findings: Vec<Finding> = t
                    .responses()
                    .filter_map(|frame| method_surface::assert_method_response(frame, target))
                    .collect();
                if findings.is_empty() {
                    eprintln!("method_surface_runtime: {target} clean");
                } else {
                    had_findings = true;
                    let report = ConformanceReport { target, findings };
                    eprintln!("{report}");
                }
            }
            DriveOutcome::Skipped(reason) => {
                eprintln!("method_surface_runtime: {target} skipped: {reason}");
            }
            DriveOutcome::Failed(err) => {
                had_findings = true;
                eprintln!("method_surface_runtime: {target} failed: {err:#}");
            }
        }
    }
    assert!(!had_findings, "runtime method-surface findings above");
}

// ============================================================================
// helpers
// ============================================================================

enum DriveOutcome {
    Ran(Box<Transcript>),
    Skipped(String),
    Failed(anyhow::Error),
}

async fn live_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    LIVE_TEST_LOCK.lock().await
}

async fn run_target(target: TargetId) {
    match drive(target).await {
        DriveOutcome::Ran(t) => {
            eprintln!("conformance({target}): {} frames", t.frames.len());
            let findings = standalone_findings(&t);
            if !findings.is_empty() {
                let report = ConformanceReport { target, findings };
                panic!(
                    "{} method(s) failed: {}\n{report}",
                    failed_methods(&report).len(),
                    failed_methods_csv(&report)
                );
            }
        }
        DriveOutcome::Skipped(reason) => {
            eprintln!("conformance({target}): skipped — {reason}");
        }
        DriveOutcome::Failed(err) => {
            panic!("conformance({target}) target failed: {err:#}");
        }
    }
}

async fn drive(target: TargetId) -> DriveOutcome {
    drive_with_options(target, true, true).await
}

async fn drive_fresh(target: TargetId) -> DriveOutcome {
    drive_with_options(target, false, false).await
}

async fn drive_with_options(
    target: TargetId,
    reuse_thread_cache: bool,
    use_stable_cwd: bool,
) -> DriveOutcome {
    let prereq = match prereq::check(target).await {
        Ok(p) => p,
        Err(SkipReason::Reason(s)) => return DriveOutcome::Skipped(s),
    };

    let spawn_opts = match build_spawn(target, &prereq, use_stable_cwd) {
        Ok(s) => s,
        Err(err) => return DriveOutcome::Failed(err),
    };

    let mut handle = match targets::spawn(spawn_opts.clone()).await {
        Ok(h) => h,
        Err(err) => return DriveOutcome::Failed(err),
    };
    let mut cfg = scenario::ScenarioConfig::for_target(target, spawn_opts.cwd.clone());
    cfg.reuse_thread_cache = reuse_thread_cache;
    let transcript = match scenario::run(&mut handle.client, &cfg, target).await {
        Ok(t) => t,
        Err(err) => return DriveOutcome::Failed(err),
    };
    if let Some(id) = transcript.disposable_thread_id.clone() {
        scenario::cleanup_disposable_thread(
            &mut handle.client,
            &id,
            cfg.default_deadline,
            cfg.post_request_idle,
        )
        .await;
    }
    if let Some(dir) = std::env::var_os("BRIDGE_CONFORMANCE_DUMP_DIR")
        && let Err(err) = dump_transcript(std::path::Path::new(&dir), &transcript)
    {
        eprintln!("conformance({target}): dump failed: {err:#}");
    }
    DriveOutcome::Ran(Box::new(transcript))
}

fn standalone_findings(transcript: &Transcript) -> Vec<Finding> {
    let mut findings = diff::schema_only(transcript);
    findings.extend(runtime_findings(transcript));
    findings
}

fn runtime_findings(transcript: &Transcript) -> Vec<Finding> {
    let mut findings = streaming::check(transcript);
    findings.extend(semantics::check_all(transcript));
    findings
}

fn failed_methods(report: &ConformanceReport) -> BTreeSet<String> {
    report
        .findings
        .iter()
        .map(|finding| finding.method().to_string())
        .collect()
}

fn failed_methods_csv(report: &ConformanceReport) -> String {
    failed_methods(report)
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ")
}

async fn probe_method_surface(target: TargetId) -> DriveOutcome {
    let prereq = match prereq::check(target).await {
        Ok(p) => p,
        Err(SkipReason::Reason(s)) => return DriveOutcome::Skipped(s),
    };
    let spawn_opts = match build_spawn(target, &prereq, false) {
        Ok(s) => s,
        Err(err) => return DriveOutcome::Failed(err),
    };
    let mut handle = match targets::spawn(spawn_opts.clone()).await {
        Ok(h) => h,
        Err(err) => return DriveOutcome::Failed(err),
    };
    let cfg = scenario::ScenarioConfig::for_target(target, spawn_opts.cwd.clone());
    let mut transcript = Transcript::new(target);

    let init = match handle
        .client
        .request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": format!("alleycat-bridge-conformance/method-surface/{}", target.label()),
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": { "experimentalApi": true },
            }),
            cfg.default_deadline,
        )
        .await
    {
        Ok(out) => out,
        Err(err) => return DriveOutcome::Failed(err),
    };
    transcript.push(method_surface::response_frame(
        "initialize",
        "initialize",
        init.response,
    ));
    let _ = handle.client.notify("initialized", None).await;

    let start = match handle
        .client
        .request(
            "thread/start",
            json!({
                "cwd": spawn_opts.cwd.to_string_lossy(),
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
            }),
            cfg.default_deadline,
        )
        .await
    {
        Ok(out) => out,
        Err(err) => return DriveOutcome::Failed(err),
    };
    let thread_id = start
        .response
        .pointer("/result/thread/id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    transcript.push(method_surface::response_frame(
        "thread/start.probe",
        "thread/start",
        start.response,
    ));

    let mut ctx = ProbeContext {
        thread_id,
        turn_id: None,
        cwd: Some(spawn_opts.cwd.to_string_lossy().to_string()),
        process_id: None,
    };
    let probe_deadline = Duration::from_secs(5);
    if let Some(thread_id) = ctx.thread_id.as_deref()
        && let Ok(seed) = handle
            .client
            .request(
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{ "type": "text", "text": "Reply with exactly OK." }],
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                }),
                cfg.turn_deadline,
            )
            .await
    {
        let _ = handle
            .client
            .drain_notifications_until(&["turn/completed"], cfg.turn_deadline)
            .await;
        ctx.turn_id = seed
            .response
            .pointer("/result/turn/id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
    }

    for method in method_surface::STANDARD_REQUEST_METHODS {
        if *method == "thread/start" {
            continue;
        }
        if matches!(
            *method,
            "command/exec/terminate" | "command/exec/write" | "command/exec/resize"
        ) && let Err(err) = start_probe_process(&mut handle.client, &mut ctx).await
        {
            transcript.push(method_surface::response_frame(
                method,
                method,
                json!({"error":{"code":0,"message":format!("failed to start probe process: {err:#}")}}),
            ));
            continue;
        }

        let params = method_surface::min_params_for_with(method, &ctx);
        let deadline = if *method == "turn/start" {
            cfg.turn_deadline
        } else {
            probe_deadline
        };
        let response = match handle.client.request(method, params, deadline).await {
            Ok(out) => out.response,
            Err(err) => json!({"error":{"code":0,"message":err.to_string()}}),
        };
        let frame = method_surface::response_frame(method, method, response.clone());
        transcript.push(frame);

        if *method == "turn/start" {
            ctx.turn_id = response
                .pointer("/result/turn/id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
        }
        if matches!(*method, "command/exec/write" | "command/exec/resize") {
            cleanup_probe_process(&mut handle.client, &ctx).await;
        }
        if *method == "command/exec/terminate" {
            wait_probe_process(&mut handle.client, &ctx).await;
        }
    }

    if let Some(thread_id) = ctx.thread_id.as_deref() {
        scenario::cleanup_disposable_thread(
            &mut handle.client,
            thread_id,
            cfg.default_deadline,
            cfg.post_request_idle,
        )
        .await;
    }

    DriveOutcome::Ran(Box::new(transcript))
}

async fn start_probe_process(
    client: &mut alleycat_bridge_conformance::transport::JsonRpcClient,
    ctx: &mut ProbeContext,
) -> anyhow::Result<()> {
    let process_id = format!(
        "method-surface-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    ctx.process_id = Some(process_id.clone());
    client
        .send_request(
            "command/exec",
            json!({
                "command": ["sh", "-c", "sleep 5"],
                "processId": process_id,
                "streamStdin": true,
                "streamStdoutStderr": true,
            }),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

async fn cleanup_probe_process(
    client: &mut alleycat_bridge_conformance::transport::JsonRpcClient,
    ctx: &ProbeContext,
) {
    if let Some(process_id) = ctx.process_id.as_deref() {
        let _ = client
            .request(
                "command/exec/terminate",
                json!({ "processId": process_id }),
                Duration::from_secs(5),
            )
            .await;
    }
}

async fn wait_probe_process(
    client: &mut alleycat_bridge_conformance::transport::JsonRpcClient,
    ctx: &ProbeContext,
) {
    if let Some(process_id) = ctx.process_id.as_deref() {
        let _ = client
            .request(
                "command/exec/terminate",
                json!({ "processId": process_id }),
                Duration::from_millis(100),
            )
            .await;
    }
}

fn dump_transcript(
    dir: &std::path::Path,
    transcript: &alleycat_bridge_conformance::Transcript,
) -> anyhow::Result<()> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.jsonl", transcript.target));
    let mut f = std::fs::File::create(&path)?;
    for frame in &transcript.frames {
        let line = serde_json::to_string(frame)?;
        writeln!(f, "{line}")?;
    }
    Ok(())
}

fn build_spawn(
    target: TargetId,
    prereq: &Prereq,
    use_stable_cwd: bool,
) -> anyhow::Result<TargetSpawn> {
    // Stable cwd lives at ~/.cache/alleycat-bridge-conformance/cwd/. Reusing
    // it run-to-run lets us also reuse the per-target thread id (cwd is part
    // of every bridge's thread-id binding). On a machine without $HOME we
    // fall back to a tempdir, which means thread ids won't be reusable —
    // acceptable degradation.
    let cwd = if use_stable_cwd {
        match cache::stable_cwd() {
            Ok(p) => p,
            Err(_) => tempfile::TempDir::new()?.keep(),
        }
    } else {
        tempfile::TempDir::new()?.keep()
    };
    Ok(match (target, prereq) {
        (TargetId::Codex, Prereq::Codex { bin }) => TargetSpawn {
            target,
            bridge_bin: None,
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Pi, Prereq::Pi { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-pi-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Amp, Prereq::Amp { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-amp-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Claude, Prereq::Claude { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-claude-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Opencode, Prereq::Opencode { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-opencode-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Droid, Prereq::Droid { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-droid-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Hermes, Prereq::Hermes { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-hermes-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (TargetId::Acp, Prereq::Acp { bin }) => TargetSpawn {
            target,
            bridge_bin: Some(workspace_bin("alleycat-acp-bridge")?),
            backend_bin: Some(bin.clone()),
            cwd,
        },
        (t, p) => anyhow::bail!("prereq {p:?} doesn't match target {t:?}"),
    })
}

/// Resolve a workspace-sibling binary by building it via cargo. We do this
/// at test time because `CARGO_BIN_EXE_<name>` is only set for binaries
/// belonging to the *same* package as the integration test. The build is
/// idempotent — cargo no-ops when already up to date.
fn workspace_bin(name: &str) -> anyhow::Result<PathBuf> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .ancestors()
        .find(|p| p.join("Cargo.lock").is_file())
        .unwrap_or(&manifest_dir)
        .to_path_buf();

    let status = StdCommand::new(&cargo)
        .arg("build")
        .arg("--package")
        .arg(name)
        .arg("--bin")
        .arg(name)
        .current_dir(&workspace_root)
        .status()?;
    if !status.success() {
        anyhow::bail!("cargo build --bin {name} failed");
    }

    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.join("target"));
    // Conformance tests build in dev profile.
    let candidate = target_dir
        .join("debug")
        .join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    if !candidate.is_file() {
        anyhow::bail!("expected binary at {} after build", candidate.display());
    }
    Ok(candidate)
}

// Suppress warnings if the test binary is built without one of the targets
// having any prereq path resolved.
#[allow(dead_code)]
fn _unused_imports_silencer(_f: &Finding) {}
