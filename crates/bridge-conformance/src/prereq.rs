//! Per-target prerequisite probes. Each test asks `check(target)` before
//! spawning anything; if the prereq isn't met we print a skip reason and
//! return `None` so the test passes silently. Same shape as
//! `crates/pi-bridge/tests/v8_live_pi.rs::check_prereqs`.

use std::env;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::TargetId;

#[derive(Debug)]
pub enum Prereq {
    Codex { bin: PathBuf },
    Pi { bin: PathBuf },
    Amp { bin: PathBuf },
    Claude { bin: PathBuf },
    Opencode { bin: PathBuf },
    Droid { bin: PathBuf },
    Hermes { bin: PathBuf },
    Acp { bin: PathBuf },
}

#[derive(Debug)]
pub enum SkipReason {
    Reason(String),
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::Reason(s) => f.write_str(s),
        }
    }
}

pub async fn check(target: TargetId) -> Result<Prereq, SkipReason> {
    match target {
        TargetId::Codex => check_codex(),
        TargetId::Pi => check_pi(),
        TargetId::Amp => check_amp(),
        TargetId::Claude => check_claude(),
        TargetId::Opencode => check_opencode(),
        TargetId::Droid => check_droid(),
        TargetId::Hermes => check_hermes(),
        TargetId::Acp => check_acp(),
    }
}

fn check_codex() -> Result<Prereq, SkipReason> {
    // Spawn `codex app-server` ourselves over stdio — no port to fight over
    // and the wire shape is the same JSON-RPC the bridges already speak.
    let bin = which_or_env("CODEX_BIN", "codex")
        .ok_or_else(|| SkipReason::Reason("codex not on PATH".into()))?;
    Ok(Prereq::Codex { bin })
}

fn check_pi() -> Result<Prereq, SkipReason> {
    if let Some(bin) = env_file("PI_BRIDGE_PI_BIN") {
        check_pi_oneshot(&bin)?;
        return Ok(Prereq::Pi { bin });
    }

    let candidates = pi_candidates();
    let mut failures = Vec::new();
    for bin in candidates {
        match check_pi_oneshot(&bin) {
            Ok(()) => return Ok(Prereq::Pi { bin }),
            Err(SkipReason::Reason(reason)) => {
                failures.push(format!("{}: {reason}", bin.display()));
            }
        }
    }

    if failures.is_empty() {
        Err(SkipReason::Reason(
            "pi-coding-agent/pi not found; set PI_BRIDGE_PI_BIN to the Pi CLI".into(),
        ))
    } else {
        Err(SkipReason::Reason(format!(
            "no usable Pi CLI found; tried {}",
            failures.join("; ")
        )))
    }
}

fn check_amp() -> Result<Prereq, SkipReason> {
    let bin = which_or_env("AMP_BRIDGE_AMP_BIN", "amp")
        .ok_or_else(|| SkipReason::Reason("amp not on PATH".into()))?;
    if has_amp_auth() {
        Ok(Prereq::Amp { bin })
    } else {
        Err(SkipReason::Reason(
            "amp auth unavailable: set AMP_API_KEY or run amp login".into(),
        ))
    }
}

fn check_claude() -> Result<Prereq, SkipReason> {
    let bin = which_or_env("CLAUDE_BRIDGE_CLAUDE_BIN", "claude")
        .ok_or_else(|| SkipReason::Reason("claude not on PATH".into()))?;
    Ok(Prereq::Claude { bin })
}

fn check_opencode() -> Result<Prereq, SkipReason> {
    let bin = which_or_env("OPENCODE_BRIDGE_BIN", "opencode")
        .ok_or_else(|| SkipReason::Reason("opencode not on PATH".into()))?;
    Ok(Prereq::Opencode { bin })
}

fn check_droid() -> Result<Prereq, SkipReason> {
    let bin = which_or_env("DROID_BRIDGE_DROID_BIN", "droid")
        .ok_or_else(|| SkipReason::Reason("droid not on PATH".into()))?;
    if has_factory_auth() {
        Ok(Prereq::Droid { bin })
    } else {
        Err(SkipReason::Reason(
            "droid auth unavailable: set FACTORY_API_KEY or run droid login".into(),
        ))
    }
}

fn check_hermes() -> Result<Prereq, SkipReason> {
    let bin = which_or_env("HERMES_BRIDGE_BIN", "hermes")
        .ok_or_else(|| SkipReason::Reason("hermes not on PATH; set HERMES_BRIDGE_BIN".into()))?;
    check_hermes_oneshot(&bin)?;
    Ok(Prereq::Hermes { bin })
}

fn check_hermes_oneshot(bin: &PathBuf) -> Result<(), SkipReason> {
    let mut child = Command::new(bin)
        .arg("-z")
        .arg("Reply with exactly OK and nothing else.")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            SkipReason::Reason(format!("hermes oneshot smoke probe failed to spawn: {err}"))
        })?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child.wait_with_output().map_err(|err| {
                    SkipReason::Reason(format!(
                        "hermes oneshot smoke probe failed to collect output: {err}"
                    ))
                })?;
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !output.status.success() {
                    return Err(SkipReason::Reason(format!(
                        "hermes oneshot smoke probe exited with {}: {}",
                        output.status,
                        stderr.trim()
                    )));
                }
                if stdout.trim().is_empty() {
                    return Err(SkipReason::Reason(
                        "hermes oneshot smoke probe produced empty stdout; run `hermes -z 'Reply with exactly OK and nothing else.'` and check Hermes auth/backend credentials".into(),
                    ));
                }
                return Ok(());
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(SkipReason::Reason(
                    "hermes oneshot smoke probe timed out after 20s".into(),
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(SkipReason::Reason(format!(
                    "hermes oneshot smoke probe failed: {err}"
                )));
            }
        }
    }
}

fn check_pi_oneshot(bin: &PathBuf) -> Result<(), SkipReason> {
    let mut child = Command::new(bin)
        .arg("-p")
        .arg("--no-session")
        .arg("Reply with exactly OK and nothing else.")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| SkipReason::Reason(format!("pi smoke probe failed to spawn: {err}")))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child.wait_with_output().map_err(|err| {
                    SkipReason::Reason(format!("pi smoke probe failed to collect output: {err}"))
                })?;
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !output.status.success() {
                    return Err(SkipReason::Reason(format!(
                        "pi smoke probe exited with {}: {}",
                        output.status,
                        stderr.trim()
                    )));
                }
                if stdout.trim() != "OK" {
                    return Err(SkipReason::Reason(format!(
                        "pi smoke probe produced unexpected stdout: {:?}",
                        stdout.trim()
                    )));
                }
                return Ok(());
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(SkipReason::Reason(
                    "pi smoke probe timed out after 30s".into(),
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(SkipReason::Reason(format!("pi smoke probe failed: {err}")));
            }
        }
    }
}

fn check_acp() -> Result<Prereq, SkipReason> {
    // Support explicit path via env (e.g. ACP_BRIDGE_AGENT_BIN=/Users/.../grok)
    if let Some(p) = std::env::var_os("ACP_BRIDGE_AGENT_BIN")
        && !p.is_empty()
    {
        let path = std::path::PathBuf::from(&p);
        if path.exists() {
            return Ok(Prereq::Acp { bin: path });
        }
    }

    // Fallback to looking for "devin" on PATH (original behavior)
    match which::which("devin") {
        Ok(bin) => Ok(Prereq::Acp { bin }),
        Err(_) => Err(SkipReason::Reason(
            "ACP agent (devin, grok, etc.) not found. Set ACP_BRIDGE_AGENT_BIN to an absolute path.".into(),
        )),
    }
}

fn which_or_env(env_var: &str, bin_name: &str) -> Option<PathBuf> {
    if let Some(p) = env::var_os(env_var)
        && !p.is_empty()
    {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }
    which::which(bin_name).ok()
}

fn pi_candidates() -> Vec<PathBuf> {
    let mut bins = Vec::new();
    push_candidate(&mut bins, home_file(".bun/bin/pi"));
    push_candidate(&mut bins, which::which("pi").ok());
    push_candidate(&mut bins, which::which("pi-coding-agent").ok());
    bins
}

fn push_candidate(candidates: &mut Vec<PathBuf>, path: Option<PathBuf>) {
    let Some(path) = path else {
        return;
    };
    if path.is_file() && !candidates.iter().any(|existing| existing == &path) {
        candidates.push(path);
    }
}

fn env_file(env_var: &str) -> Option<PathBuf> {
    let p = env::var_os(env_var)?;
    if p.is_empty() {
        return None;
    }
    let path = PathBuf::from(p);
    path.is_file().then_some(path)
}

fn has_factory_auth() -> bool {
    if env::var_os("FACTORY_API_KEY").is_some() {
        return true;
    }
    let Some(home) = env::var_os("HOME") else {
        return false;
    };
    let factory_dir = PathBuf::from(home).join(".factory");
    factory_dir.join("auth.encrypted").is_file()
        || (factory_dir.join("auth.v2.file").is_file() && factory_dir.join("auth.v2.key").is_file())
}

fn has_amp_auth() -> bool {
    if env::var_os("AMP_API_KEY").is_some() {
        return true;
    }
    let Some(home) = env::var_os("HOME") else {
        return false;
    };
    let home = PathBuf::from(home);
    home.join(".amp/oauth").is_dir()
        || (home.join(".local/share/amp/session.json").is_file()
            && home.join(".local/share/amp/secrets.json").is_file())
}

fn home_file(relative: &str) -> Option<PathBuf> {
    let home = env::var_os("HOME")?;
    let path = PathBuf::from(home).join(relative);
    path.is_file().then_some(path)
}
