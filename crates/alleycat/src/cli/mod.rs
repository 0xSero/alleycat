//! Thin client subcommands that talk to the running daemon over the IPC
//! control socket. Each subcommand opens a fresh connection, writes a single
//! length-prefixed JSON request, reads a single response, and exits.

use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};

use crate::daemon::control::{Request, Response, StatusInfo};
use crate::framing::{read_json_frame, write_json_frame};
use crate::ipc;

pub mod agents;
pub mod local_studio;
pub mod logs;
pub mod onboarding;
pub mod pair;
pub mod probe;
pub mod reload;
pub mod rotate;
pub mod status;
pub mod stop;
pub mod upgrade;

/// Send a single request to the daemon and read back the response.
/// Errors with a friendly hint if the daemon is not running.
pub async fn send(req: Request) -> anyhow::Result<Response> {
    let mut stream = ipc::connect().await.with_context(|| {
        let name = crate::binary_name();
        format!("daemon not running. start it with `{name} serve` or `{name} install`.")
    })?;
    write_json_frame(&mut stream, &req)
        .await
        .context("writing control request")?;
    let resp: Response = read_json_frame(&mut stream)
        .await
        .context("reading control response")?;
    Ok(resp)
}

/// Decode the typed payload from a successful response. Bails with the
/// daemon-supplied error message when `ok` is false.
pub fn decode_data<T: serde::de::DeserializeOwned>(resp: Response) -> anyhow::Result<T> {
    if !resp.ok {
        return Err(anyhow!(resp.error.unwrap_or_else(|| "daemon error".into())));
    }
    let data = resp
        .data
        .ok_or_else(|| anyhow!("daemon returned empty data"))?;
    Ok(serde_json::from_value(data)?)
}

/// Bail when the daemon returned `ok=false`.
pub fn require_ok(resp: &Response) -> anyhow::Result<()> {
    if !resp.ok {
        return Err(anyhow!(
            resp.error.clone().unwrap_or_else(|| "daemon error".into())
        ));
    }
    Ok(())
}

/// Make sure a daemon of *this* binary's version is running and reachable
/// on the IPC socket. Handles every state the user might be in:
///
/// - no daemon at all              → spawn detached
/// - daemon up, version matches    → no-op
/// - daemon up, version mismatch   → gracefully restart onto current_exe
/// - daemon up, version unknown    → assume stale, restart
/// - autostart installed but wedged (launchd throttle, stale plist path) →
///   fall through to manual spawn
///
/// Called at the top of subcommands that mutate user-visible state
/// (`pair`, `rotate`) and at the top of the bare-invocation onboarding
/// flow, so users never have to know whether a daemon is up, fresh, or
/// stale — `npx kittylitter` Just Works regardless of prior state.
pub async fn ensure_current_daemon() -> anyhow::Result<()> {
    if !ipc::is_daemon_running().await {
        // Nothing listening — nothing to compare versions against. Just
        // start one. restart_daemon() handles the autostart-rewrite +
        // detached-spawn dance.
        return restart_daemon().await;
    }

    // Something is listening. Ask its version. If the IPC handshake fails
    // (socket exists but daemon is wedged), treat it as stale and bounce.
    let needs_restart = match send(Request::Status).await {
        Ok(resp) => match decode_data::<StatusInfo>(resp) {
            Ok(status) => {
                let cli_version = crate::binary_version();
                let daemon_version = status.version.as_deref().unwrap_or("<unknown>");
                if daemon_version == cli_version {
                    None
                } else {
                    Some(daemon_version.to_string())
                }
            }
            Err(_) => Some("<unparseable>".to_string()),
        },
        Err(_) => Some("<unreachable>".to_string()),
    };

    let Some(stale) = needs_restart else {
        return Ok(());
    };

    eprintln!(
        "note: daemon is v{stale} but {cli} is v{cur}; restarting daemon onto v{cur}...",
        cli = crate::binary_name(),
        cur = crate::binary_version()
    );
    restart_daemon().await
}

/// Stop any running daemon and start a fresh one from `current_exe`.
/// Public so the explicit `upgrade` subcommand can reuse the same path.
///
/// If an OS-level autostart supervisor is installed (launchd / systemd-user
/// / Windows Startup `.lnk` / XDG `.desktop`), this also rewrites that
/// entry so the supervisor stops pointing at a stale path. macOS launchd
/// and Linux systemd-user re-kickstart the daemon on their own once the
/// pointer is rewritten; Windows and XDG-fallback Linux don't, so we fall
/// back to spawning ourselves. The single-instance file lock in
/// `daemon::run` is the safety net against accidentally spawning two
/// daemons if a supervisor races us.
pub async fn restart_daemon() -> anyhow::Result<()> {
    if ipc::is_daemon_running().await {
        let _ = send(Request::Stop).await;
        wait_until(Duration::from_secs(10), || async {
            !ipc::is_daemon_running().await
        })
        .await
        .context("old daemon did not exit within 10s")?;
    }

    let supervisor = crate::service::is_installed().unwrap_or(false);
    if supervisor {
        // Re-bind the supervisor entry to current_exe. Best-effort: if the
        // user's launchd/systemd state is wedged we still want to spawn the
        // new daemon ourselves below, not bail.
        if let Err(error) = crate::service::install() {
            eprintln!(
                "warning: re-installing autostart entry failed: {error:#}; falling back to manual respawn"
            );
        }
    }

    // Give a launchd/systemd supervisor a moment to bring the daemon back
    // up via the rewritten unit. macOS kickstart usually wins inside ~1s;
    // systemd-user is similar. We poll a few seconds before falling back.
    let supervisor_started = wait_until(Duration::from_secs(5), || async {
        ipc::is_daemon_running().await
    })
    .await
    .is_ok();

    if !supervisor_started {
        spawn_serve_detached().context("spawning new daemon")?;
        wait_until(Duration::from_secs(15), || async {
            ipc::is_daemon_running().await
        })
        .await
        .context("new daemon did not come up within 15s")?;
    }

    Ok(())
}

/// Spawn `current_exe serve` as a session-detached background process so
/// it survives the parent CLI exit and any controlling-terminal hangup
/// signal (Unix SIGHUP / Windows console-close).
fn spawn_serve_detached() -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("locating current executable")?;
    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("serve")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Detach from the controlling terminal: setsid() makes the child a
        // new session leader so a closing terminal can't SIGHUP it.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // DETACHED_PROCESS: child has no console (we already redirect
        // std{in,out,err} to null and the daemon writes to file logs, so
        // no console is needed).
        // CREATE_NEW_PROCESS_GROUP: child becomes its own process-group
        // leader so a Ctrl+C / Ctrl+Break in the parent's console doesn't
        // propagate to it.
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }

    cmd.spawn().context("spawning daemon child")?;
    Ok(())
}

/// Poll `cond` every 100ms until it returns true or `deadline` elapses.
/// Returns `Ok(())` on success, `Err` with a context-friendly message
/// once the deadline trips.
async fn wait_until<F, Fut>(deadline: Duration, mut cond: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if cond().await {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(anyhow!("timed out waiting for daemon state"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
