use crate::cli;
use crate::daemon::control::Request;
use crate::ipc;
use crate::service;
use crate::state;

pub async fn run() -> anyhow::Result<()> {
    if ipc::is_daemon_running().await {
        let resp = cli::send(Request::Stop).await?;
        cli::require_ok(&resp)?;
        println!("daemon stopping.");
        warn_if_autostart_installed();
        return Ok(());
    }

    // IPC isn't reachable — the socket may have been removed (e.g. the
    // daemon's accept loop aborted but the process is still alive holding
    // the lock). Fall back to SIGTERM-by-pid-file so the user has a CLI
    // escape hatch.
    let pid = state::read_pid_file()?.ok_or_else(|| {
        anyhow::anyhow!(
            "daemon not running (no control socket and no pid file at {})",
            crate::paths::daemon_pid_file()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "<unknown>".to_string())
        )
    })?;

    if !pid_alive(pid) {
        // Stale pid file — treat as "nothing to stop" and report.
        println!("daemon not running; clearing stale pid file ({pid}).");
        let _ = std::fs::remove_file(crate::paths::daemon_pid_file()?);
        return Ok(());
    }

    send_sigterm(pid)?;
    println!("daemon not responding on control socket; sent SIGTERM to pid {pid}.");
    warn_if_autostart_installed();
    Ok(())
}

/// If the OS-level autostart entry is installed, the supervisor (launchd /
/// systemd) will respawn the daemon within seconds of `stop`. Surface that
/// so users aren't surprised by uptime=2s on the next `status`.
fn warn_if_autostart_installed() {
    if let Ok(true) = service::is_installed() {
        let name = crate::binary_name();
        eprintln!(
            "note: autostart is installed; the daemon will be restarted by the OS. \
             run `{name} uninstall` to disable autostart."
        );
    }
}

#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // kill(pid, 0) returns 0 if the process exists and we can signal it.
    // ESRCH (no such process) means dead.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
fn pid_alive(_pid: u32) -> bool {
    true
}

#[cfg(unix)]
fn send_sigterm(pid: u32) -> anyhow::Result<()> {
    let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .map_err(|e| anyhow::anyhow!("kill({pid}, SIGTERM) failed: {e}"));
    }
    Ok(())
}

#[cfg(not(unix))]
fn send_sigterm(_pid: u32) -> anyhow::Result<()> {
    Err(anyhow::anyhow!(
        "pid-based fallback shutdown is not supported on this platform; \
         the control socket was unreachable so there is no way to ask the \
         daemon to stop"
    ))
}
