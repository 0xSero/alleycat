//! Cross-platform filesystem path helpers for the alleycat daemon.
//!
//! Wraps `directories::ProjectDirs` so callers never see `Option<PathBuf>`
//! ambiguity. Per-OS layout:
//!
//! | concern    | macOS                                  | Linux                       | Windows                                  |
//! | ---------- | -------------------------------------- | --------------------------- | ---------------------------------------- |
//! | config     | ~/Library/Application Support/<id>/    | $XDG_CONFIG_HOME/alleycat/  | %APPDATA%\Alleycat\alleycat\config\      |
//! | state      | (collapses to config_dir)              | $XDG_STATE_HOME/alleycat/   | %LOCALAPPDATA%\Alleycat\alleycat\data\   |
//! | logs       | ~/Library/Logs/<id>/                   | <state>/logs/               | <data_local>/logs/                       |
//! | control IPC| $TMPDIR/alleycat-<hash>/control.sock   | $XDG_RUNTIME_DIR or <state> | \\.\pipe\alleycat-control-<userhash>     |
//!
//! On Unix the control socket intentionally does **not** live under
//! `state_dir()` — `sockaddr_un.sun_path` is capped at 104 bytes on macOS /
//! BSD (108 on Linux), and the natural state-dir paths blow that limit under
//! hermetic test homes (e.g. `/var/folders/.../T/...`). See
//! `control_socket_path` for the candidate-resolution order.

use std::path::PathBuf;

use anyhow::{Context, anyhow};
use directories::{BaseDirs, ProjectDirs};

// Bundle identifiers come from the [`crate::App`] the binary supplies; the
// alleycat library doesn't know whether it's being shipped as `kittylitter`,
// `alleycat`, or anything else. See `crate::App` for the defaults used when
// no `App` has been registered (tests, doc snippets).

fn project_dirs() -> anyhow::Result<ProjectDirs> {
    let app = crate::app();
    ProjectDirs::from(app.qualifier, app.organization, app.application)
        .ok_or_else(|| anyhow!("could not determine project directories (no $HOME?)"))
}

fn base_dirs() -> anyhow::Result<BaseDirs> {
    BaseDirs::new().ok_or_else(|| anyhow!("could not determine base directories (no $HOME?)"))
}

fn ensure_dir(path: &std::path::Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("creating directory {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// Per-user config directory. Created with mode 0700 on Unix.
pub fn config_dir() -> anyhow::Result<PathBuf> {
    let dirs = project_dirs()?;
    let path = dirs.config_dir().to_path_buf();
    ensure_dir(&path)?;
    Ok(path)
}

/// Per-user state directory. Holds host.key, host.lock, daemon.pid.
pub fn state_dir() -> anyhow::Result<PathBuf> {
    let dirs = project_dirs()?;
    let path = if let Some(state) = dirs.state_dir() {
        state.to_path_buf()
    } else if cfg!(target_os = "linux") {
        let base = base_dirs()?;
        let state = base
            .state_dir()
            .ok_or_else(|| anyhow!("no XDG state dir on this Linux system"))?;
        state.join(crate::app().application)
    } else if cfg!(target_os = "windows") {
        dirs.data_local_dir().to_path_buf()
    } else {
        dirs.config_dir().to_path_buf()
    };
    ensure_dir(&path)?;
    Ok(path)
}

/// Per-user log directory.
pub fn log_dir() -> anyhow::Result<PathBuf> {
    let path: PathBuf;
    #[cfg(target_os = "macos")]
    {
        let base = base_dirs()?;
        let app = crate::app();
        path = base.home_dir().join("Library/Logs").join(format!(
            "{}.{}.{}",
            app.qualifier, app.organization, app.application
        ));
    }
    #[cfg(target_os = "linux")]
    {
        path = state_dir()?.join("logs");
    }
    #[cfg(target_os = "windows")]
    {
        let dirs = project_dirs()?;
        path = dirs.data_local_dir().join("logs");
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        path = state_dir()?.join("logs");
    }
    ensure_dir(&path)?;
    Ok(path)
}

/// `<config_dir>/host.toml`.
pub fn host_config_file() -> anyhow::Result<PathBuf> {
    Ok(config_dir()?.join("host.toml"))
}

/// `<state_dir>/host.key` — 32-byte iroh secret.
pub fn host_key_file() -> anyhow::Result<PathBuf> {
    Ok(state_dir()?.join("host.key"))
}

/// `<state_dir>/host.lock` — single-instance fd lock.
pub fn host_lock_file() -> anyhow::Result<PathBuf> {
    Ok(state_dir()?.join("host.lock"))
}

/// `<state_dir>/daemon.pid` — pid of the running daemon.
pub fn daemon_pid_file() -> anyhow::Result<PathBuf> {
    Ok(state_dir()?.join("daemon.pid"))
}

/// `<state_dir>/paired-nodes.json` — capability grants keyed by the
/// authenticated remote Iroh endpoint ID.
pub fn paired_nodes_file() -> anyhow::Result<PathBuf> {
    Ok(state_dir()?.join("paired-nodes.json"))
}

/// `sockaddr_un.sun_path` size on Darwin/BSD (104) and Linux (108). Minus the
/// trailing NUL the kernel needs.
#[cfg(target_os = "linux")]
const SUN_PATH_MAX: usize = 108 - 1;
#[cfg(not(target_os = "linux"))]
const SUN_PATH_MAX: usize = 104 - 1;

/// Unix domain socket path the daemon listens on. Errors on Windows — call
/// [`control_pipe_name`] there instead.
pub fn control_socket_path() -> anyhow::Result<PathBuf> {
    #[cfg(unix)]
    {
        let base = base_dirs()?;
        let home = base.home_dir().to_string_lossy().to_string();
        let user = short_user_hash(&home);
        let dir = control_socket_dir(&user)?;
        ensure_dir(&dir)?;
        let sock = dir.join("control.sock");
        let len = sock.as_os_str().len();
        if len > SUN_PATH_MAX {
            return Err(anyhow!(
                "control socket path is {len} bytes, exceeds sockaddr_un limit of {SUN_PATH_MAX}: {}",
                sock.display()
            ));
        }
        Ok(sock)
    }
    #[cfg(not(unix))]
    {
        Err(anyhow!(
            "control_socket_path() is unix-only; use control_pipe_name() on this platform"
        ))
    }
}

/// Pick the parent directory for the control socket. Tries, in order:
///
/// 1. `$XDG_RUNTIME_DIR/alleycat-<userhash>` (Linux ideal — tmpfs, per-user)
/// 2. `<state_dir>/alleycat-<userhash>` (Linux fallback)
/// 3. `$TMPDIR/alleycat-<userhash>` (macOS / BSD)
/// 4. `/tmp/alleycat-<userhash>` (last resort)
///
/// First candidate whose `<dir>/control.sock` fits in `SUN_PATH_MAX` wins.
#[cfg(unix)]
fn control_socket_dir(user: &str) -> anyhow::Result<PathBuf> {
    let segment = format!("{app}-{user}", app = crate::app().application);
    let mut candidates: Vec<PathBuf> = Vec::new();

    #[cfg(target_os = "linux")]
    {
        if let Some(rt) = std::env::var_os("XDG_RUNTIME_DIR") {
            let rt = PathBuf::from(rt);
            if rt.is_absolute() {
                candidates.push(rt.join(&segment));
            }
        }
        if let Ok(state) = state_dir() {
            candidates.push(state.join(&segment));
        }
    }

    if let Some(tmp) = std::env::var_os("TMPDIR") {
        let tmp = PathBuf::from(tmp);
        if tmp.is_absolute() {
            candidates.push(tmp.join(&segment));
        }
    }
    candidates.push(PathBuf::from("/tmp").join(&segment));

    let sock_overhead = "/control.sock".len();
    for cand in &candidates {
        let total = cand.as_os_str().len() + sock_overhead;
        if total <= SUN_PATH_MAX {
            return Ok(cand.clone());
        }
    }
    Err(anyhow!(
        "no candidate directory yields a control-socket path within the {SUN_PATH_MAX}-byte sockaddr_un limit; tried: {candidates:?}"
    ))
}

/// Windows named-pipe name for the control IPC.
#[allow(dead_code)] // used only on windows builds; helper kept callable everywhere
pub fn control_pipe_name() -> anyhow::Result<String> {
    #[cfg(windows)]
    {
        let base = base_dirs()?;
        let home = base.home_dir().to_string_lossy().to_string();
        let hash = short_user_hash(&home);
        Ok(format!(
            r"\\.\pipe\{app}-control-{hash}",
            app = crate::app().application
        ))
    }
    #[cfg(not(windows))]
    {
        Err(anyhow!(
            "control_pipe_name() is windows-only; use control_socket_path() on this platform"
        ))
    }
}

fn short_user_hash(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(input.as_bytes());
    hex::encode(&digest[..8])
}

/// `~/Library/LaunchAgents/dev.alleycat.alleycat.plist` on macOS.
pub fn launchd_plist_path() -> anyhow::Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let base = base_dirs()?;
        Ok(base
            .home_dir()
            .join("Library/LaunchAgents")
            .join(format!("{label}.plist", label = crate::app().label)))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err(anyhow!("launchd_plist_path() is macOS-only"))
    }
}

/// `~/.config/systemd/user/alleycat.service` on Linux.
#[allow(dead_code)]
pub fn systemd_unit_path() -> anyhow::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let base = base_dirs()?;
        Ok(base
            .config_dir()
            .join("systemd")
            .join("user")
            .join(format!("{app}.service", app = crate::app().application)))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(anyhow!("systemd_unit_path() is Linux-only"))
    }
}

/// `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\alleycat.lnk`.
#[allow(dead_code)]
pub fn windows_startup_lnk_path() -> anyhow::Result<PathBuf> {
    #[cfg(windows)]
    {
        let base = base_dirs()?;
        Ok(base
            .config_dir()
            .join(r"Microsoft\Windows\Start Menu\Programs\Startup")
            .join(format!("{app}.lnk", app = crate::app().application)))
    }
    #[cfg(not(windows))]
    {
        Err(anyhow!("windows_startup_lnk_path() is Windows-only"))
    }
}

/// `~/.config/autostart/alleycat.desktop` — fallback when systemctl --user is
/// unavailable.
#[cfg(target_os = "linux")]
pub fn xdg_autostart_path() -> anyhow::Result<PathBuf> {
    let base = base_dirs()?;
    Ok(base
        .config_dir()
        .join("autostart")
        .join(format!("{app}.desktop", app = crate::app().application)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempHome;

    #[test]
    fn config_dir_under_temp_home() {
        let h = TempHome::new();
        let cfg = config_dir().expect("config_dir");
        assert!(
            cfg.starts_with(h.path()),
            "{} should be under {}",
            cfg.display(),
            h.path().display()
        );
        assert!(cfg.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700);
        }
    }

    #[test]
    fn host_config_file_lives_under_config_dir() {
        let _h = TempHome::new();
        let cf = host_config_file().unwrap();
        assert_eq!(cf.file_name().unwrap(), "host.toml");
        assert!(cf.starts_with(config_dir().unwrap()));
    }

    #[test]
    fn host_key_lives_under_state_dir() {
        let _h = TempHome::new();
        let kf = host_key_file().unwrap();
        assert_eq!(kf.file_name().unwrap(), "host.key");
        assert!(kf.starts_with(state_dir().unwrap()));
    }

    #[test]
    fn paired_nodes_file_lives_under_state_dir() {
        let _h = TempHome::new();
        let grants = paired_nodes_file().unwrap();
        assert_eq!(grants.file_name().unwrap(), "paired-nodes.json");
        assert!(grants.starts_with(state_dir().unwrap()));
    }

    #[cfg(unix)]
    #[test]
    fn control_socket_fits_sockaddr_un_limit() {
        use std::os::unix::fs::PermissionsExt;
        let _h = TempHome::new();
        let sock = control_socket_path().unwrap();
        let len = sock.as_os_str().len();
        assert!(len <= SUN_PATH_MAX, "{len} > {SUN_PATH_MAX}");
        assert_eq!(sock.file_name().unwrap(), "control.sock");
        let parent = sock.parent().unwrap();
        assert!(parent.is_dir());
        let mode = std::fs::metadata(parent).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn control_socket_includes_userhash_segment() {
        let h1 = TempHome::new();
        let s1 = control_socket_path().unwrap();
        drop(h1);
        let h2 = TempHome::new();
        let s2 = control_socket_path().unwrap();
        drop(h2);
        assert_ne!(s1.parent().unwrap(), s2.parent().unwrap());
        for s in [&s1, &s2] {
            let dir = s.parent().unwrap().file_name().unwrap().to_string_lossy();
            assert!(dir.starts_with("alleycat-"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn control_socket_under_pathological_long_home() {
        let mut h = TempHome::new();
        let long_tmp = h
            .path()
            .join("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbbbbbb/cccc");
        std::fs::create_dir_all(&long_tmp).unwrap();
        h.override_env(&[("TMPDIR", long_tmp.to_str().unwrap())]);
        let sock = control_socket_path().unwrap();
        assert!(sock.as_os_str().len() <= SUN_PATH_MAX);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn control_socket_prefers_xdg_runtime_dir_when_set() {
        let mut h = TempHome::new();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let runtime = PathBuf::from(format!("/tmp/alleycat-rt-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&runtime).unwrap();
        h.override_env(&[("XDG_RUNTIME_DIR", runtime.to_str().unwrap())]);
        let sock = control_socket_path().unwrap();
        assert!(sock.starts_with(&runtime));
        let _ = std::fs::remove_dir_all(runtime);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_path_label() {
        let _h = TempHome::new();
        let p = launchd_plist_path().unwrap();
        assert_eq!(p.file_name().unwrap(), "dev.alleycat.alleycat.plist");
        assert!(p.to_string_lossy().contains("Library/LaunchAgents"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_unit_path_endings() {
        let _h = TempHome::new();
        let p = systemd_unit_path().unwrap();
        assert!(
            p.to_string_lossy()
                .ends_with("systemd/user/alleycat.service")
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_startup_lnk_path_endings() {
        let _h = TempHome::new();
        let p = windows_startup_lnk_path().unwrap();
        let s = p.to_string_lossy().to_string();
        assert!(s.ends_with(r"Startup\alleycat.lnk"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_state_collapses_to_config() {
        let _h = TempHome::new();
        assert_eq!(state_dir().unwrap(), config_dir().unwrap());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_log_dir_under_library_logs() {
        let _h = TempHome::new();
        let p = log_dir().unwrap();
        let s = p.to_string_lossy();
        assert!(s.contains("Library/Logs/dev.Alleycat.alleycat"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_state_under_xdg_state_home() {
        let mut h = TempHome::new();
        let state_home = h.path().join("xdg-state");
        std::fs::create_dir_all(&state_home).unwrap();
        h.override_env(&[("XDG_STATE_HOME", state_home.to_str().unwrap())]);
        let s = state_dir().unwrap();
        assert!(s.starts_with(&state_home));
        let logs = log_dir().unwrap();
        assert!(logs.starts_with(&s));
        assert_eq!(logs.file_name().unwrap(), "logs");
    }
}
