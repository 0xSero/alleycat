//! launchd user-agent install for macOS. Writes a plist under
//! `~/Library/LaunchAgents/` and bootstraps it into `gui/$UID`. No admin.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, anyhow};

use crate::paths;
use crate::service::{DAEMON_SUBCOMMAND, service_label};

pub(super) fn install() -> anyhow::Result<()> {
    let plist_path = paths::launchd_plist_path()?;
    let exe = std::env::current_exe().context("resolving current executable for launchd plist")?;
    let inherit_path = std::env::var("PATH").ok();
    let inherit_shell = std::env::var("SHELL").ok();

    write_plist(
        &plist_path,
        &exe,
        inherit_path.as_deref(),
        inherit_shell.as_deref(),
    )?;

    let uid = current_uid();
    let domain_target = format!("gui/{uid}/{label}", label = service_label());
    let plist_str = plist_path
        .to_str()
        .ok_or_else(|| anyhow!("plist path is not valid UTF-8"))?;

    let _ = Command::new("launchctl")
        .args(["bootout", &domain_target])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    // launchd retains in-memory state for a recently-unloaded service for a
    // brief window after `bootout`. Bootstrapping the same label too quickly
    // returns "Bootstrap failed: 5: Input/output error". Poll `launchctl
    // print` until the service is gone before bootstrapping.
    wait_until_unloaded(&domain_target, std::time::Duration::from_secs(5));

    // Try bootstrap up to 3 times, sleeping briefly on EIO. The race window
    // is usually <1s but launchd state can be flaky on macOS Sequoia.
    let mut last_status: Option<std::process::ExitStatus> = None;
    for attempt in 0..3 {
        let bootstrap = Command::new("launchctl")
            .args(["bootstrap", &format!("gui/{uid}"), plist_str])
            .status()
            .context("running launchctl bootstrap")?;
        if bootstrap.success() {
            last_status = Some(bootstrap);
            break;
        }
        last_status = Some(bootstrap);
        if attempt < 2 {
            std::thread::sleep(std::time::Duration::from_millis(500 * (attempt as u64 + 1)));
            // Re-poll: the previous bootstrap may have left half-state.
            wait_until_unloaded(&domain_target, std::time::Duration::from_secs(2));
        }
    }
    match last_status {
        Some(s) if s.success() => {}
        Some(s) => {
            return Err(anyhow!(
                "launchctl bootstrap failed after retries (exit {:?})",
                s.code()
            ));
        }
        None => unreachable!("loop guarantees at least one attempt"),
    }

    let _ = Command::new("launchctl")
        .args(["enable", &domain_target])
        .status();

    let _ = Command::new("launchctl")
        .args(["kickstart", &domain_target])
        .status();

    Ok(())
}

/// Block until `launchctl print <target>` reports the service is gone, or
/// `deadline` elapses. launchd returns non-zero when the target doesn't
/// exist, so a non-zero status means the prior bootout has settled.
fn wait_until_unloaded(domain_target: &str, deadline: std::time::Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        let still_loaded = Command::new("launchctl")
            .args(["print", domain_target])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !still_loaded {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

pub(super) fn uninstall() -> anyhow::Result<()> {
    let plist_path = paths::launchd_plist_path()?;
    let uid = current_uid();
    let _ = Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{uid}/{label}", label = service_label()),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if plist_path.exists() {
        std::fs::remove_file(&plist_path)
            .with_context(|| format!("removing {}", plist_path.display()))?;
    }
    Ok(())
}

pub(super) fn write_plist(
    plist_path: &Path,
    exe: &Path,
    inherit_path: Option<&str>,
    inherit_shell: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(parent) = plist_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    // Explicit service overrides must survive upgrades and `restart`, which
    // reinstall this plist. Never copy the caller's whole environment.
    let mut environment = read_plist_environment(plist_path)?;
    if let Some(path) = inherit_path {
        environment.insert("PATH".to_owned(), path.to_owned());
    }
    if let Some(shell) = inherit_shell {
        environment.insert("SHELL".to_owned(), shell.to_owned());
    }
    let body = render_plist(exe, &environment);
    let tmp = plist_path.with_extension("plist.tmp");
    std::fs::write(&tmp, body.as_bytes()).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, plist_path)
        .with_context(|| format!("renaming into {}", plist_path.display()))?;
    Ok(())
}

fn read_plist_environment(plist_path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    if !plist_path
        .try_exists()
        .context("checking existing launchd plist")?
    {
        return Ok(BTreeMap::new());
    }
    // plutil handles both XML and binary native plists. Keep its output out of
    // diagnostics: an existing service environment may contain credentials.
    let output = Command::new("/usr/bin/plutil")
        .args(["-convert", "json", "-o", "-"])
        .arg(plist_path)
        .output()
        .context("reading existing launchd plist")?;
    anyhow::ensure!(output.status.success(), "existing launchd plist is invalid");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|_| anyhow!("existing launchd plist is not a valid object"))?;
    let root = value
        .as_object()
        .ok_or_else(|| anyhow!("existing launchd plist is not an object"))?;
    match root.get("EnvironmentVariables") {
        None => Ok(BTreeMap::new()),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|_| anyhow!("existing launchd environment must contain only strings")),
    }
}

fn render_plist(exe: &Path, environment: &BTreeMap<String, String>) -> String {
    let label = service_label();
    let exe = xml_escape(&exe.to_string_lossy());
    // Normal daemon tracing has its own capped writer. Raw bootstrap/panic
    // output must not accumulate across launchd crash-loop restarts.
    // launchd sanitizes PATH to /usr/bin:/bin:/usr/sbin:/sbin by default,
    // which makes `which::which` fail for tools installed under ~/.bun/bin,
    // ~/.opencode/bin, /opt/homebrew/bin, etc. Inheriting the install-time
    // PATH preserves the user's expectation that "opencode" / "pi" resolve
    // the same way they do in the shell that ran `alleycat install`. SHELL is
    // safe to persist and lets the launch-environment resolver choose fish,
    // zsh, bash, or sh the same way the user does.
    let mut env_entries = String::new();
    for (key, value) in environment {
        env_entries.push_str(&format!(
            "        <key>{}</key>\n        <string>{}</string>\n",
            xml_escape(key),
            xml_escape(value)
        ));
    }
    let env_block = if env_entries.is_empty() {
        String::new()
    } else {
        format!("    <key>EnvironmentVariables</key>\n    <dict>\n{env_entries}    </dict>\n")
    };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTD/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>{DAEMON_SUBCOMMAND}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
{env_block}    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/dev/null</string>
</dict>
</plist>
"#
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn current_uid() -> u32 {
    unsafe { libc::getuid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tempdir() -> PathBuf {
        // Other tests temporarily point TMPDIR inside a disposable TempHome.
        let _guard = crate::test_support::lock_env();
        let mut path = std::env::temp_dir();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        path.push(format!("alleycat-svc-macos-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir");
        path
    }

    #[test]
    fn write_plist_renders_expected_keys() {
        let tmp = tempdir();
        let plist = tmp.join("dev.alleycat.alleycat.plist");
        let exe = PathBuf::from("/usr/local/bin/alleycat");
        write_plist(&plist, &exe, None, None).expect("write_plist");
        let body = std::fs::read_to_string(&plist).expect("read plist");
        assert!(body.contains("<string>dev.alleycat.alleycat</string>"));
        assert!(body.contains("<string>/usr/local/bin/alleycat</string>"));
        assert!(body.contains(&format!("<string>{DAEMON_SUBCOMMAND}</string>")));
        assert!(body.contains("<key>RunAtLoad</key>"));
        assert!(body.contains("<key>KeepAlive</key>"));
        assert!(
            !body.contains("<key>EnvironmentVariables</key>"),
            "no inherit_path → no env block"
        );
        assert!(body.contains("<key>StandardOutPath</key>\n    <string>/dev/null</string>"));
        assert!(body.contains("<key>StandardErrorPath</key>\n    <string>/dev/null</string>"));
        assert!(!body.contains("service-startup.log"));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn write_plist_includes_environment_when_inherit_path_set() {
        let tmp = tempdir();
        let plist = tmp.join("dev.alleycat.alleycat.plist");
        let exe = PathBuf::from("/usr/local/bin/alleycat");
        write_plist(
            &plist,
            &exe,
            Some("/Users/me/.bun/bin:/opt/homebrew/bin:/usr/bin:/bin"),
            Some("/opt/homebrew/bin/fish"),
        )
        .expect("write_plist");
        let body = std::fs::read_to_string(&plist).expect("read plist");
        assert!(body.contains("<key>EnvironmentVariables</key>"));
        assert!(body.contains("<key>PATH</key>"));
        assert!(body.contains("/Users/me/.bun/bin:/opt/homebrew/bin:/usr/bin:/bin"));
        assert!(body.contains("<key>SHELL</key>"));
        assert!(body.contains("/opt/homebrew/bin/fish"));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn reinstall_preserves_explicit_environment_and_updates_executable() {
        let tmp = tempdir();
        let plist = tmp.join("service.plist");
        let environment = BTreeMap::from([
            ("PATH".to_owned(), "/old/bin".to_owned()),
            ("SHELL".to_owned(), "/bin/zsh".to_owned()),
            (
                "LOCAL_STUDIO_DATA_DIR".to_owned(),
                "/Users/me/Library/Application Support/Local Studio".to_owned(),
            ),
            ("CUSTOM<&".to_owned(), "preserve <&> exactly".to_owned()),
        ]);
        std::fs::write(
            &plist,
            render_plist(Path::new("/old/kittylitter"), &environment).replace(
                "/dev/null",
                &tmp.join("service-startup.log").to_string_lossy(),
            ),
        )
        .unwrap();
        let old_log = tmp.join("service-startup.log");
        std::fs::write(&old_log, "previous startup diagnostics").unwrap();
        // Native binary plists are supported as well as our generated XML.
        assert!(
            Command::new("/usr/bin/plutil")
                .args(["-convert", "binary1"])
                .arg(&plist)
                .status()
                .unwrap()
                .success()
        );
        write_plist(
            &plist,
            Path::new("/new/kittylitter"),
            Some("/new/bin"),
            None,
        )
        .unwrap();
        let mut expected = environment;
        expected.insert("PATH".to_owned(), "/new/bin".to_owned());
        assert_eq!(read_plist_environment(&plist).unwrap(), expected);
        let body = std::fs::read_to_string(&plist).unwrap();
        assert_eq!(body.matches("<string>/dev/null</string>").count(), 2);
        assert_eq!(
            std::fs::read_to_string(old_log).unwrap(),
            "previous startup diagnostics"
        );
        assert!(
            std::fs::read_to_string(&plist)
                .unwrap()
                .contains("<string>/new/kittylitter</string>")
        );
        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn reinstall_rejects_malformed_plist_without_overwriting() {
        let tmp = tempdir();
        let plist = tmp.join("service.plist");
        for contents in [
            "not a plist",
            r#"<?xml version="1.0"?><plist version="1.0"><dict><key>EnvironmentVariables</key><dict><key>BAD</key><integer>7</integer></dict></dict></plist>"#,
        ] {
            std::fs::write(&plist, contents).unwrap();
            assert!(
                write_plist(&plist, Path::new("/new/kittylitter"), Some("/bin"), None).is_err()
            );
            assert_eq!(std::fs::read_to_string(&plist).unwrap(), contents);
        }
        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn xml_escape_handles_specials() {
        assert_eq!(xml_escape("a&b<c>d"), "a&amp;b&lt;c&gt;d");
    }
}
