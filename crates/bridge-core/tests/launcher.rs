//! Smoke tests for `LocalLauncher`. The trait shape is exercised through the
//! local impl — bridge crates are responsible for testing their own
//! launcher-driven spawn paths.

use alleycat_bridge_core::{LocalLauncher, ProcessLauncher, ProcessRole, ProcessSpec, StdioMode};
use std::ffi::OsString;
use tokio::io::AsyncReadExt;

fn shell_spec(script: &str) -> ProcessSpec {
    let (program, flag) = if cfg!(windows) {
        ("cmd", "/C")
    } else {
        ("/bin/sh", "-c")
    };
    ProcessSpec {
        role: ProcessRole::ToolCommand,
        program: program.into(),
        args: vec![OsString::from(flag), OsString::from(script)],
        cwd: None,
        env: Vec::new(),
        env_clear: false,
        stdin: StdioMode::Null,
        stdout: StdioMode::Piped,
        stderr: StdioMode::Null,
    }
}

#[tokio::test]
async fn local_launcher_round_trip_stdout() {
    let launcher = LocalLauncher::new();
    let mut child = launcher
        .launch(shell_spec("echo hello"))
        .await
        .expect("spawn");

    let mut stdout = child.take_stdout().expect("stdout pipe");
    let mut buf = String::new();
    stdout.read_to_string(&mut buf).await.expect("read stdout");

    let status = child.wait().await.expect("wait");
    assert!(status.success(), "child exited with {status:?}");

    let trimmed = buf.trim_end_matches(['\n', '\r']);
    assert_eq!(trimmed, "hello");
}

#[tokio::test]
async fn local_launcher_kill_terminates_long_running_child() {
    // Pick something that sleeps long enough to outlast the test.
    let script = if cfg!(windows) {
        // `timeout` on Windows blocks; use ping as a portable sleep.
        "ping -n 60 127.0.0.1 > NUL"
    } else {
        "sleep 60"
    };
    let launcher = LocalLauncher::new();
    let mut child = launcher.launch(shell_spec(script)).await.expect("spawn");
    assert!(child.id().is_some());

    child.kill().await.expect("kill");
    let status = child.wait().await.expect("wait");
    // Killed processes report non-success on every supported platform.
    assert!(
        !status.success(),
        "killed child reported success: {status:?}"
    );
}

#[tokio::test]
async fn local_launcher_passes_env_to_child() {
    let mut spec = shell_spec(if cfg!(windows) {
        "echo %LAUNCHER_TEST_VAR%"
    } else {
        "printf %s \"$LAUNCHER_TEST_VAR\""
    });
    spec.env.push((
        OsString::from("LAUNCHER_TEST_VAR"),
        OsString::from("from-launcher"),
    ));

    let launcher = LocalLauncher::new();
    let mut child = launcher.launch(spec).await.expect("spawn");
    let mut stdout = child.take_stdout().expect("stdout pipe");
    let mut buf = String::new();
    stdout.read_to_string(&mut buf).await.expect("read stdout");
    let _ = child.wait().await;

    assert!(
        buf.trim_end().ends_with("from-launcher"),
        "expected child to echo env var, got {buf:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn local_launcher_can_clear_parent_env() {
    if std::env::var_os("HOME").is_none() {
        return;
    }

    let mut spec = shell_spec("printf %s \"${HOME-unset}\"");
    spec.env_clear = true;

    let launcher = LocalLauncher::new();
    let mut child = launcher.launch(spec).await.expect("spawn");
    let mut stdout = child.take_stdout().expect("stdout pipe");
    let mut buf = String::new();
    stdout.read_to_string(&mut buf).await.expect("read stdout");
    let _ = child.wait().await;

    assert_eq!(buf, "unset");
}
