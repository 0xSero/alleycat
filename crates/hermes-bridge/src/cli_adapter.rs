//! Hermes CLI fallback adapter.
//!
//! When the Hermes gateway API is unavailable, this adapter spawns a `hermes`
//! binary from PATH and communicates over stdio. It tries to parse structured
//! output from the CLI when possible, falling back to treating the entire
//! output as a single text message.

use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::process::Command;
use tracing::warn;

/// Configuration for CLI fallback mode.
#[derive(Debug, Clone)]
pub struct CliConfig {
    /// Path or name of the `hermes` binary.
    pub bin: String,
    /// Working directory for spawned processes.
    pub cwd: Option<PathBuf>,
}

/// A spawned Hermes CLI process for a single turn.
pub struct HermesCliProcess {
    /// The `hermes` child process.
    child: tokio::process::Child,
}

impl HermesCliProcess {
    /// Spawn a `hermes` process for a turn.
    ///
    /// Uses argv-style spawning (no shell interpolation) and inherits
    /// environment variables. The process is killed on drop.
    pub async fn spawn(
        config: &CliConfig,
        prompt: &str,
        session_id: Option<&str>,
        cwd: Option<&PathBuf>,
        model: Option<&str>,
    ) -> Result<Self> {
        let bin = resolve_bin(&config.bin)?;
        let mut cmd = Command::new(&bin);
        cmd.arg("-z")
            .arg(prompt)
            .kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        if let Some(sid) = session_id {
            cmd.arg("--resume").arg(sid);
        }

        let (model, provider) = crate::bridge::hermes_model_selection(model);
        if let Some(model) = model {
            cmd.arg("--model").arg(model);
        }
        if let Some(provider) = provider {
            cmd.arg("--provider").arg(provider);
        }

        // Prefer the per-turn cwd, then the config-level default.
        let working_dir = cwd.or(config.cwd.as_ref());
        if let Some(dir) = working_dir {
            cmd.current_dir(dir);
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("spawning `hermes -z` via {}", bin.display()))?;

        Ok(Self { child })
    }

    /// Send a signal to interrupt the running turn.
    #[allow(dead_code)]
    pub async fn interrupt(&mut self) -> Result<()> {
        self.child.kill().await.context("killing hermes process")
    }

    /// Wait for the process to complete and return its output.
    pub async fn wait_for_output(self) -> Result<CliOutput> {
        let output = self
            .child
            .wait_with_output()
            .await
            .context("waiting for hermes process")?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        if !output.status.success() {
            warn!(
                exit_code = ?output.status.code(),
                stderr_len = stderr.len(),
                "hermes CLI exited with error"
            );
        }

        Ok(CliOutput {
            stdout,
            stderr,
            exit_code: output.status.code(),
        })
    }
}

/// Output from a CLI process invocation.
#[derive(Debug)]
pub struct CliOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
}

/// Resolve a binary name to an absolute path. Returns the configured value
/// as-is (which may be a PATH-lookup name or an absolute path).
fn resolve_bin(configured: &str) -> Result<PathBuf> {
    // If it's an absolute path, use it directly.
    let path = PathBuf::from(configured);
    if path.is_absolute() {
        if path.exists() {
            return Ok(path);
        }
        anyhow::bail!("configured hermes binary not found: {}", configured);
    }

    // Otherwise look it up on PATH.
    which::which(configured)
        .with_context(|| format!("hermes binary `{}` not found on PATH", configured))
}

/// Convenience one-shot helper used by the bridge fallback path.
pub async fn run_hermes_cli(
    bin: &str,
    prompt: &str,
    session_id: Option<&str>,
    cwd: Option<&PathBuf>,
    model: Option<&str>,
) -> Result<String> {
    let config = CliConfig {
        bin: bin.to_string(),
        cwd: cwd.cloned(),
    };
    let process = HermesCliProcess::spawn(&config, prompt, session_id, cwd, model).await?;
    let output = process.wait_for_output().await?;
    if output.exit_code.unwrap_or(1) != 0 {
        anyhow::bail!(
            "hermes CLI exited with {:?}: {}",
            output.exit_code,
            output.stderr
        );
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    #[cfg(unix)]
    async fn cli_fallback_uses_z_and_resume_flags() {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("hermes-fake");
        let log = dir.path().join("args.log");
        let mut file = std::fs::File::create(&bin).unwrap();
        writeln!(file, "#!/usr/bin/env sh").unwrap();
        writeln!(file, "printf '%s\\n' \"$@\" > {:?}", log).unwrap();
        writeln!(file, "printf 'ok'").unwrap();
        drop(file);
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&bin).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&bin, perms).unwrap();

        let output = run_hermes_cli(
            bin.to_str().unwrap(),
            "hello",
            Some("session-1"),
            None,
            Some("hermes/openrouter/vendor/new-model"),
        )
        .await
        .unwrap();
        assert_eq!(output, "ok");
        let args = std::fs::read_to_string(log).unwrap();
        assert!(args.contains("-z\nhello"));
        assert!(args.contains("--resume\nsession-1"));
        assert!(args.contains("--model\nvendor/new-model"));
        assert!(args.contains("--provider\nopenrouter"));
        assert!(!args.contains("run"));
        assert!(!args.contains("--prompt"));
    }
}
