//! Daemon logging setup. Writes to a daily-rotated file under
//! [`crate::paths::log_dir`], retaining seven daily files. When stderr is a TTY,
//! it also mirrors there so `alleycat serve` is debuggable from a terminal.
//!
//! The returned [`tracing_appender::non_blocking::WorkerGuard`] must be kept
//! alive for the daemon's lifetime — dropping it stops the background writer
//! and silently swallows pending log lines.

use std::io::IsTerminal;
use std::path::Path;

use anyhow::Context;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize daemon-side tracing.
///
/// `level` is appended to a baseline `iroh=warn,quinn=warn` filter so noisy
/// transport internals stay quiet by default. `RUST_LOG` overrides everything.
pub fn init(level: &str, log_dir: &Path) -> anyhow::Result<WorkerGuard> {
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("creating log dir {}", log_dir.display()))?;

    let appender = retained_appender(log_dir)?;
    let (writer, guard) = tracing_appender::non_blocking(appender);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("{level},iroh=warn,quinn=warn")));

    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(writer);

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer);

    if std::io::stderr().is_terminal() {
        let stderr_layer = fmt::layer()
            .with_ansi(true)
            .with_target(false)
            .with_writer(std::io::stderr);
        registry.with(stderr_layer).try_init().ok();
    } else {
        registry.try_init().ok();
    }

    Ok(guard)
}

fn retained_appender(
    log_dir: &Path,
) -> anyhow::Result<tracing_appender::rolling::RollingFileAppender> {
    tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("daemon.log")
        .max_log_files(7)
        .build(log_dir)
        .context("initializing retained daemon logs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_prunes_old_daemon_logs_and_preserves_other_files() {
        let _guard = crate::test_support::lock_env();
        let dir = tempfile::tempdir().unwrap();
        for day in 1..=10 {
            std::fs::write(
                dir.path().join(format!("daemon.log.2020-01-{day:02}")),
                "old",
            )
            .unwrap();
        }
        std::fs::write(dir.path().join("other.log"), "keep").unwrap();
        let _appender = retained_appender(dir.path()).unwrap();
        let logs = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("daemon.log.")
            })
            .count();
        assert_eq!(logs, 7);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("other.log")).unwrap(),
            "keep"
        );
    }
}
