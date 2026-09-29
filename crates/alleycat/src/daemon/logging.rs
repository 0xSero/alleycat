//! Daemon logging setup. Writes to a daily-rotated file under
//! [`crate::paths::log_dir`], retaining seven files of at most 8 MiB each.
//! A full day's file restarts with a marker, preserving the newest diagnostics.
//! This cap does not apply to inherited service stdout/stderr or session history.
//! When stderr is a TTY,
//! it also mirrors there so `alleycat serve` is debuggable from a terminal.
//!
//! The returned [`tracing_appender::non_blocking::WorkerGuard`] must be kept
//! alive for the daemon's lifetime — dropping it stops the background writer
//! and silently swallows pending log lines.

use std::fs::{self, File, OpenOptions};
use std::io::{self, IsTerminal, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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

fn retained_appender(log_dir: &Path) -> anyhow::Result<CappedDailyWriter> {
    CappedDailyWriter::open(log_dir, 8 * 1024 * 1024, 7)
        .context("initializing retained daemon logs")
}

const RESET_MARKER: &[u8] = b"[daemon log byte limit reached; older diagnostics discarded]\n";
const OVERSIZED_MARKER: &[u8] = b"[oversized diagnostic record omitted]\n";

// Only the nonblocking appender's worker owns this writer. Cloned tracing
// writers enqueue to that same worker; there are no independently cached sizes.
struct CappedDailyWriter {
    dir: PathBuf,
    day: String,
    file: File,
    bytes: u64,
    limit: u64,
    retained_files: usize,
}

impl CappedDailyWriter {
    fn open(dir: &Path, limit: u64, retained_files: usize) -> io::Result<Self> {
        Self::open_on(
            dir,
            limit,
            retained_files,
            &chrono::Utc::now().format("%Y-%m-%d").to_string(),
        )
    }

    fn open_on(dir: &Path, limit: u64, retained_files: usize, day: &str) -> io::Result<Self> {
        assert!(limit >= (RESET_MARKER.len() + OVERSIZED_MARKER.len()) as u64);
        assert!(retained_files > 0);
        let path = dir.join(format!("daemon.log.{day}"));
        let mut file = open_log(&path)?;
        cap_existing(&mut file, limit)?;
        file.seek(SeekFrom::End(0))?;
        let bytes = file.metadata()?.len();
        let writer = Self {
            dir: dir.to_owned(),
            day: day.to_owned(),
            file,
            bytes,
            limit,
            retained_files,
        };
        writer.prune()?;
        Ok(writer)
    }

    fn prune(&self) -> io::Result<()> {
        let mut old = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            // Never follow links or treat the launchd sink as a rotated log.
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(day) = name.strip_prefix("daemon.log.") else {
                continue;
            };
            let Ok(date) = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d") else {
                continue;
            };
            if date.format("%Y-%m-%d").to_string() != day || day == self.day {
                continue;
            }
            old.push(entry.path());
        }
        old.sort();
        let discard = old.len().saturating_sub(self.retained_files - 1);
        for (index, path) in old.into_iter().enumerate() {
            if index < discard {
                fs::remove_file(path)?;
            } else {
                cap_existing(&mut open_log(&path)?, self.limit)?;
            }
        }
        Ok(())
    }

    fn write_on(&mut self, buf: &[u8], day: &str) -> io::Result<usize> {
        if day != self.day {
            *self = Self::open_on(&self.dir, self.limit, self.retained_files, day)?;
        }
        // Do not split UTF-8 or emit part of an oversized formatted record.
        let record = if buf.len() as u64 > self.limit - RESET_MARKER.len() as u64 {
            OVERSIZED_MARKER
        } else {
            buf
        };
        if self.bytes + record.len() as u64 > self.limit {
            // Truncate the exact owned descriptor, never a separately opened
            // path. Reset its cursor: set_len does not move it.
            self.file.set_len(0)?;
            self.file.seek(SeekFrom::Start(0))?;
            self.bytes = 0;
            self.append(RESET_MARKER)?;
        }
        self.append(record)?;
        Ok(buf.len())
    }

    fn append(&mut self, record: &[u8]) -> io::Result<()> {
        if let Err(error) = self.file.write_all(record) {
            // A partial failed write must not leave the accounting too small.
            self.bytes = self.file.metadata().map(|m| m.len()).unwrap_or(self.limit);
            return Err(error);
        }
        self.bytes += record.len() as u64;
        Ok(())
    }
}

impl Write for CappedDailyWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_on(buf, &chrono::Utc::now().format("%Y-%m-%d").to_string())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn open_log(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    // Windows append-only handles lack FILE_WRITE_DATA, required by set_len.
    // The singleton daemon and one worker own writes, so an explicit end seek
    // on open/reset provides portable append behavior without another writer.
    options.create(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("daemon log is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn cap_existing(file: &mut File, limit: u64) -> io::Result<()> {
    if file.metadata()?.len() > limit {
        file.set_len(0)?;
        file.write_all(RESET_MARKER)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_day_flood_stays_capped_and_retains_newest_complete_records() {
        let _guard = crate::test_support::lock_env();
        let dir = tempfile::tempdir().unwrap();
        let day = "2026-01-01";
        let mut writer = CappedDailyWriter::open_on(dir.path(), 128, 7, day).unwrap();
        for index in 0..1000 {
            let line = format!("record {index:04}: café λ\n");
            writer.write_on(line.as_bytes(), day).unwrap();
            assert!(writer.file.metadata().unwrap().len() <= 128);
        }
        let text = fs::read_to_string(dir.path().join(format!("daemon.log.{day}"))).unwrap();
        assert!(text.starts_with(std::str::from_utf8(RESET_MARKER).unwrap()));
        assert!(text.ends_with("record 0999: café λ\n"));
        assert_eq!(writer.bytes, text.len() as u64);
    }

    #[test]
    fn oversized_unicode_record_is_replaced_whole() {
        let _guard = crate::test_support::lock_env();
        let dir = tempfile::tempdir().unwrap();
        let day = "2026-01-01";
        let mut writer = CappedDailyWriter::open_on(dir.path(), 128, 7, day).unwrap();
        let record = "🦀".repeat(1024);
        assert_eq!(
            writer.write_on(record.as_bytes(), day).unwrap(),
            record.len()
        );
        let contents = fs::read(dir.path().join(format!("daemon.log.{day}"))).unwrap();
        assert_eq!(contents, OVERSIZED_MARKER);
    }

    #[test]
    fn restart_appends_without_overwriting_retained_content() {
        let _guard = crate::test_support::lock_env();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.log.2026-01-01");
        fs::write(&path, b"old record\n").unwrap();
        let mut writer = CappedDailyWriter::open_on(dir.path(), 128, 7, "2026-01-01").unwrap();
        writer.write_on(b"new record\n", "2026-01-01").unwrap();
        assert_eq!(fs::read(path).unwrap(), b"old record\nnew record\n");
    }

    #[test]
    fn restart_caps_existing_files_and_day_rollover_retains_seven() {
        let _guard = crate::test_support::lock_env();
        let dir = tempfile::tempdir().unwrap();
        for day in 1..=8 {
            fs::write(
                dir.path().join(format!("daemon.log.2026-01-{day:02}")),
                vec![b'x'; 1024],
            )
            .unwrap();
        }
        for name in [
            "daemon.log",
            "service-startup.log",
            "daemon.log.not-a-date",
            "other.log",
        ] {
            fs::write(dir.path().join(name), "untouched").unwrap();
        }
        let mut writer = CappedDailyWriter::open_on(dir.path(), 128, 7, "2026-01-08").unwrap();
        writer.write_on(b"new day\n", "2026-01-09").unwrap();
        assert_eq!(
            fs::read_to_string(dir.path().join("daemon.log.2026-01-09")).unwrap(),
            "new day\n"
        );
        assert!(!dir.path().join("daemon.log.2026-01-01").exists());
        assert!(!dir.path().join("daemon.log.2026-01-02").exists());
        for day in 3..=9 {
            assert!(
                fs::metadata(dir.path().join(format!("daemon.log.2026-01-{day:02}")))
                    .unwrap()
                    .len()
                    <= 128
            );
        }
        for name in [
            "daemon.log",
            "service-startup.log",
            "daemon.log.not-a-date",
            "other.log",
        ] {
            assert_eq!(
                fs::read_to_string(dir.path().join(name)).unwrap(),
                "untouched"
            );
        }
    }

    #[test]
    fn cloned_nonblocking_writers_share_one_cap() {
        let _guard = crate::test_support::lock_env();
        let dir = tempfile::tempdir().unwrap();
        let appender = CappedDailyWriter::open(dir.path(), 256, 7).unwrap();
        let path = dir.path().join(format!("daemon.log.{}", appender.day));
        let (mut writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
            .buffered_lines_limit(32)
            .lossy(false)
            .finish(appender);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mut writer = writer.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        writer.write_all("parallel café λ\n".as_bytes()).unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        writer.write_all(b"FINAL RECORD\n").unwrap();
        drop(guard);
        let contents = fs::read_to_string(path).unwrap();
        assert!(contents.len() <= 256);
        assert!(contents.ends_with("FINAL RECORD\n"));
    }

    #[cfg(unix)]
    #[test]
    fn does_not_follow_symlinks_or_change_replaced_path() {
        let _guard = crate::test_support::lock_env();
        use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("keep");
        fs::write(&outside, "private unrelated data").unwrap();
        let path = dir.path().join("daemon.log.2026-01-01");
        symlink(&outside, &path).unwrap();
        assert!(CappedDailyWriter::open_on(dir.path(), 128, 7, "2026-01-01").is_err());
        fs::remove_file(&path).unwrap();
        let mut writer = CappedDailyWriter::open_on(dir.path(), 128, 7, "2026-01-01").unwrap();
        let inode = writer.file.metadata().unwrap().ino();
        assert_eq!(
            writer.file.metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
        writer.write_on(&[b'x'; 64], "2026-01-01").unwrap();
        fs::rename(&path, dir.path().join("moved-owned-log")).unwrap();
        symlink(&outside, &path).unwrap();
        for _ in 0..4 {
            writer.write_on(&[b'y'; 64], "2026-01-01").unwrap();
        }
        assert_eq!(writer.file.metadata().unwrap().ino(), inode);
        assert!(writer.file.metadata().unwrap().len() <= 128);
        assert_eq!(
            fs::read_to_string(outside).unwrap(),
            "private unrelated data"
        );
    }

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
