//! Rust port of `listSessionsFromDir` / `buildSessionInfo` / `SessionManager.listAll`
//! from `pi-mono/packages/coding-agent/src/core/session-manager.ts` (lines 549-656,
//! 1365-1424). Reads pi's JSONL session files and produces native `PiSessionInfo`
//! records — the conversion to codex-shape `Thread` happens later in `index/mod.rs`.
//!
//! Pi session files live under `~/.pi/agent/sessions/<encoded-cwd>/<sessionId>.jsonl`
//! (overridable via the `PI_CODING_AGENT_DIR` env var). Each file is JSONL: line 1 is
//! the `SessionHeader`, subsequent lines are `SessionEntry`s of varying types. We are
//! deliberately tolerant of malformed lines — pi skips them and so do we.
//!
//! Only the subset of entry fields that contribute to the listing surface
//! (`session_info` for the user-defined name; `message` entries for counts, the first
//! user message preview, the all-text search blob, and the modified-time fallback)
//! are parsed. Everything else is discarded. Startup reads at most 64 KiB per file;
//! larger histories retain a bounded summary and use file mtime for freshness.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

/// Maximum prefix read from one historical session during startup hydration.
///
/// The index only needs the header and a useful preview. Full session replay is
/// handled by the normal thread-read path. Bounding this scan prevents a corrupt
/// or runaway JSONL file from delaying daemon readiness or being copied into
/// several multi-gigabyte in-memory representations.
const MAX_SESSION_SCAN_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
struct SessionScanOutcome {
    info: Option<PiSessionInfo>,
    truncated: bool,
}

#[derive(Debug)]
struct DirectoryScanOutcome {
    sessions: Vec<PiSessionInfo>,
    truncated_sessions: usize,
}

/// One scanned pi session, mirroring `SessionInfo` in pi's session-manager.ts:168-182.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PiSessionInfo {
    pub path: PathBuf,
    pub id: String,
    /// Working directory recorded in the session header. Empty for old (v1) sessions.
    pub cwd: String,
    /// User-defined display name from the latest `session_info` entry.
    pub name: Option<String>,
    /// If the session was forked, the path of its parent.
    pub parent_session_path: Option<PathBuf>,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub message_count: usize,
    /// First user-message text content. Falls back to "(no messages)" like pi.
    pub first_message: String,
    /// Concatenation of all user/assistant text contents joined by single spaces.
    pub all_messages_text: String,
}

/// Resolve pi's agent directory the same way `getAgentDir` does. Honors the
/// `PI_CODING_AGENT_DIR` env var (with `~` expansion) and otherwise falls back
/// to `~/.pi/agent`.
pub fn pi_agent_dir() -> Option<PathBuf> {
    if let Ok(env_dir) = std::env::var("PI_CODING_AGENT_DIR") {
        return Some(expand_tilde(&env_dir));
    }
    let home = dirs_home()?;
    Some(home.join(".pi").join("agent"))
}

/// `~/.pi/agent/sessions` (or its env-var override).
pub fn pi_sessions_dir() -> Option<PathBuf> {
    pi_agent_dir().map(|p| p.join("sessions"))
}

fn expand_tilde(input: &str) -> PathBuf {
    if input == "~" {
        if let Some(home) = dirs_home() {
            return home;
        }
    }
    if let Some(rest) = input.strip_prefix("~/") {
        if let Some(home) = dirs_home() {
            return home.join(rest);
        }
    }
    PathBuf::from(input)
}

fn dirs_home() -> Option<PathBuf> {
    directories::UserDirs::new().map(|u| u.home_dir().to_path_buf())
}

/// Port of `listSessionsFromDir`. Reads a bounded prefix of every `*.jsonl` file
/// in `dir` and parses each into a `PiSessionInfo`, dropping files that fail to
/// parse or lack a header. Order matches filesystem iteration order — sort at the
/// call site if needed.
pub async fn list_sessions_from_dir(dir: &Path) -> Vec<PiSessionInfo> {
    let outcome = scan_sessions_from_dir(dir).await;
    if outcome.truncated_sessions > 0 {
        tracing::debug!(
            directory = %dir.display(),
            truncated_sessions = outcome.truncated_sessions,
            scan_limit_bytes = MAX_SESSION_SCAN_BYTES,
            "pi hydration used bounded summaries for oversized session files"
        );
    }
    outcome.sessions
}

async fn scan_sessions_from_dir(dir: &Path) -> DirectoryScanOutcome {
    let mut sessions = Vec::new();
    let mut read_dir = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => {
            return DirectoryScanOutcome {
                sessions,
                truncated_sessions: 0,
            };
        }
    };

    let mut paths = Vec::new();
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            paths.push(path);
        }
    }

    let mut truncated_sessions = 0usize;
    for path in paths {
        let outcome = build_session_info_with_limit(&path, MAX_SESSION_SCAN_BYTES).await;
        if outcome.truncated {
            truncated_sessions += 1;
        }
        if let Some(info) = outcome.info {
            sessions.push(info);
        }
    }
    DirectoryScanOutcome {
        sessions,
        truncated_sessions,
    }
}

/// Port of `SessionManager.listAll`. Walks every immediate subdirectory of
/// `~/.pi/agent/sessions/` (each is one encoded cwd) and concatenates their
/// scans. Sorted by `modified` descending, matching pi.
pub async fn list_all() -> Vec<PiSessionInfo> {
    let Some(sessions_dir) = pi_sessions_dir() else {
        return Vec::new();
    };
    let mut read_dir = match fs::read_dir(&sessions_dir).await {
        Ok(rd) => rd,
        Err(_) => return Vec::new(),
    };

    let mut subdirs = Vec::new();
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let ft = match entry.file_type().await {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_dir() {
            subdirs.push(entry.path());
        }
    }

    let mut sessions = Vec::new();
    let mut truncated_sessions = 0usize;
    for dir in subdirs {
        let outcome = scan_sessions_from_dir(&dir).await;
        sessions.extend(outcome.sessions);
        truncated_sessions += outcome.truncated_sessions;
    }

    if truncated_sessions > 0 {
        tracing::warn!(
            root = %sessions_dir.display(),
            truncated_sessions,
            scan_limit_bytes = MAX_SESSION_SCAN_BYTES,
            "pi hydration used bounded summaries for oversized session files"
        );
    }

    sessions.sort_by(|a, b| b.modified.cmp(&a.modified));
    sessions
}

/// Port of `buildSessionInfo`. Returns `None` if the file is unreadable, has no
/// entries, or its first non-empty line isn't a session header.
pub async fn build_session_info(path: &Path) -> Option<PiSessionInfo> {
    let outcome = build_session_info_with_limit(path, MAX_SESSION_SCAN_BYTES).await;
    if outcome.truncated {
        tracing::warn!(
            path = %path.display(),
            scan_limit_bytes = MAX_SESSION_SCAN_BYTES,
            "pi hydration retained a bounded summary for an oversized session file"
        );
    }
    outcome.info
}

/// Stream a bounded prefix of one pi JSONL session file.
///
/// Oversized files remain discoverable: the header, first-message preview, and
/// any other entries that fit in the prefix are retained. Their modified time
/// is conservatively advanced to the file mtime, while message count, name, and
/// search text are explicitly best-effort. A line cut by the byte boundary is
/// ignored as malformed JSON.
async fn build_session_info_with_limit(path: &Path, scan_limit_bytes: u64) -> SessionScanOutcome {
    let metadata = match fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(_) => {
            return SessionScanOutcome {
                info: None,
                truncated: false,
            };
        }
    };
    let truncated = metadata.len() > scan_limit_bytes;
    let file = match fs::File::open(path).await {
        Ok(file) => file,
        Err(_) => {
            return SessionScanOutcome {
                info: None,
                truncated,
            };
        }
    };
    let mut lines = BufReader::new(file.take(scan_limit_bytes)).lines();

    let mut saw_entry = false;
    let mut id = String::new();
    let mut cwd = String::new();
    let mut parent_session_path: Option<PathBuf> = None;
    let mut created: Option<DateTime<Utc>> = None;
    let mut last_activity: Option<DateTime<Utc>> = None;
    let mut message_count = 0usize;
    let mut first_message = String::new();
    let mut all_messages: Vec<String> = Vec::new();
    let mut name: Option<String> = None;

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<serde_json::Value>(trimmed) else {
            continue;
        };
        // Malformed lines are silently skipped, matching pi.
        if !saw_entry {
            saw_entry = true;
            if entry.get("type").and_then(|v| v.as_str()) != Some("session") {
                return SessionScanOutcome {
                    info: None,
                    truncated,
                };
            }
            id = entry
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            cwd = entry
                .get("cwd")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            parent_session_path = entry
                .get("parentSession")
                .and_then(|v| v.as_str())
                .map(PathBuf::from);
            created = Some(
                entry
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .and_then(parse_iso8601)
                    .unwrap_or_else(Utc::now),
            );
            continue;
        }

        let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("");

        // session_info entries set/clear the user-defined name. Latest wins,
        // including explicit blanks (which clear it).
        if entry_type == "session_info" {
            let raw = entry
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .unwrap_or("");
            name = if raw.is_empty() {
                None
            } else {
                Some(raw.to_string())
            };
            continue;
        }

        if entry_type != "message" {
            continue;
        }
        message_count += 1;

        let Some(message) = entry.get("message") else {
            continue;
        };
        let role = match message.get("role").and_then(|v| v.as_str()) {
            Some(r) if r == "user" || r == "assistant" => r,
            _ => continue,
        };
        if let Some(activity) = message_activity_time(&entry, message) {
            last_activity = Some(last_activity.map_or(activity, |previous| previous.max(activity)));
        }
        let text = extract_text_content(message);
        if text.is_empty() {
            continue;
        }
        if first_message.is_empty() && role == "user" {
            first_message = text.clone();
        }
        all_messages.push(text);
    }

    let Some(created) = created else {
        return SessionScanOutcome {
            info: None,
            truncated,
        };
    };
    let mut modified = last_activity.unwrap_or(created);
    if truncated {
        if let Some(file_modified) = metadata.modified().ok().and_then(system_time_to_utc) {
            modified = modified.max(file_modified);
        }
    }

    SessionScanOutcome {
        info: Some(PiSessionInfo {
            path: path.to_path_buf(),
            id,
            cwd,
            name,
            parent_session_path,
            created,
            modified,
            message_count,
            first_message: if first_message.is_empty() {
                "(no messages)".to_string()
            } else {
                first_message
            },
            all_messages_text: all_messages.join(" "),
        }),
        truncated,
    }
}

/// Extracts text from a pi `Message.content` (string or array of content blocks).
/// Mirrors pi's `extractTextContent` in session-manager.ts:500-509.
fn extract_text_content(message: &serde_json::Value) -> String {
    let content = match message.get("content") {
        Some(c) => c,
        None => return String::new(),
    };
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    let Some(arr) = content.as_array() else {
        return String::new();
    };
    arr.iter()
        .filter_map(|block| {
            if block.get("type").and_then(|v| v.as_str()) == Some("text") {
                block.get("text").and_then(|v| v.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn message_activity_time(
    entry: &serde_json::Value,
    message: &serde_json::Value,
) -> Option<DateTime<Utc>> {
    if let Some(ms) = message.get("timestamp").and_then(|v| v.as_i64()) {
        return (ms > 0)
            .then(|| DateTime::<Utc>::from_timestamp_millis(ms))
            .flatten();
    }
    entry
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(parse_iso8601)
}

fn system_time_to_utc(value: std::time::SystemTime) -> Option<DateTime<Utc>> {
    value
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| {
            DateTime::<Utc>::from_timestamp_millis(duration.as_millis().try_into().ok()?)
        })
}

fn parse_iso8601(input: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_jsonl(path: &Path, lines: &[&str]) {
        let mut f = std::fs::File::create(path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
    }

    #[tokio::test]
    async fn parses_basic_session() {
        let dir = TempDir::new().unwrap();
        let session_path = dir.path().join("01H.jsonl");
        write_jsonl(
            &session_path,
            &[
                r#"{"type":"session","version":3,"id":"sess-1","timestamp":"2026-04-27T10:00:00Z","cwd":"/work/proj"}"#,
                r#"{"type":"session_info","id":"e0","parentId":null,"timestamp":"2026-04-27T10:00:01Z","name":"  My Session  "}"#,
                r#"{"type":"message","id":"e1","parentId":null,"timestamp":"2026-04-27T10:00:05Z","message":{"role":"user","content":"hello there"}}"#,
                r#"{"type":"message","id":"e2","parentId":"e1","timestamp":"2026-04-27T10:00:10Z","message":{"role":"assistant","content":[{"type":"text","text":"hi!"},{"type":"thinking","thinking":"hidden"}]}}"#,
                r#"not valid json — should be skipped"#,
                r#"{"type":"message","id":"e3","parentId":"e2","timestamp":"2026-04-27T10:00:20Z","message":{"role":"user","content":[{"type":"text","text":"follow up"}]}}"#,
            ],
        );

        let info = build_session_info(&session_path).await.expect("info");
        assert_eq!(info.id, "sess-1");
        assert_eq!(info.cwd, "/work/proj");
        assert_eq!(info.name.as_deref(), Some("My Session"));
        assert_eq!(info.message_count, 3);
        assert_eq!(info.first_message, "hello there");
        assert_eq!(info.all_messages_text, "hello there hi! follow up");
        assert!(info.parent_session_path.is_none());
        assert_eq!(
            info.created,
            DateTime::parse_from_rfc3339("2026-04-27T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        // Modified should reflect the latest user/assistant entry timestamp.
        assert_eq!(
            info.modified,
            DateTime::parse_from_rfc3339("2026-04-27T10:00:20Z")
                .unwrap()
                .with_timezone(&Utc)
        );
    }

    #[tokio::test]
    async fn list_sessions_from_dir_filters_non_jsonl_and_sorts_independently() {
        let dir = TempDir::new().unwrap();
        write_jsonl(
            &dir.path().join("a.jsonl"),
            &[
                r#"{"type":"session","version":3,"id":"a","timestamp":"2026-01-01T00:00:00Z","cwd":"/x"}"#,
                r#"{"type":"message","id":"m1","parentId":null,"timestamp":"2026-01-01T00:00:05Z","message":{"role":"user","content":"a-msg"}}"#,
            ],
        );
        write_jsonl(
            &dir.path().join("b.jsonl"),
            &[
                r#"{"type":"session","version":3,"id":"b","timestamp":"2026-02-01T00:00:00Z","cwd":"/x"}"#,
            ],
        );
        // Should be ignored — wrong extension.
        std::fs::write(dir.path().join("notes.txt"), "ignore me").unwrap();
        // Should be ignored — no session header.
        write_jsonl(
            &dir.path().join("headerless.jsonl"),
            &[
                r#"{"type":"message","id":"x","parentId":null,"timestamp":"2026-01-01T00:00:00Z","message":{"role":"user","content":"oops"}}"#,
            ],
        );

        let mut found = list_sessions_from_dir(dir.path()).await;
        found.sort_by(|a, b| a.id.cmp(&b.id));
        let ids: Vec<_> = found.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b"]);

        let a = found.iter().find(|s| s.id == "a").unwrap();
        assert_eq!(a.message_count, 1);
        assert_eq!(a.first_message, "a-msg");

        let b = found.iter().find(|s| s.id == "b").unwrap();
        assert_eq!(b.message_count, 0);
        assert_eq!(b.first_message, "(no messages)");
    }

    #[tokio::test]
    async fn missing_dir_returns_empty() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("nope");
        assert!(list_sessions_from_dir(&missing).await.is_empty());
    }

    #[tokio::test]
    async fn name_can_be_cleared_by_blank_session_info() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("c.jsonl");
        write_jsonl(
            &path,
            &[
                r#"{"type":"session","version":3,"id":"c","timestamp":"2026-03-01T00:00:00Z","cwd":"/x"}"#,
                r#"{"type":"session_info","id":"si1","parentId":null,"timestamp":"2026-03-01T00:00:01Z","name":"first"}"#,
                r#"{"type":"session_info","id":"si2","parentId":null,"timestamp":"2026-03-01T00:00:02Z","name":"   "}"#,
            ],
        );
        let info = build_session_info(&path).await.unwrap();
        assert_eq!(info.name, None);
    }

    #[tokio::test]
    async fn parent_session_propagates() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("d.jsonl");
        write_jsonl(
            &path,
            &[
                r#"{"type":"session","version":3,"id":"d","timestamp":"2026-03-01T00:00:00Z","cwd":"/x","parentSession":"/some/parent.jsonl"}"#,
            ],
        );
        let info = build_session_info(&path).await.unwrap();
        assert_eq!(
            info.parent_session_path,
            Some(PathBuf::from("/some/parent.jsonl"))
        );
    }

    #[tokio::test]
    async fn bounded_scan_retains_summary_and_ignores_entries_past_limit() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("oversized.jsonl");
        let scan_limit = 512usize;
        let prefix = concat!(
            "{\"type\":\"session\",\"version\":3,\"id\":\"large\",",
            "\"timestamp\":\"2020-01-01T00:00:00Z\",\"cwd\":\"/large\"}\n",
            "{\"type\":\"message\",\"timestamp\":\"2020-01-01T00:00:01Z\",",
            "\"message\":{\"role\":\"user\",\"content\":\"preview survives\"}}\n"
        );
        assert!(prefix.len() < scan_limit);

        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(prefix.as_bytes()).unwrap();
        file.write_all(&vec![b' '; scan_limit - prefix.len()])
            .unwrap();
        file.write_all(
            br#"{"type":"session_info","name":"must not be parsed past the budget"}
"#,
        )
        .unwrap();
        drop(file);

        let outcome = build_session_info_with_limit(&path, scan_limit as u64).await;
        assert!(outcome.truncated);
        let info = outcome.info.expect("bounded summary");
        assert_eq!(info.id, "large");
        assert_eq!(info.cwd, "/large");
        assert_eq!(info.first_message, "preview survives");
        assert_eq!(info.name, None);
        assert!(info.modified >= info.created);
    }

    #[tokio::test]
    async fn bounded_scan_is_independent_of_sparse_file_logical_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sparse.jsonl");
        let header = concat!(
            "{\"type\":\"session\",\"version\":3,\"id\":\"sparse\",",
            "\"timestamp\":\"2020-01-01T00:00:00Z\",\"cwd\":\"/sparse\"}\n"
        );
        std::fs::write(&path, header).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(512 * 1024 * 1024).unwrap();
        drop(file);

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            build_session_info_with_limit(&path, 4 * 1024),
        )
        .await
        .expect("bounded scan must not traverse the logical file size");
        assert!(outcome.truncated);
        assert_eq!(outcome.info.expect("summary").id, "sparse");
    }
}
