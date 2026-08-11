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
//! are parsed. Everything else is discarded.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;

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
    if input == "~"
        && let Some(home) = dirs_home()
    {
        return home;
    }
    if let Some(rest) = input.strip_prefix("~/")
        && let Some(home) = dirs_home()
    {
        return home.join(rest);
    }
    PathBuf::from(input)
}

fn dirs_home() -> Option<PathBuf> {
    directories::UserDirs::new().map(|u| u.home_dir().to_path_buf())
}

/// Port of `listSessionsFromDir`. Reads every `*.jsonl` file in `dir` and parses
/// each into a `PiSessionInfo`, dropping files that fail to parse or lack a header.
/// Order matches filesystem iteration order — sort at the call site if needed.
pub async fn list_sessions_from_dir(dir: &Path) -> Vec<PiSessionInfo> {
    let mut sessions = Vec::new();
    let mut read_dir = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return sessions,
    };

    let mut paths = Vec::new();
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
            paths.push(path);
        }
    }

    for path in paths {
        if let Some(info) = build_session_info(&path).await {
            sessions.push(info);
        }
    }
    sessions
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
    for dir in subdirs {
        sessions.extend(list_sessions_from_dir(&dir).await);
    }

    sessions.sort_by(|a, b| b.modified.cmp(&a.modified));
    sessions
}

/// Port of `buildSessionInfo`. Returns `None` if the file is unreadable, has no
/// entries, or its first non-empty line isn't a session header.
pub async fn build_session_info(path: &Path) -> Option<PiSessionInfo> {
    let content = fs::read_to_string(path).await.ok()?;
    let metadata = fs::metadata(path).await.ok()?;
    build_session_info_from_content(path, &content, metadata.modified().ok())
}

/// Parse one pi JSONL session file from already-loaded content.
fn build_session_info_from_content(
    path: &Path,
    content: &str,
    file_mtime: Option<std::time::SystemTime>,
) -> Option<PiSessionInfo> {
    let mut entries: Vec<serde_json::Value> = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            entries.push(value);
        }
        // Malformed lines are silently skipped, matching pi.
    }

    if entries.is_empty() {
        return None;
    }

    let header = entries.first()?;
    if header.get("type").and_then(|v| v.as_str()) != Some("session") {
        return None;
    }

    let id = header
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let cwd = header
        .get("cwd")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let parent_session_path = header
        .get("parentSession")
        .and_then(|v| v.as_str())
        .map(PathBuf::from);
    let created = header
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(parse_iso8601)
        .unwrap_or_else(Utc::now);

    let mut message_count = 0usize;
    let mut first_message = String::new();
    let mut all_messages: Vec<String> = Vec::new();
    let mut name: Option<String> = None;

    for entry in &entries {
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
        let text = extract_text_content(message);
        if text.is_empty() {
            continue;
        }
        if first_message.is_empty() && role == "user" {
            first_message = text.clone();
        }
        all_messages.push(text);
    }

    let modified = session_modified_date(&entries, &created, file_mtime);

    Some(PiSessionInfo {
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
    })
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

/// Mirrors `getSessionModifiedDate` + `getLastActivityTime` in session-manager.ts:511-547.
/// Picks the latest user/assistant message timestamp; falls back to the header's
/// timestamp; falls back to the file mtime.
fn session_modified_date(
    entries: &[serde_json::Value],
    header_created: &DateTime<Utc>,
    file_mtime: Option<std::time::SystemTime>,
) -> DateTime<Utc> {
    let mut last_activity: Option<i64> = None;

    for entry in entries {
        if entry.get("type").and_then(|v| v.as_str()) != Some("message") {
            continue;
        }
        let Some(message) = entry.get("message") else {
            continue;
        };
        let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
        if role != "user" && role != "assistant" {
            continue;
        }

        // Pi prefers a numeric `message.timestamp` (epoch millis), falling back
        // to the entry-level ISO timestamp.
        if let Some(ms) = message.get("timestamp").and_then(|v| v.as_i64()) {
            last_activity = Some(last_activity.map_or(ms, |prev| prev.max(ms)));
            continue;
        }
        if let Some(ts) = entry.get("timestamp").and_then(|v| v.as_str())
            && let Some(dt) = parse_iso8601(ts)
        {
            let ms = dt.timestamp_millis();
            last_activity = Some(last_activity.map_or(ms, |prev| prev.max(ms)));
        }
    }

    if let Some(ms) = last_activity
        && ms > 0
        && let Some(dt) = DateTime::<Utc>::from_timestamp_millis(ms)
    {
        return dt;
    }

    if header_created.timestamp_millis() > 0 {
        return *header_created;
    }

    file_mtime
        .and_then(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| DateTime::<Utc>::from_timestamp_millis(d.as_millis() as i64))
        })
        .unwrap_or_else(Utc::now)
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
}
