//! Walk `~/.claude/projects/<encoded-cwd>/<session_id>.jsonl` and produce
//! [`ClaudeSessionInfo`] records for hydration into the bridge index.
//!
//! Encoded-cwd convention (per the live wire `system/init.cwd` matching what
//! claude writes on disk): the cwd is mangled into a directory name by
//! replacing `/` with `-` and dropping the leading separator. For example,
//! cwd `/Users/sigkitten/dev/alleycat` becomes
//! `-Users-sigkitten-dev-alleycat`.
//!
//! Each `<session_id>.jsonl` is tolerated permissively: we read the first
//! `user` record's text content for `first_message`, and use file `created`
//! /`modified` mtimes as fallbacks when the JSONL has no parseable
//! timestamps. Files that fail to open or parse are skipped quietly.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaudeSessionInfo {
    /// Absolute path to the JSONL file.
    pub path: PathBuf,
    /// Session id (== filename stem; stable across the session lifetime).
    pub session_id: String,
    /// Working directory the session was bound to. Empty when the JSONL
    /// header is missing (very old sessions).
    pub cwd: String,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub first_message: String,
}

/// `~/.claude/projects/`. Honors `CLAUDE_PROJECTS_DIR` for tests.
pub fn claude_projects_dir() -> Option<PathBuf> {
    if let Ok(env_dir) = std::env::var("CLAUDE_PROJECTS_DIR") {
        return Some(expand_tilde(&env_dir));
    }
    let home = directories::UserDirs::new()?.home_dir().to_path_buf();
    Some(home.join(".claude").join("projects"))
}

fn expand_tilde(input: &str) -> PathBuf {
    if input == "~"
        && let Some(home) = directories::UserDirs::new()
    {
        return home.home_dir().to_path_buf();
    }
    if let Some(rest) = input.strip_prefix("~/")
        && let Some(home) = directories::UserDirs::new()
    {
        return home.home_dir().join(rest);
    }
    PathBuf::from(input)
}

/// Walk every encoded-cwd subdirectory of `~/.claude/projects/` and return
/// every parseable session.
pub async fn list_all() -> Vec<ClaudeSessionInfo> {
    let Some(root) = claude_projects_dir() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut read_dir = match fs::read_dir(&root).await {
        Ok(rd) => rd,
        Err(_) => return out,
    };
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        if entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
        {
            out.extend(list_sessions_from_dir(&entry.path()).await);
        }
    }
    out
}

/// Read every `*.jsonl` in `dir`. Each parses into a `ClaudeSessionInfo`;
/// unparseable files are skipped quietly.
pub async fn list_sessions_from_dir(dir: &Path) -> Vec<ClaudeSessionInfo> {
    let mut out = Vec::new();
    let mut read_dir = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return out,
    };
    while let Ok(Some(entry)) = read_dir.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        if let Some(info) = build_session_info(&path).await {
            out.push(info);
        }
    }
    out
}

async fn build_session_info(path: &Path) -> Option<ClaudeSessionInfo> {
    let session_id = path.file_stem()?.to_string_lossy().to_string();
    let metadata = fs::metadata(path).await.ok()?;
    let modified: DateTime<Utc> = metadata
        .modified()
        .ok()
        .and_then(|t| {
            DateTime::<Utc>::from_timestamp(
                t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64,
                0,
            )
        })
        .unwrap_or_else(Utc::now);
    let created: DateTime<Utc> = metadata
        .created()
        .ok()
        .and_then(|t| {
            DateTime::<Utc>::from_timestamp(
                t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs() as i64,
                0,
            )
        })
        .unwrap_or(modified);

    let text = fs::read_to_string(path).await.ok()?;
    let mut cwd = String::new();
    let mut first_message = String::new();
    let mut first_message_ts: Option<DateTime<Utc>> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if cwd.is_empty()
            && let Some(c) = value.get("cwd").and_then(|v| v.as_str())
        {
            cwd = c.to_string();
        }
        if first_message.is_empty()
            && value.get("type").and_then(|v| v.as_str()) == Some("user")
            && let Some(content) = value.get("message").and_then(|m| m.get("content"))
        {
            if let Some(s) = content.as_str() {
                first_message = s.lines().next().unwrap_or("").to_string();
            } else if let Some(arr) = content.as_array() {
                for entry in arr {
                    if entry.get("type").and_then(|t| t.as_str()) == Some("text")
                        && let Some(t) = entry.get("text").and_then(|v| v.as_str())
                    {
                        first_message = t.lines().next().unwrap_or("").to_string();
                        break;
                    }
                }
            }
            if first_message_ts.is_none()
                && let Some(ts) = value
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            {
                first_message_ts = Some(ts.with_timezone(&Utc));
            }
        }
        if !cwd.is_empty() && !first_message.is_empty() {
            break;
        }
    }

    let created = first_message_ts.unwrap_or(created);
    if first_message.is_empty() {
        first_message = "(no messages)".to_string();
    }
    Some(ClaudeSessionInfo {
        path: path.to_path_buf(),
        session_id,
        cwd,
        created,
        modified,
        first_message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn lists_session_with_first_user_message_and_cwd() {
        let dir = TempDir::new().unwrap();
        let cwd_dir = dir.path().join("-private-tmp");
        std::fs::create_dir_all(&cwd_dir).unwrap();
        let session_path = cwd_dir.join("abc-123.jsonl");
        let mut f = std::fs::File::create(&session_path).unwrap();
        writeln!(
            f,
            "{{\"type\":\"permission-mode\",\"sessionId\":\"abc-123\",\"permissionMode\":\"bypassPermissions\"}}"
        )
        .unwrap();
        writeln!(
            f,
            "{{\"type\":\"user\",\"cwd\":\"/private/tmp\",\"message\":{{\"role\":\"user\",\"content\":\"hello world\"}},\"timestamp\":\"2026-04-27T10:00:00Z\"}}"
        )
        .unwrap();
        drop(f);

        let sessions = list_sessions_from_dir(&cwd_dir).await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "abc-123");
        assert_eq!(sessions[0].cwd, "/private/tmp");
        assert_eq!(sessions[0].first_message, "hello world");
    }

    #[tokio::test]
    async fn skips_non_jsonl_files() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("notes.txt"), "hi").unwrap();
        let sessions = list_sessions_from_dir(dir.path()).await;
        assert!(sessions.is_empty());
    }

    #[tokio::test]
    async fn missing_dir_returns_empty_not_error() {
        let sessions = list_sessions_from_dir(Path::new("/nonexistent/missing")).await;
        assert!(sessions.is_empty());
    }

    #[tokio::test]
    async fn handles_jsonl_with_no_user_message() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("only-meta.jsonl");
        std::fs::write(
            &path,
            r#"{"type":"permission-mode","permissionMode":"bypassPermissions"}
"#,
        )
        .unwrap();
        let sessions = list_sessions_from_dir(dir.path()).await;
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].first_message, "(no messages)");
    }
}
