//! Grok-specific bridge (ACP over `grok agent stdio`).
//!
//! Grok (the xAI coding agent) speaks ACP. **All Grok-specific behavior lives
//! in this crate** (`grok-bridge`), not in the generic `acp-bridge`.
//!
//! Currently provides:
//! - Correct launch command construction (`grok agent ... stdio`)
//! - Proper `thread/list` by reading Grok's on-disk session storage
//!   (because Grok does not implement ACP `session/list`).
//!
//! The canonical way to obtain a working Grok ACP agent is:
//!
//! ```rust,ignore
//! let bridge = GrokBridge::build(
//!     "grok",
//!     /* no_leader */ true,
//!     /* model */ None,
//!     /* always_approve */ false,
//!     /* reasoning_effort */ Some("medium".into()),
//!     launcher,
//! )?;
//! ```
//!
//! This produces the correct command line:
//! `grok agent --no-leader --reasoning-effort medium stdio`
//!
//! The `acp-bridge` crate itself remains completely unaware of Grok's CLI
//! structure (`agent`, `stdio`, `--no-leader`, etc.).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_acp_bridge::AcpBridge;
use alleycat_bridge_core::{Bridge, Conn, JsonRpcError, ProcessLauncher, error_codes};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::Connection;
use serde_json::{Value, json};
use tracing::warn;

/// Thin wrapper around the generic ACP bridge for the Grok agent.
///
/// `GrokBridge` owns all Grok-specific launch logic so that `acp-bridge`
/// can stay 100% generic.
pub struct GrokBridge {
    inner: Arc<AcpBridge>,
    sessions_dir: PathBuf,
}

impl GrokBridge {
    pub fn new(inner: Arc<AcpBridge>) -> Self {
        Self {
            inner,
            sessions_dir: default_grok_sessions_dir(),
        }
    }

    /// Build a ready-to-use `GrokBridge`.
    ///
    /// This is the official constructor. It knows the correct Grok CLI shape:
    /// `grok agent [flags...] stdio`
    pub async fn build(
        bin: impl Into<PathBuf>,
        no_leader: bool,
        model: Option<String>,
        always_approve: bool,
        reasoning_effort: Option<String>,
        launcher: Arc<dyn ProcessLauncher>,
    ) -> Result<Arc<Self>> {
        let mut args = vec!["agent".to_string()];

        if no_leader {
            args.push("--no-leader".to_string());
        }
        if let Some(m) = model {
            args.push("-m".to_string());
            args.push(m);
        }
        if always_approve {
            args.push("--always-approve".to_string());
        }
        if let Some(effort) = reasoning_effort {
            args.push("--reasoning-effort".to_string());
            args.push(effort);
        }

        args.push("stdio".to_string());

        let acp = AcpBridge::builder()
            .agent_bin(bin)
            .agent_args(args)
            .launcher(launcher)
            .build()
            .await
            .context("building inner AcpBridge for Grok")?;

        Ok(Arc::new(Self::new(acp)))
    }

    /// Override `thread/list` by reading directly from Grok's session storage.
    /// Grok does not implement the ACP `session/list` method (or returns very
    /// limited results), similar to Devin.
    async fn handle_thread_list(&self) -> Result<Value, JsonRpcError> {
        let dir = self.sessions_dir.clone();

        let threads = tokio::task::spawn_blocking(move || read_grok_sessions(&dir))
            .await
            .map_err(|e| JsonRpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("grok thread/list worker panicked: {e}"),
                data: None,
            })?
            .map_err(|e| {
                warn!(error = %e, "Failed to read Grok sessions from disk");
                JsonRpcError {
                    code: error_codes::INTERNAL_ERROR,
                    message: format!("Failed to read Grok sessions: {e}"),
                    data: None,
                }
            })?;

        Ok(json!({
            "data": threads,
            "nextCursor": null,
            "backwardsCursor": null,
        }))
    }

    /// Grok-specific handling for `thread/resume`.
    ///
    /// The generic acp-bridge handler for `thread/resume` translates to ACP
    /// `session/load` and may send an empty `cwd` (or wrong one) if the client
    /// didn't provide it. Grok rejects `session/load` with invalid/empty cwd.
    ///
    /// We look up the authoritative original `cwd` from Grok's on-disk session
    /// storage and inject it before delegating.
    async fn handle_thread_resume(
        &self,
        ctx: &Conn,
        mut params: Value,
    ) -> Result<Value, JsonRpcError> {
        // Try to extract thread_id (sessionId)
        let thread_id = params
            .get("threadId")
            .or_else(|| params.get("thread_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if let Some(ref id) = thread_id {
            // Look up the real cwd from our Grok session index
            if let Some(real_cwd) = self.lookup_cwd_for_session(id) {
                // Inject/override cwd so the generic handler sends the correct value
                // to Grok's session/load.
                if let Some(obj) = params.as_object_mut() {
                    obj.insert("cwd".to_string(), json!(real_cwd));
                }
            }
        }

        // Delegate to the generic handler (which will call session/load with fixed cwd)
        let mut resp = self.inner.dispatch(ctx, "thread/resume", params).await?;

        // Grok-specific sanitization: the generic translator sometimes produces
        // userMessage content with `type: "content"` (from Grok's session/load replay).
        // The iOS client only accepts `inputText` / `inputImage` in that position.
        // We fix it here so Grok-specific resume always produces valid Codex shapes.
        resp = self.sanitize_user_message_content(resp);
        Ok(resp)
    }

    /// Grok-specific post-processing for `thread/resume` responses.
    /// Converts any `userMessage.content` items that have `type: "content"`
    /// into the client-expected `inputText` / `inputImage` shapes.
    fn sanitize_user_message_content(&self, mut response: Value) -> Value {
        if let Some(data) = response.get_mut("data")
            && let Some(turns) = data.get_mut("turns").and_then(|v| v.as_array_mut())
        {
            for turn in turns {
                if let Some(items) = turn.get_mut("items").and_then(|v| v.as_array_mut()) {
                    for item in items {
                        // Look for userMessage items
                        if let Some(user_msg) = item.get_mut("userMessage")
                            && let Some(content_arr) =
                                user_msg.get_mut("content").and_then(|v| v.as_array_mut())
                        {
                            for content_item in content_arr {
                                if let Some(obj) = content_item.as_object_mut()
                                    && obj.get("type") == Some(&json!("content"))
                                {
                                    // Unwrap the inner content
                                    if let Some(inner) =
                                        obj.get_mut("content").and_then(|v| v.as_object_mut())
                                        && let Some(text) =
                                            inner.get("text").and_then(|v| v.as_str())
                                    {
                                        // Convert to inputText
                                        *obj = serde_json::Map::from_iter([
                                            ("type".to_string(), json!("inputText")),
                                            ("text".to_string(), json!(text)),
                                        ]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        response
    }

    /// Look up the original working directory for a Grok session ID.
    fn lookup_cwd_for_session(&self, session_id: &str) -> Option<String> {
        // Fast path via sqlite
        let sqlite_path = self.sessions_dir.join("session_search.sqlite");
        if sqlite_path.exists()
            && let Ok(conn) = Connection::open_with_flags(
                &sqlite_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
            )
            && let Ok(mut stmt) =
                conn.prepare("SELECT cwd FROM session_docs WHERE session_id = ?1 LIMIT 1")
            && let Ok(mut rows) = stmt.query([session_id])
            && let Ok(Some(row)) = rows.next()
            && let Ok(cwd) = row.get::<_, String>(0)
            && !cwd.is_empty()
        {
            return Some(cwd);
        }

        // Fallback: walk and read summary.json
        let _ = std::fs::read_dir(&self.sessions_dir).ok()?;
        // (simplified walk - for production we'd cache this)
        for project_dir in std::fs::read_dir(&self.sessions_dir).ok()?.flatten() {
            let project_path = project_dir.path();
            if !project_path.is_dir() {
                continue;
            }

            let summary_path = project_path.join(session_id).join("summary.json");
            if let Ok(content) = std::fs::read_to_string(&summary_path)
                && let Ok(summary) = serde_json::from_str::<Value>(&content)
                && let Some(cwd) = summary
                    .get("info")
                    .and_then(|i| i.get("cwd"))
                    .and_then(|v| v.as_str())
                && !cwd.is_empty()
            {
                return Some(cwd.to_string());
            }
        }

        None
    }
}

/// Default location of Grok's session storage.
pub fn default_grok_sessions_dir() -> PathBuf {
    let home = std::env::var_os("HOME").unwrap_or_else(|| "/tmp".into());
    PathBuf::from(home).join(".grok/sessions")
}

/// Reads sessions from Grok's `session_search.sqlite` (preferred) or falls
/// back to walking the directory + reading `summary.json`.
fn read_grok_sessions(sessions_dir: &Path) -> Result<Vec<Value>> {
    let sqlite_path = sessions_dir.join("session_search.sqlite");

    if sqlite_path.exists() {
        return read_from_session_search_sqlite(&sqlite_path);
    }

    // Fallback: walk directories and read summary.json
    read_from_summary_files(sessions_dir)
}

fn read_from_session_search_sqlite(path: &Path) -> Result<Vec<Value>> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;

    let mut stmt = conn.prepare(
        "SELECT session_id, cwd, title, updated_at FROM session_docs ORDER BY updated_at DESC",
    )?;

    let rows = stmt.query_map([], |row| {
        let id: String = row.get(0)?;
        let cwd: String = row.get(1)?;
        let title: String = row.get(2)?;
        let updated_secs: i64 = row.get(3)?;

        let updated_ms = updated_secs.saturating_mul(1000);

        Ok(json!({
            "id": id,
            "sessionId": id,
            "preview": title,
            "name": if title.is_empty() { Value::Null } else { json!(title) },
            "ephemeral": false,
            "modelProvider": "grok",
            "createdAt": updated_ms,   // best we have from the index
            "updatedAt": updated_ms,
            "status": { "type": "notLoaded" },
            "cwd": cwd,
            "cliVersion": "",
            "source": "appServer",
            "agentNickname": null,
            "agentRole": null,
            "turns": [],
        }))
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn read_from_summary_files(sessions_dir: &Path) -> Result<Vec<Value>> {
    let mut out = Vec::new();

    // Walk URL-encoded CWD directories
    let Ok(entries) = std::fs::read_dir(sessions_dir) else {
        return Ok(vec![]);
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        for session_entry in std::fs::read_dir(&path)? {
            let session_entry = session_entry?;
            let session_path = session_entry.path();
            if !session_path.is_dir() {
                continue;
            }

            let summary_path = session_path.join("summary.json");
            if !summary_path.exists() {
                continue;
            }

            if let Ok(content) = std::fs::read_to_string(&summary_path)
                && let Ok(summary) = serde_json::from_str::<Value>(&content)
                && let Some(info) = summary.get("info")
            {
                let id = info.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let cwd = info.get("cwd").and_then(|v| v.as_str()).unwrap_or("");

                let title = summary
                    .get("session_summary")
                    .or_else(|| summary.get("generated_title"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let created = summary.get("created_at").and_then(|v| v.as_str());
                let updated = summary.get("updated_at").and_then(|v| v.as_str());

                // Convert ISO timestamps to millis (best effort)
                let created_ms = parse_iso_to_millis(created);
                let updated_ms = parse_iso_to_millis(updated).or(created_ms).unwrap_or(0);

                out.push(json!({
                    "id": id,
                    "sessionId": id,
                    "preview": title,
                    "name": if title.is_empty() { Value::Null } else { json!(title) },
                    "ephemeral": false,
                    "modelProvider": "grok",
                    "createdAt": created_ms,
                    "updatedAt": updated_ms,
                    "status": { "type": "notLoaded" },
                    "cwd": cwd,
                    "cliVersion": "",
                    "source": "appServer",
                    "agentNickname": null,
                    "agentRole": null,
                    "turns": [],
                }));
            }
        }
    }

    // Sort by updatedAt desc
    out.sort_by(|a, b| {
        let ta = a.get("updatedAt").and_then(|v| v.as_i64()).unwrap_or(0);
        let tb = b.get("updatedAt").and_then(|v| v.as_i64()).unwrap_or(0);
        tb.cmp(&ta)
    });

    Ok(out)
}

fn parse_iso_to_millis(iso: Option<&str>) -> Option<i64> {
    iso.and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.timestamp_millis())
    })
}

#[async_trait]
impl Bridge for GrokBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        self.inner.initialize(ctx, params).await
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        if method == "thread/list" {
            return self.handle_thread_list().await;
        }

        if method == "thread/resume" {
            return self.handle_thread_resume(ctx, params).await;
        }

        self.inner.dispatch(ctx, method, params).await
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        self.inner.notification(ctx, method, params).await;
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}
