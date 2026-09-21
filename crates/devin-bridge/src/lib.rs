//! Devin-specific bridge.
//!
//! Devin speaks ACP, so the bulk of the work (initialize, thread/start,
//! thread/resume, turn/start, etc.) lives in `alleycat-acp-bridge`. The one
//! place ACP isn't enough is `thread/list`: devin's ACP `session/list` filters
//! its sessions (untitled / low-message sessions are omitted), and the iOS
//! app wants every saved session regardless of working directory. So this
//! crate is a thin wrapper that delegates everything to `AcpBridge` *except*
//! `thread/list`, which it answers by reading devin's local SQLite store at
//! `~/.local/share/devin/cli/sessions.db` directly.
//!
//! Keeping this devin-aware logic out of `acp-bridge` lets that crate stay a
//! generic adapter usable by any ACP agent.

mod models;
pub use models::parse_model_catalog;

use std::path::PathBuf;
use std::sync::Arc;

use alleycat_acp_bridge::AcpBridge;
use alleycat_bridge_core::{Bridge, Conn, JsonRpcError, error_codes};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::Connection;
use serde_json::{Value, json};
use tracing::warn;

/// Default location of devin's SQLite session store on every platform the
/// devin CLI supports today.
pub fn default_sessions_db() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(PathBuf::from(home).join(".local/share/devin/cli/sessions.db"))
}

pub struct DevinBridge {
    inner: Arc<AcpBridge>,
    sessions_db: PathBuf,
}

impl DevinBridge {
    pub fn new(inner: Arc<AcpBridge>, sessions_db: PathBuf) -> Self {
        Self { inner, sessions_db }
    }

    pub fn with_default_db(inner: Arc<AcpBridge>) -> Result<Self> {
        let path =
            default_sessions_db().context("could not determine $HOME for devin sessions.db")?;
        Ok(Self::new(inner, path))
    }

    /// Synchronous SQLite read on a blocking thread. The list is small
    /// (~hundreds of rows) so we don't bother with prepared-statement
    /// caching; cold open + scan is well under 10 ms in practice.
    fn read_sessions(path: &PathBuf) -> Result<Vec<Value>> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .with_context(|| format!("opening devin sessions.db at {}", path.display()))?;

        // Activity timestamps are stored as epoch-seconds (verified against
        // the running devin store at 2026-05-13; the schema comment "INTEGER"
        // is ambiguous and a `datetime(col/1000,'unixepoch')` query renders
        // everything as 1970, confirming seconds rather than millis).
        let mut stmt = conn.prepare(
            "SELECT id, title, working_directory, created_at, last_activity_at \
             FROM sessions \
             WHERE hidden = 0 \
             ORDER BY last_activity_at DESC",
        )?;

        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let title: Option<String> = row.get(1)?;
            let cwd: String = row.get(2)?;
            let created_secs: i64 = row.get(3)?;
            let updated_secs: i64 = row.get(4)?;
            Ok(SessionRow {
                id,
                title,
                cwd,
                created_secs,
                updated_secs,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            let row = row?;
            // codex-proto `Thread.created_at` / `updated_at` are epoch-millis
            // i64s. Devin stores seconds, so multiply.
            let created_ms = row.created_secs.saturating_mul(1000);
            let updated_ms = row.updated_secs.saturating_mul(1000);
            out.push(json!({
                "id": row.id,
                "sessionId": row.id,
                "preview": row.title.clone().unwrap_or_default(),
                "ephemeral": false,
                "modelProvider": "devin",
                "createdAt": created_ms,
                "updatedAt": updated_ms,
                "status": { "type": "notLoaded" },
                "cwd": row.cwd,
                "cliVersion": "",
                "source": "appServer",
                "agentNickname": null,
                "agentRole": null,
                "name": row.title,
                "turns": [],
            }));
        }
        Ok(out)
    }

    async fn handle_thread_list(&self) -> Result<Value, JsonRpcError> {
        let path = self.sessions_db.clone();
        let threads = tokio::task::spawn_blocking(move || Self::read_sessions(&path))
            .await
            .map_err(|join_err| JsonRpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("devin thread/list worker panicked: {join_err}"),
                data: None,
            })?
            .map_err(|err| {
                warn!(error = %err, "devin thread/list local read failed; returning empty list");
                JsonRpcError {
                    code: error_codes::INTERNAL_ERROR,
                    message: format!("devin sessions.db read failed: {err}"),
                    data: None,
                }
            })?;

        Ok(json!({
            "data": threads,
            "nextCursor": null,
            "backwardsCursor": null,
        }))
    }
}

struct SessionRow {
    id: String,
    title: Option<String>,
    cwd: String,
    created_secs: i64,
    updated_secs: i64,
}

#[async_trait]
impl Bridge for DevinBridge {
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
        self.inner.dispatch(ctx, method, params).await
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        self.inner.notification(ctx, method, params).await;
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::tempdir;

    fn seed_db(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                working_directory TEXT NOT NULL,
                backend_type TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '',
                agent_mode TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                title TEXT,
                hidden INTEGER NOT NULL DEFAULT 0
            );",
        )
        .unwrap();
        conn
    }

    #[test]
    fn read_sessions_returns_visible_in_recency_order_with_seconds_timestamps() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let conn = seed_db(&path);
        conn.execute(
            "INSERT INTO sessions (id, working_directory, created_at, last_activity_at, title, hidden) VALUES (?,?,?,?,?,?)",
            rusqlite::params!["older", "/a", 1_700_000_000_i64, 1_700_000_000_i64, rusqlite::types::Null, 0],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, working_directory, created_at, last_activity_at, title, hidden) VALUES (?,?,?,?,?,?)",
            rusqlite::params!["newer", "/b", 1_778_684_792_i64, 1_778_687_034_i64, "Devin ACP Bridge Implementation", 0],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, working_directory, created_at, last_activity_at, title, hidden) VALUES (?,?,?,?,?,?)",
            rusqlite::params!["hidden", "/c", 1_778_000_000_i64, 1_778_000_000_i64, "Hidden", 1],
        ).unwrap();
        drop(conn);

        let rows = DevinBridge::read_sessions(&path).expect("read_sessions");
        let ids: Vec<&str> = rows
            .iter()
            .map(|v| v.get("id").and_then(|x| x.as_str()).unwrap())
            .collect();
        assert_eq!(
            ids,
            vec!["newer", "older"],
            "ordered by last_activity_at DESC; hidden filtered out"
        );

        // `updatedAt` is epoch-millis i64 to match codex-proto `Thread`.
        // Devin's column is seconds, so we expect *1000.
        let updated = rows[0].get("updatedAt").and_then(|v| v.as_i64()).unwrap();
        assert_eq!(updated, 1_778_687_034_000);
        assert_eq!(
            rows[0].get("sessionId").and_then(|v| v.as_str()),
            Some("newer")
        );
        assert_eq!(
            rows[0]
                .get("status")
                .and_then(|v| v.get("type"))
                .and_then(|v| v.as_str()),
            Some("notLoaded")
        );
        assert_eq!(
            rows[0].get("source").and_then(|v| v.as_str()),
            Some("appServer")
        );
        assert_eq!(rows[0].get("cwd").and_then(|v| v.as_str()), Some("/b"));
        assert_eq!(
            rows[0].get("preview").and_then(|v| v.as_str()),
            Some("Devin ACP Bridge Implementation"),
        );
        // Untitled sessions: preview empty, name null.
        assert_eq!(rows[1].get("preview").and_then(|v| v.as_str()), Some(""));
    }
}
