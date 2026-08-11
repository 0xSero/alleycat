//! Session persistence to disk.
//!
//! Stores one `<session_id>.json` per thread containing the full
//! `Vec<StoredTurn>` we've observed. The on-disk schema is intentionally
//! the same shape as `StoredTurn` itself (via `Serialize`/`Deserialize`)
//! so future changes only need to add `#[serde(default)]` to stay
//! backward-compatible.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tracing::{debug, info};

use crate::bridge::StoredTurn;

/// Persisted session data.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSession {
    session_id: String,
    turns: Vec<StoredTurn>,
    session_status: String, // "Idle" or "Active"
    timestamp: i64,
}

/// Session persistence manager.
pub struct SessionPersistence {
    state_dir: PathBuf,
}

impl SessionPersistence {
    /// Create a new session persistence manager.
    pub fn new(state_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&state_dir)?;
        info!(
            "Session persistence initialized with state dir: {:?}",
            state_dir
        );
        Ok(Self { state_dir })
    }

    /// Get the path for a session file.
    fn session_path(&self, session_id: &str) -> PathBuf {
        self.state_dir.join(format!("{}.json", session_id))
    }

    /// Save a session to disk.
    pub fn save_session(
        &self,
        session_id: &str,
        turns: &[StoredTurn],
        session_status: &str,
    ) -> Result<()> {
        let path = self.session_path(session_id);
        let persisted = PersistedSession {
            session_id: session_id.to_string(),
            turns: turns.to_vec(),
            session_status: session_status.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        let json = serde_json::to_string_pretty(&persisted)?;
        let mut file = fs::File::create(&path)?;
        file.write_all(json.as_bytes())?;
        debug!("Saved session {} to disk", session_id);
        Ok(())
    }

    /// Load a session from disk.
    pub fn load_session(&self, session_id: &str) -> Result<Option<Vec<StoredTurn>>> {
        let path = self.session_path(session_id);
        if !path.exists() {
            debug!("Session {} not found on disk", session_id);
            return Ok(None);
        }

        let json = fs::read_to_string(&path)?;
        let persisted: PersistedSession = serde_json::from_str(&json)?;
        info!(
            "Loaded session {} from disk ({} turns)",
            session_id,
            persisted.turns.len()
        );
        Ok(Some(persisted.turns))
    }

    /// Delete a session from disk.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        let path = self.session_path(session_id);
        if path.exists() {
            fs::remove_file(&path)?;
            debug!("Deleted session {} from disk", session_id);
        }
        Ok(())
    }

    /// List all persisted sessions.
    pub fn list_sessions(&self) -> Result<Vec<String>> {
        let mut sessions = Vec::new();
        for entry in fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "json")
                && let Some(session_id) = entry
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            {
                sessions.push(session_id);
            }
        }
        debug!("Listed {} persisted sessions", sessions.len());
        Ok(sessions)
    }

    /// Clear all persisted sessions.
    pub fn clear_all(&self) -> Result<()> {
        for entry in fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "json") {
                fs::remove_file(entry.path())?;
            }
        }
        info!("Cleared all persisted sessions");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn sample_turn(id: &str, started_at_ms: i64) -> StoredTurn {
        StoredTurn {
            id: id.to_string(),
            items: vec![
                json!({
                    "id": format!("acp-user-{id}"),
                    "type": "userMessage",
                    "content": [{"type": "text", "text": "hi"}],
                }),
                json!({
                    "id": format!("acp-agent-{id}"),
                    "type": "agentMessage",
                    "text": "hello",
                    "phase": null,
                    "memoryCitation": null,
                }),
            ],
            status: "completed".to_string(),
            started_at_ms,
            completed_at_ms: Some(started_at_ms + 500),
            error: None,
        }
    }

    #[test]
    fn save_and_load_session_roundtrips_turns() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = SessionPersistence::new(temp_dir.path().to_path_buf()).unwrap();

        let turns = vec![sample_turn("t1", 1_000), sample_turn("t2", 2_000)];
        persistence
            .save_session("session-x", &turns, "Idle")
            .unwrap();

        let loaded = persistence.load_session("session-x").unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "t1");
        assert_eq!(loaded[0].items.len(), 2);
        assert_eq!(loaded[1].started_at_ms, 2_000);
    }

    #[test]
    fn load_nonexistent_session() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = SessionPersistence::new(temp_dir.path().to_path_buf()).unwrap();
        assert!(persistence.load_session("missing").unwrap().is_none());
    }

    #[test]
    fn delete_session_removes_file() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = SessionPersistence::new(temp_dir.path().to_path_buf()).unwrap();

        persistence
            .save_session("s", &[sample_turn("t", 0)], "Idle")
            .unwrap();
        persistence.delete_session("s").unwrap();
        assert!(persistence.load_session("s").unwrap().is_none());
    }

    #[test]
    fn list_sessions_returns_all_saved() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = SessionPersistence::new(temp_dir.path().to_path_buf()).unwrap();

        persistence
            .save_session("a", &[sample_turn("t", 0)], "Idle")
            .unwrap();
        persistence
            .save_session("b", &[sample_turn("t", 0)], "Idle")
            .unwrap();

        let sessions = persistence.list_sessions().unwrap();
        assert_eq!(sessions.len(), 2);
        assert!(sessions.contains(&"a".to_string()));
        assert!(sessions.contains(&"b".to_string()));
    }
}
