//! Wire-compat: today's `threads.json` (with `claudeSessionPath` /
//! `claudeSessionId` flat at row level) round-trips through the new
//! `bridge_core::ThreadIndex<ClaudeSessionRef>` cleanly.

use std::path::PathBuf;

use alleycat_bridge_core::ThreadIndex;
use alleycat_claude_bridge::index::ClaudeSessionRef;
use alleycat_codex_proto::ThreadSourceKind;
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn legacy_threads_json_round_trips_through_new_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("threads.json");

    let legacy = serde_json::to_string_pretty(&json!({
        "threads": [
            {
                "threadId": "abc-123",
                "claudeSessionPath": "/Users/me/.claude/projects/-tmp/abc-123.jsonl",
                "claudeSessionId": "abc-123",
                "cwd": "/tmp/work",
                "createdAt": 1_700_000_000_000_i64,
                "updatedAt": 1_700_000_001_000_i64,
                "archived": false,
                "preview": "hello world",
                "modelProvider": "anthropic",
                "source": "appServer"
            }
        ]
    }))
    .unwrap();
    std::fs::write(&path, legacy).unwrap();

    let index: std::sync::Arc<ThreadIndex<ClaudeSessionRef>> =
        ThreadIndex::open_at(path.clone()).await.unwrap();
    let row = index.lookup("abc-123").await.expect("row");
    assert_eq!(row.thread_id, "abc-123");
    assert_eq!(row.cwd, "/tmp/work");
    assert_eq!(row.preview, "hello world");
    assert!(!row.archived);
    assert_eq!(row.created_at, 1_700_000_000_000);
    assert_eq!(row.updated_at, 1_700_000_001_000);
    assert!(matches!(row.source, ThreadSourceKind::AppServer));
    assert_eq!(row.metadata.claude_session_id, "abc-123");
    assert_eq!(
        row.metadata.claude_session_path,
        PathBuf::from("/Users/me/.claude/projects/-tmp/abc-123.jsonl")
    );

    // Re-persist via insert and verify on-disk shape preserves both legacy
    // keys at the row's top level (i.e. flatten worked).
    index
        .update_preview_and_updated_at(
            "abc-123",
            "updated".into(),
            chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_002, 0).unwrap(),
        )
        .await
        .unwrap();
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.contains("\"claudeSessionPath\""), "raw=\n{raw}");
    assert!(raw.contains("\"claudeSessionId\""), "raw=\n{raw}");
    assert!(raw.contains("\"abc-123\""), "raw=\n{raw}");
    assert!(raw.contains("\"updated\""), "raw=\n{raw}");
}
