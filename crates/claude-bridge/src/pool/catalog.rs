//! No-prompt discovery through Claude's SDK initialization response.
use anyhow::{Context, bail};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::io::BufReader;

use alleycat_bridge_core::framing::{read_json_line, write_json_line};
use alleycat_bridge_core::{ProcessSpec, StdioMode};

use super::ClaudePool;

impl ClaudePool {
    /// Use the configured launcher so remote hosts and their credentials remain
    /// authoritative. This transient process never receives a prompt or creates
    /// a persisted chat, and never changes a running conversation's settings.
    pub async fn catalog(&self) -> anyhow::Result<Value> {
        let mut spec = ProcessSpec::new(self.claude_bin.clone());
        spec.args = [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--no-session-persistence",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        spec.stderr = StdioMode::Null;
        let mut child = tokio::time::timeout(Duration::from_secs(5), self.launcher.launch(spec))
            .await
            .context("Claude catalog launch timed out")??;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut input = child.take_stdin().context("Claude catalog has no stdin")?;
            let output = child
                .take_stdout()
                .context("Claude catalog has no stdout")?;
            let mut output = BufReader::new(output);
            write_json_line(
                &mut input,
                &json!({
                    "type": "control_request", "request_id": "catalog",
                    "request": {"subtype": "initialize"}
                }),
            )
            .await?;
            while let Some(frame) = read_json_line::<Value, _>(&mut output).await? {
                if frame["type"] != "control_response"
                    || frame["response"]["request_id"] != "catalog"
                {
                    continue;
                }
                let response = &frame["response"];
                if response["subtype"] != "success" {
                    bail!("Claude rejected catalog initialization");
                }
                return Ok(response["response"].clone());
            }
            bail!("Claude exited before returning its catalog")
        })
        .await
        .context("Claude catalog initialization timed out");
        // Also reap on protocol errors; launcher drop is the cancellation guard.
        let _ = tokio::time::timeout(Duration::from_secs(2), child.kill()).await;
        result?
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn discovers_fresh_catalog_without_a_prompt_or_persisted_session() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("claude");
        let fixture = dir.path().join("catalog.json");
        // The fake rejects all calls except the no-prompt SDK discovery shape.
        std::fs::write(&script, format!(r#"#!/bin/sh
test "$*" = '-p --input-format stream-json --output-format stream-json --verbose --no-session-persistence' || exit 11
IFS= read -r request
case "$request" in *'"subtype":"initialize"'*) ;; *) exit 12 ;; esac
cat '{}'
"#, fixture.display())).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let pool = ClaudePool::new(script);
        for model in ["initial-model", "newly-enabled-model"] {
            std::fs::write(
                &fixture,
                serde_json::to_vec(&json!({
                    "type":"control_response", "response":{
                        "request_id":"catalog", "subtype":"success",
                        "response":{"models":[{"value":model}]}
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(pool.catalog().await.unwrap()["models"][0]["value"], model);
            assert!(
                pool.is_empty().await,
                "discovery must not occupy a conversation slot"
            );
        }
        std::fs::write(&fixture, r#"{"type":"control_response","response":{"request_id":"catalog","subtype":"error","error":"not authorized"}}"#).unwrap();
        assert!(pool.catalog().await.is_err());
    }
}
