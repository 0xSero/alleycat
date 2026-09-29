//! Bounded, no-session native model discovery for ACP wrappers.
use alleycat_bridge_core::{ProcessLauncher, ProcessSpec, StdioMode};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::io::AsyncReadExt;

pub type CatalogParser = fn(&str) -> Result<Vec<Value>>;

pub(crate) struct ModelCatalogCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub launcher: Arc<dyn ProcessLauncher>,
    pub parse: CatalogParser,
}

impl ModelCatalogCommand {
    pub async fn discover(&self) -> Result<Vec<Value>> {
        let mut spec = ProcessSpec::new(self.program.clone());
        spec.args = self.args.iter().map(Into::into).collect();
        spec.stdin = StdioMode::Null;
        spec.stderr = StdioMode::Null;
        let mut child = tokio::time::timeout(Duration::from_secs(5), self.launcher.launch(spec))
            .await.context("model catalog launch timed out")??;
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            const MAX_BYTES: u64 = 1024 * 1024;
            let mut output = child.take_stdout().context("model catalog has no stdout")?.take(MAX_BYTES + 1);
            let mut bytes = Vec::new();
            output.read_to_end(&mut bytes).await?;
            ensure!(bytes.len() <= MAX_BYTES as usize, "model catalog exceeded output limit");
            ensure!(child.wait().await?.success(), "native model catalog command failed");
            let mut models = (self.parse)(std::str::from_utf8(&bytes)?)?;
            let mut seen = std::collections::HashSet::new();
            models.retain(|m| m["value"].as_str().is_some_and(|id| !id.is_empty() && seen.insert(id.to_owned())));
            ensure!(!models.is_empty(), "native model catalog returned no selectable models");
            Ok(models)
        }).await.context("native model catalog timed out");
        let _ = tokio::time::timeout(Duration::from_secs(2), child.kill()).await;
        result?
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn refreshes_without_session_and_rejects_failed_commands() {
        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("agent");
        let fixture = dir.path().join("models.json");
        std::fs::write(&program, format!("#!/bin/sh\ntest \"$*\" = 'models list --format json' || exit 2\ncat '{}'\n", fixture.display())).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        use alleycat_bridge_core::{Bridge, Conn, SessionRegistry, SessionRegistryConfig};
        let bridge = crate::AcpBridge::builder().agent_bin(&program)
            .model_catalog_command(["models", "list", "--format", "json"].map(str::to_owned).to_vec(),
                |text| Ok(serde_json::from_str(text)?))
            .build().await.unwrap();
        let registry = SessionRegistry::new(SessionRegistryConfig::default());
        let conn = Conn::from_session(registry.get_or_create("test".into(), "devin"));
        for id in ["old-model", "new-model"] {
            std::fs::write(&fixture, serde_json::json!([{"value": id}, {"value":id}]).to_string()).unwrap();
            // No initialize/session/new call: this must never launch the ACP argv.
            let response = bridge.dispatch(&conn, "model/list", serde_json::json!({})).await.unwrap();
            assert_eq!(response["data"].as_array().unwrap().len(), 1);
            assert_eq!(response["data"][0]["model"], id);
            assert_eq!(response["data"][0]["supportedReasoningEfforts"], serde_json::json!([]));
        }
        std::fs::write(&program, "#!/bin/sh\nprintf '[{\"value\":\"bad\"}]'\nexit 1\n").unwrap();
        assert!(bridge.dispatch(&conn, "model/list", serde_json::json!({})).await.is_err());
        bridge.shutdown().await;
    }
}
