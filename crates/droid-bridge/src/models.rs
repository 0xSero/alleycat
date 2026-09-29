//! Discover the account's current models without creating a Droid session.
use alleycat_bridge_core::{ProcessLauncher, ProcessSpec, StdioMode};
use alleycat_bridge_core::framing::{read_json_line, write_json_line};
use alleycat_codex_proto as p;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::io::BufReader;

pub async fn discover(launcher: &dyn ProcessLauncher, binary: &Path) -> Result<Vec<p::Model>> {
    let mut spec = ProcessSpec::new(binary);
    spec.args = ["exec", "--input-format", "stream-jsonrpc", "--output-format", "stream-jsonrpc"].map(Into::into).to_vec();
    spec.stderr = StdioMode::Null;
    let mut child = tokio::time::timeout(Duration::from_secs(5), launcher.launch(spec)).await.context("Droid catalog launch timed out")??;
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let mut stdin = child.take_stdin().context("Droid catalog missing stdin")?;
        let mut stdout = BufReader::new(child.take_stdout().context("Droid catalog missing stdout")?);
        write_json_line(&mut stdin, &json!({"type":"request","jsonrpc":"2.0","factoryApiVersion":"1.0.0","factoryProtocolVersion":"1.36.0","id":"catalog","method":"droid.list_models","params":{}})).await?;
        while let Some(frame) = read_json_line::<Value, _>(&mut stdout).await? {
            if frame["id"] == "catalog" {
                ensure!(frame.get("error").is_none(), "Droid rejected native model discovery");
                return parse_catalog(&frame["result"]);
            }
        }
        anyhow::bail!("Droid exited before returning its catalog")
    }).await.context("Droid model discovery timed out");
    let _ = tokio::time::timeout(Duration::from_secs(2), child.kill()).await;
    result?
}

pub fn effort(value: &str) -> Option<p::ReasoningEffort> {
    serde_json::from_value(json!(if value == "off" { "none" } else { value })).ok()
}

pub fn parse_catalog(catalog: &Value) -> Result<Vec<p::Model>> {
    let entries = catalog["models"].as_array().context("Droid catalog missing models")?;
    let mut seen = std::collections::HashSet::new();
    let mut models = Vec::new();
    for entry in entries {
        if entry["disabled"] == true { continue; }
        let id = entry["id"].as_str().context("Droid catalog model missing id")?;
        if id.is_empty() || !seen.insert(id) { continue; }
        let efforts = entry["supportedReasoningEfforts"].as_array().into_iter().flatten().filter_map(|e| {
            let native = e.as_str()?;
            Some(p::ReasoningEffortOption { reasoning_effort: effort(native)?, description: native.to_string() })
        }).collect();
        models.push(p::Model {
            id:id.into(), model:id.into(), display_name:entry["displayName"].as_str().unwrap_or(id).into(),
            description:entry["modelProvider"].as_str().unwrap_or("Factory Droid").into(),
            upgrade:None, upgrade_info:None, availability_nux:None,
            hidden:entry["deprecated"].as_bool().unwrap_or(false),
            supported_reasoning_efforts:efforts,
            default_reasoning_effort:entry["defaultReasoningEffort"].as_str().and_then(effort).unwrap_or(p::ReasoningEffort::None),
            input_modalities: if entry["noImageSupport"] == true { vec![json!("text")] } else { vec![json!("text"),json!("image")] },
            supports_personality:false, additional_speed_tiers:vec![], service_tiers:vec![],
            is_default:entry["isDefault"].as_bool().unwrap_or(false),
        });
    }
    ensure!(!models.is_empty(), "Droid returned no available models");
    Ok(models)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_native_catalog_preserves_custom_ids_and_model_capabilities() {
        let catalog: Value = serde_json::from_str(include_str!("../tests/fixtures/models.json")).unwrap();
        let models = parse_catalog(&catalog).unwrap();
        assert_eq!(models.len(), 4);
        let opus = models.iter().find(|m| m.id == "claude-opus-5").unwrap();
        assert!(opus.supported_reasoning_efforts.iter().any(|e| e.reasoning_effort == p::ReasoningEffort::XHigh));
        assert!(opus.supported_reasoning_efforts.iter().any(|e| e.reasoning_effort == p::ReasoningEffort::Max));
        assert!(!opus.supported_reasoning_efforts.iter().any(|e| e.reasoning_effort == p::ReasoningEffort::Minimal));
        assert!(models.iter().any(|m| m.id == "custom:Direct-Claude-Opus-4.6-0"));
        let glm = models.iter().find(|m| m.id == "glm-5.3").unwrap();
        assert_eq!(glm.input_modalities, vec![json!("text")]);
        assert!(parse_catalog(&json!({"models":[{"id":"disabled","disabled":true}]})).is_err());
    }
}
