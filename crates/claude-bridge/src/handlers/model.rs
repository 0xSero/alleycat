//! Model choices come from the installed CLI's account/provider-aware catalog.
use crate::state::ConnectionState;
use alleycat_codex_proto as p;
use anyhow::Context;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

pub const MODEL_PROVIDER: &str = "anthropic";

pub fn normalize_claude_model_id(model: &str) -> String {
    let model = model.trim();
    model
        .strip_prefix(&format!("{MODEL_PROVIDER}/"))
        .unwrap_or(model)
        .to_string()
}

pub fn normalize_claude_model(model: Option<String>) -> Option<String> {
    model.map(|value| normalize_claude_model_id(&value))
}

pub async fn handle_model_list(
    state: &Arc<ConnectionState>,
    _params: p::ModelListParams,
) -> anyhow::Result<p::ModelListResponse> {
    models_from_catalog(&state.claude_pool().catalog().await?)
}

fn models_from_catalog(catalog: &Value) -> anyhow::Result<p::ModelListResponse> {
    let entries = catalog["models"]
        .as_array()
        .context("Claude catalog is missing models")?;
    let mut seen = HashSet::new();
    let mut data: Vec<p::Model> = entries
        .iter()
        .filter_map(|entry| {
            let id = entry["value"].as_str().filter(|id| !id.trim().is_empty())?;
            if !seen.insert(id) {
                return None;
            }
            let efforts: Vec<p::ReasoningEffortOption> = entry["supportedEffortLevels"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|value| {
                    let effort: p::ReasoningEffort = serde_json::from_value(value.clone()).ok()?;
                    Some(p::ReasoningEffortOption {
                        reasoning_effort: effort,
                        description: value.as_str().unwrap_or_default().to_string(),
                    })
                })
                .collect();
            let default_effort = efforts
                .iter()
                .find(|option| option.reasoning_effort == p::ReasoningEffort::Medium)
                .or_else(|| efforts.first())
                .map(|option| option.reasoning_effort)
                .unwrap_or(p::ReasoningEffort::None);
            Some(p::Model {
                // Keep CLI selection values (including aliases and context suffixes),
                // rather than replacing them with a provider-specific resolved id.
                id: id.into(),
                model: id.into(),
                upgrade: None,
                upgrade_info: None,
                availability_nux: None,
                display_name: entry["displayName"].as_str().unwrap_or(id).into(),
                description: entry["description"].as_str().unwrap_or_default().into(),
                hidden: false,
                supported_reasoning_efforts: efforts,
                default_reasoning_effort: default_effort,
                input_modalities: vec![json!("text"), json!("image")],
                supports_personality: false,
                additional_speed_tiers: Vec::new(),
                service_tiers: Vec::new(),
                is_default: id == "default",
            })
        })
        .collect();
    anyhow::ensure!(!data.is_empty(), "Claude returned no selectable models");
    if !data.iter().any(|model| model.is_default) {
        data[0].is_default = true;
    }
    Ok(p::ModelListResponse {
        data,
        next_cursor: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_sdk_catalog_preserves_account_choices_and_capabilities() {
        let response = models_from_catalog(&json!({"models": [
            {"value":"default","resolvedModel":"claude-opus-5[1m]","displayName":"Default (recommended)","supportedEffortLevels":["low","medium","high","xhigh","max"]},
            {"value":"claude-fable-5-1[1m]","resolvedModel":"claude-fable-5-1","displayName":"Fable","supportedEffortLevels":["high","max"]},
            {"value":"haiku","displayName":"Haiku"},
            {"value":"custom-deployment","displayName":"Private deployment"}
        ]})).unwrap();
        assert_eq!(response.data.len(), 4);
        assert_eq!(response.data[0].model, "default");
        assert_eq!(response.data[0].supported_reasoning_efforts.len(), 5);
        assert_eq!(response.data[1].id, "claude-fable-5-1[1m]");
        assert_eq!(
            response.data[1].default_reasoning_effort,
            p::ReasoningEffort::High
        );
        assert!(response.data[2].supported_reasoning_efforts.is_empty());
        assert_eq!(
            response.data[2].default_reasoning_effort,
            p::ReasoningEffort::None
        );
        assert_eq!(response.data[3].model, "custom-deployment");
        assert_eq!(
            response
                .data
                .iter()
                .filter(|model| model.is_default)
                .count(),
            1
        );
    }

    #[test]
    fn refresh_replaces_retired_models_without_inventing_choices() {
        let before = models_from_catalog(&json!({"models":[{"value":"old"}]})).unwrap();
        let after =
            models_from_catalog(&json!({"models":[{"value":"new"},{"value":"new"},{"value":""}]}))
                .unwrap();
        assert_eq!(before.data[0].id, "old");
        assert_eq!(after.data.len(), 1);
        assert_eq!(after.data[0].id, "new");
        assert!(after.data[0].is_default);
        assert!(models_from_catalog(&json!({"models":[]})).is_err());
        assert!(models_from_catalog(&json!({})).is_err());
    }
}
