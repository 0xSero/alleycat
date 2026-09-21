//! Grok's native read-only model list includes configured provider models.
use anyhow::{Result, ensure};
use serde_json::{Value, json};

pub fn parse_model_catalog(text: &str) -> Result<Vec<Value>> {
    let (_, list) = text.split_once("Available models:").ok_or_else(|| anyhow::anyhow!("Grok catalog missing available models section"))?;
    let mut models = Vec::new();
    for line in list.lines() {
        let line = line.trim();
        let Some(id) = line.strip_prefix("- ").or_else(|| line.strip_prefix("* ")) else { continue };
        let is_default = id.ends_with(" (default)");
        let id = id.strip_suffix(" (default)").unwrap_or(id).trim();
        ensure!(!id.is_empty() && !id.chars().any(char::is_whitespace), "Grok catalog contains an invalid model row");
        models.push(json!({"value":id,"name":id,"isDefault":is_default}));
    }
    Ok(models)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_native_list_ignores_notices_and_keeps_custom_models() {
        let models = parse_model_catalog(include_str!("../tests/fixtures/models.txt")).unwrap();
        assert_eq!(models.len(), 3);
        assert_eq!(models[0]["value"], "grok-4.6");
        assert_eq!(models[1]["value"], "grok-4.5");
        assert_eq!(models[1]["isDefault"], true);
        assert_eq!(models[2]["value"], "kimi-k2.6");
        assert!(parse_model_catalog("Please log in").is_err());
    }
}
