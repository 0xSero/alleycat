//! Devin's native account catalog; no ACP session is created.
use anyhow::{Context, Result};
use serde_json::{Value, json};

pub fn parse_model_catalog(text: &str) -> Result<Vec<Value>> {
    let catalog: Value = serde_json::from_str(text)?;
    let families = catalog["families"].as_array().context("Devin catalog missing families")?;
    let mut models = Vec::new();
    for family in families {
        let variants = family["variants"].as_array().context("Devin model family missing variants")?;
        for variant in variants {
            let id = variant["model_uid"].as_str().context("Devin variant missing model_uid")?;
            models.push(json!({
                "value":id, "name":variant["label"].as_str().unwrap_or(id),
                "description":variant["cost_summary"].as_str().unwrap_or("")
            }));
        }
    }
    Ok(models)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_native_families_preserve_exact_variants() {
        let models = parse_model_catalog(include_str!("../tests/fixtures/models.json")).unwrap();
        assert_eq!(models.len(), 4);
        assert_eq!(models[0]["value"], "claude-opus-5-medium");
        assert_eq!(models[1]["value"], "claude-opus-5-max");
        assert_eq!(models[2]["value"], "gpt-6-astra-medium");
        assert_eq!(models[3]["value"], "gpt-6-astra-max-priority");
        assert!(!models.iter().any(|m| m["value"] == "opus"));
        assert!(parse_model_catalog("{}").is_err());
    }
}
