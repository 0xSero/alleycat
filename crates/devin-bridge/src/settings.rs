//! User overrides from Devin's documented native files, not effective ACP state.
use alleycat_bridge_core::settings;
use alleycat_codex_proto as p;
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
const REFERENCE: &str = "https://docs.devin.ai/cli/reference/configuration/config-file.md";

pub fn path() -> Result<PathBuf> {
    settings::home_path(".config/devin/config.json")
}

pub async fn read(path: &Path) -> Result<p::ConfigReadResponse> {
    let mut response = settings::read_response(path)?;
    settings::append_published_settings(&mut response, REFERENCE, schema).await;
    // Since v3000.3 native MCP configuration is a separate file.
    let mcp_path = path
        .parent()
        .context("Devin settings directory missing")?
        .join("mcp_config.json");
    let mcp = settings::read_response(&mcp_path)?;
    let descriptors = response.config["_litterSettings"].as_array_mut().unwrap();
    for mut row in mcp.config["_litterSettings"].as_array().unwrap().clone() {
        row["key"] = json!(format!("mcp.{}", row["key"].as_str().unwrap()));
        row["label"] = json!(format!("MCP: {}", row["label"].as_str().unwrap()));
        descriptors.push(row);
    }
    for row in descriptors {
        row["scope"] = json!(if row["valueJson"] == "null" {
            "user override (unset)"
        } else {
            "user override"
        });
    }
    Ok(response)
}

pub fn write(path: &Path, mut params: p::ConfigBatchWriteParams) -> Result<p::ConfigWriteResponse> {
    let is_mcp = params
        .edits
        .first()
        .is_some_and(|e| e.key_path.starts_with("mcp."));
    anyhow::ensure!(
        params
            .edits
            .iter()
            .all(|e| e.key_path.starts_with("mcp.") == is_mcp),
        "one native file per settings write"
    );
    if is_mcp {
        for edit in &mut params.edits {
            edit.key_path = edit.key_path.strip_prefix("mcp.").unwrap().into();
        }
        settings::write(
            &path
                .parent()
                .context("Devin settings directory missing")?
                .join("mcp_config.json"),
            params,
        )
    } else {
        settings::write(path, params)
    }
}

fn schema(bytes: &[u8]) -> Result<Value> {
    let text = std::str::from_utf8(bytes)?;
    let options = text
        .split_once("## Options Reference")
        .context("Devin options boundary missing")?
        .1;
    let mut properties = serde_json::Map::new();
    for section in options.split("\n### ").skip(1) {
        let name = section
            .lines()
            .next()
            .unwrap_or("")
            .split_whitespace()
            .next()
            .unwrap_or("")
            .replace("\\_", "_");
        if name == "mcpServers"
            || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            || name.is_empty()
        {
            continue;
        }
        let mut children = serde_json::Map::new();
        let mut typed = false;
        let mut value_table = false;
        let mut values = Vec::new();
        for line in section
            .lines()
            .skip(1)
            .take_while(|s| !s.starts_with("## "))
        {
            if line.starts_with("| Option") {
                typed = true;
                value_table = false;
                continue;
            }
            if line.starts_with("| Value") {
                value_table = true;
                typed = false;
                continue;
            }
            if !line.starts_with('|') {
                value_table = false;
                typed = false;
                continue;
            }
            if !typed && !value_table {
                continue;
            }
            let cols: Vec<_> = line.split('|').map(str::trim).collect();
            if value_table && cols.len() > 2 {
                if let Some(literal) = cols[1].strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
                    if let Ok(value) = serde_json::from_str::<Value>(literal) {
                        values.push(value);
                    }
                }
            }
            if cols.len() > 3 {
                if let Some(key) = cols[1].strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
                    if key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
                        if typed {
                            children.insert(key.into(), primitive_type(cols[2]));
                        }
                    }
                }
            }
        }
        properties.insert(
            name,
            if children.is_empty() {
                let strings: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();
                if !strings.is_empty() && strings.iter().all(|v| v.is_boolean()) {
                    json!({"type":"boolean"})
                } else if !strings.is_empty()
                    && strings
                        .iter()
                        .all(|v| v.as_str().is_some_and(|v| !v.contains(['<', '>'])))
                {
                    json!({"type":"string","enum":strings})
                } else {
                    json!({})
                }
            } else {
                json!({"properties":children})
            },
        );
    }
    anyhow::ensure!(!properties.is_empty(), "Devin documented keys missing");
    Ok(json!({"properties":properties}))
}

fn primitive_type(text: &str) -> Value {
    let types: Vec<_> = text.split('/').map(str::trim).collect();
    if types.iter().all(|t| {
        matches!(
            *t,
            "string" | "boolean" | "number" | "integer" | "array" | "object" | "null"
        )
    }) {
        if types.len() == 1 {
            json!({"type":types[0]})
        } else {
            json!({"type":types})
        }
    } else {
        json!({})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires official public documentation endpoint"]
    async fn live_published_settings_are_unset_not_document_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let response = read(&dir.path().join("config.json")).await.unwrap();
        let rows = response.config["_litterSettings"].as_array().unwrap();
        assert!(
            rows.len() > 20,
            "native reference discovery returned only {} fields",
            rows.len()
        );
        assert!(
            rows.iter()
                .any(|v| v["key"] == "agent.model" && v["valueJson"] == "null")
        );
        assert!(rows.iter().any(|v| v["key"] == "mcp.$native"));
        eprintln!("Devin public metadata: {} descriptors", rows.len());
    }
    #[test]
    fn metadata_keeps_user_keys_and_typed_children_without_example_defaults() {
        let doc = b"## Options Reference\n### agent <span>(user only)</span>\n| Option | Type | Default |\n| --- | --- | --- |\n| `model` | string | secret-default |\n### theme\\_mode\n| Value | Meaning |\n| `dark` | Dark |\n### mcpServers\n## JSON with Comments\n";
        let parsed = schema(doc).unwrap();
        assert_eq!(
            parsed["properties"]["agent"]["properties"]["model"]["type"],
            "string"
        );
        assert_eq!(
            primitive_type("boolean/null")["type"],
            json!(["boolean", "null"])
        );
        assert!(parsed["properties"]["theme_mode"].is_object());
        assert!(parsed["properties"].get("mcpServers").is_none());
        assert!(!parsed.to_string().contains("secret-default"));
    }
    #[test]
    fn mcp_write_uses_separate_native_file_and_preserves_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let main = dir.path().join("config.json");
        let mcp = dir.path().join("mcp_config.json");
        std::fs::write(&main, "{\"show_hints\":true}").unwrap();
        std::fs::write(
            &mcp,
            r#"{"mcpServers":{"test":{"command":"old","env":{"TOKEN":"private"}}}}"#,
        )
        .unwrap();
        let params = serde_json::from_value(json!({"edits":[{"keyPath":"mcp.mcpServers.test.command","value":"new","mergeStrategy":"replace"}]})).unwrap();
        write(&main, params).unwrap();
        assert_eq!(settings::read(&main).unwrap()["show_hints"], true);
        let actual = settings::read(&mcp).unwrap();
        assert_eq!(actual["mcpServers"]["test"]["command"], "new");
        assert_eq!(actual["mcpServers"]["test"]["env"]["TOKEN"], "private");
        assert!(!settings::sanitize(&actual).to_string().contains("private"));
    }
}
