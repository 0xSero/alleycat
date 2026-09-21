//! Native Grok user overrides. Requirements remain authoritative/read-only.
use alleycat_bridge_core::settings;
use alleycat_codex_proto as p;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
const REFERENCE: &str = "https://docs.x.ai/build/settings/reference.md";
pub fn path() -> Result<PathBuf> {
    path_for(
        std::env::var_os("GROK_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}
fn path_for(grok_home: Option<PathBuf>, home: Option<PathBuf>) -> Result<PathBuf> {
    Ok(match grok_home {
        Some(path) => path,
        None => home.context("home directory unavailable")?.join(".grok"),
    }
    .join("config.toml"))
}

fn requirements(path: &Path) -> Result<Vec<(PathBuf, Value)>> {
    [
        path.with_file_name("requirements.toml"),
        PathBuf::from("/etc/grok/requirements.toml"),
    ]
    .into_iter()
    .map(|p| Ok((p.clone(), settings::read(&p)?)))
    .collect()
}
fn pinned(key: &str, pins: &Value) -> bool {
    if key == "$native" {
        return pins.as_object().is_some_and(|p| !p.is_empty());
    }
    pins.as_object().is_some_and(|map| {
        map.iter().any(|(name, value)| {
            name == "version_overrides"
                || key == name
                || key
                    .strip_prefix(&format!("{name}."))
                    .is_some_and(|rest| !value.is_object() || pinned(rest, value))
        })
    })
}
pub async fn read(path: &Path) -> Result<p::ConfigReadResponse> {
    let mut response = settings::read_response(path)?;
    settings::append_published_settings(&mut response, REFERENCE, schema).await;
    let profiles_path = path.with_file_name("sandbox.toml");
    let profiles = settings::read_response(&profiles_path)?;
    append_rows(
        &mut response,
        profiles,
        "sandboxProfiles",
        "user sandbox profiles",
    );
    for source in [
        path.with_file_name("managed_config.toml"),
        PathBuf::from("/etc/grok/managed_config.toml"),
    ] {
        if !source.exists() {
            continue;
        }
        let managed = settings::response(
            settings::read(&source)?,
            &source.to_string_lossy(),
            false,
            Some("Managed native defaults; edit the user override instead"),
        );
        append_rows(
            &mut response,
            managed,
            if source.starts_with("/etc/") {
                "managed.system"
            } else {
                "managed.user"
            },
            "managed defaults",
        );
    }
    for (source, pins) in requirements(path)? {
        for row in response.config["_litterSettings"].as_array_mut().unwrap() {
            let key = row["key"].as_str().unwrap_or("");
            if pinned(key, &pins)
                || key.starts_with("sandboxProfiles.") && pins.get("sandbox").is_some()
            {
                row["writable"] = json!(false);
                row["readOnlyReason"] = json!(format!(
                    "Pinned by {}; edit policy in native Grok",
                    source.display()
                ));
            }
        }
        let policy = settings::response(
            pins,
            &source.to_string_lossy(),
            false,
            Some("Native Grok requirements policy"),
        );
        for mut row in policy.config["_litterSettings"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .cloned()
        {
            row["key"] = json!(format!(
                "requirements.{}.{}",
                if source.starts_with("/etc/") {
                    "system"
                } else {
                    "user"
                },
                row["key"].as_str().unwrap()
            ));
            row["scope"] = json!("requirements policy");
            response.config["_litterSettings"]
                .as_array_mut()
                .unwrap()
                .push(row);
        }
    }
    for row in response.config["_litterSettings"].as_array_mut().unwrap() {
        if !["requirements.", "managed.", "sandboxProfiles."]
            .iter()
            .any(|prefix| row["key"].as_str().unwrap_or("").starts_with(prefix))
        {
            row["scope"] = json!(if row["valueJson"] == "null" {
                "user override (unset)"
            } else {
                "user override"
            });
        }
    }
    Ok(response)
}
fn append_rows(
    response: &mut p::ConfigReadResponse,
    extra: p::ConfigReadResponse,
    prefix: &str,
    scope: &str,
) {
    for mut row in extra.config["_litterSettings"]
        .as_array()
        .unwrap()
        .iter()
        .cloned()
    {
        row["key"] = json!(format!("{prefix}.{}", row["key"].as_str().unwrap()));
        row["scope"] = json!(scope);
        row["label"] = json!(format!("{scope}: {}", row["label"].as_str().unwrap()));
        response.config["_litterSettings"]
            .as_array_mut()
            .unwrap()
            .push(row);
    }
}
pub fn write(path: &Path, mut params: p::ConfigBatchWriteParams) -> Result<p::ConfigWriteResponse> {
    let policies = requirements(path)?;
    let profiles = params
        .edits
        .first()
        .is_some_and(|e| e.key_path.starts_with("sandboxProfiles."));
    ensure!(
        params
            .edits
            .iter()
            .all(|e| e.key_path.starts_with("sandboxProfiles.") == profiles),
        "one native file per settings write"
    );
    for edit in &mut params.edits {
        ensure!(
            !edit.key_path.starts_with("requirements.") && !edit.key_path.starts_with("managed."),
            "native policy sources are read-only"
        );
        for (_, policy) in &policies {
            ensure!(
                !pinned(&edit.key_path, policy) && !(profiles && policy.get("sandbox").is_some()),
                "setting is pinned by native Grok requirements"
            );
        }
        if profiles {
            edit.key_path = edit
                .key_path
                .strip_prefix("sandboxProfiles.")
                .unwrap()
                .into();
        }
    }
    if profiles {
        settings::write(&path.with_file_name("sandbox.toml"), params)
    } else {
        settings::write(path, params)
    }
}

fn tokens(s: &str) -> Vec<&str> {
    s.split('`')
        .enumerate()
        .filter_map(|(i, s)| (i % 2 == 1).then_some(s))
        .collect()
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'.')
}
fn schema(bytes: &[u8]) -> Result<Value> {
    let text = std::str::from_utf8(bytes)?;
    let (_, text) = text
        .split_once("## TOML Values")
        .context("Grok TOML scope boundary missing")?;
    let mut root = json!({"properties":{}});
    for section in text.split("\n### ").skip(1) {
        let heading = section.lines().next().unwrap_or("");
        if heading.contains("sandbox.toml") {
            continue;
        }
        let sections: Vec<_> = tokens(heading)
            .into_iter()
            .filter_map(|s| s.strip_prefix('[')?.strip_suffix(']'))
            .collect();
        let mut has_section_column = false;
        let mut in_table = false;
        for line in section.lines().skip(1) {
            if line.starts_with("| Setting") {
                has_section_column = line.contains("| Section");
                in_table = true;
                continue;
            }
            if !line.starts_with('|') {
                in_table = false;
                continue;
            }
            if !in_table {
                continue;
            }
            let cols: Vec<_> = line.split('|').map(str::trim).collect();
            if cols.len() < 4 {
                continue;
            }
            let value_start = if has_section_column { 3 } else { 2 };
            if cols.len() <= value_start + 1 {
                continue;
            }
            let metadata = value_metadata(&cols[value_start..cols.len() - 2].join(" | "));
            let names = tokens(cols[1]);
            let row_sections: Vec<_> = if has_section_column {
                tokens(cols[2])
                    .into_iter()
                    .filter_map(|s| s.strip_prefix('[')?.strip_suffix(']'))
                    .collect()
            } else {
                sections.clone()
            };
            for group in row_sections {
                if !identifier(group) {
                    // Dynamic model/MCP names belong to the JSON object editor.
                    if let Some(base) = group.split('.').next().filter(|s| identifier(s)) {
                        root["properties"]
                            .as_object_mut()
                            .unwrap()
                            .entry(base.to_owned())
                            .or_insert(json!({}));
                    }
                    continue;
                }
                for name in &names {
                    if !identifier(name) {
                        continue;
                    }
                    let path = if group.rsplit('.').next() == Some(*name) {
                        group.to_owned()
                    } else {
                        format!("{group}.{name}")
                    };
                    let mut current = &mut root;
                    for part in path.split('.') {
                        if !current["properties"].is_object() {
                            current["properties"] = json!({});
                        }
                        current = current["properties"]
                            .as_object_mut()
                            .unwrap()
                            .entry(part.to_owned())
                            .or_insert(json!({}));
                    }
                    if !current["properties"].is_object() {
                        *current = if group.rsplit('.').next() == Some(*name) {
                            json!({"type":"object"})
                        } else {
                            metadata.clone()
                        };
                    }
                }
            }
        }
    }
    ensure!(
        !root["properties"].as_object().unwrap().is_empty(),
        "Grok documented keys missing"
    );
    Ok(root)
}
fn value_metadata(text: &str) -> Value {
    let values = tokens(text);
    if values.iter().any(|v| *v == "true") && values.iter().any(|v| *v == "false") {
        return json!({"type":"boolean"});
    }
    if matches!(text, "number" | "numbers") {
        return json!({"type":"number"});
    }
    if text == "string" {
        return json!({"type":"string"});
    }
    if text == "string array" {
        return json!({"type":"array"});
    }
    // Only a complete, closed list of literal alternatives becomes a picker.
    // Prose such as "model id (for example ...)" or "(or custom)" stays open.
    let outside = text
        .split('`')
        .enumerate()
        .filter_map(|(i, s)| (i % 2 == 0).then_some(s))
        .collect::<String>();
    let mut rest = outside.as_str();
    let mut residue = String::new();
    while let Some((before, tail)) = rest.split_once("(default") {
        residue.push_str(before);
        let Some((_, after)) = tail.split_once(')') else {
            return json!({});
        };
        rest = after;
    }
    residue.push_str(rest);
    if !values.is_empty()
        && residue
            .chars()
            .all(|c| c.is_whitespace() || matches!(c, '|' | '/'))
        && values.iter().all(|v| {
            !v.is_empty()
                && v.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
                && !matches!(*v, "true" | "false")
        })
        && values.iter().all(|v| v.parse::<f64>().is_err())
    {
        let mut choices = values;
        choices.dedup();
        return json!({"type":"string","enum":choices});
    }
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_path_honors_grok_home_without_home_dependency() {
        assert_eq!(
            path_for(Some("/custom/grok".into()), None).unwrap(),
            PathBuf::from("/custom/grok/config.toml")
        );
        assert_eq!(
            path_for(None, Some("/users/test".into())).unwrap(),
            PathBuf::from("/users/test/.grok/config.toml")
        );
        assert!(path_for(None, None).is_err());
    }
    #[tokio::test]
    #[ignore = "requires official public documentation endpoint"]
    async fn live_published_settings_are_unset_not_document_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let response = read(&dir.path().join("config.toml")).await.unwrap();
        let rows = response.config["_litterSettings"].as_array().unwrap();
        assert!(
            rows.len() > 75,
            "native reference discovery returned only {} fields",
            rows.len()
        );
        assert!(
            rows.iter()
                .any(|v| v["key"] == "ui.theme" && v["valueJson"] == "null")
        );
        assert!(
            !rows
                .iter()
                .any(|v| v["key"].as_str().unwrap().contains("api_key"))
        );
        eprintln!("Grok public metadata: {} descriptors", rows.len());
    }
    #[test]
    fn documented_schema_excludes_env_and_separate_sandbox_file() {
        let parsed=schema(b"## Environment variables\n| `GROK_HOME` | string |\n## TOML Values\n### `[models]`\n| Setting | Values | Description |\n| --- | --- | --- |\n| `default` | example | model |\n### `[ui]` and `[ui.display_refresh]`\n| Setting | Section | Values | Description |\n| --- | --- | --- | --- |\n| `theme` | `[ui]` | dark | theme |\n| `auto_cadence_enabled` | `[ui.display_refresh]` | false | cadence |\n### `sandbox.toml` custom profiles\n| Setting | Values | Description |\n| `extends` | default | profile |\n").unwrap();
        assert!(
            parsed
                .pointer("/properties/models/properties/default")
                .is_some()
        );
        assert!(
            parsed
                .pointer(
                    "/properties/ui/properties/display_refresh/properties/auto_cadence_enabled"
                )
                .is_some()
        );
        assert!(!parsed.to_string().contains("GROK_HOME"));
        assert!(!parsed.to_string().contains("extends"));
        assert!(!parsed.to_string().contains("dark"));
        assert_eq!(
            value_metadata("`true` / `false` (default `true`)")["type"],
            "boolean"
        );
        assert_eq!(
            value_metadata("`fullscreen` (default when unset) | `minimal`")["enum"],
            json!(["fullscreen", "minimal"])
        );
        let group = schema(b"## TOML Values\n### `[ui.status_line]`\n| Setting | Section | Values | Description |\n| `status_line` | `[ui.status_line]` | `builtin` | `command` | structured section |\n").unwrap();
        assert_eq!(
            group["properties"]["ui"]["properties"]["status_line"]["type"],
            "object"
        );
        assert!(
            value_metadata("model id (for example `grok-build`)")
                .get("enum")
                .is_none()
        );
        assert!(
            value_metadata("`off` (default) | `workspace` (or custom)")
                .get("enum")
                .is_none()
        );
    }
    #[test]
    fn native_toml_writes_preserve_secrets_and_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[ui]\ntheme = 'dark'\n[model.'grok-4.6']\napi_key = 'private'\nname = 'old'\n",
        )
        .unwrap();
        let params=serde_json::from_value(json!({"edits":[{"keyPath":"model.grok-4.6.name","value":"new","mergeStrategy":"replace"}]})).unwrap();
        write(&path, params).unwrap();
        let actual = settings::read(&path).unwrap();
        assert_eq!(actual["model"]["grok-4.6"]["name"], "new");
        assert_eq!(actual["model"]["grok-4.6"]["api_key"], "private");
        assert!(
            !settings::read_response(&path)
                .unwrap()
                .config
                .to_string()
                .contains("private")
        );
        std::fs::write(
            path.with_file_name("requirements.toml"),
            "[ui]\ntheme = 'light'\n",
        )
        .unwrap();
        let params = serde_json::from_value(
            json!({"edits":[{"keyPath":"ui.theme","value":"new","mergeStrategy":"replace"}]}),
        )
        .unwrap();
        assert!(write(&path, params).is_err());
        assert_eq!(settings::read(&path).unwrap()["ui"]["theme"], "dark");
        assert!(pinned("ui", &json!({"ui":{"theme":"light"}})));
        assert!(!pinned("ui.scroll_speed", &json!({"ui":{"theme":"light"}})));
        let params = serde_json::from_value(json!({"edits":[{"keyPath":"sandboxProfiles.profiles.test.extends","value":"workspace","mergeStrategy":"replace"}]})).unwrap();
        write(&path, params).unwrap();
        assert_eq!(
            settings::read(&path.with_file_name("sandbox.toml")).unwrap()["profiles"]["test"]["extends"],
            "workspace"
        );
        assert!(
            settings::read(&path)
                .unwrap()
                .get("sandboxProfiles")
                .is_none()
        );
        let params = serde_json::from_value(json!({"edits":[{"keyPath":"managed.user.ui.theme","value":"bad","mergeStrategy":"replace"}]})).unwrap();
        assert!(write(&path, params).is_err());
        std::fs::write(
            path.with_file_name("requirements.toml"),
            "[sandbox]\nprofile='strict'\n",
        )
        .unwrap();
        let params = serde_json::from_value(json!({"edits":[{"keyPath":"sandboxProfiles.profiles.test.extends","value":"off","mergeStrategy":"replace"}]})).unwrap();
        assert!(write(&path, params).is_err());
    }
}
