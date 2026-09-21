//! Native user settings. Never report a write that did not reach the native file.
use alleycat_codex_proto as p;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};
#[path = "settings_schema.rs"]
mod public_schema;
pub use public_schema::{
    append_claude_declared_settings, append_droid_declared_settings, append_published_settings,
};

static WRITES: Mutex<()> = Mutex::new(());

pub fn sensitive(key: &str) -> bool {
    let key = key.to_ascii_lowercase().replace(['_', '-'], "");
    key.split('.').any(|part| part == "token" || part == "auth")
        || [
            "apikey",
            "secret",
            "password",
            "credential",
            "authorization",
            "accesstoken",
            "refreshtoken",
            "bearer",
            "headers",
            "env",
        ]
        .iter()
        .any(|s| key.contains(s))
}
pub fn sanitize(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(k, _)| !sensitive(k))
                .map(|(k, v)| (k.clone(), sanitize(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(sanitize).collect()),
        other => other.clone(),
    }
}
pub fn home_path(relative: &str) -> Result<PathBuf> {
    Ok(
        PathBuf::from(std::env::var_os("HOME").context("home directory unavailable")?)
            .join(relative),
    )
}
pub fn read(path: &Path) -> Result<Value> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(json!({})),
        Err(e) => return Err(e.into()),
    };
    let value: Value = if matches!(
        path.extension().and_then(|s| s.to_str()),
        Some("yaml" | "yml")
    ) {
        serde_yaml::from_str(&text)?
    } else if path.extension().and_then(|s| s.to_str()) == Some("toml") {
        let native: toml::Value =
            toml::from_str(&text).map_err(|_| anyhow::anyhow!("invalid native TOML settings"))?;
        ensure_json_toml(&native)?;
        serde_json::to_value(native)?
    } else {
        serde_json::from_str(&strip_json_comments(&text)?)
            .map_err(|_| anyhow::anyhow!("invalid native JSON settings"))?
    };
    if !value.is_object() {
        bail!("native settings must be an object");
    }
    Ok(value)
}
// JSON has no date/time type. Refuse these documents rather than silently
// changing an unknown native TOML datetime into a string or private serde table.
fn ensure_json_toml(value: &toml::Value) -> Result<()> {
    match value {
        toml::Value::Datetime(_) => {
            bail!("native TOML date/time values require the native configuration editor")
        }
        toml::Value::Float(value) if !value.is_finite() => {
            bail!("non-finite native TOML numbers require the native configuration editor")
        }
        toml::Value::Array(values) => {
            for value in values {
                ensure_json_toml(value)?;
            }
        }
        toml::Value::Table(values) => {
            for value in values.values() {
                ensure_json_toml(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
// Native Devin configuration permits JSON comments. Preserve string contents,
// including URLs and escaped quotes; malformed block comments fail closed.
fn strip_json_comments(text: &str) -> Result<String> {
    let mut out = Vec::with_capacity(text.len());
    let mut bytes = text.bytes().peekable();
    let mut quoted = false;
    let mut escaped = false;
    while let Some(c) = bytes.next() {
        if quoted {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                quoted = false;
            }
        } else if c == b'"' {
            quoted = true;
            out.push(c);
        } else if c == b'/' && bytes.peek() == Some(&b'/') {
            bytes.next();
            for c in bytes.by_ref() {
                if c == b'\n' {
                    break;
                }
            }
            out.push(b'\n');
        } else if c == b'/' && bytes.peek() == Some(&b'*') {
            bytes.next();
            let mut closed = false;
            while let Some(c) = bytes.next() {
                if c == b'*' && bytes.peek() == Some(&b'/') {
                    bytes.next();
                    closed = true;
                    break;
                }
            }
            if !closed {
                bail!("unterminated native JSON comment");
            }
            out.push(b' ');
        } else {
            out.push(c);
        }
    }
    Ok(String::from_utf8(out)?)
}
pub fn response(
    config: Value,
    source: &str,
    writable: bool,
    reason: Option<&str>,
) -> p::ConfigReadResponse {
    let mut clean = sanitize(&config);
    let mut descriptors = Vec::new();
    describe(&clean, "", source, writable, reason, &mut descriptors);
    descriptors.insert(0,json!({"key":"$native","label":"All native settings (JSON)","valueJson":serde_json::to_string(&clean).unwrap(),"valueKind":"json","choices":[],"scope":"user","source":source,"writable":writable,"readOnlyReason":reason}));
    clean
        .as_object_mut()
        .expect("settings object")
        .insert("_litterSettings".into(), json!(descriptors));
    p::ConfigReadResponse {
        config: clean,
        origins: Default::default(),
        layers: None,
    }
}
fn describe(
    value: &Value,
    prefix: &str,
    source: &str,
    writable: bool,
    reason: Option<&str>,
    out: &mut Vec<Value>,
) {
    if let Some(map) = value.as_object() {
        for (key, value) in map {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            if value.is_object() && !value.as_object().unwrap().is_empty() {
                describe(value, &path, source, writable, reason, out);
            } else {
                out.push(json!({"key":path,"label":path,"valueJson":serde_json::to_string(value).unwrap(),"valueKind":match value { Value::Bool(_)=>"boolean", Value::Number(_)=>"number", Value::String(_)=>"string",_=>"json" },"choices":[],"scope":"user","source":source,"writable":writable,"readOnlyReason":reason}));
            }
        }
    }
}
pub fn read_response(path: &Path) -> Result<p::ConfigReadResponse> {
    Ok(response(read(path)?, &path.to_string_lossy(), true, None))
}
/// Prefer a literal key (Amp uses dots in native keys), otherwise descend.
pub fn set(root: &mut Value, key: &str, value: Value, merge: p::MergeStrategy) -> Result<()> {
    if key.is_empty()
        || key.split('.').any(|s| s.is_empty() || sensitive(s))
        || key.starts_with('_')
    {
        bail!("invalid or sensitive setting key");
    }
    if sanitize(&value) != value {
        bail!("credential settings cannot be edited through the mobile settings surface");
    }
    let map = root
        .as_object_mut()
        .context("settings parent is not an object")?;
    if map.contains_key(key) || !key.contains('.') {
        if matches!(merge, p::MergeStrategy::Upsert) && value.is_object() {
            let dest = map.entry(key).or_insert_with(|| json!({}));
            merge_values(dest, value);
        } else {
            let target = map.entry(key).or_insert(Value::Null);
            replace_visible(target, value)?;
        }
    } else {
        // Native provider/model IDs may contain dots (for example grok-4.6).
        let (head, tail) = key
            .rmatch_indices('.')
            .find_map(|(i, _)| {
                map.contains_key(&key[..i])
                    .then_some((&key[..i], &key[i + 1..]))
            })
            .unwrap_or_else(|| key.split_once('.').unwrap());
        set(
            map.entry(head).or_insert_with(|| json!({})),
            tail,
            value,
            merge,
        )?;
    }
    Ok(())
}
fn merge_values(dest: &mut Value, source: Value) {
    if let (Some(to), Some(from)) = (dest.as_object_mut(), source.as_object()) {
        for (key, value) in from {
            merge_values(to.entry(key).or_insert(Value::Null), value.clone());
        }
    } else {
        *dest = source;
    }
}
pub fn write(path: &Path, params: p::ConfigBatchWriteParams) -> Result<p::ConfigWriteResponse> {
    write_mode(path, params, false)
}
pub fn write_flat(
    path: &Path,
    params: p::ConfigBatchWriteParams,
) -> Result<p::ConfigWriteResponse> {
    write_mode(path, params, true)
}
fn write_mode(
    path: &Path,
    params: p::ConfigBatchWriteParams,
    literal_keys: bool,
) -> Result<p::ConfigWriteResponse> {
    let _guard = WRITES
        .lock()
        .map_err(|_| anyhow::anyhow!("settings lock poisoned"))?;
    if params
        .file_path
        .as_deref()
        .is_some_and(|p| Path::new(p) != path)
    {
        bail!("only the native user settings file can be edited");
    }
    if params.expected_version.is_some() {
        bail!("native settings version preconditions are not supported");
    }
    // Match native readers when users keep their config as a dotfiles symlink.
    let resolved = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Some(path.canonicalize()?),
        _ => None,
    };
    // Format follows the native filename even when a dotfiles symlink target
    // has no extension. Persistence follows the target without replacing the link.
    let source_path = path;
    let path = resolved.as_deref().unwrap_or(path);
    let mut value = read(source_path)?;
    for edit in params.edits {
        if edit.key_path == "$native" {
            if !edit.value.is_object() || sanitize(&edit.value) != edit.value {
                bail!("native settings require a credential-free object");
            }
            replace_visible(&mut value, edit.value)?;
        } else {
            if literal_keys {
                value
                    .as_object_mut()
                    .unwrap()
                    .entry(edit.key_path.clone())
                    .or_insert(Value::Null);
            }
            set(&mut value, &edit.key_path, edit.value, edit.merge_strategy)?;
        }
    }
    let bytes = if matches!(
        source_path.extension().and_then(|s| s.to_str()),
        Some("yaml" | "yml")
    ) {
        serde_yaml::to_string(&value)?.into_bytes()
    } else if source_path.extension().and_then(|s| s.to_str()) == Some("toml") {
        toml::to_string_pretty(&value)
            .map_err(|_| anyhow::anyhow!("settings contain a value unsupported by TOML"))?
            .into_bytes()
    } else {
        serde_json::to_vec_pretty(&value)?
    };
    let parent = path.parent().context("settings path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    if path.exists() {
        file.as_file()
            .set_permissions(std::fs::metadata(path)?.permissions())?;
    }
    file.write_all(&bytes)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| e.error)?;
    if read(source_path)? != value {
        bail!("native settings read-back did not match write");
    }
    Ok(p::ConfigWriteResponse {
        status: p::WriteStatus::Ok,
        version: String::new(),
        file_path: path.to_string_lossy().into_owned(),
        overridden_metadata: None,
    })
}
/// Replace the visible tree while retaining credentials omitted from its projection.
fn replace_visible(original: &mut Value, mut replacement: Value) -> Result<()> {
    if original.is_array() && sanitize(original) != *original {
        if sanitize(original) != replacement {
            bail!("edit credential-bearing arrays in the native runtime");
        }
        return Ok(());
    }
    if let Some(old) = original.as_object() {
        if !replacement.is_object() && sanitize(original) != *original {
            bail!("cannot replace a credential-bearing settings group");
        }
        if let Some(new) = replacement.as_object_mut() {
            for (key, value) in old {
                if sensitive(key) {
                    new.insert(key.clone(), value.clone());
                } else if let Some(target) = new.get_mut(key) {
                    let mut preserved = value.clone();
                    replace_visible(&mut preserved, target.clone())?;
                    *target = preserved;
                } else if sanitize(value) != *value {
                    bail!("cannot remove a credential-bearing settings group");
                }
            }
        }
    }
    *original = replacement;
    Ok(())
}
pub fn write_one(path: &Path, params: p::ConfigValueWriteParams) -> Result<p::ConfigWriteResponse> {
    write(
        path,
        p::ConfigBatchWriteParams {
            edits: vec![p::ConfigEdit {
                key_path: params.key_path,
                value: params.value,
                merge_strategy: params.merge_strategy,
            }],
            file_path: params.file_path,
            expected_version: params.expected_version,
            reload_user_config: false,
        },
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persists_native_json_and_yaml_preserving_secrets() {
        for name in ["settings.json", "config.yml"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(name);
            std::fs::write(&path, "{\"apiKey\":\"private\",\"model\":\"old\"}").unwrap();
            write_one(
                &path,
                p::ConfigValueWriteParams {
                    key_path: "model".into(),
                    value: json!("new"),
                    merge_strategy: p::MergeStrategy::Replace,
                    file_path: None,
                    expected_version: None,
                },
            )
            .unwrap();
            assert_eq!(read(&path).unwrap()["model"], "new");
            assert_eq!(read(&path).unwrap()["apiKey"], "private");
            assert!(
                !read_response(&path)
                    .unwrap()
                    .config
                    .to_string()
                    .contains("private")
            );
        }
    }
    #[test]
    fn whole_config_edit_preserves_nested_credentials_and_removes_visible_fields() {
        let mut value = json!({"model":"old","obsolete":true,"providers":{"private":{"apiKey":"secret","url":"old"}}});
        replace_visible(
            &mut value,
            json!({"model":"new","providers":{"private":{"url":"new"}}}),
        )
        .unwrap();
        assert_eq!(
            value,
            json!({"model":"new","providers":{"private":{"apiKey":"secret","url":"new"}}})
        );
        assert!(replace_visible(&mut value, json!({"model":"next"})).is_err());
    }
    #[test]
    fn refuses_destructive_edits_to_redacted_arrays() {
        let mut value = json!({"customModels":[{"name":"x","apiKey":"secret"}]});
        assert!(
            set(
                &mut value,
                "customModels",
                json!([]),
                p::MergeStrategy::Replace
            )
            .is_err()
        );
        set(
            &mut value,
            "customModels",
            json!([{"name":"x"}]),
            p::MergeStrategy::Replace,
        )
        .unwrap();
        assert_eq!(value["customModels"][0]["apiKey"], "secret");
    }
    #[test]
    fn flat_native_keys_remain_literal_when_previously_unset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        let params=serde_json::from_value(json!({"edits":[{"keyPath":"amp.notifications.enabled","value":false,"mergeStrategy":"replace"}]})).unwrap();
        write_flat(&path, params).unwrap();
        assert_eq!(
            read(&path).unwrap(),
            json!({"amp.notifications.enabled":false})
        );
    }
    #[cfg(unix)]
    #[test]
    fn native_dotfiles_symlink_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let actual = dir.path().join("actual.json");
        let link = dir.path().join("settings.json");
        std::fs::write(&actual, "{}").unwrap();
        std::os::unix::fs::symlink(&actual, &link).unwrap();
        let params = serde_json::from_value(
            json!({"keyPath":"model","value":"new","mergeStrategy":"replace"}),
        )
        .unwrap();
        write_one(&link, params).unwrap();
        assert!(
            std::fs::symlink_metadata(link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(read(&actual).unwrap()["model"], "new");
    }
    #[test]
    fn rejects_arbitrary_file_override_without_modifying_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        let params = p::ConfigValueWriteParams {
            key_path: "model".into(),
            value: json!("new"),
            merge_strategy: p::MergeStrategy::Replace,
            file_path: Some("/tmp/elsewhere".into()),
            expected_version: None,
        };
        assert!(write_one(&path, params).is_err());
        assert!(!path.exists());
    }
    #[test]
    fn rejects_corrupt_settings_and_secret_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, "broken").unwrap();
        assert!(read(&path).is_err());
        assert!(
            set(
                &mut json!({}),
                "api_key",
                json!("secret"),
                p::MergeStrategy::Replace
            )
            .is_err()
        );
    }
    #[test]
    fn preserves_literal_dotted_keys() {
        let mut value = json!({"amp.notifications.enabled":true});
        set(
            &mut value,
            "amp.notifications.enabled",
            json!(false),
            p::MergeStrategy::Replace,
        )
        .unwrap();
        assert_eq!(value, json!({"amp.notifications.enabled":false}));
    }
}

/// Resolve the settings path in the selected launcher's environment. The remote
/// process sanitizes before returning so credentials never cross this boundary.
pub async fn remote_read(
    launcher: &dyn crate::ProcessLauncher,
    runtime: &str,
) -> Result<p::ConfigReadResponse> {
    let result = remote_command(launcher, runtime, "read", None).await?;
    let source = result["source"]
        .as_str()
        .context("native settings source missing")?;
    let config = result
        .get("config")
        .filter(|v| v.is_object())
        .context("invalid native config response")?
        .clone();
    Ok(response(config, source, true, None))
}
pub async fn remote_write(
    launcher: &dyn crate::ProcessLauncher,
    runtime: &str,
    params: p::ConfigBatchWriteParams,
) -> Result<p::ConfigWriteResponse> {
    for edit in &params.edits {
        if sensitive(&edit.key_path) || sanitize(&edit.value) != edit.value {
            bail!("credential settings cannot be sent through the mobile settings surface");
        }
    }
    let result = remote_command(
        launcher,
        runtime,
        "write",
        Some(serde_json::to_string(&params)?),
    )
    .await?;
    Ok(serde_json::from_value(result)?)
}
pub fn batch_from_one(params: p::ConfigValueWriteParams) -> p::ConfigBatchWriteParams {
    p::ConfigBatchWriteParams {
        edits: vec![p::ConfigEdit {
            key_path: params.key_path,
            value: params.value,
            merge_strategy: params.merge_strategy,
        }],
        file_path: params.file_path,
        expected_version: params.expected_version,
        reload_user_config: false,
    }
}
async fn remote_command(
    launcher: &dyn crate::ProcessLauncher,
    runtime: &str,
    operation: &str,
    params: Option<String>,
) -> Result<Value> {
    if !matches!(runtime, "pi" | "claude") {
        bail!("unsupported remote settings runtime");
    }
    let mut args = vec![runtime.into(), operation.into()];
    if let Some(params) = params {
        args.push(params.into());
    }
    python_json(launcher, include_str!("native_settings.py"), args).await
}

async fn python_json(
    launcher: &dyn crate::ProcessLauncher,
    script: &str,
    args: Vec<std::ffi::OsString>,
) -> Result<Value> {
    use tokio::io::AsyncReadExt;
    let mut spec = crate::ProcessSpec::new("python3");
    spec.args = vec!["-c".into(), script.into()];
    spec.args.extend(args);
    spec.stdin = crate::StdioMode::Null;
    spec.stderr = crate::StdioMode::Null;
    let mut child = launcher
        .launch(spec)
        .await
        .context("remote settings require python3 on the selected host")?;
    let mut stdout = child
        .take_stdout()
        .context("remote settings stdout unavailable")?
        .take(4 * 1024 * 1024);
    let result=tokio::time::timeout(std::time::Duration::from_secs(10),async {
        let mut bytes=Vec::new();stdout.read_to_end(&mut bytes).await?;
        if !child.wait().await?.success() {bail!("native settings operation failed on the selected host; check python3 and native configuration");}
        Ok::<Value,anyhow::Error>(serde_json::from_slice(&bytes)?)
    }).await;
    match result {
        Ok(result) => result,
        Err(_) => {
            let _ = child.kill().await;
            bail!("remote native settings operation timed out");
        }
    }
}

/// Discover supported unset overrides from the exact installed Pi package. This
/// deliberately does not claim optional declarations are effective defaults.
pub async fn append_pi_declared_settings(
    response: &mut p::ConfigReadResponse,
    launcher: &dyn crate::ProcessLauncher,
    binary: &Path,
) {
    let Ok(catalog) = python_json(
        launcher,
        include_str!("pi_settings_schema.py"),
        vec![binary.as_os_str().to_owned()],
    )
    .await
    else {
        return;
    };
    let Some(fields) = catalog["fields"].as_array() else {
        return;
    };
    let source = catalog["source"]
        .as_str()
        .unwrap_or("Installed Pi Settings declaration");
    let mut extra = Vec::new();
    for field in fields {
        let Some(key) = field["key"].as_str() else {
            continue;
        };
        if sensitive(key) || config_contains(&response.config, key) {
            continue;
        }
        extra.push(json!({"key":key,"label":key,"valueJson":"null","valueKind":"json",
            "choices":field["choices"],"scope":"user override (unset)",
            "source":format!("{source}; native type {}; default chosen by Pi",field["type"].as_str().unwrap_or("unknown")),
            "writable":true,"readOnlyReason":null}));
    }
    if let Some(descriptors) = response.config["_litterSettings"].as_array_mut() {
        descriptors.extend(extra);
    }
}
fn config_contains(config: &Value, key: &str) -> bool {
    if config.get(key).is_some() {
        return true;
    }
    match key.split_once('.') {
        Some((head, tail)) => config
            .get(head)
            .is_some_and(|value| config_contains(value, tail)),
        None => false,
    }
}

#[cfg(all(test, unix))]
mod remote_tests {
    use super::*;
    struct FixtureLauncher {
        root: PathBuf,
    }
    impl crate::ProcessLauncher for FixtureLauncher {
        fn launch(
            &self,
            mut spec: crate::ProcessSpec,
        ) -> futures::future::BoxFuture<'_, std::io::Result<Box<dyn crate::ChildProcess>>> {
            spec.env
                .push(("CLAUDE_CONFIG_DIR".into(), self.root.as_os_str().to_owned()));
            spec.env.push((
                "PI_CODING_AGENT_DIR".into(),
                self.root.as_os_str().to_owned(),
            ));
            Box::pin(async move { crate::LocalLauncher.launch(spec).await })
        }
    }
    #[tokio::test]
    async fn pi_shipped_declarations_show_unset_fields_without_invented_defaults() {
        let fixture = tempfile::tempdir().unwrap();
        let package = fixture.path().join("pi");
        let dist = package.join("dist/core");
        std::fs::create_dir_all(&dist).unwrap();
        std::fs::write(
            package.join("package.json"),
            r#"{"name":"@earendil-works/pi-coding-agent"}"#,
        )
        .unwrap();
        let binary = package.join("pi");
        std::fs::write(&binary, "#!/bin/sh\nexit 99\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(
            dist.join("settings-manager.d.ts"),
            r#"
            export type Mode = "one" | "two";
            export interface Nested { enabled?: boolean; count?: number; }
            export interface Settings {
                nested?: Nested; mode?: Mode; custom?: Record<string, unknown>;
                apiKey?: string; model?: string;
            }
        "#,
        )
        .unwrap();
        let mut result = response(
            json!({"nested":{"enabled":false},"model":"configured"}),
            "fixture",
            true,
            None,
        );
        append_pi_declared_settings(&mut result, &crate::LocalLauncher, &binary).await;
        let rows = result.config["_litterSettings"].as_array().unwrap();
        assert_eq!(
            rows.iter().filter(|r| r["key"] == "nested.enabled").count(),
            1
        );
        let count = rows.iter().find(|r| r["key"] == "nested.count").unwrap();
        assert_eq!(count["valueJson"], "null");
        assert_eq!(count["scope"], "user override (unset)");
        assert!(
            count["source"]
                .as_str()
                .unwrap()
                .contains("native type number")
        );
        assert_eq!(
            rows.iter().find(|r| r["key"] == "mode").unwrap()["choices"],
            json!(["one", "two"])
        );
        assert!(rows.iter().all(|r| r["key"] != "apiKey"));
        assert_eq!(
            serde_json::from_str::<Value>(rows[0]["valueJson"].as_str().unwrap()).unwrap(),
            json!({"nested":{"enabled":false},"model":"configured"})
        );
        let before = result.clone();
        append_pi_declared_settings(&mut result, &crate::LocalLauncher, Path::new("/missing/pi"))
            .await;
        assert_eq!(result.config, before.config);
    }

    #[tokio::test]
    async fn remote_json_uses_selected_launch_environment_and_preserves_hidden_fields() {
        for runtime in ["claude", "pi"] {
            let fixture = tempfile::tempdir().unwrap();
            let file = fixture.path().join("settings.json");
            std::fs::write(&file,r#"{"model":"old","apiKey":"test-only-private","provider":{"token":"nested-private","url":"old"}}"#).unwrap();
            let launcher = FixtureLauncher {
                root: fixture.path().into(),
            };
            let before = remote_read(&launcher, runtime).await.unwrap();
            assert!(!before.config.to_string().contains("test-only-private"));
            assert!(!before.config.to_string().contains("nested-private"));
            assert_eq!(before.config["model"], "old");
            let params=serde_json::from_value(json!({"edits":[{"keyPath":"$native","value":{"model":"new","provider":{"url":"new"}},"mergeStrategy":"replace"}]})).unwrap();
            let written = remote_write(&launcher, runtime, params).await.unwrap();
            assert_eq!(Path::new(&written.file_path), file.canonicalize().unwrap());
            let after = remote_read(&launcher, runtime).await.unwrap();
            assert_eq!(after.config["model"], "new");
            let persisted: Value =
                serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
            assert_eq!(persisted["apiKey"], "test-only-private");
            assert_eq!(persisted["provider"]["token"], "nested-private");
            let invalid = serde_json::from_value(
                json!({"edits":[{"keyPath":"apiKey","value":"bad","mergeStrategy":"replace"}]}),
            )
            .unwrap();
            assert!(remote_write(&launcher, runtime, invalid).await.is_err());
            assert_eq!(
                serde_json::from_str::<Value>(&std::fs::read_to_string(&file).unwrap()).unwrap(),
                persisted
            );
        }
    }
}

#[cfg(test)]
mod native_format_tests {
    use super::*;
    #[test]
    fn unknown_toml_datetime_is_rejected_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        for value in [
            "1979-05-27T07:32:00Z",
            "1979-05-27",
            "07:32:00",
            "[1979-05-27T07:32:00-07:00]",
        ] {
            let path = dir.path().join("config.toml");
            let text = format!("flag=false\n[unknown]\ndate={value}\n");
            std::fs::write(&path, &text).unwrap();
            let params = serde_json::from_value(
                json!({"edits":[{"keyPath":"flag","value":true,"mergeStrategy":"replace"}]}),
            )
            .unwrap();
            assert!(
                write(&path, params)
                    .unwrap_err()
                    .to_string()
                    .contains("date/time")
            );
            assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        }
    }
    #[test]
    fn malformed_native_files_fail_closed_and_toml_types_survive() {
        let dir = tempfile::tempdir().unwrap();
        for (name, text) in [
            ("config.json", "{ /* missing end"),
            ("config.toml", "[invalid"),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, text).unwrap();
            let params = serde_json::from_value(
                json!({"edits":[{"keyPath":"flag","value":true,"mergeStrategy":"replace"}]}),
            )
            .unwrap();
            assert!(write(&path, params).is_err());
            assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        }
        let path = dir.path().join("types.toml");
        std::fs::write(
            &path,
            "future_unknown = [1, 2]\nflag = false\nname = 'old'\n",
        )
        .unwrap();
        let params=serde_json::from_value(json!({"edits":[{"keyPath":"flag","value":true,"mergeStrategy":"replace"},{"keyPath":"name","value":"new","mergeStrategy":"replace"},{"keyPath":"number","value":42,"mergeStrategy":"replace"}]})).unwrap();
        write(&path, params).unwrap();
        assert_eq!(
            read(&path).unwrap(),
            json!({"future_unknown":[1,2],"flag":true,"name":"new","number":42})
        );
    }
    #[test]
    fn comments_preserve_escaped_strings_and_toml_symlinks_preserve_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("config.json");
        std::fs::write(
            &json_path,
            r#"{/* note */"url":"https://host/a//b","quote":"a\"/*b*/",// tail
            "flag":true}"#,
        )
        .unwrap();
        let data = read(&json_path).unwrap();
        assert_eq!(data["url"], "https://host/a//b");
        assert_eq!(data["quote"], "a\"/*b*/");
        std::fs::write(&json_path, "{/* unfinished").unwrap();
        assert!(read(&json_path).is_err());
        let target = dir.path().join("native-dotfile");
        std::fs::write(
            &target,
            "[model.'grok-4.6']\nname='old'\napi_key='PRIVATE'\n",
        )
        .unwrap();
        let path = dir.path().join("config.toml");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &path).unwrap();
        #[cfg(not(unix))]
        std::fs::copy(&target, &path).unwrap();
        let params = serde_json::from_value(json!({"edits":[{"keyPath":"$native", "value":{"model":{"grok-4.6":{"name":"new"}}},"mergeStrategy":"replace"}]})).unwrap();
        write(&path, params).unwrap();
        assert_eq!(
            read(&path).unwrap()["model"]["grok-4.6"]["api_key"],
            "PRIVATE"
        );
        #[cfg(unix)]
        assert!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            !read_response(&path)
                .unwrap()
                .config
                .to_string()
                .contains("PRIVATE")
        );
        let original = std::fs::read(&path).unwrap();
        let params = serde_json::from_value(
            json!({"edits":[{"keyPath":"ui.theme", "value":null,"mergeStrategy":"replace"}]}),
        )
        .unwrap();
        assert!(write(&path, params).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}
