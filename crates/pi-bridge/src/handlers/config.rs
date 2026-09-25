//! Runtime-specific native settings. OMP and Pi never share a settings file.
use crate::{codex_proto as p, state::ConnectionState};
use alleycat_bridge_core::settings;
use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
pub fn pi_settings_path() -> Option<PathBuf> {
    std::env::var_os("PI_CODING_AGENT_DIR")
        .map(PathBuf::from)
        .map(|p| p.join("settings.json"))
        .or_else(|| settings::home_path(".pi/agent/settings.json").ok())
}
pub fn bridge_config_path(codex_home: &Path) -> PathBuf {
    codex_home.join("config.json")
}
fn path(state: &ConnectionState) -> Result<PathBuf> {
    state
        .native_settings_path()
        .map(Path::to_path_buf)
        .or_else(pi_settings_path)
        .ok_or_else(|| anyhow::anyhow!("native settings path unavailable"))
}
pub async fn handle_config_read(
    state: &Arc<ConnectionState>,
    _: &Path,
    _: p::ConfigReadParams,
) -> Result<p::ConfigReadResponse> {
    let path = path(state)?;
    let remote = state.trust_persisted_cwd();
    let omp = path.extension().is_some_and(|v| v == "yml" || v == "yaml");
    let mut response = if remote && !omp {
        settings::remote_read(state.launcher().as_ref(), "pi").await?
    } else if remote {
        settings::response(
            serde_json::json!({}),
            &path.to_string_lossy(),
            false,
            Some("Raw remote settings file editing is unavailable; use native fields below"),
        )
    } else {
        settings::read_response(&path)?
    };
    if !omp {
        settings::append_pi_declared_settings(
            &mut response,
            state.launcher().as_ref(),
            state.pi_pool().pi_bin(),
        )
        .await;
    }
    if omp {
        let catalog = omp_command(
            state,
            &path,
            vec!["config".into(), "list".into(), "--json".into()],
        )
        .await?;
        let descriptors = response.config["_litterSettings"].as_array_mut().unwrap();
        for (key, entry) in catalog
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("invalid OMP settings catalog"))?
        {
            if settings::sensitive(key)
                || entry.get("redacted").and_then(|v| v.as_bool()) == Some(true)
            {
                continue;
            }
            let value = entry
                .get("value")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            if settings::sanitize(&value) != value {
                continue;
            }
            if descriptors.iter().any(|d| d["key"] == key.as_str()) {
                continue;
            }
            descriptors.push(serde_json::json!({"key":key,"label":key,"valueJson":value.to_string(),"valueKind":match entry["type"].as_str(){Some("boolean")=>"boolean",Some("number")=>"number",Some("string"|"enum")=>"string",_=>"json"},"choices":[],"scope":"user","source":format!("{} (native defaults and user overrides)",path.display()),"writable":true,"readOnlyReason":null}));
        }
    }
    Ok(response)
}
pub async fn handle_config_value_write(
    state: &Arc<ConnectionState>,
    _: &Path,
    params: p::ConfigValueWriteParams,
) -> Result<p::ConfigWriteResponse> {
    let path = path(state)?;
    if path.extension().is_some_and(|v| v == "yml" || v == "yaml") && params.key_path != "$native" {
        if params.file_path.is_some() || params.expected_version.is_some() {
            anyhow::bail!("Native OMP setters do not accept file/version overrides");
        }
        if !matches!(params.merge_strategy, p::MergeStrategy::Replace) {
            anyhow::bail!("OMP native setters require replacement values");
        }
        if settings::sensitive(&params.key_path)
            || settings::sanitize(&params.value) != params.value
        {
            anyhow::bail!("Credential settings cannot be edited here");
        }
        let value = params
            .value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| params.value.to_string());
        let result = omp_command(
            state,
            &path,
            vec![
                "config".into(),
                "set".into(),
                params.key_path.into(),
                value.into(),
                "--json".into(),
            ],
        )
        .await?;
        if result.get("value") != Some(&params.value) {
            anyhow::bail!("OMP native setter read-back differs from requested value");
        }
        return Ok(p::ConfigWriteResponse {
            status: p::WriteStatus::Ok,
            version: String::new(),
            file_path: path.to_string_lossy().into(),
            overridden_metadata: None,
        });
    }
    if state.trust_persisted_cwd() {
        if path.extension().is_some_and(|v| v == "yml" || v == "yaml") {
            anyhow::bail!(
                "Raw OMP settings editing is unavailable over SSH; use its native schema fields"
            );
        }
        return settings::remote_write(
            state.launcher().as_ref(),
            "pi",
            settings::batch_from_one(params),
        )
        .await;
    }
    settings::write_one(&path, params)
}
pub async fn handle_config_batch_write(
    state: &Arc<ConnectionState>,
    codex_home: &Path,
    params: p::ConfigBatchWriteParams,
) -> Result<p::ConfigWriteResponse> {
    let path = path(state)?;
    if state.trust_persisted_cwd() || path.extension().is_some_and(|v| v == "yml" || v == "yaml") {
        if params.edits.len() != 1 {
            anyhow::bail!("OMP native settings require one verified edit at a time");
        }
        let edit = params.edits.into_iter().next().unwrap();
        return handle_config_value_write(
            state,
            codex_home,
            p::ConfigValueWriteParams {
                key_path: edit.key_path,
                value: edit.value,
                merge_strategy: edit.merge_strategy,
                file_path: params.file_path,
                expected_version: params.expected_version,
            },
        )
        .await;
    }
    settings::write(&path, params)
}
async fn omp_command(
    state: &ConnectionState,
    path: &Path,
    args: Vec<std::ffi::OsString>,
) -> Result<serde_json::Value> {
    let mut spec = alleycat_bridge_core::ProcessSpec::new(state.pi_pool().pi_bin());
    spec.args = args;
    spec.cwd = path.parent().map(Path::to_path_buf);
    spec.env.push((
        "PI_CODING_AGENT_DIR".into(),
        path.parent().unwrap().as_os_str().to_owned(),
    ));
    spec.stdin = alleycat_bridge_core::StdioMode::Null;
    spec.stderr = alleycat_bridge_core::StdioMode::Null;
    let mut child = state.launcher().launch(spec).await?;
    use tokio::io::AsyncReadExt;
    let mut stdout = child
        .take_stdout()
        .ok_or_else(|| anyhow::anyhow!("OMP config has no stdout"))?
        .take(4 * 1024 * 1024);
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut output = Vec::new();
        stdout.read_to_end(&mut output).await?;
        if !child.wait().await?.success() {
            anyhow::bail!("OMP rejected the native configuration command");
        }
        Ok::<serde_json::Value, anyhow::Error>(serde_json::from_slice(&output)?)
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            let _ = child.kill().await;
            anyhow::bail!("OMP configuration command timed out");
        }
    }
}
pub fn handle_config_requirements_read(
    _: &Arc<ConnectionState>,
) -> p::ConfigRequirementsReadResponse {
    p::ConfigRequirementsReadResponse { requirements: None }
}
