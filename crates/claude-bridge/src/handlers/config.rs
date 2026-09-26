//! Native Claude Code user settings, with real persistence and redaction.
use crate::state::ConnectionState;
use alleycat_bridge_core::settings;
use alleycat_codex_proto as p;
use anyhow::Result;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
pub fn claude_settings_path() -> Option<PathBuf> {
    std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .map(|p| p.join("settings.json"))
        .or_else(|| settings::home_path(".claude/settings.json").ok())
}
fn path() -> Result<PathBuf> {
    claude_settings_path().ok_or_else(|| anyhow::anyhow!("Claude settings path unavailable"))
}
pub async fn handle_config_read(
    state: &Arc<ConnectionState>,
    _: &Path,
    _: p::ConfigReadParams,
) -> Result<p::ConfigReadResponse> {
    let mut response = if state.trust_persisted_cwd() {
        settings::remote_read(
            state
                .launcher()
                .ok_or_else(|| anyhow::anyhow!("remote launcher unavailable"))?
                .as_ref(),
            "claude",
        )
        .await?
    } else {
        settings::read_response(&path()?)?
    };
    settings::append_claude_declared_settings(&mut response).await;
    Ok(response)
}
pub async fn handle_config_value_write(
    state: &Arc<ConnectionState>,
    _: &Path,
    params: p::ConfigValueWriteParams,
) -> Result<p::ConfigWriteResponse> {
    if state.trust_persisted_cwd() {
        return settings::remote_write(
            state
                .launcher()
                .ok_or_else(|| anyhow::anyhow!("remote launcher unavailable"))?
                .as_ref(),
            "claude",
            settings::batch_from_one(params),
        )
        .await;
    }
    settings::write_one(&path()?, params)
}
pub async fn handle_config_batch_write(
    state: &Arc<ConnectionState>,
    _: &Path,
    params: p::ConfigBatchWriteParams,
) -> Result<p::ConfigWriteResponse> {
    if state.trust_persisted_cwd() {
        return settings::remote_write(
            state
                .launcher()
                .ok_or_else(|| anyhow::anyhow!("remote launcher unavailable"))?
                .as_ref(),
            "claude",
            params,
        )
        .await;
    }
    settings::write(&path()?, params)
}
pub fn handle_config_requirements_read(
    _: &Arc<ConnectionState>,
) -> p::ConfigRequirementsReadResponse {
    p::ConfigRequirementsReadResponse { requirements: None }
}
