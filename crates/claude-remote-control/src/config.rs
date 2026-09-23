use serde::{Deserialize, Serialize};
use url::Url;

pub const CCR_BYOC_BETA: &str = "ccr-byoc-2025-07-29";
pub const ENVIRONMENTS_BETA: &str = "environments-2025-11-01";
pub const MANAGED_AGENTS_BETA: &str = "managed-agents-2026-04-01";
pub const ANTHROPIC_VERSION: &str = "2023-06-01";
pub const DEFAULT_USER_AGENT: &str =
    concat!("alleycat-claude-remote-control/", env!("CARGO_PKG_VERSION"));
pub const DEFAULT_RUNNER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const CLAUDE_CODE_REMOTE_TRUE: &str = "true";
pub const CLAUDE_CODE_ENVIRONMENT_KIND_BRIDGE: &str = "bridge";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvironmentKind {
    #[default]
    Prod,
    Staging,
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointConfig {
    pub base_api_url: Url,
    pub claude_ai_origin: Url,
    pub authorize_url: Url,
    pub token_url: Url,
}

impl EndpointConfig {
    pub fn for_kind(kind: EnvironmentKind) -> Self {
        match kind {
            EnvironmentKind::Prod => Self {
                base_api_url: Url::parse("https://api.anthropic.com").unwrap(),
                claude_ai_origin: Url::parse("https://claude.ai").unwrap(),
                authorize_url: Url::parse("https://claude.com/cai/oauth/authorize").unwrap(),
                token_url: Url::parse("https://platform.claude.com/v1/oauth/token").unwrap(),
            },
            EnvironmentKind::Staging => Self {
                base_api_url: Url::parse("https://api.staging.ant.dev").unwrap(),
                claude_ai_origin: Url::parse("https://claude-ai.staging.ant.dev").unwrap(),
                authorize_url: Url::parse("https://claude.com/cai/oauth/authorize").unwrap(),
                token_url: Url::parse("https://platform.claude.com/v1/oauth/token").unwrap(),
            },
            EnvironmentKind::Local => Self {
                base_api_url: Url::parse("http://localhost:8000").unwrap(),
                claude_ai_origin: Url::parse("http://localhost:4000").unwrap(),
                authorize_url: Url::parse("http://localhost:4000/cai/oauth/authorize").unwrap(),
                token_url: Url::parse("http://localhost:8000/v1/oauth/token").unwrap(),
            },
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RemoteControlSettings {
    pub disable_remote_control: Option<bool>,
    pub remote_control_at_startup: Option<bool>,
    pub isolate_peer_machines: Option<bool>,
    pub auto_upload_sessions: Option<bool>,
    pub input_needed_notif_enabled: Option<bool>,
    pub agent_push_notif_enabled: Option<bool>,
    pub remote_control_spawn_mode: Option<String>,
    pub remote_dialog_seen: Option<bool>,
    pub has_used_remote_control: Option<bool>,
    pub remote_control_upsell_seen_count: Option<u32>,
    pub push_notif_upsell_seen_count: Option<u32>,
    pub auto_add_remote_control_daemon_worker: Option<bool>,
}

impl RemoteControlSettings {
    pub fn hard_disabled(&self) -> bool {
        self.disable_remote_control.unwrap_or(false)
    }

    pub fn peer_machine_isolation_enabled(&self) -> bool {
        self.isolate_peer_machines.unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HeartbeatConfig {
    pub init_retry_max_attempts: u32,
    pub init_retry_base_delay_ms: u64,
    pub init_retry_jitter_fraction: u8,
    pub init_retry_max_delay_ms: u64,
    pub http_timeout_ms: u64,
    pub uuid_dedup_buffer_size: usize,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_jitter_fraction: u8,
    pub token_refresh_buffer_ms: u64,
    pub teardown_archive_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub min_version: String,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            init_retry_max_attempts: 3,
            init_retry_base_delay_ms: 500,
            init_retry_jitter_fraction: 25,
            init_retry_max_delay_ms: 4000,
            http_timeout_ms: 10000,
            uuid_dedup_buffer_size: 2000,
            heartbeat_interval_ms: 20000,
            heartbeat_jitter_fraction: 10,
            token_refresh_buffer_ms: 300000,
            teardown_archive_timeout_ms: 1500,
            connect_timeout_ms: 15000,
            min_version: "0.0.0".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PollLoopConfig {
    pub poll_interval_ms_not_at_capacity: u64,
    pub poll_interval_ms_at_capacity: u64,
    pub non_exclusive_heartbeat_interval_ms: u64,
    pub multisession_poll_interval_ms_not_at_capacity: u64,
    pub multisession_poll_interval_ms_partial_capacity: u64,
    pub multisession_poll_interval_ms_at_capacity: u64,
    pub reclaim_older_than_ms: u64,
    pub session_keepalive_interval_v2_ms: u64,
}

impl Default for PollLoopConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms_not_at_capacity: 2000,
            poll_interval_ms_at_capacity: 600000,
            non_exclusive_heartbeat_interval_ms: 0,
            multisession_poll_interval_ms_not_at_capacity: 2000,
            multisession_poll_interval_ms_partial_capacity: 2000,
            multisession_poll_interval_ms_at_capacity: 600000,
            reclaim_older_than_ms: 5000,
            session_keepalive_interval_v2_ms: 120000,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuningConfig {
    pub heartbeat: HeartbeatConfig,
    pub poll_loop: PollLoopConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvConfig {
    pub remote_control_session_name_prefix: Option<String>,
    pub claude_code_remote: bool,
    pub claude_code_environment_kind: Option<String>,
    pub claude_bridge_reattach_session: Option<String>,
    pub claude_bridge_reattach_seq: Option<u64>,
    pub claude_bridge_use_ccr_v2: bool,
    pub claude_code_use_ccr_v2: bool,
    pub claude_code_post_for_session_ingress_v2: bool,
    pub claude_trusted_device_token: Option<String>,
    pub claude_code_remote_session_id: Option<String>,
    pub claude_code_organization_uuid: Option<String>,
    pub session_ingress_url: Option<String>,
    pub anthropic_base_url: Option<String>,
    pub websocket_auth_file_descriptor: Option<String>,
    pub entrypoint: Option<String>,
    pub provider_env_disables_remote_control: bool,
}

impl EnvConfig {
    pub fn from_env() -> Self {
        Self {
            remote_control_session_name_prefix: std::env::var(
                "CLAUDE_REMOTE_CONTROL_SESSION_NAME_PREFIX",
            )
            .ok(),
            claude_code_remote: env_equals("CLAUDE_CODE_REMOTE", CLAUDE_CODE_REMOTE_TRUE),
            claude_code_environment_kind: std::env::var("CLAUDE_CODE_ENVIRONMENT_KIND").ok(),
            claude_bridge_reattach_session: std::env::var("CLAUDE_BRIDGE_REATTACH_SESSION").ok(),
            claude_bridge_reattach_seq: std::env::var("CLAUDE_BRIDGE_REATTACH_SEQ")
                .ok()
                .and_then(|s| s.parse().ok()),
            claude_bridge_use_ccr_v2: env_truthy("CLAUDE_BRIDGE_USE_CCR_V2"),
            claude_code_use_ccr_v2: env_truthy("CLAUDE_CODE_USE_CCR_V2"),
            claude_code_post_for_session_ingress_v2: env_truthy(
                "CLAUDE_CODE_POST_FOR_SESSION_INGRESS_V2",
            ),
            claude_trusted_device_token: std::env::var("CLAUDE_TRUSTED_DEVICE_TOKEN").ok(),
            claude_code_remote_session_id: std::env::var("CLAUDE_CODE_REMOTE_SESSION_ID").ok(),
            claude_code_organization_uuid: std::env::var("CLAUDE_CODE_ORGANIZATION_UUID").ok(),
            session_ingress_url: std::env::var("SESSION_INGRESS_URL").ok(),
            anthropic_base_url: std::env::var("ANTHROPIC_BASE_URL").ok(),
            websocket_auth_file_descriptor: std::env::var(
                "CLAUDE_CODE_WEBSOCKET_AUTH_FILE_DESCRIPTOR",
            )
            .ok(),
            entrypoint: std::env::var("CLAUDE_CODE_ENTRYPOINT").ok(),
            provider_env_disables_remote_control: provider_env_disables_remote_control(),
        }
    }
}

pub fn default_session_name_prefix() -> String {
    let raw = std::env::var("CLAUDE_REMOTE_CONTROL_SESSION_NAME_PREFIX")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            hostname::get()
                .ok()
                .and_then(|s| s.into_string().ok())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "localhost".to_string())
        });
    sanitize_session_name_prefix(&raw).unwrap_or_else(|| "remote-control".to_string())
}

pub fn sanitize_session_name_prefix(value: &str) -> Option<String> {
    let mut out = String::with_capacity(value.len());
    let mut previous_was_dash = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            previous_was_dash = false;
        } else if !previous_was_dash {
            out.push('-');
            previous_was_dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

pub fn remote_control_http_base_url_allowed(url: &Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => is_loopback_host(url),
        _ => false,
    }
}

fn env_truthy(key: &str) -> bool {
    std::env::var(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
}

fn env_equals(key: &str, expected: &str) -> bool {
    std::env::var(key)
        .map(|value| value == expected)
        .unwrap_or(false)
}

fn provider_env_disables_remote_control() -> bool {
    [
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
        "CLAUDE_CODE_USE_ANTHROPIC_AWS",
        "CLAUDE_CODE_USE_MANTLE",
    ]
    .iter()
    .any(|key| env_truthy(key))
}

fn is_loopback_host(url: &Url) -> bool {
    matches!(
        url.host_str(),
        Some("localhost") | Some("127.0.0.1") | Some("::1")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_is_allowed_only_for_loopback() {
        assert!(remote_control_http_base_url_allowed(
            &Url::parse("http://localhost:8000").unwrap()
        ));
        assert!(remote_control_http_base_url_allowed(
            &Url::parse("http://127.0.0.1:8000").unwrap()
        ));
        assert!(!remote_control_http_base_url_allowed(
            &Url::parse("http://example.com").unwrap()
        ));
        assert!(remote_control_http_base_url_allowed(
            &Url::parse("https://api.anthropic.com").unwrap()
        ));
    }

    #[test]
    fn default_tuning_matches_reverse_engineered_values() {
        let tuning = TuningConfig::default();
        assert_eq!(tuning.heartbeat.heartbeat_interval_ms, 20000);
        assert_eq!(tuning.poll_loop.reclaim_older_than_ms, 5000);
        assert_eq!(tuning.poll_loop.poll_interval_ms_at_capacity, 600000);
    }
}
