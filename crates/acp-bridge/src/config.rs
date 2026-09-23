//! Configuration for the ACP bridge.

use std::path::PathBuf;

/// Configuration for the ACP bridge.
#[derive(Debug, Clone)]
pub struct AcpBridgeConfig {
    /// Path to the ACP agent binary (e.g., `devin`)
    pub agent_bin: PathBuf,
    /// Arguments to pass to the agent binary (e.g., `["acp"]`)
    pub agent_args: Vec<String>,
    /// State directory for the bridge
    pub state_dir: Option<PathBuf>,
    /// Maximum number of agent processes to keep in the pool
    pub pool_capacity: Option<usize>,
    /// Idle TTL for agent processes
    pub idle_ttl_secs: Option<u64>,
    /// Request timeout in seconds
    pub request_timeout_secs: Option<u64>,
    /// Maximum number of retries for transient failures
    pub max_retries: Option<usize>,
    /// Retry backoff multiplier
    pub retry_backoff_ms: Option<u64>,
}

impl Default for AcpBridgeConfig {
    fn default() -> Self {
        Self {
            agent_bin: PathBuf::from("devin"),
            agent_args: vec!["acp".to_string()],
            state_dir: None,
            pool_capacity: None,
            idle_ttl_secs: None,
            request_timeout_secs: None,
            max_retries: None,
            retry_backoff_ms: None,
        }
    }
}

impl AcpBridgeConfig {
    /// Populate configuration from environment variables.
    /// Environment variables:
    /// - `ACP_BRIDGE_AGENT_BIN` - Path to the ACP agent binary
    /// - `ACP_BRIDGE_AGENT_ARGS` - Arguments to pass to the agent (space-separated)
    /// - `ACP_BRIDGE_STATE_DIR` - State directory for the bridge
    /// - `ACP_BRIDGE_POOL_CAPACITY` - Maximum number of agent processes in the pool
    /// - `ACP_BRIDGE_IDLE_TTL_SECS` - Idle TTL for agent processes in seconds
    /// - `ACP_BRIDGE_REQUEST_TIMEOUT_SECS` - Request timeout in seconds
    /// - `ACP_BRIDGE_MAX_RETRIES` - Maximum number of retries for transient failures
    /// - `ACP_BRIDGE_RETRY_BACKOFF_MS` - Retry backoff in milliseconds
    ///
    /// Values set via this method take precedence over defaults,
    /// but can be overridden by explicit builder methods.
    pub fn from_env(mut self) -> Self {
        if let Some(bin) = std::env::var_os("ACP_BRIDGE_AGENT_BIN") {
            self.agent_bin = PathBuf::from(bin);
        }
        if let Ok(args_str) = std::env::var("ACP_BRIDGE_AGENT_ARGS") {
            self.agent_args = args_str.split_whitespace().map(|s| s.to_string()).collect();
        }
        if let Some(state_dir) = std::env::var_os("ACP_BRIDGE_STATE_DIR") {
            self.state_dir = Some(PathBuf::from(state_dir));
        }
        if let Ok(capacity) = std::env::var("ACP_BRIDGE_POOL_CAPACITY")
            && let Ok(cap) = capacity.parse::<usize>()
        {
            self.pool_capacity = Some(cap);
        }
        if let Ok(ttl) = std::env::var("ACP_BRIDGE_IDLE_TTL_SECS")
            && let Ok(secs) = ttl.parse::<u64>()
        {
            self.idle_ttl_secs = Some(secs);
        }
        if let Ok(timeout) = std::env::var("ACP_BRIDGE_REQUEST_TIMEOUT_SECS")
            && let Ok(secs) = timeout.parse::<u64>()
        {
            self.request_timeout_secs = Some(secs);
        }
        if let Ok(retries) = std::env::var("ACP_BRIDGE_MAX_RETRIES")
            && let Ok(max) = retries.parse::<usize>()
        {
            self.max_retries = Some(max);
        }
        if let Ok(backoff) = std::env::var("ACP_BRIDGE_RETRY_BACKOFF_MS")
            && let Ok(ms) = backoff.parse::<u64>()
        {
            self.retry_backoff_ms = Some(ms);
        }
        self
    }
}
