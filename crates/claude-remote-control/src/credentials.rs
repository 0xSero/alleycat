use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::FutureExt;
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::process::Command;

use crate::api::{AuthRefreshCallback, BridgeAuth};
use crate::auth::{AuthContext, TokenKind};
use crate::config::{EndpointConfig, EnvConfig, EnvironmentKind, RemoteControlSettings};
use crate::error::RemoteControlError;

pub const CLAUDE_AI_PROFILE_SCOPE: &str = "user:profile";
pub const CLAUDE_AI_INFERENCE_SCOPE: &str = "user:inference";
pub const CLAUDE_AI_SESSIONS_SCOPE: &str = "user:sessions:claude_code";
pub const CLAUDE_CODE_OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
pub const REMOTE_CONTROL_CREDENTIALS_SUFFIX: &str = "-credentials";
pub const REMOTE_TOKEN_DIR: &str = "/home/claude/.claude/remote";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeConfigPaths {
    pub config_dir: PathBuf,
    pub global_config: PathBuf,
    pub credentials_json: PathBuf,
    pub oauth_refresh_lock: PathBuf,
    pub keychain_service: String,
    pub keychain_account: String,
    pub config_dir_was_overridden: bool,
}

impl ClaudeConfigPaths {
    pub fn from_env() -> Self {
        let config_dir_was_overridden = std::env::var_os("CLAUDE_CONFIG_DIR").is_some();
        let config_dir = std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| home_dir().join(".claude"));
        let keychain_service = claude_code_keychain_service(&config_dir, config_dir_was_overridden);
        let global_config = home_dir().join(".claude.json");
        Self {
            credentials_json: config_dir.join(".credentials.json"),
            oauth_refresh_lock: config_dir.join(".oauth_refresh.lock"),
            config_dir,
            global_config,
            keychain_service,
            keychain_account: keychain_account(),
            config_dir_was_overridden,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ClaudeCredentials {
    pub claude_ai_oauth: Option<ClaudeAiOauth>,
    pub trusted_device_token: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ClaudeAiOauth {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<u64>,
    pub scopes: Vec<String>,
    pub subscription_type: Option<String>,
    pub rate_limit_tier: Option<String>,
    pub client_id: Option<String>,
    pub profile: Option<Value>,
    pub token_account: Option<TokenAccount>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl ClaudeAiOauth {
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|value| value == scope)
    }

    pub fn token_kind(&self) -> Option<TokenKind> {
        if self.access_token.is_empty() {
            return None;
        }
        if self.has_scope(CLAUDE_AI_INFERENCE_SCOPE) && self.has_scope(CLAUDE_AI_PROFILE_SCOPE) {
            Some(TokenKind::FullScopeLogin)
        } else if self.has_scope(CLAUDE_AI_INFERENCE_SCOPE) {
            Some(TokenKind::InferenceOnly)
        } else {
            Some(TokenKind::LongLivedSetupToken)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TokenAccount {
    pub uuid: Option<String>,
    pub email_address: Option<String>,
    pub organization_uuid: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ClaudeGlobalConfig {
    pub oauth_account: Option<OAuthAccount>,
    pub remote_control_at_startup: Option<bool>,
    pub disable_remote_control: Option<bool>,
    pub bridge_oauth_dead_expires_at: Option<u64>,
    pub bridge_oauth_dead_fail_count: Option<u32>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct OAuthAccount {
    pub account_uuid: Option<String>,
    pub email_address: Option<String>,
    pub organization_uuid: Option<String>,
    pub organization_name: Option<String>,
    pub organization_role: Option<String>,
    pub workspace_role: Option<String>,
    pub display_name: Option<String>,
    pub billing_type: Option<String>,
    pub subscription_type: Option<String>,
    pub seat_tier: Option<String>,
    pub user_rate_limit_tier: Option<String>,
    pub organization_rate_limit_tier: Option<String>,
    pub has_extra_usage_enabled: Option<bool>,
    pub account_created_at: Option<String>,
    pub subscription_created_at: Option<String>,
    pub organization_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialBackend {
    Keychain,
    PlaintextFallback,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessTokenSource {
    ClaudeCodeOauthTokenEnv,
    ClaudeCodeOauthTokenFileDescriptor,
    CcrOauthTokenFile,
    ClaudeAiOauthStore,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaudeAuthState {
    pub access_token_source: AccessTokenSource,
    pub credential_backend: CredentialBackend,
    pub has_access_token: bool,
    pub organization_uuid: Option<String>,
    pub trusted_device_token_present: bool,
    pub oauth: Option<ClaudeAiOauth>,
    pub oauth_account: Option<OAuthAccount>,
    pub settings: Option<RemoteControlSettings>,
    pub global_config: Option<ClaudeGlobalConfig>,
    pub env: EnvConfig,
    #[serde(skip)]
    trusted_device_token: Option<String>,
}

impl ClaudeAuthState {
    pub fn bridge_auth(&self) -> Option<BridgeAuth> {
        let access_token = self.access_token()?;
        let organization_uuid = self.organization_uuid.clone()?;
        let mut auth = BridgeAuth::bearer(access_token, organization_uuid);
        auth.trusted_device_token = self.trusted_device_token();
        Some(auth)
    }

    pub fn auth_context(&self) -> AuthContext {
        AuthContext {
            running_in_remote_environment: self.env.claude_code_remote,
            managed_disable_remote_control: self
                .settings
                .as_ref()
                .and_then(|settings| settings.disable_remote_control)
                .or(self
                    .global_config
                    .as_ref()
                    .and_then(|config| config.disable_remote_control))
                .unwrap_or(false),
            provider_env_disables_remote_control: self.env.provider_env_disables_remote_control,
            token_kind: self.token_kind(),
            has_claude_ai_subscription: self
                .oauth
                .as_ref()
                .map(|oauth| oauth.has_scope(CLAUDE_AI_INFERENCE_SCOPE))
                .unwrap_or(self.has_access_token),
            organization_uuid: self.organization_uuid.clone(),
            bridge_entitlement_enabled: self.has_access_token,
            allow_remote_control_policy: true,
            trusted_devices_required: false,
            trusted_device_enrolled: self.trusted_device_token_present,
            ..AuthContext::default()
        }
    }

    pub fn access_token(&self) -> Option<String> {
        self.oauth
            .as_ref()
            .map(|oauth| oauth.access_token.clone())
            .filter(|token| !token.is_empty())
    }

    pub fn token_kind(&self) -> Option<TokenKind> {
        match self.access_token_source {
            AccessTokenSource::ClaudeAiOauthStore => {
                self.oauth.as_ref().and_then(ClaudeAiOauth::token_kind)
            }
            AccessTokenSource::Missing => None,
            AccessTokenSource::ClaudeCodeOauthTokenEnv
            | AccessTokenSource::ClaudeCodeOauthTokenFileDescriptor
            | AccessTokenSource::CcrOauthTokenFile => Some(TokenKind::LongLivedSetupToken),
        }
    }

    pub fn trusted_device_token(&self) -> Option<String> {
        self.trusted_device_token.clone()
    }
}

#[derive(Debug, Clone)]
pub struct ClaudeCredentialStore {
    paths: ClaudeConfigPaths,
    endpoints: EndpointConfig,
    http: reqwest::Client,
}

impl Default for ClaudeCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeCredentialStore {
    pub fn new() -> Self {
        Self {
            paths: ClaudeConfigPaths::from_env(),
            endpoints: EndpointConfig::for_kind(EnvironmentKind::Prod),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_paths(paths: ClaudeConfigPaths) -> Self {
        Self {
            paths,
            endpoints: EndpointConfig::for_kind(EnvironmentKind::Prod),
            http: reqwest::Client::new(),
        }
    }

    pub fn paths(&self) -> &ClaudeConfigPaths {
        &self.paths
    }

    pub async fn load_auth_state(&self) -> Result<ClaudeAuthState, RemoteControlError> {
        let env = EnvConfig::from_env();
        let (credentials, credential_backend) = self.read_credentials_with_backend().await?;
        let global_config = self.read_global_config().await?;
        let settings = self.read_settings().await?;
        let (access_token_source, access_token) = read_oauth_token_override().await?;
        let oauth = match (access_token_source, access_token) {
            (AccessTokenSource::Missing, None) => credentials.claude_ai_oauth.clone(),
            (source, Some(token)) => {
                return Ok(self.auth_state_from_token(
                    source,
                    token,
                    credential_backend,
                    credentials,
                    global_config,
                    settings,
                    env,
                ));
            }
            _ => None,
        };
        let organization_uuid = env
            .claude_code_organization_uuid
            .clone()
            .or_else(|| {
                global_config
                    .as_ref()
                    .and_then(|config| config.oauth_account.as_ref())
                    .and_then(|account| account.organization_uuid.clone())
            })
            .or_else(|| {
                oauth
                    .as_ref()
                    .and_then(|oauth| oauth.token_account.as_ref())
                    .and_then(|account| account.organization_uuid.clone())
            });
        let trusted_device_token = env
            .claude_trusted_device_token
            .clone()
            .or_else(|| credentials.trusted_device_token.clone());
        let has_access_token = oauth
            .as_ref()
            .map(|oauth| !oauth.access_token.is_empty())
            .unwrap_or(false);
        Ok(ClaudeAuthState {
            access_token_source: if has_access_token {
                AccessTokenSource::ClaudeAiOauthStore
            } else {
                AccessTokenSource::Missing
            },
            credential_backend,
            has_access_token,
            organization_uuid,
            trusted_device_token_present: trusted_device_token.is_some(),
            oauth,
            oauth_account: global_config
                .as_ref()
                .and_then(|config| config.oauth_account.clone()),
            settings,
            global_config,
            env,
            trusted_device_token,
        })
    }

    pub async fn read_credentials(&self) -> Result<ClaudeCredentials, RemoteControlError> {
        Ok(self.read_credentials_with_backend().await?.0)
    }

    pub async fn read_credentials_with_backend(
        &self,
    ) -> Result<(ClaudeCredentials, CredentialBackend), RemoteControlError> {
        if let Some(credentials) = self.read_keychain_credentials().await? {
            return Ok((credentials, CredentialBackend::Keychain));
        }
        if let Some(credentials) = read_json_file(&self.paths.credentials_json).await? {
            return Ok((credentials, CredentialBackend::PlaintextFallback));
        }
        Ok((ClaudeCredentials::default(), CredentialBackend::Missing))
    }

    pub async fn read_global_config(
        &self,
    ) -> Result<Option<ClaudeGlobalConfig>, RemoteControlError> {
        read_json_file(&self.paths.global_config).await
    }

    pub async fn read_settings(&self) -> Result<Option<RemoteControlSettings>, RemoteControlError> {
        read_json_file(&self.paths.config_dir.join("settings.json")).await
    }

    pub async fn refresh_oauth_token(
        &self,
        current_access_token: Option<&str>,
    ) -> Result<Option<ClaudeAiOauth>, RemoteControlError> {
        let mut credentials = self.read_credentials().await?;
        let Some(existing) = credentials.claude_ai_oauth.clone() else {
            return Ok(None);
        };
        if let Some(current) = current_access_token
            && existing.access_token != current
        {
            return Ok(Some(existing));
        }
        let Some(refresh_token) = existing.refresh_token.clone() else {
            return Ok(None);
        };
        let refreshed = self
            .request_oauth_refresh(&refresh_token, &existing)
            .await?;
        credentials.claude_ai_oauth = Some(refreshed.clone());
        self.write_credentials(&credentials).await?;
        Ok(Some(refreshed))
    }

    pub fn auth_refresh_callback(
        &self,
        organization_uuid: String,
        trusted_device_token: Option<String>,
    ) -> AuthRefreshCallback {
        let store = self.clone();
        Arc::new(move |old_access_token| {
            let store = store.clone();
            let organization_uuid = organization_uuid.clone();
            let trusted_device_token = trusted_device_token.clone();
            async move {
                let Some(refreshed) = store.refresh_oauth_token(Some(&old_access_token)).await?
                else {
                    return Ok(None);
                };
                let mut auth = BridgeAuth::bearer(refreshed.access_token, organization_uuid);
                auth.trusted_device_token = trusted_device_token;
                Ok(Some(auth))
            }
            .boxed()
        })
    }

    async fn request_oauth_refresh(
        &self,
        refresh_token: &str,
        existing: &ClaudeAiOauth,
    ) -> Result<ClaudeAiOauth, RemoteControlError> {
        let scopes = if existing.scopes.is_empty() {
            vec![
                CLAUDE_AI_PROFILE_SCOPE.to_string(),
                CLAUDE_AI_INFERENCE_SCOPE.to_string(),
                CLAUDE_AI_SESSIONS_SCOPE.to_string(),
                "user:mcp_servers".to_string(),
                "user:file_upload".to_string(),
            ]
        } else {
            existing.scopes.clone()
        };
        let body = json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": existing
                .client_id
                .as_deref()
                .unwrap_or(CLAUDE_CODE_OAUTH_CLIENT_ID),
            "scope": scopes.join(" "),
        });
        let resp = self
            .http
            .post(self.endpoints.token_url.clone())
            .header(CONTENT_TYPE, "application/json")
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        let value: Value = resp.json().await?;
        if !status.is_success() {
            return Err(RemoteControlError::Protocol(format!(
                "OAuth token refresh failed with HTTP {status}"
            )));
        }
        let access_token = value
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| RemoteControlError::Protocol("missing access_token".to_string()))?;
        let new_refresh_token = value
            .get("refresh_token")
            .and_then(Value::as_str)
            .unwrap_or(refresh_token);
        let expires_at = value
            .get("expires_in")
            .and_then(Value::as_u64)
            .map(|seconds| epoch_ms() + seconds * 1000);
        let returned_scopes = value
            .get("scope")
            .and_then(Value::as_str)
            .map(parse_scope_list)
            .filter(|scopes| !scopes.is_empty())
            .unwrap_or(scopes);
        Ok(ClaudeAiOauth {
            access_token: access_token.to_string(),
            refresh_token: Some(new_refresh_token.to_string()),
            expires_at,
            scopes: returned_scopes,
            subscription_type: existing.subscription_type.clone(),
            rate_limit_tier: existing.rate_limit_tier.clone(),
            client_id: existing
                .client_id
                .clone()
                .or_else(|| Some(CLAUDE_CODE_OAUTH_CLIENT_ID.to_string())),
            profile: existing.profile.clone(),
            token_account: value
                .get("account")
                .map(|account| TokenAccount {
                    uuid: account
                        .get("uuid")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    email_address: account
                        .get("email_address")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    organization_uuid: value
                        .get("organization")
                        .and_then(|org| org.get("uuid"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    extra: BTreeMap::new(),
                })
                .or_else(|| existing.token_account.clone()),
            extra: existing.extra.clone(),
        })
    }

    async fn read_keychain_credentials(
        &self,
    ) -> Result<Option<ClaudeCredentials>, RemoteControlError> {
        #[cfg(target_os = "macos")]
        {
            let output = Command::new("security")
                .args([
                    "find-generic-password",
                    "-a",
                    &self.paths.keychain_account,
                    "-w",
                    "-s",
                    &self.paths.keychain_service,
                ])
                .output()
                .await?;
            if !output.status.success() {
                return Ok(None);
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            let trimmed = stdout.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            Ok(Some(serde_json::from_str(trimmed)?))
        }
        #[cfg(not(target_os = "macos"))]
        {
            Ok(None)
        }
    }

    async fn write_credentials(
        &self,
        credentials: &ClaudeCredentials,
    ) -> Result<(), RemoteControlError> {
        if self.write_keychain_credentials(credentials).await? {
            let _ = fs::remove_file(&self.paths.credentials_json).await;
            return Ok(());
        }
        write_json_file_0600(&self.paths.credentials_json, credentials).await
    }

    async fn write_keychain_credentials(
        &self,
        credentials: &ClaudeCredentials,
    ) -> Result<bool, RemoteControlError> {
        #[cfg(target_os = "macos")]
        {
            let bytes = serde_json::to_vec(credentials)?;
            let output = Command::new("security")
                .arg("add-generic-password")
                .arg("-U")
                .arg("-a")
                .arg(&self.paths.keychain_account)
                .arg("-s")
                .arg(&self.paths.keychain_service)
                .arg("-X")
                .arg(hex_encode(&bytes))
                .output()
                .await?;
            Ok(output.status.success())
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = credentials;
            Ok(false)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn auth_state_from_token(
        &self,
        source: AccessTokenSource,
        access_token: String,
        credential_backend: CredentialBackend,
        credentials: ClaudeCredentials,
        global_config: Option<ClaudeGlobalConfig>,
        settings: Option<RemoteControlSettings>,
        env: EnvConfig,
    ) -> ClaudeAuthState {
        let organization_uuid = env
            .claude_code_organization_uuid
            .clone()
            .or_else(|| {
                global_config
                    .as_ref()
                    .and_then(|config| config.oauth_account.as_ref())
                    .and_then(|account| account.organization_uuid.clone())
            })
            .or_else(|| {
                credentials
                    .claude_ai_oauth
                    .as_ref()
                    .and_then(|oauth| oauth.token_account.as_ref())
                    .and_then(|account| account.organization_uuid.clone())
            });
        let trusted_device_token = env
            .claude_trusted_device_token
            .clone()
            .or_else(|| credentials.trusted_device_token.clone());
        ClaudeAuthState {
            access_token_source: source,
            credential_backend,
            has_access_token: true,
            organization_uuid,
            trusted_device_token_present: trusted_device_token.is_some(),
            oauth: Some(ClaudeAiOauth {
                access_token,
                token_account: credentials
                    .claude_ai_oauth
                    .as_ref()
                    .and_then(|oauth| oauth.token_account.clone()),
                ..ClaudeAiOauth::default()
            }),
            oauth_account: global_config
                .as_ref()
                .and_then(|config| config.oauth_account.clone()),
            settings,
            global_config,
            env,
            trusted_device_token,
        }
    }
}

async fn read_oauth_token_override()
-> Result<(AccessTokenSource, Option<String>), RemoteControlError> {
    if let Ok(token) = std::env::var("CLAUDE_CODE_OAUTH_TOKEN")
        && !token.trim().is_empty()
    {
        return Ok((
            AccessTokenSource::ClaudeCodeOauthTokenEnv,
            Some(token.trim().to_string()),
        ));
    }
    if let Some(token) = read_token_from_fd_env("CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR").await? {
        return Ok((
            AccessTokenSource::ClaudeCodeOauthTokenFileDescriptor,
            Some(token),
        ));
    }
    let well_known = Path::new(REMOTE_TOKEN_DIR).join(".oauth_token");
    if let Some(token) = read_text_file_trimmed(&well_known).await? {
        return Ok((AccessTokenSource::CcrOauthTokenFile, Some(token)));
    }
    Ok((AccessTokenSource::Missing, None))
}

pub async fn read_api_key_override() -> Result<Option<String>, RemoteControlError> {
    if let Ok(token) = std::env::var("ANTHROPIC_API_KEY")
        && !token.trim().is_empty()
    {
        return Ok(Some(token.trim().to_string()));
    }
    if let Some(token) = read_token_from_fd_env("CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR").await? {
        return Ok(Some(token));
    }
    read_text_file_trimmed(&Path::new(REMOTE_TOKEN_DIR).join(".api_key")).await
}

pub async fn read_session_ingress_token_override() -> Result<Option<String>, RemoteControlError> {
    if let Ok(token) = std::env::var("CLAUDE_CODE_SESSION_ACCESS_TOKEN")
        && !token.trim().is_empty()
    {
        return Ok(Some(token.trim().to_string()));
    }
    if let Some(token) =
        read_token_from_fd_env("CLAUDE_CODE_SESSION_INGRESS_TOKEN_FILE_DESCRIPTOR").await?
    {
        return Ok(Some(token));
    }
    let path = std::env::var_os("CLAUDE_SESSION_INGRESS_TOKEN_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(REMOTE_TOKEN_DIR).join(".session_ingress_token"));
    read_text_file_trimmed(&path).await
}

async fn read_token_from_fd_env(key: &str) -> Result<Option<String>, RemoteControlError> {
    let Some(value) = std::env::var(key).ok() else {
        return Ok(None);
    };
    let fd: i32 = value.parse().map_err(|_| {
        RemoteControlError::Protocol(format!("{key} must be a valid file descriptor number"))
    })?;
    read_text_file_trimmed(&PathBuf::from(format!("/dev/fd/{fd}"))).await
}

async fn read_json_file<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, RemoteControlError> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn read_text_file_trimmed(path: &Path) -> Result<Option<String>, RemoteControlError> {
    match fs::read_to_string(path).await {
        Ok(value) => {
            let value = value.trim();
            Ok((!value.is_empty()).then(|| value.to_string()))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn write_json_file_0600<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), RemoteControlError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("json")
    ));
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&tmp, bytes).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await?;
    }
    fs::rename(tmp, path).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    Ok(())
}

pub fn claude_code_keychain_service(config_dir: &Path, config_dir_was_overridden: bool) -> String {
    let oauth_file_suffix = if std::env::var_os("CLAUDE_CODE_CUSTOM_OAUTH_URL").is_some() {
        "-custom-oauth"
    } else {
        ""
    };
    let config_suffix = if config_dir_was_overridden {
        let mut hasher = Sha256::new();
        hasher.update(config_dir.to_string_lossy().as_bytes());
        let digest = hasher.finalize();
        format!("-{}", hex_encode(&digest[..4]))
    } else {
        String::new()
    };
    format!("Claude Code{oauth_file_suffix}{REMOTE_CONTROL_CREDENTIALS_SUFFIX}{config_suffix}")
}

fn keychain_account() -> String {
    let value = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "claude-code-user".to_string());
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        value
    } else {
        "claude-code-user".to_string()
    }
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn parse_scope_list(value: &str) -> Vec<String> {
    value.split_whitespace().map(str::to_string).collect()
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_keychain_service_matches_claude_code_shape() {
        let service = claude_code_keychain_service(Path::new("/Users/me/.claude"), false);
        assert_eq!(service, "Claude Code-credentials");
    }

    #[test]
    fn overridden_config_dir_adds_hash_suffix() {
        let service = claude_code_keychain_service(Path::new("/tmp/custom-claude"), true);
        assert!(service.starts_with("Claude Code-credentials-"));
        assert_eq!(service.len(), "Claude Code-credentials-".len() + 8);
    }

    #[test]
    fn oauth_scope_kind_requires_inference_and_profile() {
        let oauth = ClaudeAiOauth {
            access_token: "tok".to_string(),
            scopes: vec![
                CLAUDE_AI_PROFILE_SCOPE.to_string(),
                CLAUDE_AI_INFERENCE_SCOPE.to_string(),
            ],
            ..ClaudeAiOauth::default()
        };
        assert_eq!(oauth.token_kind(), Some(TokenKind::FullScopeLogin));
    }
}
