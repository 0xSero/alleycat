use std::fs::OpenOptions;
use std::io::Read;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use alleycat_bridge_core::{Bridge, Conn, JsonRpcError};
use alleycat_local_studio_proto::{
    BridgeError, CapabilitiesManifest, CapabilitiesManifestKind, Capability, ControllerSnapshot,
    ControllerSnapshotRequest, ErrorCode, ErrorResult, ErrorResultKind, LocalStudioAdvertisement,
    ProtocolVersion, SessionAuthority, SessionPage, SessionReadRequest,
};
use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use futures::StreamExt;
use iroh::EndpointId;
use reqwest::header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use url::Host;

use crate::grants::{EffectiveGrant, GrantStore};
use crate::protocol::{AgentInfo, AgentPresentation, AgentWire};

const AGENT_NAME: &str = "local-studio";
const AGENT_DISPLAY_NAME: &str = "Local Studio";
const GATEWAY_PATH: &str = "/api/litter-bridge/v1";
const GATEWAY_SECRET_HEADER: &str = "x-local-studio-litter-bridge-secret";
const MAX_HTTP_BODY_BYTES: usize = 1 << 20;
const MAX_METADATA_BYTES: u64 = 1 << 20;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(750);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SESSION_PAGE_ITEMS: usize = 200;
const IMPLEMENTED_CAPABILITIES: [Capability; 2] = [Capability::StatsRead, Capability::SessionsRead];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayMetadataFile {
    protocol_version: ProtocolVersion,
    url: String,
    secret_header: String,
    secret: String,
    controller_id: String,
    pid: u32,
    issued_at: String,
}

struct GatewayMetadata {
    url: Url,
    secret: HeaderValue,
    controller_id: String,
    issued_at: String,
}

impl GatewayMetadata {
    fn bridge_id(&self) -> String {
        format!("alleycat:{}", self.controller_id)
    }
}

struct AuthorizedGateway {
    metadata: GatewayMetadata,
    grant: EffectiveGrant,
}

fn implemented_capabilities(grant: &EffectiveGrant) -> Vec<Capability> {
    IMPLEMENTED_CAPABILITIES
        .into_iter()
        .filter(|capability| grant.allows_capability(*capability))
        .collect()
}

pub(crate) fn local_studio_agent_info(authenticated_node: Option<&EndpointId>) -> AgentInfo {
    // The agent's availability describes the local gateway, not the caller's
    // authority. Keeping those two states separate lets a paired caller open
    // the bridge and receive a typed capability_denied response instead of an
    // indistinguishable "agent unavailable" error. Anonymous callers still do
    // not receive controller metadata.
    let local_studio = authenticated_node.and_then(|node| {
        let metadata = load_gateway_metadata().ok()?;
        // A missing, malformed, expired, or revoked grant is deliberately an
        // empty authority set. Grant-store corruption must never inherit or
        // synthesize permissions, but it also must not hide a valid gateway.
        let effective = GrantStore::load()
            .ok()
            .and_then(|store| store.effective(node, Utc::now()));
        // Intersect durable grants with live methods so a hand-edited future
        // grant cannot make an unimplemented write/control path appear live.
        let capabilities = effective
            .as_ref()
            .map(implemented_capabilities)
            .unwrap_or_default();
        let actions = Vec::new();
        Some(LocalStudioAdvertisement {
            protocol_version: ProtocolVersion,
            bridge_id: metadata.bridge_id(),
            controller_id: metadata.controller_id,
            issued_at: metadata.issued_at,
            capabilities,
            actions,
        })
    });
    AgentInfo {
        name: AGENT_NAME.to_string(),
        display_name: AGENT_DISPLAY_NAME.to_string(),
        wire: AgentWire::Jsonl,
        available: local_studio.is_some(),
        presentation: Some(AgentPresentation {
            title: Some("Local Studio Controller".into()),
            is_beta: true,
            sort_order: 1_000,
            description: Some("Private controller status from this paired computer".into()),
            aliases: Vec::new(),
        }),
        capabilities: None,
        local_studio,
    }
}

pub(crate) fn gateway_available() -> bool {
    load_gateway_metadata().is_ok()
}

fn authorized_gateway(authenticated_node: &EndpointId) -> anyhow::Result<AuthorizedGateway> {
    let store = GrantStore::load().context("loading paired-node authorization")?;
    let grant = store
        .effective(authenticated_node, Utc::now())
        .ok_or_else(|| anyhow!("paired node has no active Local Studio grant"))?;
    if grant.grants.is_empty() {
        bail!("paired node has no active Local Studio capability");
    }
    let metadata = load_gateway_metadata().context("loading Local Studio gateway metadata")?;
    Ok(AuthorizedGateway { metadata, grant })
}

fn resolve_gateway_metadata_path() -> anyhow::Result<PathBuf> {
    if let Some(path) = env_path("LOCAL_STUDIO_LITTER_BRIDGE_FILE")? {
        return require_absolute(path, "LOCAL_STUDIO_LITTER_BRIDGE_FILE");
    }
    if let Some(data_dir) = env_path("LOCAL_STUDIO_DATA_DIR")? {
        return Ok(require_absolute(data_dir, "LOCAL_STUDIO_DATA_DIR")?.join("litter-bridge.json"));
    }

    let base = directories::BaseDirs::new()
        .ok_or_else(|| anyhow!("could not determine the current user's home directory"))?;
    #[cfg(target_os = "macos")]
    let candidates = vec![
        base.home_dir()
            .join("Library/Application Support/Local Studio/litter-bridge.json"),
        base.home_dir().join(".local-studio/litter-bridge.json"),
    ];
    #[cfg(not(target_os = "macos"))]
    let candidates = vec![base.home_dir().join(".local-studio/litter-bridge.json")];
    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }
    candidates
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no Local Studio metadata location is supported"))
}

fn env_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    if value.is_empty() {
        bail!("{name} is set but empty");
    }
    Ok(Some(PathBuf::from(value)))
}

fn require_absolute(path: PathBuf, source: &str) -> anyhow::Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{source} must be an absolute path");
    }
    Ok(path)
}

fn load_gateway_metadata() -> anyhow::Result<GatewayMetadata> {
    let path = resolve_gateway_metadata_path()?;
    let file = open_metadata_file(&path)?;
    let metadata = file
        .metadata()
        .with_context(|| format!("reading metadata for {}", path.display()))?;
    validate_metadata_file(&path, &metadata)?;
    if metadata.len() > MAX_METADATA_BYTES {
        bail!("Local Studio gateway metadata exceeds the size limit");
    }
    let mut raw = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut raw)
        .with_context(|| format!("reading {}", path.display()))?;
    if raw.len() as u64 > MAX_METADATA_BYTES {
        bail!("Local Studio gateway metadata exceeds the size limit");
    }
    let parsed: GatewayMetadataFile =
        serde_json::from_slice(&raw).context("parsing Local Studio gateway metadata")?;
    validate_gateway_metadata(parsed)
}

fn open_metadata_file(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .with_context(|| format!("opening Local Studio gateway metadata {}", path.display()))
}

fn validate_metadata_file(path: &Path, metadata: &std::fs::Metadata) -> anyhow::Result<()> {
    if !metadata.file_type().is_file() {
        bail!("Local Studio gateway metadata is not a regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            bail!("Local Studio gateway metadata must have mode 0600");
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("Local Studio gateway metadata is not owned by the current user");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("secure Local Studio metadata permissions are unsupported on this platform");
    }
    let _ = path;
    Ok(())
}

fn validate_gateway_metadata(parsed: GatewayMetadataFile) -> anyhow::Result<GatewayMetadata> {
    let _ = parsed.protocol_version;
    if parsed.secret_header != GATEWAY_SECRET_HEADER {
        bail!("Local Studio gateway metadata names an unsupported secret header");
    }
    if parsed.secret.len() < 32
        || parsed.secret.len() > 512
        || parsed.secret.trim() != parsed.secret
    {
        bail!("Local Studio gateway metadata contains an invalid secret");
    }
    if parsed.controller_id.is_empty()
        || parsed.controller_id.trim() != parsed.controller_id
        || parsed.controller_id.len() > 512
    {
        bail!("Local Studio gateway metadata contains an invalid controller ID");
    }
    if parsed.pid == 0 {
        bail!("Local Studio gateway metadata contains an invalid process ID");
    }
    if !is_utc_timestamp(&parsed.issued_at) {
        bail!("Local Studio gateway metadata contains an invalid timestamp");
    }

    let url = Url::parse(&parsed.url).context("parsing Local Studio gateway URL")?;
    validate_gateway_url(&url)?;
    let mut secret = HeaderValue::from_str(&parsed.secret)
        .map_err(|_| anyhow!("Local Studio gateway metadata contains an invalid secret"))?;
    secret.set_sensitive(true);
    Ok(GatewayMetadata {
        url,
        secret,
        controller_id: parsed.controller_id,
        issued_at: parsed.issued_at,
    })
}

fn validate_gateway_url(url: &Url) -> anyhow::Result<()> {
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != GATEWAY_PATH
        || url.port().is_none()
    {
        bail!("Local Studio gateway URL does not match the fixed loopback route");
    }
    let ip = match url.host() {
        Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
        _ => bail!("Local Studio gateway URL must use a numeric loopback address"),
    };
    if !ip.is_loopback() {
        bail!("Local Studio gateway URL must use a numeric loopback address");
    }
    Ok(())
}

fn is_utc_timestamp(value: &str) -> bool {
    value.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn generated_request_id() -> String {
    format!("bridge-{:016x}", rand::random::<u64>())
}

fn request_id_from(params: &Value) -> String {
    params
        .get("auth")
        .and_then(|auth| auth.get("requestId"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.trim() == *value && value.len() <= 512)
        .map(ToOwned::to_owned)
        .unwrap_or_else(generated_request_id)
}

fn typed_error(code: ErrorCode, message: &str, request_id: String, retriable: bool) -> Value {
    serde_json::to_value(ErrorResult {
        kind: ErrorResultKind::Error,
        protocol_version: ProtocolVersion,
        request_id: request_id.clone(),
        error: BridgeError {
            code,
            message: message.to_string(),
            retriable,
            request_id: Some(request_id),
            details: None,
        },
    })
    .expect("typed Local Studio error must serialize")
}

#[derive(Clone)]
pub(crate) struct LocalStudioBridge {
    authenticated_node: EndpointId,
}

impl LocalStudioBridge {
    pub(crate) fn new(authenticated_node: EndpointId) -> Self {
        Self { authenticated_node }
    }

    fn authorize(&self, capability: Capability) -> anyhow::Result<AuthorizedGateway> {
        let gateway = authorized_gateway(&self.authenticated_node)?;
        if !gateway.grant.allows_capability(capability) {
            bail!("paired node lacks {capability}");
        }
        Ok(gateway)
    }

    async fn capabilities(&self, params: Value) -> Value {
        if !valid_capabilities_params(&params) {
            return typed_error(
                ErrorCode::InvalidRequest,
                "Capabilities request is invalid",
                generated_request_id(),
                false,
            );
        }
        let context = match authorized_gateway(&self.authenticated_node) {
            Ok(context) => context,
            Err(_) => {
                return typed_error(
                    ErrorCode::CapabilityDenied,
                    "Local Studio capability is not granted",
                    generated_request_id(),
                    false,
                );
            }
        };
        serde_json::to_value(CapabilitiesManifest {
            kind: CapabilitiesManifestKind::Capabilities,
            protocol_version: ProtocolVersion,
            bridge_id: context.metadata.bridge_id(),
            controller_id: context.metadata.controller_id,
            issued_at: now_timestamp(),
            capabilities: implemented_capabilities(&context.grant),
        })
        .expect("capabilities manifest must serialize")
    }

    async fn controller_read(&self, params: Value) -> Value {
        let request_id = request_id_from(&params);
        let request: ControllerSnapshotRequest = match serde_json::from_value(params.clone()) {
            Ok(request) => request,
            Err(_) => {
                return typed_error(
                    ErrorCode::InvalidRequest,
                    "Controller read request is invalid",
                    request_id,
                    false,
                );
            }
        };
        if request.auth.capability != Capability::StatsRead {
            return typed_error(
                ErrorCode::CapabilityDenied,
                "Controller read requires stats.read",
                request.auth.request_id,
                false,
            );
        }
        let node_id = self.authenticated_node.to_string();
        if request.auth.device.device_id != node_id || request.auth.device.key_id != node_id {
            return typed_error(
                ErrorCode::Unauthorized,
                "Request identity does not match the authenticated connection",
                request.auth.request_id,
                false,
            );
        }
        let context = match self.authorize(Capability::StatsRead) {
            Ok(context) => context,
            Err(_) => {
                return typed_error(
                    ErrorCode::CapabilityDenied,
                    "Local Studio capability is not granted",
                    request.auth.request_id,
                    false,
                );
            }
        };
        if request.controller_id != context.metadata.controller_id {
            return typed_error(
                ErrorCode::NotFound,
                "Controller identity was not found",
                request.auth.request_id,
                false,
            );
        }
        let body = match serde_json::to_vec(&request) {
            Ok(body) if body.len() <= MAX_HTTP_BODY_BYTES => body,
            _ => {
                return typed_error(
                    ErrorCode::PayloadTooLarge,
                    "Controller read request exceeds the size limit",
                    request.auth.request_id,
                    false,
                );
            }
        };
        match forward_controller_read(&context.metadata, &request, body).await {
            Ok(value) => value,
            Err(error) => typed_error(
                error.code,
                error.message,
                request.auth.request_id,
                error.retriable,
            ),
        }
    }

    async fn session_read(&self, params: Value) -> Value {
        let request_id = request_id_from(&params);
        let request: SessionReadRequest = match serde_json::from_value(params) {
            Ok(request) => request,
            Err(_) => {
                return typed_error(
                    ErrorCode::InvalidRequest,
                    "Session read request is invalid",
                    request_id,
                    false,
                );
            }
        };
        let node_id = self.authenticated_node.to_string();
        if request.auth.device.device_id != node_id || request.auth.device.key_id != node_id {
            return typed_error(
                ErrorCode::Unauthorized,
                "Request identity does not match the authenticated connection",
                request.auth.request_id,
                false,
            );
        }
        let context = match self.authorize(Capability::SessionsRead) {
            Ok(context) => context,
            Err(_) => {
                return typed_error(
                    ErrorCode::CapabilityDenied,
                    "sessions.read is not granted",
                    request.auth.request_id,
                    false,
                );
            }
        };
        let body = match serde_json::to_vec(&request) {
            Ok(body) if body.len() <= MAX_HTTP_BODY_BYTES => body,
            _ => {
                return typed_error(
                    ErrorCode::PayloadTooLarge,
                    "Session read request exceeds the size limit",
                    request.auth.request_id,
                    false,
                );
            }
        };
        match forward_session_read(&context.metadata, &request, body).await {
            Ok(value) => value,
            Err(error) => typed_error(
                error.code,
                error.message,
                request.auth.request_id,
                error.retriable,
            ),
        }
    }
}

#[async_trait]
impl Bridge for LocalStudioBridge {
    async fn initialize(&self, _ctx: &Conn, _params: Value) -> Result<Value, JsonRpcError> {
        Ok(json!({
            "userAgent": format!("alleycat-local-studio/{}", env!("CARGO_PKG_VERSION")),
            "capabilities": {
                "methods": [
                    "localStudio/capabilities",
                    "localStudio/controller/read",
                    "localStudio/session/read"
                ]
            }
        }))
    }

    async fn dispatch(
        &self,
        _ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "localStudio/capabilities" => Ok(self.capabilities(params).await),
            "localStudio/controller/read" => Ok(self.controller_read(params).await),
            "localStudio/session/read" => Ok(self.session_read(params).await),
            other => Err(JsonRpcError::method_not_found(other)),
        }
    }
}

fn valid_capabilities_params(params: &Value) -> bool {
    if params.is_null() {
        return true;
    }
    let Some(fields) = params.as_object() else {
        return false;
    };
    if fields.is_empty() {
        return true;
    }
    fields.len() == 1
        && fields.get("protocolVersion").is_some_and(|version| {
            serde_json::from_value::<ProtocolVersion>(version.clone()).is_ok()
        })
}

struct ForwardError {
    code: ErrorCode,
    message: &'static str,
    retriable: bool,
}

impl ForwardError {
    fn unavailable() -> Self {
        Self {
            code: ErrorCode::ControllerUnavailable,
            message: "Local Studio gateway is unavailable",
            retriable: true,
        }
    }

    fn too_large() -> Self {
        Self {
            code: ErrorCode::PayloadTooLarge,
            message: "Local Studio gateway response exceeds the size limit",
            retriable: false,
        }
    }
}

async fn forward_controller_read(
    metadata: &GatewayMetadata,
    request: &ControllerSnapshotRequest,
    body: Vec<u8>,
) -> Result<Value, ForwardError> {
    let response = forward_gateway(metadata, body).await?;
    if response.status.is_success() {
        let snapshot: ControllerSnapshot = serde_json::from_value(response.value.clone())
            .map_err(|_| ForwardError::unavailable())?;
        if snapshot.controller_id != metadata.controller_id
            || !snapshot.capabilities.contains(&Capability::StatsRead)
            || snapshot
                .capabilities
                .iter()
                .any(|capability| !IMPLEMENTED_CAPABILITIES.contains(capability))
        {
            return Err(ForwardError::unavailable());
        }
        return Ok(response.value);
    }

    validate_gateway_error(&response.value, &request.auth.request_id)?;
    Ok(response.value)
}

async fn forward_session_read(
    metadata: &GatewayMetadata,
    request: &SessionReadRequest,
    body: Vec<u8>,
) -> Result<Value, ForwardError> {
    let response = forward_gateway(metadata, body).await?;
    if response.status.is_success() {
        let page: SessionPage = serde_json::from_value(response.value.clone())
            .map_err(|_| ForwardError::unavailable())?;
        if page.request_id != request.auth.request_id
            || page.canonical_session.authority != SessionAuthority::LocalStudio
            || page.canonical_session.installation_id != metadata.controller_id
            || page.origin.application != SessionAuthority::LocalStudio
            || page.origin.installation_id != metadata.controller_id
            || request
                .session
                .as_ref()
                .is_some_and(|requested| requested != &page.canonical_session)
            || request
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.revision != page.revision)
            || page.messages.len() > usize::from(request.limit)
            || page.messages.len() > MAX_SESSION_PAGE_ITEMS
            || page.tools.len() > MAX_SESSION_PAGE_ITEMS
            || page.attachments.len() > MAX_SESSION_PAGE_ITEMS
            || page
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.revision != page.revision)
        {
            return Err(ForwardError::unavailable());
        }
        return Ok(response.value);
    }

    validate_gateway_error(&response.value, &request.auth.request_id)?;
    Ok(response.value)
}

fn validate_gateway_error(value: &Value, request_id: &str) -> Result<(), ForwardError> {
    let error: ErrorResult =
        serde_json::from_value(value.clone()).map_err(|_| ForwardError::unavailable())?;
    if error.request_id != request_id
        || error
            .error
            .request_id
            .as_deref()
            .is_some_and(|nested| nested != request_id)
    {
        return Err(ForwardError::unavailable());
    }
    Ok(())
}

struct GatewayResponse {
    status: reqwest::StatusCode,
    value: Value,
}

async fn forward_gateway(
    metadata: &GatewayMetadata,
    body: Vec<u8>,
) -> Result<GatewayResponse, ForwardError> {
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .timeout(TOTAL_TIMEOUT)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .build()
        .map_err(|_| ForwardError::unavailable())?;
    let secret_header = HeaderName::from_static(GATEWAY_SECRET_HEADER);
    let deadline = tokio::time::Instant::now() + TOTAL_TIMEOUT;
    let response = tokio::time::timeout_at(
        deadline,
        client
            .post(metadata.url.clone())
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .header(secret_header, metadata.secret.clone())
            .body(body)
            .send(),
    )
    .await
    .map_err(|_| ForwardError::unavailable())?
    .map_err(|_| ForwardError::unavailable())?;

    if response.status().is_redirection() {
        return Err(ForwardError::unavailable());
    }
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_HTTP_BODY_BYTES)
    {
        return Err(ForwardError::too_large());
    }
    let content_type_ok = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(';').next() == Some("application/json"));
    if !content_type_ok {
        return Err(ForwardError::unavailable());
    }
    let status = response.status();
    let bytes = tokio::time::timeout_at(deadline, read_bounded_response(response))
        .await
        .map_err(|_| ForwardError::unavailable())??;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| ForwardError::unavailable())?;
    Ok(GatewayResponse { status, value })
}

async fn read_bounded_response(response: reqwest::Response) -> Result<Vec<u8>, ForwardError> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = tokio::time::timeout(READ_TIMEOUT, stream.next())
        .await
        .map_err(|_| ForwardError::unavailable())?
    {
        let chunk = chunk.map_err(|_| ForwardError::unavailable())?;
        if bytes.len().saturating_add(chunk.len()) > MAX_HTTP_BODY_BYTES {
            return Err(ForwardError::too_large());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::grants::{ActionTargetGrant, PairedNodeGrant};
    use crate::test_support::TempHome;

    const TEST_SECRET: &str = "test-secret-that-is-at-least-thirty-two-bytes-long";
    const CONTROLLER_ID: &str = "controller-test";

    fn configure_metadata_path(home: &mut TempHome) -> PathBuf {
        let path = home.path().join("litter-bridge.json");
        home.override_env(&[("LOCAL_STUDIO_LITTER_BRIDGE_FILE", path.to_str().unwrap())]);
        path
    }

    fn write_metadata(path: &Path, url: &str) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "protocolVersion": 1,
                "url": url,
                "secretHeader": GATEWAY_SECRET_HEADER,
                "secret": TEST_SECRET,
                "controllerId": CONTROLLER_ID,
                "pid": std::process::id(),
                "issuedAt": "2026-07-20T18:30:00.000Z"
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn grant_stats(node: EndpointId) {
        grant_capabilities(node, &[Capability::StatsRead]);
    }

    fn grant_capabilities(node: EndpointId, capabilities: &[Capability]) {
        let mut store = GrantStore::empty();
        store.grant_capabilities(node, capabilities, None).unwrap();
        store.save().unwrap();
    }

    fn error_code(value: &Value) -> &str {
        value["error"]["code"].as_str().unwrap()
    }

    fn controller_request(node: &EndpointId) -> Value {
        json!({
            "type": "controller_snapshot_request",
            "protocolVersion": 1,
            "auth": {
                "device": {
                    "deviceId": node.to_string(),
                    "keyId": node.to_string(),
                    "algorithm": "ed25519"
                },
                "requestId": "request-1",
                "issuedAt": "2026-07-20T18:29:50.000Z",
                "expiresAt": "2026-07-20T18:30:20.000Z",
                "nonce": "nonce-0123456789",
                "bodyHash": "a".repeat(64),
                "signature": "A".repeat(86),
                "capability": "stats.read"
            },
            "controllerId": CONTROLLER_ID
        })
    }

    fn session_request(node: &EndpointId) -> Value {
        json!({
            "type": "session_read_request",
            "protocolVersion": 1,
            "auth": {
                "device": {
                    "deviceId": node.to_string(),
                    "keyId": node.to_string(),
                    "algorithm": "ed25519"
                },
                "requestId": "request-session-1",
                "issuedAt": "2026-07-20T18:29:50.000Z",
                "expiresAt": "2026-07-20T18:30:20.000Z",
                "nonce": "session-nonce-1234",
                "bodyHash": "b".repeat(64),
                "signature": "B".repeat(86),
                "capability": "sessions.read"
            },
            "session": {
                "kind": "external_session",
                "authority": "local-studio",
                "installationId": CONTROLLER_ID,
                "sessionId": "019f7ca0-1f06-78a3-b4f2-58b6672994af"
            },
            "cursor": null,
            "limit": 50
        })
    }

    fn session_page_body() -> Vec<u8> {
        let hash = "c".repeat(64);
        serde_json::to_vec(&json!({
            "type": "session_page",
            "protocolVersion": 1,
            "requestId": "request-session-1",
            "pageId": "page-1",
            "canonicalSession": {
                "kind": "external_session",
                "authority": "local-studio",
                "installationId": CONTROLLER_ID,
                "sessionId": "019f7ca0-1f06-78a3-b4f2-58b6672994af"
            },
            "origin": {
                "application": "local-studio",
                "installationId": CONTROLLER_ID,
                "deviceId": null,
                "exportedAt": "2026-07-20T18:30:00.000Z"
            },
            "metadata": {
                "title": "Acceptance session",
                "cwd": null,
                "createdAt": "2026-07-20T18:00:00.000Z",
                "updatedAt": "2026-07-20T18:30:00.000Z",
                "modelId": null,
                "providerId": null
            },
            "revision": 1,
            "messages": [],
            "tools": [],
            "attachments": [],
            "contentHashes": {
                "algorithm": "sha256",
                "session": hash,
                "messages": [],
                "tools": [],
                "attachments": []
            },
            "cursor": null
        }))
        .unwrap()
    }

    fn failed_section(request_id: &str) -> Value {
        json!({
            "value": null,
            "error": {
                "code": "section_unavailable",
                "message": "section unavailable",
                "retriable": true,
                "requestId": request_id,
                "details": null
            },
            "freshness": {
                "observedAt": null,
                "ageMs": null,
                "maxAgeMs": 5000,
                "stale": false,
                "sourceRevision": null
            }
        })
    }

    fn snapshot_body() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "type": "controller_snapshot",
            "protocolVersion": 1,
            "snapshotId": "snapshot-1",
            "controllerId": CONTROLLER_ID,
            "displayName": "Test Studio",
            "generatedAt": "2026-07-20T18:30:00.000Z",
            "revision": 1,
            "state": "degraded",
            "capabilities": ["stats.read"],
            "sections": {
                "health": {
                    "value": {
                        "state": "ok",
                        "reachable": true,
                        "checkedAt": "2026-07-20T18:30:00.000Z",
                        "latencyMs": 1,
                        "controllerVersion": null
                    },
                    "error": null,
                    "freshness": {
                        "observedAt": "2026-07-20T18:30:00.000Z",
                        "ageMs": 0,
                        "maxAgeMs": 5000,
                        "stale": false,
                        "sourceRevision": null
                    }
                },
                "status": failed_section("request-1"),
                "gpus": failed_section("request-1"),
                "metrics": failed_section("request-1"),
                "agentRuntime": failed_section("request-1")
            }
        }))
        .unwrap()
    }

    async fn spawn_http_response(
        status: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };
                let header_text = String::from_utf8_lossy(&request[..header_end]);
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
            let mut response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
            for (name, value) in headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (
            format!("http://127.0.0.1:{}{GATEWAY_PATH}", address.port()),
            task,
        )
    }

    #[test]
    fn advertisement_reports_per_caller_authority_without_hiding_gateway() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");
        let granted = iroh::SecretKey::generate().public();
        let other = iroh::SecretKey::generate().public();

        let ungranted = local_studio_agent_info(Some(&granted));
        assert!(ungranted.available);
        assert_eq!(ungranted.local_studio.unwrap().capabilities, Vec::new());
        grant_stats(granted);
        let visible = local_studio_agent_info(Some(&granted));
        assert!(visible.available);
        assert_eq!(
            visible.local_studio.unwrap().capabilities,
            vec![Capability::StatsRead]
        );
        let other_info = local_studio_agent_info(Some(&other));
        assert!(other_info.available);
        let other_advertisement = other_info.local_studio.unwrap();
        assert!(other_advertisement.capabilities.is_empty());
        assert!(other_advertisement.actions.is_empty());

        let mut store = GrantStore::load().unwrap();
        assert!(store.revoke(&granted, Utc::now()));
        store.save().unwrap();
        let revoked = local_studio_agent_info(Some(&granted));
        assert!(revoked.available);
        assert!(revoked.local_studio.unwrap().capabilities.is_empty());

        let mut store = GrantStore::empty();
        store
            .replace(PairedNodeGrant {
                endpoint_id: granted.to_string(),
                protocol_version: ProtocolVersion,
                grants: vec![Capability::StatsRead],
                expires_at: Some(Utc::now() - chrono::Duration::seconds(1)),
                revoked_at: None,
                actions: Vec::new(),
            })
            .unwrap();
        store.save().unwrap();
        let expired = local_studio_agent_info(Some(&granted));
        assert!(expired.available);
        assert!(expired.local_studio.unwrap().capabilities.is_empty());

        let anonymous = local_studio_agent_info(None);
        assert!(!anonymous.available);
        assert!(anonymous.local_studio.is_none());
    }

    #[test]
    fn advertisement_includes_sessions_read_and_suppresses_unimplemented_authority() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");
        let node = iroh::SecretKey::generate().public();
        let mut store = GrantStore::empty();
        store
            .replace(PairedNodeGrant {
                endpoint_id: node.to_string(),
                protocol_version: ProtocolVersion,
                grants: vec![
                    Capability::SessionsRead,
                    Capability::ModelsControl,
                    Capability::StatsRead,
                ],
                expires_at: None,
                revoked_at: None,
                actions: vec![ActionTargetGrant::EvictModel {
                    targets: vec!["model-1".into()],
                }],
            })
            .unwrap();
        store.save().unwrap();

        let advertisement = local_studio_agent_info(Some(&node)).local_studio.unwrap();
        assert_eq!(
            advertisement.capabilities,
            vec![Capability::StatsRead, Capability::SessionsRead]
        );
        assert!(advertisement.actions.is_empty());
    }

    #[test]
    fn malformed_or_insecure_metadata_fails_closed() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        let node = iroh::SecretKey::generate().public();
        grant_stats(node);

        write_metadata(&metadata, "http://localhost:54321/api/litter-bridge/v1");
        assert!(!local_studio_agent_info(Some(&node)).available);

        let mut extra =
            serde_json::from_slice::<Value>(&std::fs::read(&metadata).unwrap()).unwrap();
        extra["token"] = json!("unexpected");
        std::fs::write(&metadata, serde_json::to_vec(&extra).unwrap()).unwrap();
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(!local_studio_agent_info(Some(&node)).available);

        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!local_studio_agent_info(Some(&node)).available);

        std::fs::remove_file(&metadata).unwrap();
        std::os::unix::fs::symlink(home.path().join("missing-real-metadata"), &metadata).unwrap();
        assert!(!local_studio_agent_info(Some(&node)).available);
    }

    #[test]
    fn malformed_grant_store_has_zero_advertised_authority() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");
        let grants = crate::paths::paired_nodes_file().unwrap();
        std::fs::write(grants, b"{\"version\":1,\"nodes\":\"invalid\"}").unwrap();
        let node = iroh::SecretKey::generate().public();
        let info = local_studio_agent_info(Some(&node));
        assert!(info.available);
        let advertisement = info.local_studio.unwrap();
        assert!(advertisement.capabilities.is_empty());
        assert!(advertisement.actions.is_empty());
        assert!(gateway_available());
    }

    #[test]
    fn connect_availability_depends_on_secure_gateway_not_grant() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");

        assert!(gateway_available());

        let grants = crate::paths::paired_nodes_file().unwrap();
        std::fs::write(grants, b"{\"version\":1,\"nodes\":\"invalid\"}").unwrap();
        assert!(gateway_available());

        std::fs::remove_file(metadata).unwrap();
        assert!(!gateway_available());
    }

    #[test]
    fn explicit_metadata_file_has_precedence_over_data_directory() {
        let mut home = TempHome::new();
        let explicit = home.path().join("explicit.json");
        let data_dir = home.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        home.override_env(&[
            (
                "LOCAL_STUDIO_LITTER_BRIDGE_FILE",
                explicit.to_str().unwrap(),
            ),
            ("LOCAL_STUDIO_DATA_DIR", data_dir.to_str().unwrap()),
        ]);
        assert_eq!(resolve_gateway_metadata_path().unwrap(), explicit);
    }

    #[tokio::test]
    async fn capabilities_reloads_grants_and_revocation_returns_typed_error() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");
        let node = iroh::SecretKey::generate().public();
        grant_capabilities(node, &[Capability::StatsRead, Capability::SessionsRead]);
        let bridge = LocalStudioBridge::new(node);

        let manifest = bridge.capabilities(json!({"protocolVersion": 1})).await;
        assert_eq!(manifest["type"], "capabilities");
        assert_eq!(
            manifest["capabilities"],
            json!(["stats.read", "sessions.read"])
        );

        let mut store = GrantStore::load().unwrap();
        assert!(
            store
                .revoke_capabilities(&node, &[Capability::SessionsRead])
                .unwrap()
        );
        store.save().unwrap();
        let narrowed = bridge.capabilities(Value::Null).await;
        assert_eq!(narrowed["capabilities"], json!(["stats.read"]));

        let mut store = GrantStore::load().unwrap();
        assert!(store.revoke(&node, Utc::now()));
        store.save().unwrap();
        let denied = bridge.capabilities(Value::Null).await;
        assert_eq!(error_code(&denied), "capability_denied");
    }

    #[tokio::test]
    async fn controller_read_requires_connection_identity_before_gateway_io() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:9/api/litter-bridge/v1");
        let node = iroh::SecretKey::generate().public();
        let other = iroh::SecretKey::generate().public();
        grant_stats(node);
        let bridge = LocalStudioBridge::new(node);

        let denied = bridge.controller_read(controller_request(&other)).await;
        assert_eq!(error_code(&denied), "unauthorized");
    }

    #[tokio::test]
    async fn controller_read_posts_fixed_secret_and_parses_strict_snapshot() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        let node = iroh::SecretKey::generate().public();
        grant_stats(node);
        let body = snapshot_body();
        let (url, server) = spawn_http_response(
            "200 OK",
            vec![
                ("Content-Type".into(), "application/json".into()),
                ("Content-Length".into(), body.len().to_string()),
            ],
            body,
        )
        .await;
        write_metadata(&metadata, &url);
        let bridge = LocalStudioBridge::new(node);

        let response = bridge.controller_read(controller_request(&node)).await;
        assert_eq!(response["type"], "controller_snapshot");
        let received = server.await.unwrap().to_ascii_lowercase();
        assert!(received.starts_with("post /api/litter-bridge/v1 http/1.1"));
        assert!(received.contains(&format!(
            "{gateway_secret_header}: {test_secret}",
            gateway_secret_header = GATEWAY_SECRET_HEADER,
            test_secret = TEST_SECRET.to_ascii_lowercase()
        )));
    }

    #[tokio::test]
    async fn session_read_requires_connection_identity_before_gateway_io() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        write_metadata(&metadata, "http://127.0.0.1:9/api/litter-bridge/v1");
        let node = iroh::SecretKey::generate().public();
        let other = iroh::SecretKey::generate().public();
        grant_capabilities(node, &[Capability::SessionsRead]);
        let bridge = LocalStudioBridge::new(node);

        let denied = bridge.session_read(session_request(&other)).await;
        assert_eq!(error_code(&denied), "unauthorized");
    }

    #[tokio::test]
    async fn session_read_forwards_only_the_signed_request_and_live_revoke_denies_open_bridge() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        let node = iroh::SecretKey::generate().public();
        grant_capabilities(node, &[Capability::SessionsRead]);
        let body = session_page_body();
        let (url, server) = spawn_http_response(
            "200 OK",
            vec![
                ("Content-Type".into(), "application/json".into()),
                ("Content-Length".into(), body.len().to_string()),
            ],
            body,
        )
        .await;
        write_metadata(&metadata, &url);
        let bridge = LocalStudioBridge::new(node);
        let signed_request = session_request(&node);

        let response = bridge.session_read(signed_request.clone()).await;
        assert_eq!(response["type"], "session_page");
        assert_eq!(
            response["canonicalSession"]["sessionId"],
            "019f7ca0-1f06-78a3-b4f2-58b6672994af"
        );
        let received = server.await.unwrap();
        let forwarded_body = received.split("\r\n\r\n").nth(1).unwrap();
        let forwarded: Value = serde_json::from_str(forwarded_body).unwrap();
        assert_eq!(forwarded, signed_request);
        assert!(forwarded.get("method").is_none());

        let mut store = GrantStore::load().unwrap();
        assert!(
            store
                .revoke_capabilities(&node, &[Capability::SessionsRead])
                .unwrap()
        );
        store.save().unwrap();
        let denied = bridge.session_read(session_request(&node)).await;
        assert_eq!(error_code(&denied), "capability_denied");
    }

    #[tokio::test]
    async fn session_read_accepts_only_strict_pages_or_matching_typed_errors() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        let node = iroh::SecretKey::generate().public();
        grant_capabilities(node, &[Capability::SessionsRead]);
        let bridge = LocalStudioBridge::new(node);

        let mut wrong_page: Value = serde_json::from_slice(&session_page_body()).unwrap();
        wrong_page["requestId"] = json!("request-other");
        let wrong_body = serde_json::to_vec(&wrong_page).unwrap();
        let (wrong_url, wrong_server) = spawn_http_response(
            "200 OK",
            vec![
                ("Content-Type".into(), "application/json".into()),
                ("Content-Length".into(), wrong_body.len().to_string()),
            ],
            wrong_body,
        )
        .await;
        write_metadata(&metadata, &wrong_url);
        let unavailable = bridge.session_read(session_request(&node)).await;
        assert_eq!(error_code(&unavailable), "controller_unavailable");
        wrong_server.await.unwrap();

        let error_body = serde_json::to_vec(&json!({
            "type": "error",
            "protocolVersion": 1,
            "requestId": "request-session-1",
            "error": {
                "code": "not_found",
                "message": "session not found",
                "retriable": false,
                "requestId": "request-session-1",
                "details": null
            }
        }))
        .unwrap();
        let (error_url, error_server) = spawn_http_response(
            "404 Not Found",
            vec![
                ("Content-Type".into(), "application/json".into()),
                ("Content-Length".into(), error_body.len().to_string()),
            ],
            error_body,
        )
        .await;
        write_metadata(&metadata, &error_url);
        let not_found = bridge.session_read(session_request(&node)).await;
        assert_eq!(error_code(&not_found), "not_found");
        error_server.await.unwrap();
    }

    #[tokio::test]
    async fn redirect_and_oversized_gateway_responses_fail_closed() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);
        let node = iroh::SecretKey::generate().public();
        grant_stats(node);
        let bridge = LocalStudioBridge::new(node);

        let (redirect_url, redirect_server) = spawn_http_response(
            "302 Found",
            vec![
                (
                    "Location".into(),
                    "http://127.0.0.1:1/api/litter-bridge/v1".into(),
                ),
                ("Content-Length".into(), "0".into()),
            ],
            Vec::new(),
        )
        .await;
        write_metadata(&metadata, &redirect_url);
        let redirected = bridge.controller_read(controller_request(&node)).await;
        assert_eq!(error_code(&redirected), "controller_unavailable");
        redirect_server.await.unwrap();

        let (oversized_url, oversized_server) = spawn_http_response(
            "200 OK",
            vec![
                ("Content-Type".into(), "application/json".into()),
                (
                    "Content-Length".into(),
                    (MAX_HTTP_BODY_BYTES + 1).to_string(),
                ),
            ],
            Vec::new(),
        )
        .await;
        write_metadata(&metadata, &oversized_url);
        let oversized = bridge.controller_read(controller_request(&node)).await;
        assert_eq!(error_code(&oversized), "payload_too_large");
        oversized_server.await.unwrap();
    }
}
