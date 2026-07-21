use std::collections::HashSet;
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const PROTOCOL_VERSION: u32 = 1;
pub const SIGNATURE_DOMAIN: &[u8] = b"litter-bridge-request-v1";
pub const MAX_SAFE_JSON_INTEGER: u64 = 9_007_199_254_740_991;
pub const MAX_AGENT_TURN_CONTENT_LENGTH: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProtocolVersion;

impl Serialize for ProtocolVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(PROTOCOL_VERSION)
    }
}

impl<'de> Deserialize<'de> for ProtocolVersion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let version = u32::deserialize(deserializer)?;
        if version == PROTOCOL_VERSION {
            Ok(Self)
        } else {
            Err(D::Error::custom(format!(
                "unsupported Local Studio bridge protocol version {version}"
            )))
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Capability {
    #[serde(rename = "stats.read")]
    StatsRead,
    #[serde(rename = "models.control")]
    ModelsControl,
    #[serde(rename = "sessions.read")]
    SessionsRead,
    #[serde(rename = "sessions.write")]
    SessionsWrite,
    #[serde(rename = "agent.turn")]
    AgentTurn,
}

impl Capability {
    pub const ALL: [Self; 5] = [
        Self::StatsRead,
        Self::ModelsControl,
        Self::SessionsRead,
        Self::SessionsWrite,
        Self::AgentTurn,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StatsRead => "stats.read",
            Self::ModelsControl => "models.control",
            Self::SessionsRead => "sessions.read",
            Self::SessionsWrite => "sessions.write",
            Self::AgentTurn => "agent.turn",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ControllerActionKind {
    StartRecipe,
    CancelLaunch,
    EvictModel,
}

impl ControllerActionKind {
    pub const ALL: [Self; 3] = [Self::StartRecipe, Self::CancelLaunch, Self::EvictModel];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StartRecipe => "start_recipe",
            Self::CancelLaunch => "cancel_launch",
            Self::EvictModel => "evict_model",
        }
    }
}

impl fmt::Display for ControllerActionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControllerAction {
    StartRecipe {
        #[serde(rename = "recipeId", deserialize_with = "deserialize_identifier")]
        recipe_id: String,
    },
    CancelLaunch {
        #[serde(rename = "launchId", deserialize_with = "deserialize_identifier")]
        launch_id: String,
    },
    EvictModel {
        #[serde(rename = "modelId", deserialize_with = "deserialize_identifier")]
        model_id: String,
    },
}

impl ControllerAction {
    pub const fn kind(&self) -> ControllerActionKind {
        match self {
            Self::StartRecipe { .. } => ControllerActionKind::StartRecipe,
            Self::CancelLaunch { .. } => ControllerActionKind::CancelLaunch,
            Self::EvictModel { .. } => ControllerActionKind::EvictModel,
        }
    }

    pub fn target(&self) -> &str {
        match self {
            Self::StartRecipe { recipe_id } => recipe_id,
            Self::CancelLaunch { launch_id } => launch_id,
            Self::EvictModel { model_id } => model_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalStudioAdvertisement {
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub bridge_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub controller_id: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub issued_at: String,
    #[serde(deserialize_with = "deserialize_unique_capabilities")]
    pub capabilities: Vec<Capability>,
    #[serde(deserialize_with = "deserialize_unique_actions")]
    pub actions: Vec<ControllerActionKind>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceAuth {
    #[serde(deserialize_with = "deserialize_identifier")]
    pub device_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub key_id: String,
    pub algorithm: SignatureAlgorithm,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SignatureAlgorithm {
    Ed25519,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestAuth {
    pub device: DeviceAuth,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub issued_at: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub expires_at: String,
    #[serde(deserialize_with = "deserialize_nonce")]
    pub nonce: String,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub body_hash: String,
    #[serde(deserialize_with = "deserialize_signature")]
    pub signature: String,
    pub capability: Capability,
}

pub type SessionsReadAuth = RequestAuth;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MutationAuth {
    pub device: DeviceAuth,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub issued_at: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub expires_at: String,
    #[serde(deserialize_with = "deserialize_nonce")]
    pub nonce: String,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub body_hash: String,
    #[serde(deserialize_with = "deserialize_signature")]
    pub signature: String,
    pub capability: Capability,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CapabilitiesManifestKind {
    #[serde(rename = "capabilities")]
    Capabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CapabilitiesManifest {
    #[serde(rename = "type")]
    pub kind: CapabilitiesManifestKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub bridge_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub controller_id: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub issued_at: String,
    #[serde(deserialize_with = "deserialize_unique_capabilities")]
    pub capabilities: Vec<Capability>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControllerSnapshotRequestKind {
    #[serde(rename = "controller_snapshot_request")]
    ControllerSnapshotRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControllerSnapshotRequest {
    #[serde(rename = "type")]
    pub kind: ControllerSnapshotRequestKind,
    pub protocol_version: ProtocolVersion,
    pub auth: RequestAuth,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub controller_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SectionName {
    Health,
    Status,
    Gpus,
    Metrics,
    #[serde(rename = "agent-runtime")]
    AgentRuntime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRequest,
    Unauthorized,
    Forbidden,
    ExpiredRequest,
    ReplayDetected,
    UnsupportedVersion,
    CapabilityDenied,
    NotFound,
    RevisionConflict,
    RateLimited,
    PayloadTooLarge,
    IntegrityFailed,
    ControllerUnavailable,
    SectionUnavailable,
    AgentRuntimeUnavailable,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ErrorDetails {
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub field: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub section: Option<SectionName>,
    #[serde(deserialize_with = "deserialize_optional_safe_u64")]
    pub expected_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_safe_u64")]
    pub current_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_safe_u64")]
    pub retry_after_ms: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_safe_u64")]
    pub limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BridgeError {
    pub code: ErrorCode,
    #[serde(deserialize_with = "deserialize_short_text")]
    pub message: String,
    pub retriable: bool,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub request_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub details: Option<ErrorDetails>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ErrorResultKind {
    #[serde(rename = "error")]
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ErrorResult {
    #[serde(rename = "type")]
    pub kind: ErrorResultKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    pub error: BridgeError,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Freshness {
    #[serde(deserialize_with = "deserialize_optional_timestamp")]
    pub observed_at: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub age_ms: Option<u64>,
    pub max_age_ms: u64,
    pub stale: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub source_revision: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(
    deny_unknown_fields,
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>")
)]
pub struct SnapshotSection<T> {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub value: Option<T>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub error: Option<BridgeError>,
    pub freshness: Freshness,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AvailabilityState {
    Ok,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControllerHealth {
    pub state: AvailabilityState,
    pub reachable: bool,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub checked_at: String,
    #[serde(deserialize_with = "deserialize_optional_nonnegative_f64")]
    pub latency_ms: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub controller_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControllerStatus {
    pub running: bool,
    #[serde(deserialize_with = "deserialize_optional_positive_u16")]
    pub inference_port: Option<u16>,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub launching_recipe_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub active_launch_id: Option<String>,
    #[serde(deserialize_with = "deserialize_identifiers")]
    pub active_model_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GpuDevice {
    #[serde(deserialize_with = "deserialize_identifier")]
    pub id: String,
    pub index: u32,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub name: String,
    pub memory_total_bytes: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub memory_used_bytes: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub memory_free_bytes: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_percentage")]
    pub utilization_percent: Option<f64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub temperature_celsius: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_nonnegative_f64")]
    pub power_watts: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GpuSnapshot {
    pub count: u32,
    pub devices: Vec<GpuDevice>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Metrics {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub requests_active: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub requests_queued: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_nonnegative_f64")]
    pub prompt_tokens_per_second: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_nonnegative_f64")]
    pub generation_tokens_per_second: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_nonnegative_f64")]
    pub time_to_first_token_ms: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_percentage")]
    pub cache_usage_percent: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentRuntimeStats {
    pub state: AvailabilityState,
    pub reachable: bool,
    pub running_session_count: u64,
    pub active_turn_count: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub persisted_session_count: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub event_sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControllerSnapshotSections {
    pub health: SnapshotSection<ControllerHealth>,
    pub status: SnapshotSection<ControllerStatus>,
    pub gpus: SnapshotSection<GpuSnapshot>,
    pub metrics: SnapshotSection<Metrics>,
    pub agent_runtime: SnapshotSection<AgentRuntimeStats>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControllerSnapshot {
    #[serde(rename = "type")]
    pub kind: ControllerSnapshotKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub snapshot_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub controller_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub display_name: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub generated_at: String,
    pub revision: u64,
    pub state: SnapshotState,
    #[serde(deserialize_with = "deserialize_unique_capabilities")]
    pub capabilities: Vec<Capability>,
    pub sections: ControllerSnapshotSections,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ControllerSnapshotResult {
    Snapshot(Box<ControllerSnapshot>),
    Error(ErrorResult),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControllerSnapshotKind {
    #[serde(rename = "controller_snapshot")]
    ControllerSnapshot,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionAuthority {
    #[serde(rename = "local-studio")]
    LocalStudio,
    #[serde(rename = "litter")]
    Litter,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExternalSessionIdentityKind {
    #[serde(rename = "external_session")]
    ExternalSession,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExternalSessionIdentity {
    pub kind: ExternalSessionIdentityKind,
    pub authority: SessionAuthority,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub installation_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionOrigin {
    pub application: SessionAuthority,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub installation_id: String,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub device_id: Option<String>,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub exported_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionMetadata {
    #[serde(deserialize_with = "deserialize_optional_short_text")]
    pub title: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub cwd: Option<String>,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub created_at: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub updated_at: String,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub model_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub provider_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionListCursorKind {
    #[serde(rename = "session_list_cursor")]
    SessionListCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionListCursor {
    #[serde(rename = "type")]
    pub kind: SessionListCursorKind,
    #[serde(deserialize_with = "deserialize_opaque_token")]
    pub token: String,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub revision: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionDescriptor {
    pub session: ExternalSessionIdentity,
    pub metadata: SessionMetadata,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub revision: u64,
    pub archived: bool,
    pub active: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionListRequestKind {
    #[serde(rename = "session_list_request")]
    SessionListRequest,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionListRequest {
    #[serde(rename = "type")]
    pub kind: SessionListRequestKind,
    pub protocol_version: ProtocolVersion,
    pub auth: SessionsReadAuth,
    pub cursor: Option<SessionListCursor>,
    pub limit: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionListRequestWire {
    #[serde(rename = "type")]
    kind: SessionListRequestKind,
    protocol_version: ProtocolVersion,
    auth: SessionsReadAuth,
    #[serde(deserialize_with = "deserialize_required_option")]
    cursor: Option<SessionListCursor>,
    #[serde(deserialize_with = "deserialize_session_limit")]
    limit: u16,
}

impl<'de> Deserialize<'de> for SessionListRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let request = SessionListRequestWire::deserialize(deserializer)?;
        if request.auth.capability != Capability::SessionsRead {
            return Err(D::Error::custom(
                "session list auth must require sessions.read",
            ));
        }
        if request
            .cursor
            .as_ref()
            .is_some_and(|cursor| !cursor.has_more)
        {
            return Err(D::Error::custom(
                "session list continuation cursor must advertise another page",
            ));
        }
        Ok(Self {
            kind: request.kind,
            protocol_version: request.protocol_version,
            auth: request.auth,
            cursor: request.cursor,
            limit: request.limit,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionListPageKind {
    #[serde(rename = "session_list_page")]
    SessionListPage,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionListPage {
    #[serde(rename = "type")]
    pub kind: SessionListPageKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub controller_id: String,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub revision: u64,
    pub sessions: Vec<SessionDescriptor>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cursor: Option<SessionListCursor>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionListPageWire {
    #[serde(rename = "type")]
    kind: SessionListPageKind,
    protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    request_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    controller_id: String,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    revision: u64,
    sessions: Vec<SessionDescriptor>,
    #[serde(deserialize_with = "deserialize_required_option")]
    cursor: Option<SessionListCursor>,
}

impl<'de> Deserialize<'de> for SessionListPage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let page = SessionListPageWire::deserialize(deserializer)?;
        if page.sessions.len() > 200 {
            return Err(D::Error::custom(
                "session list page must contain at most 200 sessions",
            ));
        }
        if page
            .cursor
            .as_ref()
            .is_some_and(|cursor| !cursor.has_more || cursor.revision != page.revision)
        {
            return Err(D::Error::custom(
                "session list cursor must continue the page revision",
            ));
        }
        Ok(Self {
            kind: page.kind,
            protocol_version: page.protocol_version,
            request_id: page.request_id,
            controller_id: page.controller_id,
            revision: page.revision,
            sessions: page.sessions,
            cursor: page.cursor,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessagePart {
    Text {
        #[serde(deserialize_with = "deserialize_wire_text")]
        text: String,
    },
    Reasoning {
        #[serde(deserialize_with = "deserialize_wire_text")]
        text: String,
    },
    ToolRef {
        #[serde(rename = "toolCallId", deserialize_with = "deserialize_identifier")]
        tool_call_id: String,
    },
    AttachmentRef {
        #[serde(rename = "attachmentId", deserialize_with = "deserialize_identifier")]
        attachment_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageDescriptor {
    #[serde(deserialize_with = "deserialize_identifier")]
    pub message_id: String,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub parent_message_id: Option<String>,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub sequence: u64,
    pub role: MessageRole,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub created_at: String,
    #[serde(deserialize_with = "deserialize_optional_timestamp")]
    pub edited_at: Option<String>,
    pub parts: Vec<MessagePart>,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub content_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolState {
    Requested,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolDescriptor {
    #[serde(deserialize_with = "deserialize_identifier")]
    pub tool_call_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub message_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub name: String,
    pub state: ToolState,
    #[serde(deserialize_with = "deserialize_json_text")]
    pub arguments_json: String,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub arguments_hash: String,
    #[serde(deserialize_with = "deserialize_optional_json_text")]
    pub result_json: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_sha256")]
    pub result_hash: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_timestamp")]
    pub started_at: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_timestamp")]
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentAvailability {
    MetadataOnly,
    Available,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttachmentDescriptor {
    #[serde(deserialize_with = "deserialize_identifier")]
    pub attachment_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub message_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub file_name: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub media_type: String,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub byte_length: u64,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub content_hash: String,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    pub blob_id: Option<String>,
    pub availability: AttachmentAvailability,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HashReference {
    #[serde(deserialize_with = "deserialize_identifier")]
    pub id: String,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContentHashAlgorithm {
    #[serde(rename = "sha256")]
    Sha256,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ContentHashes {
    pub algorithm: ContentHashAlgorithm,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub session: String,
    pub messages: Vec<HashReference>,
    pub tools: Vec<HashReference>,
    pub attachments: Vec<HashReference>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferCursorKind {
    #[serde(rename = "session_transfer_cursor")]
    SessionTransferCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransferCursor {
    #[serde(rename = "type")]
    pub kind: TransferCursorKind,
    #[serde(deserialize_with = "deserialize_opaque_token")]
    pub token: String,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub revision: u64,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub after_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionReadRequestKind {
    #[serde(rename = "session_read_request")]
    SessionReadRequest,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionReadRequest {
    #[serde(rename = "type")]
    pub kind: SessionReadRequestKind,
    pub protocol_version: ProtocolVersion,
    pub auth: RequestAuth,
    pub session: Option<ExternalSessionIdentity>,
    pub cursor: Option<TransferCursor>,
    pub limit: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionReadRequestWire {
    #[serde(rename = "type")]
    kind: SessionReadRequestKind,
    protocol_version: ProtocolVersion,
    auth: RequestAuth,
    #[serde(deserialize_with = "deserialize_required_option")]
    session: Option<ExternalSessionIdentity>,
    #[serde(deserialize_with = "deserialize_required_option")]
    cursor: Option<TransferCursor>,
    #[serde(deserialize_with = "deserialize_session_limit")]
    limit: u16,
}

impl<'de> Deserialize<'de> for SessionReadRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let request = SessionReadRequestWire::deserialize(deserializer)?;
        if request.auth.capability != Capability::SessionsRead {
            return Err(D::Error::custom(
                "session read auth must require sessions.read",
            ));
        }
        if request.session.is_some() == request.cursor.is_some() {
            return Err(D::Error::custom(
                "provide a session for the first page or a cursor for continuation",
            ));
        }
        Ok(Self {
            kind: request.kind,
            protocol_version: request.protocol_version,
            auth: request.auth,
            session: request.session,
            cursor: request.cursor,
            limit: request.limit,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionPageKind {
    #[serde(rename = "session_page")]
    SessionPage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPage {
    #[serde(rename = "type")]
    pub kind: SessionPageKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub page_id: String,
    pub canonical_session: ExternalSessionIdentity,
    pub origin: SessionOrigin,
    pub metadata: SessionMetadata,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub revision: u64,
    pub messages: Vec<MessageDescriptor>,
    pub tools: Vec<ToolDescriptor>,
    pub attachments: Vec<AttachmentDescriptor>,
    pub content_hashes: ContentHashes,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cursor: Option<TransferCursor>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AgentTurnRequestKind {
    #[serde(rename = "agent_turn_request")]
    AgentTurnRequest,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentTurnRequest {
    #[serde(rename = "type")]
    pub kind: AgentTurnRequestKind,
    pub protocol_version: ProtocolVersion,
    pub auth: MutationAuth,
    pub session: ExternalSessionIdentity,
    pub expected_revision: u64,
    pub message_id: String,
    pub model_id: Option<String>,
    pub content: String,
    pub content_hash: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentTurnRequestWire {
    #[serde(rename = "type")]
    kind: AgentTurnRequestKind,
    protocol_version: ProtocolVersion,
    auth: MutationAuth,
    session: ExternalSessionIdentity,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    expected_revision: u64,
    #[serde(deserialize_with = "deserialize_identifier")]
    message_id: String,
    #[serde(deserialize_with = "deserialize_optional_identifier")]
    model_id: Option<String>,
    #[serde(deserialize_with = "deserialize_agent_turn_content")]
    content: String,
    #[serde(deserialize_with = "deserialize_sha256")]
    content_hash: String,
}

impl<'de> Deserialize<'de> for AgentTurnRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let request = AgentTurnRequestWire::deserialize(deserializer)?;
        if request.auth.capability != Capability::AgentTurn {
            return Err(D::Error::custom("agent turn auth must require agent.turn"));
        }
        Ok(Self {
            kind: request.kind,
            protocol_version: request.protocol_version,
            auth: request.auth,
            session: request.session,
            expected_revision: request.expected_revision,
            message_id: request.message_id,
            model_id: request.model_id,
            content: request.content,
            content_hash: request.content_hash,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AgentTurnAckKind {
    #[serde(rename = "agent_turn_ack")]
    AgentTurnAck,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AgentTurnOutcome {
    #[serde(rename = "accepted")]
    Accepted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentTurnAck {
    #[serde(rename = "type")]
    pub kind: AgentTurnAckKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub idempotency_key: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub dispatch_id: String,
    pub canonical_session: ExternalSessionIdentity,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub message_id: String,
    #[serde(deserialize_with = "deserialize_sha256")]
    pub content_hash: String,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub base_revision: u64,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub pi_session_id: String,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub model_id: String,
    pub outcome: AgentTurnOutcome,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub accepted_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictResultKind {
    #[serde(rename = "conflict")]
    Conflict,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictOperation {
    ControllerAction,
    SessionTransfer,
    AgentTurn,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictResolution {
    Retry,
    ForkRequired,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConflictResult {
    #[serde(rename = "type")]
    pub kind: ConflictResultKind,
    pub protocol_version: ProtocolVersion,
    #[serde(deserialize_with = "deserialize_identifier")]
    pub request_id: String,
    pub operation: ConflictOperation,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub expected_revision: u64,
    #[serde(deserialize_with = "deserialize_safe_u64")]
    pub current_revision: u64,
    pub resolution: ConflictResolution,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub canonical_session: Option<ExternalSessionIdentity>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cursor: Option<TransferCursor>,
    pub error: BridgeError,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum AgentTurnResult {
    Ack(AgentTurnAck),
    Conflict(ConflictResult),
    Error(ErrorResult),
}

fn deserialize_identifier<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_identifier(&value).map_err(D::Error::custom)?;
    Ok(value)
}

fn deserialize_optional_identifier<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_identifier(value).map_err(D::Error::custom)?;
    }
    Ok(value)
}

fn deserialize_identifiers<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<String>::deserialize(deserializer)?;
    for value in &values {
        validate_identifier(value).map_err(D::Error::custom)?;
    }
    Ok(values)
}

fn validate_identifier(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.trim() != value || value.len() > 512 {
        return Err("identifier must be non-empty, trimmed, and at most 512 bytes");
    }
    Ok(())
}

fn deserialize_short_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_short_text(&value).map_err(D::Error::custom)?;
    Ok(value)
}

fn deserialize_optional_short_text<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_short_text(value).map_err(D::Error::custom)?;
    }
    Ok(value)
}

fn validate_short_text(value: &str) -> Result<(), &'static str> {
    if value.len() > 4_096 {
        return Err("text exceeds 4096 byte limit");
    }
    Ok(())
}

fn deserialize_agent_turn_content<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(D::Error::custom("agent turn content must not be empty"));
    }
    // Effect's String max-length check follows JavaScript String.length, so
    // count UTF-16 code units here rather than UTF-8 bytes or Unicode scalars.
    if value
        .encode_utf16()
        .take(MAX_AGENT_TURN_CONTENT_LENGTH + 1)
        .count()
        > MAX_AGENT_TURN_CONTENT_LENGTH
    {
        return Err(D::Error::custom(
            "agent turn content exceeds the 100000 character limit",
        ));
    }
    Ok(value)
}

fn deserialize_wire_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.len() > 4_000_000 {
        return Err(D::Error::custom("wire text exceeds 4000000 byte limit"));
    }
    Ok(value)
}

fn deserialize_json_text<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_json_text(&value).map_err(D::Error::custom)?;
    Ok(value)
}

fn deserialize_optional_json_text<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_json_text(value).map_err(D::Error::custom)?;
    }
    Ok(value)
}

fn validate_json_text(value: &str) -> Result<(), &'static str> {
    if value.len() > 1_000_000 {
        return Err("JSON text exceeds 1000000 byte limit");
    }
    serde_json::from_str::<Value>(value)
        .map(|_| ())
        .map_err(|_| "invalid JSON text")
}

fn deserialize_opaque_token<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() || value.trim() != value || value.len() > 2_048 {
        return Err(D::Error::custom(
            "opaque token must be non-empty, trimmed, and at most 2048 bytes",
        ));
    }
    Ok(value)
}

fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if !is_protocol_timestamp(&value) {
        return Err(D::Error::custom(
            "timestamp must be UTC RFC3339 with Z suffix",
        ));
    }
    Ok(value)
}

fn deserialize_optional_timestamp<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if value
        .as_deref()
        .is_some_and(|value| !is_protocol_timestamp(value))
    {
        return Err(D::Error::custom(
            "timestamp must be UTC RFC3339 with Z suffix",
        ));
    }
    Ok(value)
}

fn is_protocol_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    let valid_length = bytes.len() == 20 || (22..=30).contains(&bytes.len());
    if !valid_length
        || bytes.last() != Some(&b'Z')
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
    }
    let whole_seconds_digits = [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    if !whole_seconds_digits
        .iter()
        .all(|index| bytes[*index].is_ascii_digit())
    {
        return false;
    }
    if bytes.len() == 20 {
        return bytes[19] == b'Z';
    }
    bytes[19] == b'.' && bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit)
}

fn deserialize_nonce<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_base64url(deserializer, 16, 512, "nonce")
}

fn deserialize_signature<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_base64url(deserializer, 43, 512, "signature")
}

fn deserialize_bounded_base64url<'de, D>(
    deserializer: D,
    minimum: usize,
    maximum: usize,
    label: &str,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if !(minimum..=maximum).contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(D::Error::custom(format!("invalid {label}")));
    }
    Ok(value)
}

fn deserialize_sha256<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_sha256(&value).map_err(D::Error::custom)?;
    Ok(value)
}

fn deserialize_optional_sha256<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_sha256(value).map_err(D::Error::custom)?;
    }
    Ok(value)
}

fn validate_sha256(value: &str) -> Result<(), &'static str> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("SHA-256 must be 64 lowercase hexadecimal digits");
    }
    Ok(())
}

fn deserialize_safe_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value > MAX_SAFE_JSON_INTEGER {
        return Err(D::Error::custom(
            "integer exceeds the JSON safe-integer range",
        ));
    }
    Ok(value)
}

fn deserialize_optional_safe_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u64>::deserialize(deserializer)?;
    if value.is_some_and(|value| value > MAX_SAFE_JSON_INTEGER) {
        return Err(D::Error::custom(
            "integer exceeds the JSON safe-integer range",
        ));
    }
    Ok(value)
}

fn deserialize_session_limit<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if !(1..=200).contains(&value) {
        return Err(D::Error::custom(
            "session page limit must be between 1 and 200",
        ));
    }
    Ok(value as u16)
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn deserialize_optional_nonnegative_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
        return Err(D::Error::custom("number must be finite and non-negative"));
    }
    Ok(value)
}

fn deserialize_optional_percentage<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<f64>::deserialize(deserializer)?;
    if value.is_some_and(|value| !value.is_finite() || !(0.0..=100.0).contains(&value)) {
        return Err(D::Error::custom("percentage must be between 0 and 100"));
    }
    Ok(value)
}

fn deserialize_optional_positive_u16<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u16>::deserialize(deserializer)?;
    if value == Some(0) {
        return Err(D::Error::custom("port must be positive"));
    }
    Ok(value)
}

fn deserialize_unique_capabilities<'de, D>(deserializer: D) -> Result<Vec<Capability>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_unique(deserializer, "capability")
}

fn deserialize_unique_actions<'de, D>(
    deserializer: D,
) -> Result<Vec<ControllerActionKind>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_unique(deserializer, "controller action")
}

fn deserialize_unique<'de, D, T>(deserializer: D, label: &str) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Eq + std::hash::Hash + Copy,
{
    let values = Vec::<T>::deserialize(deserializer)?;
    let mut seen = HashSet::with_capacity(values.len());
    for value in &values {
        if !seen.insert(*value) {
            return Err(D::Error::custom(format!("duplicate {label}")));
        }
    }
    Ok(values)
}

#[derive(Debug, Error)]
pub enum CanonicalJsonError {
    #[error("canonical JSON accepts integers only")]
    NonIntegerNumber,
    #[error("integer {value} is outside the JSON safe-integer range")]
    UnsafeInteger { value: String },
    #[error("request body must be a JSON object")]
    RequestBodyMustBeObject,
    #[error("signature field exceeds the u32 length-prefix limit")]
    SignatureFieldTooLong,
    #[error("serializing canonical JSON string: {0}")]
    SerializeString(#[from] serde_json::Error),
}

pub fn canonical_json(value: &Value) -> Result<Vec<u8>, CanonicalJsonError> {
    let mut output = Vec::new();
    write_canonical(value, &mut output)?;
    Ok(output)
}

pub fn canonical_json_sha256(value: &Value) -> Result<String, CanonicalJsonError> {
    let encoded = canonical_json(value)?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

pub fn canonical_request_body_sha256(request: &Value) -> Result<String, CanonicalJsonError> {
    let Value::Object(fields) = request else {
        return Err(CanonicalJsonError::RequestBodyMustBeObject);
    };
    let mut body = fields.clone();
    body.remove("auth");
    canonical_json_sha256(&Value::Object(body))
}

fn write_canonical(value: &Value, output: &mut Vec<u8>) -> Result<(), CanonicalJsonError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::String(value) => serde_json::to_writer(output, value)?,
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                if value.unsigned_abs() > MAX_SAFE_JSON_INTEGER {
                    return Err(CanonicalJsonError::UnsafeInteger {
                        value: number.to_string(),
                    });
                }
            } else if let Some(value) = number.as_u64() {
                if value > MAX_SAFE_JSON_INTEGER {
                    return Err(CanonicalJsonError::UnsafeInteger {
                        value: number.to_string(),
                    });
                }
            } else {
                return Err(CanonicalJsonError::NonIntegerNumber);
            }
            output.extend_from_slice(number.to_string().as_bytes());
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(fields) => {
            output.push(b'{');
            let mut entries = fields.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_canonical(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct SignaturePreimage<'a> {
    pub device_id: &'a str,
    pub key_id: &'a str,
    pub request_id: &'a str,
    pub issued_at: &'a str,
    pub expires_at: &'a str,
    pub nonce: &'a str,
    pub capability: Capability,
    pub idempotency_key: Option<&'a str>,
    pub body_hash: &'a str,
}

impl<'a> SignaturePreimage<'a> {
    pub fn from_request(auth: &'a RequestAuth) -> Self {
        Self {
            device_id: &auth.device.device_id,
            key_id: &auth.device.key_id,
            request_id: &auth.request_id,
            issued_at: &auth.issued_at,
            expires_at: &auth.expires_at,
            nonce: &auth.nonce,
            capability: auth.capability,
            idempotency_key: None,
            body_hash: &auth.body_hash,
        }
    }

    pub fn from_mutation(auth: &'a MutationAuth) -> Self {
        Self {
            device_id: &auth.device.device_id,
            key_id: &auth.device.key_id,
            request_id: &auth.request_id,
            issued_at: &auth.issued_at,
            expires_at: &auth.expires_at,
            nonce: &auth.nonce,
            capability: auth.capability,
            idempotency_key: Some(&auth.idempotency_key),
            body_hash: &auth.body_hash,
        }
    }
}

pub fn ed25519_signature_preimage(
    fields: SignaturePreimage<'_>,
) -> Result<Vec<u8>, CanonicalJsonError> {
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(SIGNATURE_DOMAIN);
    for field in [
        fields.device_id,
        fields.key_id,
        fields.request_id,
        fields.issued_at,
        fields.expires_at,
        fields.nonce,
        fields.capability.as_str(),
        fields.idempotency_key.unwrap_or(""),
        fields.body_hash,
    ] {
        let length =
            u32::try_from(field.len()).map_err(|_| CanonicalJsonError::SignatureFieldTooLong)?;
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(field.as_bytes());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn golden_agent_turn_request() -> Value {
        json!({
            "type": "agent_turn_request",
            "protocolVersion": 1,
            "auth": {
                "device": {
                    "deviceId": "a".repeat(64),
                    "keyId": "a".repeat(64),
                    "algorithm": "ed25519"
                },
                "requestId": "request-turn-golden",
                "issuedAt": "2026-07-20T18:29:50.000Z",
                "expiresAt": "2026-07-20T18:30:20.000Z",
                "nonce": "agent-turn-nonce-1234",
                "bodyHash": "6aa1290ac6b0e8cd86e0d4791f0f5c5ff69cac4a299a29d5ba06381bf8345ab5",
                "signature": "S".repeat(86),
                "capability": "agent.turn",
                "idempotencyKey": "idempotency-turn-golden"
            },
            "session": {
                "kind": "external_session",
                "authority": "local-studio",
                "installationId": "controller-test",
                "sessionId": "session-test"
            },
            "expectedRevision": 7,
            "messageId": "message-test",
            "modelId": "model-test",
            "content": "Continue from the phone",
            "contentHash": "32f130f835f3486ce0a366936d5cc5c32752f92084795766fee6278b7e5643d4"
        })
    }

    #[test]
    fn capabilities_and_actions_are_closed_and_strict() {
        assert!(serde_json::from_str::<Capability>(r#""stats.read""#).is_ok());
        assert!(serde_json::from_str::<Capability>(r#""controller.read""#).is_err());
        assert!(
            serde_json::from_value::<ControllerAction>(json!({
                "type": "start_recipe",
                "recipeId": "recipe-1"
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<ControllerAction>(json!({
                "type": "start_recipe",
                "recipeId": "recipe-1",
                "command": "arbitrary"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ControllerAction>(json!({
                "type": "restart_controller",
                "controllerId": "controller-1"
            }))
            .is_err()
        );
    }

    #[test]
    fn advertisement_rejects_duplicates_unknown_fields_and_wrong_versions() {
        let base = json!({
            "protocolVersion": 1,
            "bridgeId": "bridge-1",
            "controllerId": "controller-1",
            "issuedAt": "2026-07-20T12:00:00Z",
            "capabilities": ["stats.read"],
            "actions": []
        });
        assert!(serde_json::from_value::<LocalStudioAdvertisement>(base.clone()).is_ok());

        let mut duplicate = base.clone();
        duplicate["capabilities"] = json!(["stats.read", "stats.read"]);
        assert!(serde_json::from_value::<LocalStudioAdvertisement>(duplicate).is_err());

        let mut unknown = base.clone();
        unknown["token"] = json!("must-not-round-trip");
        assert!(serde_json::from_value::<LocalStudioAdvertisement>(unknown).is_err());

        let mut wrong_version = base;
        wrong_version["protocolVersion"] = json!(2);
        assert!(serde_json::from_value::<LocalStudioAdvertisement>(wrong_version).is_err());
    }

    #[test]
    fn auth_rejects_invalid_hashes_signatures_timestamps_and_extra_fields() {
        let base = json!({
            "device": {
                "deviceId": "device-1",
                "keyId": "key-1",
                "algorithm": "ed25519"
            },
            "requestId": "request-1",
            "issuedAt": "2026-07-20T12:00:00Z",
            "expiresAt": "2026-07-20T12:00:30Z",
            "nonce": "abcdefghijklmnop",
            "bodyHash": "a".repeat(64),
            "signature": "A".repeat(43),
            "capability": "stats.read"
        });
        assert!(serde_json::from_value::<RequestAuth>(base.clone()).is_ok());

        let mut invalid_hash = base.clone();
        invalid_hash["bodyHash"] = json!("A".repeat(64));
        assert!(serde_json::from_value::<RequestAuth>(invalid_hash).is_err());

        let mut invalid_timestamp = base.clone();
        invalid_timestamp["expiresAt"] = json!("2026-07-20T12:00:30+00:00");
        assert!(serde_json::from_value::<RequestAuth>(invalid_timestamp).is_err());

        let mut invalid_signature = base.clone();
        invalid_signature["signature"] = json!("padding=is-not-base64url");
        assert!(serde_json::from_value::<RequestAuth>(invalid_signature).is_err());

        let mut extra = base;
        extra["token"] = json!("secret");
        assert!(serde_json::from_value::<RequestAuth>(extra).is_err());
    }

    #[test]
    fn manifest_request_and_error_result_match_the_version_one_wire() {
        let manifest = json!({
            "type": "capabilities",
            "protocolVersion": 1,
            "bridgeId": "alleycat:controller-1",
            "controllerId": "controller-1",
            "issuedAt": "2026-07-20T12:00:00Z",
            "capabilities": ["stats.read"]
        });
        assert!(serde_json::from_value::<CapabilitiesManifest>(manifest.clone()).is_ok());
        let mut extra_manifest = manifest;
        extra_manifest["actions"] = json!(["start_recipe"]);
        assert!(serde_json::from_value::<CapabilitiesManifest>(extra_manifest).is_err());

        let request = json!({
            "type": "controller_snapshot_request",
            "protocolVersion": 1,
            "controllerId": "controller-1",
            "auth": {
                "device": {
                    "deviceId": "a".repeat(64),
                    "keyId": "a".repeat(64),
                    "algorithm": "ed25519"
                },
                "requestId": "request-1",
                "issuedAt": "2026-07-20T12:00:00Z",
                "expiresAt": "2026-07-20T12:00:30Z",
                "nonce": "abcdefghijklmnop",
                "bodyHash": "b".repeat(64),
                "signature": "A".repeat(86),
                "capability": "stats.read"
            }
        });
        assert!(serde_json::from_value::<ControllerSnapshotRequest>(request).is_ok());

        let error = json!({
            "type": "error",
            "protocolVersion": 1,
            "requestId": "request-1",
            "error": {
                "code": "capability_denied",
                "message": "denied",
                "retriable": false,
                "requestId": "request-1",
                "details": null
            }
        });
        assert!(serde_json::from_value::<ErrorResult>(error.clone()).is_ok());

        let mut unsafe_detail = error;
        unsafe_detail["error"]["details"] = json!({
            "field": null,
            "section": null,
            "expectedRevision": MAX_SAFE_JSON_INTEGER + 1,
            "currentRevision": null,
            "retryAfterMs": null,
            "limitBytes": null
        });
        assert!(serde_json::from_value::<ErrorResult>(unsafe_detail).is_err());
    }

    #[test]
    fn session_read_request_is_strict_bounded_and_requires_one_selector() {
        let auth = json!({
            "device": {
                "deviceId": "a".repeat(64),
                "keyId": "a".repeat(64),
                "algorithm": "ed25519"
            },
            "requestId": "request-read-1",
            "issuedAt": "2026-07-20T12:00:00Z",
            "expiresAt": "2026-07-20T12:00:30Z",
            "nonce": "nonce_read_1234567890",
            "bodyHash": "b".repeat(64),
            "signature": "A".repeat(86),
            "capability": "sessions.read"
        });
        let session = json!({
            "kind": "external_session",
            "authority": "local-studio",
            "installationId": "controller-1",
            "sessionId": "session-1"
        });
        let cursor = json!({
            "type": "session_transfer_cursor",
            "token": "cursor-token-1",
            "revision": 4,
            "afterSequence": 8,
            "hasMore": true
        });
        let first = json!({
            "type": "session_read_request",
            "protocolVersion": 1,
            "auth": auth,
            "session": session,
            "cursor": null,
            "limit": 50
        });
        let parsed = serde_json::from_value::<SessionReadRequest>(first.clone()).unwrap();
        assert_eq!(parsed.limit, 50);
        assert!(parsed.session.is_some());

        let mut continuation = first.clone();
        continuation["session"] = Value::Null;
        continuation["cursor"] = cursor.clone();
        assert!(serde_json::from_value::<SessionReadRequest>(continuation).is_ok());

        let mut neither = first.clone();
        neither["session"] = Value::Null;
        assert!(serde_json::from_value::<SessionReadRequest>(neither).is_err());

        let mut both = first.clone();
        both["cursor"] = cursor.clone();
        assert!(serde_json::from_value::<SessionReadRequest>(both).is_err());

        for invalid_limit in [0, 201] {
            let mut invalid = first.clone();
            invalid["limit"] = json!(invalid_limit);
            assert!(serde_json::from_value::<SessionReadRequest>(invalid).is_err());
        }

        let mut wrong_capability = first.clone();
        wrong_capability["auth"]["capability"] = json!("stats.read");
        assert!(serde_json::from_value::<SessionReadRequest>(wrong_capability).is_err());

        let mut unknown = first.clone();
        unknown["path"] = json!("/private/session.jsonl");
        assert!(serde_json::from_value::<SessionReadRequest>(unknown).is_err());

        let mut unsafe_cursor = first;
        unsafe_cursor["session"] = Value::Null;
        unsafe_cursor["cursor"] = cursor;
        unsafe_cursor["cursor"]["revision"] = json!(MAX_SAFE_JSON_INTEGER + 1);
        assert!(serde_json::from_value::<SessionReadRequest>(unsafe_cursor).is_err());
    }

    #[test]
    fn session_list_wire_is_strict_bounded_and_revision_bound() {
        let auth = json!({
            "device": {
                "deviceId": "a".repeat(64),
                "keyId": "a".repeat(64),
                "algorithm": "ed25519"
            },
            "requestId": "request-list-1",
            "issuedAt": "2026-07-20T12:00:00Z",
            "expiresAt": "2026-07-20T12:00:30Z",
            "nonce": "nonce_list_1234567890",
            "bodyHash": "b".repeat(64),
            "signature": "A".repeat(86),
            "capability": "sessions.read"
        });
        let first = json!({
            "type": "session_list_request",
            "protocolVersion": 1,
            "auth": auth,
            "cursor": null,
            "limit": 50
        });
        let parsed = serde_json::from_value::<SessionListRequest>(first.clone()).unwrap();
        assert_eq!(parsed.limit, 50);
        assert!(parsed.cursor.is_none());

        let cursor = json!({
            "type": "session_list_cursor",
            "token": "cursor-token-1",
            "revision": 4,
            "hasMore": true
        });
        let mut continuation = first.clone();
        continuation["cursor"] = cursor.clone();
        assert!(serde_json::from_value::<SessionListRequest>(continuation).is_ok());

        let mut exhausted = first.clone();
        exhausted["cursor"] = cursor.clone();
        exhausted["cursor"]["hasMore"] = json!(false);
        assert!(serde_json::from_value::<SessionListRequest>(exhausted).is_err());

        for invalid_limit in [0, 201] {
            let mut invalid = first.clone();
            invalid["limit"] = json!(invalid_limit);
            assert!(serde_json::from_value::<SessionListRequest>(invalid).is_err());
        }
        let mut wrong_capability = first.clone();
        wrong_capability["auth"]["capability"] = json!("stats.read");
        assert!(serde_json::from_value::<SessionListRequest>(wrong_capability).is_err());
        let mut missing_cursor = first.clone();
        missing_cursor.as_object_mut().unwrap().remove("cursor");
        assert!(serde_json::from_value::<SessionListRequest>(missing_cursor).is_err());
        let mut unknown = first;
        unknown["path"] = json!("/private/sessions");
        assert!(serde_json::from_value::<SessionListRequest>(unknown).is_err());

        let descriptor = json!({
            "session": {
                "kind": "external_session",
                "authority": "local-studio",
                "installationId": "controller-1",
                "sessionId": "session-1"
            },
            "metadata": {
                "title": "A session",
                "cwd": "/tmp/project",
                "createdAt": "2026-07-20T11:00:00Z",
                "updatedAt": "2026-07-20T12:00:00Z",
                "modelId": "model-1",
                "providerId": null
            },
            "revision": 4,
            "archived": false,
            "active": true
        });
        let page = json!({
            "type": "session_list_page",
            "protocolVersion": 1,
            "requestId": "request-list-1",
            "controllerId": "controller-1",
            "revision": 4,
            "sessions": [descriptor],
            "cursor": cursor
        });
        let parsed = serde_json::from_value::<SessionListPage>(page.clone()).unwrap();
        assert_eq!(parsed.sessions.len(), 1);

        let mut wrong_revision = page.clone();
        wrong_revision["cursor"]["revision"] = json!(3);
        assert!(serde_json::from_value::<SessionListPage>(wrong_revision).is_err());
        let mut false_cursor = page.clone();
        false_cursor["cursor"]["hasMore"] = json!(false);
        assert!(serde_json::from_value::<SessionListPage>(false_cursor).is_err());
        let mut leaked = page.clone();
        leaked["sessions"][0]["sourcePath"] = json!("/private/session.jsonl");
        assert!(serde_json::from_value::<SessionListPage>(leaked).is_err());
        let mut too_many = page;
        too_many["cursor"] = Value::Null;
        too_many["sessions"] = Value::Array(vec![too_many["sessions"][0].clone(); 201]);
        assert!(serde_json::from_value::<SessionListPage>(too_many).is_err());
    }

    #[test]
    fn session_page_mirrors_bounded_identity_message_tool_and_attachment_wire() {
        let hash = "a".repeat(64);
        let page = json!({
            "type": "session_page",
            "protocolVersion": 1,
            "requestId": "request-read-1",
            "pageId": "page-1",
            "canonicalSession": {
                "kind": "external_session",
                "authority": "local-studio",
                "installationId": "controller-1",
                "sessionId": "session-1"
            },
            "origin": {
                "application": "local-studio",
                "installationId": "controller-1",
                "deviceId": null,
                "exportedAt": "2026-07-20T12:00:00Z"
            },
            "metadata": {
                "title": "A session",
                "cwd": "/tmp/project",
                "createdAt": "2026-07-20T11:00:00Z",
                "updatedAt": "2026-07-20T12:00:00Z",
                "modelId": "model-1",
                "providerId": null
            },
            "revision": 4,
            "messages": [{
                "messageId": "message-1",
                "parentMessageId": null,
                "sequence": 0,
                "role": "assistant",
                "createdAt": "2026-07-20T11:30:00Z",
                "editedAt": null,
                "parts": [
                    {"type": "reasoning", "text": "checking"},
                    {"type": "text", "text": "done"},
                    {"type": "tool_ref", "toolCallId": "tool-1"},
                    {"type": "attachment_ref", "attachmentId": "attachment-1"}
                ],
                "contentHash": hash
            }],
            "tools": [{
                "toolCallId": "tool-1",
                "messageId": "message-1",
                "name": "read",
                "state": "completed",
                "argumentsJson": "{\"path\":\"README.md\"}",
                "argumentsHash": hash,
                "resultJson": "{\"ok\":true}",
                "resultHash": hash,
                "startedAt": "2026-07-20T11:30:01Z",
                "completedAt": "2026-07-20T11:30:02Z"
            }],
            "attachments": [{
                "attachmentId": "attachment-1",
                "messageId": "message-1",
                "fileName": "result.txt",
                "mediaType": "text/plain",
                "byteLength": 4,
                "contentHash": hash,
                "blobId": null,
                "availability": "metadata_only"
            }],
            "contentHashes": {
                "algorithm": "sha256",
                "session": hash,
                "messages": [{"id": "message-1", "sha256": hash}],
                "tools": [{"id": "tool-1", "sha256": hash}],
                "attachments": [{"id": "attachment-1", "sha256": hash}]
            },
            "cursor": null
        });
        let parsed = serde_json::from_value::<SessionPage>(page.clone()).unwrap();
        assert_eq!(parsed.messages.len(), 1);
        assert_eq!(parsed.tools.len(), 1);
        assert_eq!(parsed.attachments.len(), 1);

        let mut invalid_json = page.clone();
        invalid_json["tools"][0]["argumentsJson"] = json!("not-json");
        assert!(serde_json::from_value::<SessionPage>(invalid_json).is_err());

        let mut unsafe_length = page.clone();
        unsafe_length["attachments"][0]["byteLength"] = json!(MAX_SAFE_JSON_INTEGER + 1);
        assert!(serde_json::from_value::<SessionPage>(unsafe_length).is_err());

        let mut leaked_path = page.clone();
        leaked_path["messages"][0]["sourcePath"] = json!("/private/session.jsonl");
        assert!(serde_json::from_value::<SessionPage>(leaked_path).is_err());

        let mut missing_required_null = page;
        missing_required_null["metadata"]
            .as_object_mut()
            .unwrap()
            .remove("providerId");
        assert!(serde_json::from_value::<SessionPage>(missing_required_null).is_err());
    }

    #[test]
    fn agent_turn_request_matches_local_studio_serde_and_signature_golden() {
        // These hashes were generated by Local Studio d2114ea4 using
        // litterBridgeSha256Utf8, litterBridgeBodyHash, and
        // litterBridgeSignaturePreimage.
        let golden = golden_agent_turn_request();
        let request = serde_json::from_value::<AgentTurnRequest>(golden.clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), golden);
        assert_eq!(request.auth.capability, Capability::AgentTurn);
        assert_eq!(
            hex::encode(Sha256::digest(request.content.as_bytes())),
            request.content_hash
        );
        assert_eq!(
            canonical_request_body_sha256(&golden).unwrap(),
            "6aa1290ac6b0e8cd86e0d4791f0f5c5ff69cac4a299a29d5ba06381bf8345ab5"
        );
        let preimage =
            ed25519_signature_preimage(SignaturePreimage::from_mutation(&request.auth)).unwrap();
        assert_eq!(
            hex::encode(Sha256::digest(preimage)),
            "ec98f0280915153819b6b3697c37ced34e58aa65e34e5fbb3cd067331107be5b"
        );
    }

    #[test]
    fn agent_turn_request_is_closed_capability_bound_and_uses_js_length_limit() {
        let golden = golden_agent_turn_request();

        let mut wrong_capability = golden.clone();
        wrong_capability["auth"]["capability"] = json!("sessions.write");
        assert!(serde_json::from_value::<AgentTurnRequest>(wrong_capability).is_err());

        let mut unknown = golden.clone();
        unknown["operation"] = json!("steer");
        assert!(serde_json::from_value::<AgentTurnRequest>(unknown).is_err());

        let mut missing_model = golden.clone();
        missing_model.as_object_mut().unwrap().remove("modelId");
        assert!(serde_json::from_value::<AgentTurnRequest>(missing_model).is_err());

        let mut empty = golden.clone();
        empty["content"] = json!("");
        assert!(serde_json::from_value::<AgentTurnRequest>(empty).is_err());

        let mut exact_ascii = golden.clone();
        exact_ascii["content"] = json!("x".repeat(MAX_AGENT_TURN_CONTENT_LENGTH));
        assert!(serde_json::from_value::<AgentTurnRequest>(exact_ascii).is_ok());

        let mut over_ascii = golden.clone();
        over_ascii["content"] = json!("x".repeat(MAX_AGENT_TURN_CONTENT_LENGTH + 1));
        assert!(serde_json::from_value::<AgentTurnRequest>(over_ascii).is_err());

        let mut exact_utf16 = golden.clone();
        exact_utf16["content"] = json!("😀".repeat(MAX_AGENT_TURN_CONTENT_LENGTH / 2));
        assert!(serde_json::from_value::<AgentTurnRequest>(exact_utf16).is_ok());
        let mut over_utf16 = golden;
        over_utf16["content"] = json!("😀".repeat(MAX_AGENT_TURN_CONTENT_LENGTH / 2 + 1));
        assert!(serde_json::from_value::<AgentTurnRequest>(over_utf16).is_err());
    }

    #[test]
    fn agent_turn_result_strictly_distinguishes_ack_conflict_and_error() {
        let hash = "3".repeat(64);
        let session = json!({
            "kind": "external_session",
            "authority": "local-studio",
            "installationId": "controller-test",
            "sessionId": "session-test"
        });
        let ack = json!({
            "type": "agent_turn_ack",
            "protocolVersion": 1,
            "requestId": "request-turn-1",
            "idempotencyKey": "idempotency-turn-1",
            "dispatchId": "dispatch-turn-1",
            "canonicalSession": session,
            "messageId": "message-turn-1",
            "contentHash": hash,
            "baseRevision": 7,
            "piSessionId": "session-test",
            "modelId": "model-test",
            "outcome": "accepted",
            "acceptedAt": "2026-07-20T18:30:00.000Z"
        });
        assert!(matches!(
            serde_json::from_value::<AgentTurnResult>(ack.clone()).unwrap(),
            AgentTurnResult::Ack(_)
        ));
        let mut extra_ack = ack;
        extra_ack["steerable"] = json!(true);
        assert!(serde_json::from_value::<AgentTurnResult>(extra_ack).is_err());

        let conflict = json!({
            "type": "conflict",
            "protocolVersion": 1,
            "requestId": "request-turn-1",
            "operation": "agent_turn",
            "expectedRevision": 7,
            "currentRevision": 8,
            "resolution": "retry",
            "canonicalSession": session,
            "cursor": null,
            "error": {
                "code": "revision_conflict",
                "message": "Session changed before the prompt was accepted",
                "retriable": true,
                "requestId": "request-turn-1",
                "details": {
                    "field": null,
                    "section": null,
                    "expectedRevision": 7,
                    "currentRevision": 8,
                    "retryAfterMs": null,
                    "limitBytes": null
                }
            }
        });
        assert!(matches!(
            serde_json::from_value::<AgentTurnResult>(conflict.clone()).unwrap(),
            AgentTurnResult::Conflict(_)
        ));
        let mut invalid_conflict = conflict;
        invalid_conflict["operation"] = json!("agent_steer");
        assert!(serde_json::from_value::<AgentTurnResult>(invalid_conflict).is_err());

        let error = json!({
            "type": "error",
            "protocolVersion": 1,
            "requestId": "request-turn-1",
            "error": {
                "code": "agent_runtime_unavailable",
                "message": "Prompt preflight was rejected",
                "retriable": true,
                "requestId": "request-turn-1",
                "details": null
            }
        });
        assert!(matches!(
            serde_json::from_value::<AgentTurnResult>(error).unwrap(),
            AgentTurnResult::Error(_)
        ));
    }

    #[test]
    fn degraded_controller_snapshot_preserves_partial_sections_and_is_strict() {
        fn freshness(observed_at: Option<&str>) -> Value {
            json!({
                "observedAt": observed_at,
                "ageMs": observed_at.map(|_| 10),
                "maxAgeMs": 15_000,
                "stale": observed_at.is_none(),
                "sourceRevision": observed_at.map(|_| 7)
            })
        }

        fn failed_section(section: &str) -> Value {
            json!({
                "value": null,
                "error": {
                    "code": "section_unavailable",
                    "message": "section unavailable",
                    "retriable": true,
                    "requestId": null,
                    "details": {
                        "field": null,
                        "section": section,
                        "expectedRevision": null,
                        "currentRevision": null,
                        "retryAfterMs": null,
                        "limitBytes": null
                    }
                },
                "freshness": freshness(None)
            })
        }

        let snapshot = json!({
            "type": "controller_snapshot",
            "protocolVersion": 1,
            "snapshotId": "snapshot-1",
            "controllerId": "controller-1",
            "displayName": "Local Studio",
            "generatedAt": "2026-07-20T12:00:00Z",
            "revision": 7,
            "state": "degraded",
            "capabilities": ["stats.read"],
            "sections": {
                "health": {
                    "value": {
                        "state": "ok",
                        "reachable": true,
                        "checkedAt": "2026-07-20T12:00:00Z",
                        "latencyMs": 3.5,
                        "controllerVersion": "2.1.0"
                    },
                    "error": null,
                    "freshness": freshness(Some("2026-07-20T12:00:00Z"))
                },
                "status": failed_section("status"),
                "gpus": failed_section("gpus"),
                "metrics": failed_section("metrics"),
                "agentRuntime": failed_section("agent-runtime")
            }
        });
        let parsed = serde_json::from_value::<ControllerSnapshot>(snapshot.clone()).unwrap();
        assert_eq!(parsed.state, SnapshotState::Degraded);
        assert!(parsed.sections.health.value.is_some());
        assert!(parsed.sections.metrics.error.is_some());

        let mut leaked = snapshot.clone();
        leaked["controllerToken"] = json!("must-not-cross-wire");
        assert!(serde_json::from_value::<ControllerSnapshot>(leaked).is_err());

        let mut invalid_metric = snapshot.clone();
        invalid_metric["sections"]["health"]["value"]["latencyMs"] = json!(-1);
        assert!(serde_json::from_value::<ControllerSnapshot>(invalid_metric).is_err());

        let mut missing_required_null = snapshot;
        missing_required_null["sections"]["metrics"]
            .as_object_mut()
            .unwrap()
            .remove("value");
        assert!(serde_json::from_value::<ControllerSnapshot>(missing_required_null).is_err());
    }

    #[test]
    fn canonical_json_sorts_every_object_and_preserves_array_order() {
        let value = json!({
            "z": {"b": 2, "a": 1},
            "a": [3, {"d": false, "c": null}],
            "text": "line\nvalue"
        });
        let encoded = canonical_json(&value).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            r#"{"a":[3,{"c":null,"d":false}],"text":"line\nvalue","z":{"a":1,"b":2}}"#
        );
        assert_eq!(
            canonical_json_sha256(&value).unwrap(),
            "ea8387b29f85f8ecb82002adffdcdd5fb31950c51e2feadb5cb9f09b0c00e8ff"
        );
    }

    #[test]
    fn canonical_json_rejects_floats_and_unsafe_integers() {
        assert!(matches!(
            canonical_json(&json!(1.5)),
            Err(CanonicalJsonError::NonIntegerNumber)
        ));
        assert!(matches!(
            canonical_json(&json!(9_007_199_254_740_992_u64)),
            Err(CanonicalJsonError::UnsafeInteger { .. })
        ));
        assert!(matches!(
            canonical_json(&json!(-9_007_199_254_740_992_i64)),
            Err(CanonicalJsonError::UnsafeInteger { .. })
        ));
    }

    #[test]
    fn request_hash_omits_only_top_level_auth() {
        let request = json!({
            "type": "controller_snapshot_request",
            "auth": {"signature": "not-hashed"},
            "payload": {"auth": "ordinary-body-field"}
        });
        assert_eq!(
            canonical_request_body_sha256(&request).unwrap(),
            canonical_json_sha256(&json!({
                "payload": {"auth": "ordinary-body-field"},
                "type": "controller_snapshot_request"
            }))
            .unwrap()
        );
    }

    #[test]
    fn signature_preimage_is_domain_then_nine_length_prefixed_fields() {
        let fields = SignaturePreimage {
            device_id: "d",
            key_id: "key",
            request_id: "r",
            issued_at: "i",
            expires_at: "e",
            nonce: "n",
            capability: Capability::ModelsControl,
            idempotency_key: Some("idem"),
            body_hash: "hash",
        };
        let encoded = ed25519_signature_preimage(fields).unwrap();
        let mut expected = SIGNATURE_DOMAIN.to_vec();
        for field in [
            "d",
            "key",
            "r",
            "i",
            "e",
            "n",
            "models.control",
            "idem",
            "hash",
        ] {
            expected.extend_from_slice(&(field.len() as u32).to_be_bytes());
            expected.extend_from_slice(field.as_bytes());
        }
        assert_eq!(encoded, expected);
    }

    #[test]
    fn read_signature_preimage_uses_empty_idempotency_field() {
        let auth = RequestAuth {
            device: DeviceAuth {
                device_id: "device".into(),
                key_id: "key".into(),
                algorithm: SignatureAlgorithm::Ed25519,
            },
            request_id: "request".into(),
            issued_at: "issued".into(),
            expires_at: "expires".into(),
            nonce: "nonce".into(),
            body_hash: "hash".into(),
            signature: "signature".into(),
            capability: Capability::StatsRead,
        };
        let encoded = ed25519_signature_preimage(SignaturePreimage::from_request(&auth)).unwrap();
        let empty_length = 0_u32.to_be_bytes();
        let needle = [
            10_u32.to_be_bytes().as_slice(),
            b"stats.read",
            empty_length.as_slice(),
            4_u32.to_be_bytes().as_slice(),
            b"hash",
        ]
        .concat();
        assert!(encoded.windows(needle.len()).any(|window| window == needle));
    }
}
