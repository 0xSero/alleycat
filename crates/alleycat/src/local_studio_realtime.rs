//! Consumer-side types for Local Studio's `realtime.session` v1 contract.
//!
//! Keeping these types separate from Alleycat's public wire protocol is
//! deliberate: this establishes a strict, fixture-backed contract without
//! advertising or forwarding realtime traffic before the transport policy is
//! implemented.

#![allow(dead_code)]

use std::collections::HashSet;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const PROTOCOL_VERSION: u32 = 1;
const REALTIME_CONTRACT_VERSION: u32 = 1;
const IDENTIFIER_MAX_UTF16_UNITS: usize = 512;
const SHORT_TEXT_MAX_UTF16_UNITS: usize = 4_096;
const WIRE_TEXT_MAX_UTF16_UNITS: usize = 4_000_000;
const OPAQUE_TOKEN_MAX_UTF16_UNITS: usize = 2_048;

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn validate_identifier(value: &str) -> Result<(), &'static str> {
    if value.is_empty() {
        return Err("identifier must not be empty");
    }
    if value.trim() != value {
        return Err("identifier must be trimmed");
    }
    if utf16_len(value) > IDENTIFIER_MAX_UTF16_UNITS {
        return Err("identifier exceeds 512 UTF-16 code units");
    }
    Ok(())
}

fn validate_short_text(value: &str) -> Result<(), &'static str> {
    if utf16_len(value) > SHORT_TEXT_MAX_UTF16_UNITS {
        return Err("short text exceeds 4096 UTF-16 code units");
    }
    Ok(())
}

fn validate_wire_text(value: &str) -> Result<(), &'static str> {
    if utf16_len(value) > WIRE_TEXT_MAX_UTF16_UNITS {
        return Err("wire text exceeds 4000000 UTF-16 code units");
    }
    Ok(())
}

fn validate_opaque_token(value: &str) -> Result<(), &'static str> {
    if value.is_empty() {
        return Err("opaque token must not be empty");
    }
    if value.trim() != value {
        return Err("opaque token must be trimmed");
    }
    if utf16_len(value) > OPAQUE_TOKEN_MAX_UTF16_UNITS {
        return Err("opaque token exceeds 2048 UTF-16 code units");
    }
    Ok(())
}

fn validate_timestamp(value: &str) -> Result<(), &'static str> {
    let bytes = value.as_bytes();
    let base_matches = bytes.len() >= 20
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
        && bytes[10] == b'T'
        && bytes[11..13].iter().all(u8::is_ascii_digit)
        && bytes[13] == b':'
        && bytes[14..16].iter().all(u8::is_ascii_digit)
        && bytes[16] == b':'
        && bytes[17..19].iter().all(u8::is_ascii_digit);
    let suffix_matches = match bytes.len() {
        20 => bytes[19] == b'Z',
        22..=30 => {
            bytes[19] == b'.'
                && bytes[20..bytes.len() - 1].iter().all(u8::is_ascii_digit)
                && bytes[bytes.len() - 1] == b'Z'
        }
        _ => false,
    };
    if base_matches && suffix_matches {
        Ok(())
    } else {
        Err("timestamp must use the realtime contract UTC wire format")
    }
}

fn validate_nonce(value: &str) -> Result<(), &'static str> {
    if value.len() < 16 || value.len() > 512 {
        return Err("nonce length must be between 16 and 512 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err("nonce must be unpadded base64url text");
    }
    Ok(())
}

fn validate_signature(value: &str) -> Result<(), &'static str> {
    if value.len() < 43 || value.len() > 512 {
        return Err("signature length must be between 43 and 512 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err("signature must be unpadded base64url text");
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), &'static str> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err("SHA-256 value must be 64 lowercase hexadecimal characters")
    }
}

macro_rules! validated_string {
    ($name:ident, $validate:ident) => {
        #[derive(Clone, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub(crate) struct $name(String);

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                $validate(&value).map_err(de::Error::custom)?;
                Ok(Self(value))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl $name {
            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

macro_rules! sensitive_string {
    ($name:ident, $validate:ident) => {
        #[derive(Clone, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub(crate) struct $name(String);

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                $validate(&value).map_err(de::Error::custom)?;
                Ok(Self(value))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("[REDACTED]")
            }
        }

        impl $name {
            pub(crate) fn expose_secret(&self) -> &str {
                &self.0
            }
        }
    };
}

validated_string!(Identifier, validate_identifier);
validated_string!(ShortText, validate_short_text);
validated_string!(Timestamp, validate_timestamp);
validated_string!(Nonce, validate_nonce);
validated_string!(ContentHash, validate_sha256);
sensitive_string!(Signature, validate_signature);
sensitive_string!(OpaqueToken, validate_opaque_token);
sensitive_string!(SensitiveWireText, validate_wire_text);

macro_rules! literal_u32 {
    ($name:ident, $expected:expr, $label:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) struct $name;

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_u32($expected)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = u32::deserialize(deserializer)?;
                if value == $expected {
                    Ok(Self)
                } else {
                    Err(de::Error::custom(concat!("unsupported ", $label)))
                }
            }
        }
    };
}

macro_rules! literal_string {
    ($name:ident, $expected:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) struct $name;

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str($expected)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                if value == $expected {
                    Ok(Self)
                } else {
                    Err(de::Error::custom(concat!("expected literal ", $expected)))
                }
            }
        }
    };
}

literal_u32!(ProtocolVersion, PROTOCOL_VERSION, "bridge protocol version");
literal_u32!(
    RealtimeContractVersion,
    REALTIME_CONTRACT_VERSION,
    "realtime contract version"
);
literal_string!(Ed25519Algorithm, "ed25519");
literal_string!(RealtimeSessionCapability, "realtime.session");
literal_string!(CapabilitiesRequestType, "realtime_capabilities_request");
literal_string!(CapabilitiesResultType, "realtime_capabilities");
literal_string!(SessionCreateRequestType, "realtime_session_create_request");
literal_string!(SessionCreateResultType, "realtime_session_created");
literal_string!(OfferType, "webrtc_offer");
literal_string!(AnswerType, "webrtc_answer");
literal_string!(SignalRequestType, "realtime_signal_request");
literal_string!(SessionUpdateRequestType, "realtime_session_update_request");
literal_string!(SessionCloseRequestType, "realtime_session_close_request");
literal_string!(SessionStatusType, "realtime_session_status");
literal_string!(MutationAckType, "realtime_mutation_ack");
literal_string!(ErrorResultType, "error");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct PositiveInteger(u64);

impl<'de> Deserialize<'de> for PositiveInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(de::Error::custom("expected a positive integer"))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct NonNegativeNumber(f64);

impl<'de> Deserialize<'de> for NonNegativeNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(de::Error::custom("expected a finite non-negative number"))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DeviceAuth {
    pub(crate) device_id: Identifier,
    pub(crate) key_id: Identifier,
    pub(crate) algorithm: Ed25519Algorithm,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeReadAuth {
    pub(crate) device: DeviceAuth,
    pub(crate) request_id: Identifier,
    pub(crate) issued_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) nonce: Nonce,
    pub(crate) body_hash: ContentHash,
    pub(crate) signature: Signature,
    pub(crate) capability: RealtimeSessionCapability,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeMutationAuth {
    pub(crate) device: DeviceAuth,
    pub(crate) request_id: Identifier,
    pub(crate) issued_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) nonce: Nonce,
    pub(crate) body_hash: ContentHash,
    pub(crate) signature: Signature,
    pub(crate) capability: RealtimeSessionCapability,
    pub(crate) idempotency_key: Identifier,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RealtimeProvider {
    ProviderNative,
    LocalPipeline,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RealtimeModality {
    Audio,
    Text,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RealtimeSignaling {
    WebrtcOfferAnswer,
    LocalWebsocket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RealtimeSessionState {
    Creating,
    Negotiating,
    Active,
    Reconnecting,
    Closing,
    Closed,
    Expired,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RealtimeUnavailableReason {
    ProviderNotConfigured,
    ModelNotLoaded,
    ModelUnsupported,
    SpeechPluginUnavailable,
    RuntimeUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeVoice {
    pub(crate) id: Identifier,
    pub(crate) label: Identifier,
}

fn deserialize_unique_modalities<'de, D>(deserializer: D) -> Result<Vec<RealtimeModality>, D::Error>
where
    D: Deserializer<'de>,
{
    let modalities = Vec::<RealtimeModality>::deserialize(deserializer)?;
    let unique = modalities.iter().copied().collect::<HashSet<_>>();
    if unique.len() == modalities.len() {
        Ok(modalities)
    } else {
        Err(de::Error::custom("realtime modalities must be unique"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RealtimeCapability {
    pub(crate) capability_id: Identifier,
    pub(crate) provider: RealtimeProvider,
    pub(crate) model_id: Identifier,
    pub(crate) available: bool,
    pub(crate) unavailable_reason: Option<RealtimeUnavailableReason>,
    pub(crate) input_modalities: Vec<RealtimeModality>,
    pub(crate) output_modalities: Vec<RealtimeModality>,
    pub(crate) signaling: RealtimeSignaling,
    pub(crate) voices: Vec<RealtimeVoice>,
    pub(crate) supports_reconnect: bool,
    pub(crate) supports_update: bool,
    pub(crate) session_ttl_seconds: PositiveInteger,
    pub(crate) max_signal_bytes: PositiveInteger,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RealtimeCapabilityWire {
    capability_id: Identifier,
    provider: RealtimeProvider,
    model_id: Identifier,
    available: bool,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    unavailable_reason: Option<RealtimeUnavailableReason>,
    #[serde(deserialize_with = "deserialize_unique_modalities")]
    input_modalities: Vec<RealtimeModality>,
    #[serde(deserialize_with = "deserialize_unique_modalities")]
    output_modalities: Vec<RealtimeModality>,
    signaling: RealtimeSignaling,
    voices: Vec<RealtimeVoice>,
    supports_reconnect: bool,
    supports_update: bool,
    session_ttl_seconds: PositiveInteger,
    max_signal_bytes: PositiveInteger,
}

impl<'de> Deserialize<'de> for RealtimeCapability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = RealtimeCapabilityWire::deserialize(deserializer)?;
        if value.available != value.unavailable_reason.is_none() {
            return Err(de::Error::custom(
                "availability and unavailableReason must agree",
            ));
        }
        Ok(Self {
            capability_id: value.capability_id,
            provider: value.provider,
            model_id: value.model_id,
            available: value.available,
            unavailable_reason: value.unavailable_reason,
            input_modalities: value.input_modalities,
            output_modalities: value.output_modalities,
            signaling: value.signaling,
            voices: value.voices,
            supports_reconnect: value.supports_reconnect,
            supports_update: value.supports_update,
            session_ttl_seconds: value.session_ttl_seconds,
            max_signal_bytes: value.max_signal_bytes,
        })
    }
}

fn deserialize_accepted_versions<'de, D>(
    deserializer: D,
) -> Result<Vec<RealtimeContractVersion>, D::Error>
where
    D: Deserializer<'de>,
{
    let versions = Vec::<RealtimeContractVersion>::deserialize(deserializer)?;
    if versions.len() == 1 {
        Ok(versions)
    } else {
        Err(de::Error::custom(
            "acceptedContractVersions must contain exactly one unique supported version",
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeCapabilitiesRequest {
    #[serde(rename = "type")]
    pub(crate) kind: CapabilitiesRequestType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) auth: RealtimeReadAuth,
    pub(crate) controller_id: Identifier,
    #[serde(deserialize_with = "deserialize_accepted_versions")]
    pub(crate) accepted_contract_versions: Vec<RealtimeContractVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeCapabilitiesResult {
    #[serde(rename = "type")]
    pub(crate) kind: CapabilitiesResultType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) request_id: Identifier,
    pub(crate) controller_id: Identifier,
    pub(crate) generated_at: Timestamp,
    pub(crate) capabilities: Vec<RealtimeCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeOffer {
    #[serde(rename = "type")]
    pub(crate) kind: OfferType,
    pub(crate) sdp: SensitiveWireText,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeAnswer {
    #[serde(rename = "type")]
    pub(crate) kind: AnswerType,
    pub(crate) sdp: SensitiveWireText,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSession {
    pub(crate) session_id: Identifier,
    pub(crate) client_session_id: Identifier,
    pub(crate) capability_id: Identifier,
    pub(crate) device_id: Identifier,
    pub(crate) state: RealtimeSessionState,
    pub(crate) created_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) reconnect_token: Option<OpaqueToken>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSessionCreateRequest {
    #[serde(rename = "type")]
    pub(crate) kind: SessionCreateRequestType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) auth: RealtimeMutationAuth,
    pub(crate) controller_id: Identifier,
    pub(crate) client_session_id: Identifier,
    pub(crate) capability_id: Identifier,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) voice_id: Option<Identifier>,
    pub(crate) offer: RealtimeOffer,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSessionCreateResult {
    #[serde(rename = "type")]
    pub(crate) kind: SessionCreateResultType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) request_id: Identifier,
    pub(crate) idempotency_key: Identifier,
    pub(crate) session: RealtimeSession,
    pub(crate) answer: RealtimeAnswer,
    pub(crate) broker_latency_ms: NonNegativeNumber,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RealtimeSignal {
    IceCandidate {
        candidate: SensitiveWireText,
        #[serde(rename = "sdpMid", deserialize_with = "deserialize_required_nullable")]
        sdp_mid: Option<Identifier>,
        #[serde(
            rename = "sdpMLineIndex",
            deserialize_with = "deserialize_required_nullable"
        )]
        sdp_m_line_index: Option<u64>,
    },
    IceComplete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSignalRequest {
    #[serde(rename = "type")]
    pub(crate) kind: SignalRequestType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) auth: RealtimeMutationAuth,
    pub(crate) session_id: Identifier,
    pub(crate) signal: RealtimeSignal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSessionUpdateRequest {
    #[serde(rename = "type")]
    pub(crate) kind: SessionUpdateRequestType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) auth: RealtimeMutationAuth,
    pub(crate) session_id: Identifier,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) voice_id: Option<Identifier>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) instructions: Option<ShortText>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RealtimeCloseReason {
    User,
    Handoff,
    Timeout,
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSessionCloseRequest {
    #[serde(rename = "type")]
    pub(crate) kind: SessionCloseRequestType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) auth: RealtimeMutationAuth,
    pub(crate) session_id: Identifier,
    pub(crate) reason: RealtimeCloseReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BridgeSectionName {
    Health,
    Status,
    Gpus,
    Metrics,
    #[serde(rename = "agent-runtime")]
    AgentRuntime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BridgeErrorCode {
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
    RealtimeUnavailable,
    RealtimeSessionExpired,
    RealtimeStateConflict,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BridgeErrorDetails {
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) field: Option<Identifier>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) section: Option<BridgeSectionName>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) expected_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) current_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) retry_after_ms: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) limit_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BridgeError {
    pub(crate) code: BridgeErrorCode,
    pub(crate) message: ShortText,
    pub(crate) retriable: bool,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) request_id: Option<Identifier>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) details: Option<BridgeErrorDetails>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeSessionStatus {
    #[serde(rename = "type")]
    pub(crate) kind: SessionStatusType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) event_id: Identifier,
    pub(crate) sequence: u64,
    pub(crate) observed_at: Timestamp,
    pub(crate) session: RealtimeSession,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) broker_latency_ms: Option<NonNegativeNumber>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) media_connection_latency_ms: Option<NonNegativeNumber>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub(crate) error: Option<BridgeError>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RealtimeMutationAck {
    #[serde(rename = "type")]
    pub(crate) kind: MutationAckType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) contract_version: RealtimeContractVersion,
    pub(crate) request_id: Identifier,
    pub(crate) idempotency_key: Identifier,
    pub(crate) session: RealtimeSession,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BridgeErrorResult {
    #[serde(rename = "type")]
    pub(crate) kind: ErrorResultType,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) request_id: Identifier,
    pub(crate) error: BridgeError,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum RealtimeRequest {
    Capabilities(RealtimeCapabilitiesRequest),
    Create(RealtimeSessionCreateRequest),
    Signal(RealtimeSignalRequest),
    Update(RealtimeSessionUpdateRequest),
    Close(RealtimeSessionCloseRequest),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum RealtimeResult {
    Capabilities(RealtimeCapabilitiesResult),
    Created(RealtimeSessionCreateResult),
    Status(RealtimeSessionStatus),
    MutationAck(RealtimeMutationAck),
    Error(BridgeErrorResult),
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    const GOLDEN_FIXTURE: &str =
        include_str!("../tests/fixtures/litter-bridge-realtime-v1.fixture.json");

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct GoldenFixture {
        contract_version: RealtimeContractVersion,
        capabilities_request: RealtimeCapabilitiesRequest,
        capabilities_result: RealtimeCapabilitiesResult,
        create_request: RealtimeSessionCreateRequest,
        create_result: RealtimeSessionCreateResult,
        signal_request: RealtimeSignalRequest,
        update_request: RealtimeSessionUpdateRequest,
        close_request: RealtimeSessionCloseRequest,
        status: RealtimeSessionStatus,
    }

    fn fixture_value() -> Value {
        serde_json::from_str(GOLDEN_FIXTURE).expect("golden fixture is valid JSON")
    }

    fn parse_fixture() -> GoldenFixture {
        serde_json::from_str(GOLDEN_FIXTURE)
            .expect("Alleycat must consume Local Studio's exact realtime v1 fixture")
    }

    #[test]
    fn consumes_and_round_trips_local_studio_realtime_v1_fixture() {
        let fixture = parse_fixture();

        let requests = [
            serde_json::to_value(&fixture.capabilities_request).unwrap(),
            serde_json::to_value(&fixture.create_request).unwrap(),
            serde_json::to_value(&fixture.signal_request).unwrap(),
            serde_json::to_value(&fixture.update_request).unwrap(),
            serde_json::to_value(&fixture.close_request).unwrap(),
        ];
        for request in requests {
            let typed: RealtimeRequest = serde_json::from_value(request.clone()).unwrap();
            let encoded = serde_json::to_value(&typed).unwrap();
            assert_eq!(
                serde_json::from_value::<RealtimeRequest>(encoded).unwrap(),
                typed
            );
        }

        let results = [
            serde_json::to_value(&fixture.capabilities_result).unwrap(),
            serde_json::to_value(&fixture.create_result).unwrap(),
            serde_json::to_value(&fixture.status).unwrap(),
        ];
        for result in results {
            let typed: RealtimeResult = serde_json::from_value(result.clone()).unwrap();
            let encoded = serde_json::to_value(&typed).unwrap();
            assert_eq!(
                serde_json::from_value::<RealtimeResult>(encoded).unwrap(),
                typed
            );
        }

        let encoded = serde_json::to_string(&fixture).unwrap();
        assert_eq!(
            serde_json::from_str::<GoldenFixture>(&encoded).unwrap(),
            fixture
        );
    }

    #[test]
    fn rejects_excess_properties_and_wrong_literal_versions() {
        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["unexpected"] = json!(true);
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["createRequest"].clone();
        value["auth"]["device"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<RealtimeSessionCreateRequest>(value).is_err());

        let mut value = fixture_value()["createRequest"].clone();
        value["protocolVersion"] = json!(2);
        assert!(serde_json::from_value::<RealtimeSessionCreateRequest>(value).is_err());

        let mut value = fixture_value()["createRequest"].clone();
        value["contractVersion"] = json!(2);
        assert!(serde_json::from_value::<RealtimeSessionCreateRequest>(value).is_err());

        let mut value = fixture_value()["createRequest"].clone();
        value["type"] = json!("realtime_create");
        assert!(serde_json::from_value::<RealtimeSessionCreateRequest>(value).is_err());
    }

    #[test]
    fn rejects_missing_fields_that_are_required_even_when_nullable() {
        let mut value = fixture_value()["capabilitiesResult"]["capabilities"][0].clone();
        value.as_object_mut().unwrap().remove("unavailableReason");
        assert!(serde_json::from_value::<RealtimeCapability>(value).is_err());

        let mut value = fixture_value()["createRequest"].clone();
        value.as_object_mut().unwrap().remove("voiceId");
        assert!(serde_json::from_value::<RealtimeSessionCreateRequest>(value).is_err());

        let mut value = fixture_value()["signalRequest"].clone();
        value["signal"].as_object_mut().unwrap().remove("sdpMid");
        assert!(serde_json::from_value::<RealtimeSignalRequest>(value).is_err());

        let mut value = fixture_value()["updateRequest"].clone();
        value.as_object_mut().unwrap().remove("instructions");
        assert!(serde_json::from_value::<RealtimeSessionUpdateRequest>(value).is_err());

        let mut value = fixture_value()["status"].clone();
        value["session"]
            .as_object_mut()
            .unwrap()
            .remove("reconnectToken");
        assert!(serde_json::from_value::<RealtimeSessionStatus>(value).is_err());

        let mut value = fixture_value()["status"].clone();
        value.as_object_mut().unwrap().remove("error");
        assert!(serde_json::from_value::<RealtimeSessionStatus>(value).is_err());
    }

    #[test]
    fn consumes_mutation_ack_and_error_result_variants() {
        let session = fixture_value()["status"]["session"].clone();
        let ack = json!({
            "type": "realtime_mutation_ack",
            "protocolVersion": 1,
            "contractVersion": 1,
            "requestId": "request-update-1",
            "idempotencyKey": "update-session-1",
            "session": session,
        });
        assert!(matches!(
            serde_json::from_value::<RealtimeResult>(ack).unwrap(),
            RealtimeResult::MutationAck(_)
        ));

        let error = json!({
            "type": "error",
            "protocolVersion": 1,
            "requestId": "request-create-2",
            "error": {
                "code": "realtime_unavailable",
                "message": "Realtime is not available for this model.",
                "retriable": true,
                "requestId": "request-create-2",
                "details": {
                    "field": null,
                    "section": null,
                    "expectedRevision": null,
                    "currentRevision": null,
                    "retryAfterMs": 500,
                    "limitBytes": null
                }
            }
        });
        assert!(matches!(
            serde_json::from_value::<RealtimeResult>(error.clone()).unwrap(),
            RealtimeResult::Error(_)
        ));

        let mut missing_nullable_error_field = error;
        missing_nullable_error_field["error"]["details"]
            .as_object_mut()
            .unwrap()
            .remove("field");
        assert!(serde_json::from_value::<RealtimeResult>(missing_nullable_error_field).is_err());
    }

    #[test]
    fn enforces_capability_invariants_and_integer_bounds() {
        let capability = fixture_value()["capabilitiesResult"]["capabilities"][0].clone();

        let mut value = capability.clone();
        value["available"] = json!(false);
        assert!(serde_json::from_value::<RealtimeCapability>(value).is_err());

        let mut value = capability.clone();
        value["inputModalities"] = json!(["audio", "audio"]);
        assert!(serde_json::from_value::<RealtimeCapability>(value).is_err());

        let mut value = capability.clone();
        value["sessionTtlSeconds"] = json!(0);
        assert!(serde_json::from_value::<RealtimeCapability>(value).is_err());

        let mut value = capability;
        value["maxSignalBytes"] = json!(-1);
        assert!(serde_json::from_value::<RealtimeCapability>(value).is_err());

        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["acceptedContractVersions"] = json!([]);
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["acceptedContractVersions"] = json!([1, 1]);
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["status"].clone();
        value["brokerLatencyMs"] = json!(-0.1);
        assert!(serde_json::from_value::<RealtimeSessionStatus>(value).is_err());

        let mut value = fixture_value()["signalRequest"].clone();
        value["signal"]["sdpMLineIndex"] = json!(-1);
        assert!(serde_json::from_value::<RealtimeSignalRequest>(value).is_err());
    }

    #[test]
    fn enforces_string_auth_and_wire_bounds() {
        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["controllerId"] = json!(" controller-1");
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["auth"]["nonce"] = json!("too-short");
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["auth"]["bodyHash"] =
            json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["capabilitiesRequest"].clone();
        value["auth"]["signature"] = json!("not-valid");
        assert!(serde_json::from_value::<RealtimeCapabilitiesRequest>(value).is_err());

        let mut value = fixture_value()["createRequest"].clone();
        value["offer"]["sdp"] = json!("x".repeat(WIRE_TEXT_MAX_UTF16_UNITS + 1));
        assert!(serde_json::from_value::<RealtimeSessionCreateRequest>(value).is_err());

        let mut value = fixture_value()["updateRequest"].clone();
        value["instructions"] = json!("x".repeat(SHORT_TEXT_MAX_UTF16_UNITS + 1));
        assert!(serde_json::from_value::<RealtimeSessionUpdateRequest>(value).is_err());

        let mut value = fixture_value()["status"].clone();
        value["session"]["reconnectToken"] = json!("x".repeat(OPAQUE_TOKEN_MAX_UTF16_UNITS + 1));
        assert!(serde_json::from_value::<RealtimeSessionStatus>(value).is_err());

        let mut value = fixture_value()["status"].clone();
        value["observedAt"] = json!("2026-08-04T12:00:05+00:00");
        assert!(serde_json::from_value::<RealtimeSessionStatus>(value).is_err());
    }

    #[test]
    fn debug_output_redacts_signatures_sdp_candidates_and_reconnect_tokens() {
        let fixture = parse_fixture();
        let debug = format!(
            "{:?} {:?} {:?} {:?}",
            fixture.create_request, fixture.create_result, fixture.signal_request, fixture.status
        );

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("sssssssssssssssssssssssssssssssssssssssssss"));
        assert!(!debug.contains("127.0.0.1"));
        assert!(!debug.contains("192.0.2.1"));
        assert!(!debug.contains("fake-reconnect-token-1"));
    }
}
