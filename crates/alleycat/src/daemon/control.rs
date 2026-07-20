//! Control protocol types exchanged over the IPC stream between the CLI and
//! the daemon. One request per connection, one response, then close. The
//! wire frame is a length-prefixed JSON envelope provided by
//! `crate::framing::{read_json_frame, write_json_frame}`.

use serde::{Deserialize, Serialize};

use alleycat_local_studio_proto::Capability;

use crate::protocol::{AgentInfo, PairPayload};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Aggregate status: pid, node id, token fingerprint, agent availability.
    Status,
    /// Pair payload as the daemon would emit it right now.
    Pair,
    /// Mint a fresh token. Node id is preserved.
    Rotate,
    /// Re-read host.toml and swap agent config.
    Reload,
    /// Graceful shutdown.
    Stop,
    /// Agent introspection.
    AgentsList,
    /// List host-owned Local Studio paired-node grants.
    LocalStudioGrantsList,
    /// Grant only protocol-v1 `stats.read` to one authenticated endpoint ID.
    LocalStudioGrantStatsRead {
        endpoint_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at: Option<String>,
    },
    /// Grant an explicit non-empty set from the closed protocol-v1 capability enum.
    LocalStudioGrantCapabilities {
        endpoint_id: String,
        capabilities: Vec<Capability>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at: Option<String>,
    },
    /// Revoke protocol-v1 `stats.read` for one authenticated endpoint ID.
    LocalStudioRevokeStatsRead { endpoint_id: String },
    /// Revoke an explicit non-empty set from the closed protocol-v1 capability enum.
    LocalStudioRevokeCapabilities {
        endpoint_id: String,
        capabilities: Vec<Capability>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl Response {
    pub fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            data: None,
        }
    }

    pub fn ok_with<T: Serialize>(data: &T) -> anyhow::Result<Self> {
        Ok(Self {
            ok: true,
            error: None,
            data: Some(serde_json::to_value(data)?),
        })
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(msg.into()),
            data: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInfo {
    pub pid: u32,
    pub node_id: String,
    pub token_short: String,
    pub relay: Option<String>,
    pub config_path: String,
    pub uptime_secs: u64,
    pub agents: Vec<AgentInfo>,
    /// SemVer of the *binary* that's currently running the daemon (e.g.
    /// `kittylitter 0.2.1`). The CLI compares this against its own version
    /// to detect a stale daemon and offer a transparent restart. Optional
    /// for forwards compatibility with daemons that predate the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateResult {
    pub token_short: String,
    pub payload: PairPayload,
}

/// First 16 hex chars of SHA-256(token).
pub fn token_fingerprint(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(&digest[..8])
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn status_request_serializes_with_op_tag() {
        let r = Request::Status;
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#"{"op":"status"}"#);
    }

    #[test]
    fn rotate_request_round_trips() {
        let s = serde_json::to_string(&Request::Rotate).unwrap();
        let back: Request = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, Request::Rotate));
    }

    #[test]
    fn local_studio_grant_requests_are_closed_host_operations() {
        let endpoint = "a".repeat(64);
        let request = Request::LocalStudioGrantStatsRead {
            endpoint_id: endpoint.clone(),
            expires_at: Some("2026-07-21T12:00:00Z".into()),
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["op"], "local_studio_grant_stats_read");
        assert_eq!(encoded["endpoint_id"], endpoint);
        assert!(matches!(
            serde_json::from_value::<Request>(encoded).unwrap(),
            Request::LocalStudioGrantStatsRead { .. }
        ));
        assert_eq!(
            serde_json::to_value(Request::LocalStudioRevokeStatsRead {
                endpoint_id: "b".repeat(64)
            })
            .unwrap()["op"],
            "local_studio_revoke_stats_read"
        );

        let explicit = Request::LocalStudioGrantCapabilities {
            endpoint_id: "c".repeat(64),
            capabilities: vec![Capability::StatsRead, Capability::SessionsRead],
            expires_at: None,
        };
        let encoded = serde_json::to_value(explicit).unwrap();
        assert_eq!(encoded["op"], "local_studio_grant_capabilities");
        assert_eq!(
            encoded["capabilities"],
            json!(["stats.read", "sessions.read"])
        );
        assert!(matches!(
            serde_json::from_value::<Request>(encoded).unwrap(),
            Request::LocalStudioGrantCapabilities { .. }
        ));
        assert!(
            serde_json::from_value::<Request>(json!({
                "op": "local_studio_grant_capabilities",
                "endpoint_id": "d".repeat(64),
                "capabilities": ["all"]
            }))
            .is_err()
        );
    }

    #[test]
    fn response_ok_skips_optionals() {
        let r = Response::ok();
        assert_eq!(serde_json::to_string(&r).unwrap(), r#"{"ok":true}"#);
    }

    #[test]
    fn response_err_includes_error() {
        let s = serde_json::to_string(&Response::err("boom")).unwrap();
        assert!(s.contains(r#""ok":false"#));
        assert!(s.contains(r#""error":"boom""#));
    }

    #[test]
    fn token_fingerprint_is_16_hex() {
        let f = token_fingerprint("deadbeef");
        assert_eq!(f.len(), 16);
        assert!(f.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
