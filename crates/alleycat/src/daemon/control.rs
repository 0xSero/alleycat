//! Control protocol types exchanged over the IPC stream between the CLI and
//! the daemon. One request per connection, one response, then close. The
//! wire frame is a length-prefixed JSON envelope provided by
//! `crate::framing::{read_json_frame, write_json_frame}`.

use serde::{Deserialize, Serialize};

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
