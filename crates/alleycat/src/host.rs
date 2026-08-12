use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use arc_swap::ArcSwap;
use iroh::endpoint::QuicTransportConfig;
use iroh::endpoint::{IdleTimeout, presets};
use iroh::{Endpoint, RelayMap, RelayMode, SecretKey};
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::agents::AgentManager;
use crate::config::HostConfig;
use crate::framing::{read_json_frame, write_json_frame};
use crate::protocol::{ALLEYCAT_ALPN, PROTOCOL_VERSION, Request, Response, Resume, SessionInfo};
use crate::stream::IrohStream;

/// Bind the iroh endpoint with the given identity and ALPN, returning it
/// ready to be passed to [`accept_loop`]. Spawns a background "online" probe
/// that logs when the endpoint reports relay connectivity.
///
/// When `relay` is `Some`, the endpoint is pinned to that single relay
/// (`RelayMode::Custom`) instead of the default number0 relays. This lets an
/// operator point the daemon at a closer / self-hosted relay via the `relay`
/// field in `host.toml`.
pub async fn bind_endpoint(
    secret_key: SecretKey,
    relay: Option<&str>,
) -> anyhow::Result<Endpoint> {
    // iroh defaults already PING every 5s (HEARTBEAT_INTERVAL) which would
    // normally keep the connection alive — but the connection-wide
    // `max_idle_timeout` is still 30s by default, and once the holepunched
    // direct path's per-path 15s timer fires plus the relay path drops, the
    // connection has no live paths left and the 30s idle clock kicks in.
    // Raise the connection-level idle timeout to 10 minutes so phone-side
    // agent tunnels (pi/opencode sitting between thread/list calls) don't
    // get torn down with `connection lost: timed out` while idle. Default
    // path keep-alive (5s) keeps the actual paths alive in normal cases.
    let idle_timeout = IdleTimeout::try_from(Duration::from_secs(600))
        .context("constructing iroh idle timeout")?;
    let transport = QuicTransportConfig::builder()
        .max_idle_timeout(Some(idle_timeout))
        .build();

    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![ALLEYCAT_ALPN.to_vec()])
        .transport_config(transport);

    // Honor a relay pinned in host.toml (`relay = "https://..."`). Without an
    // explicit override, presets::N0 always uses the number0 relays, which may
    // be geographically distant and add latency on relayed paths; pinning a
    // closer self-hosted relay lets operators cut that latency.
    if let Some(relay_url) = relay {
        let relay_map = RelayMap::try_from_iter([relay_url])
            .with_context(|| format!("parsing configured relay URL {relay_url:?}"))?;
        builder = builder.relay_mode(RelayMode::Custom(relay_map));
        info!(relay = %relay_url, "using custom relay from host.toml");
    }

    let endpoint = builder
        .bind()
        .await
        .context("binding iroh endpoint")?;

    info!(node_id = %endpoint.id(), "alleycat endpoint bound");
    let endpoint_for_online = endpoint.clone();
    tokio::spawn(async move {
        if tokio::time::timeout(Duration::from_secs(8), endpoint_for_online.online())
            .await
            .is_ok()
        {
            info!(addr = ?endpoint_for_online.addr(), "alleycat endpoint online");
        } else {
            warn!("alleycat endpoint did not report relay connectivity within timeout");
        }
    });

    Ok(endpoint)
}

/// Run the iroh accept loop until `shutdown` fires or the endpoint stops
/// yielding incoming connections. Caller owns the [`Endpoint`] and the
/// [`AgentManager`]; both are passed by `Arc`/clone so control-side handlers
/// can keep using them concurrently.
pub async fn accept_loop(
    endpoint: Endpoint,
    agents: AgentManager,
    config: Arc<ArcSwap<HostConfig>>,
    shutdown: Arc<Notify>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => {
                info!("iroh accept loop received shutdown");
                endpoint.close().await;
                break;
            }
            incoming = endpoint.accept() => {
                let Some(connecting) = incoming else {
                    break;
                };
                let agents = agents.clone();
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    match connecting.await {
                        Ok(conn) => {
                            let conn_id = conn.stable_id();
                            // `remote_id` is the cryptographic identity we
                            // key sessions on. It's stable across all
                            // bi-streams of this connection.
                            let node_id = conn.remote_id().to_string();
                            info!(
                                conn = conn_id,
                                node_id = %node_id,
                                "iroh connection accepted"
                            );
                            while let Ok((send, recv)) = conn.accept_bi().await {
                                let agents = agents.clone();
                                let config = Arc::clone(&config);
                                let node_id = node_id.clone();
                                tokio::spawn(async move {
                                    if let Err(error) = handle_stream(
                                        send, recv, agents, config, conn_id, node_id,
                                    )
                                    .await
                                    {
                                        info!(conn = conn_id, "alleycat stream ended: {error:#}");
                                    }
                                });
                            }
                            info!(conn = conn_id, "iroh connection closed");
                        }
                        Err(error) => warn!("alleycat incoming connection failed: {error:#}"),
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_stream(
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    agents: AgentManager,
    config: Arc<ArcSwap<HostConfig>>,
    conn: usize,
    node_id: String,
) -> anyhow::Result<()> {
    let request: Request = read_json_frame(&mut recv).await?;
    if let Err(error) = validate_version(&request) {
        write_json_frame(&mut send, &Response::error(&error)).await?;
        return Err(anyhow!(error));
    }

    let token = config.load().token.clone();
    if let Err(error) = validate_token(&request, &token) {
        warn!(conn = conn, "rejecting stream: {error}");
        write_json_frame(&mut send, &Response::error(&error)).await?;
        return Err(anyhow!(error));
    }

    match request {
        Request::ListAgents { .. } => {
            info!(conn = conn, "list_agents");
            let list = agents.list_agents().await;
            write_json_frame(&mut send, &Response::agents(list)).await?;
            Ok(())
        }
        Request::RestartAgent { agent, .. } => {
            info!(conn = conn, %agent, "restart_agent");
            if !agents.agent_enabled(&agent) {
                warn!(conn = conn, %agent, "rejecting restart: agent disabled or unknown");
                write_json_frame(
                    &mut send,
                    &Response::error(format!("agent `{agent}` is disabled or unknown")),
                )
                .await?;
                return Err(anyhow!("agent disabled or unknown: {agent}"));
            }
            if let Err(error) = agents.restart_agent(&agent).await {
                warn!(conn = conn, %agent, "restart_agent failed: {error:#}");
                write_json_frame(&mut send, &Response::error(error.to_string())).await?;
                return Err(error);
            }
            write_json_frame(&mut send, &Response::ok()).await?;
            Ok(())
        }
        Request::Connect { agent, resume, .. } => {
            if !agents.agent_enabled(&agent) {
                warn!(conn = conn, %agent, "rejecting: agent disabled or unknown");
                write_json_frame(
                    &mut send,
                    &Response::error(format!("agent `{agent}` is disabled or unknown")),
                )
                .await?;
                return Err(anyhow!("agent disabled or unknown: {agent}"));
            }
            let agent_static = match AgentManager::agent_id(&agent) {
                Some(id) => id,
                None => {
                    write_json_frame(
                        &mut send,
                        &Response::error(format!("agent `{agent}` is unknown")),
                    )
                    .await?;
                    return Err(anyhow!("unknown agent: {agent}"));
                }
            };

            let last_seen = resume.as_ref().map(|r: &Resume| r.last_seq);
            let resolved =
                agents
                    .session_registry()
                    .resolve_attach(node_id.clone(), agent_static, last_seen);
            let session_info = SessionInfo {
                attached: resolved.kind.into(),
                current_seq: resolved.current_seq,
                floor_seq: resolved.floor_seq,
            };
            info!(
                conn = conn,
                %agent,
                attached = ?session_info.attached,
                current_seq = session_info.current_seq,
                floor_seq = session_info.floor_seq,
                "connect: dispatching to agent"
            );
            write_json_frame(&mut send, &Response::ok_with_session(session_info.clone())).await?;
            // The registry already decided what cursor to actually replay from —
            // either the client's explicit resume hint, or the server's own
            // `last_attempted_seq` for a known session attaching without one.
            // For Fresh and DriftReload paths it returned None, so the
            // dispatcher sees an empty backlog.
            let dispatch_last_seen = match resolved.kind {
                alleycat_bridge_core::session::AttachKind::Resumed => resolved.effective_last_seen,
                _ => None,
            };
            let result = agents
                .serve_agent_with_session(
                    &agent,
                    IrohStream::new(send, recv),
                    resolved.session,
                    dispatch_last_seen,
                )
                .await
                .with_context(|| format!("serving agent `{agent}`"));
            match &result {
                Ok(()) => info!(conn = conn, %agent, "agent stream finished"),
                Err(error) => warn!(conn = conn, %agent, "agent stream errored: {error:#}"),
            }
            result
        }
    }
}

pub fn pair_payload(
    secret_key: &iroh::SecretKey,
    config: &HostConfig,
    endpoint: Option<&Endpoint>,
) -> crate::protocol::PairPayload {
    crate::protocol::PairPayload {
        v: PROTOCOL_VERSION,
        node_id: secret_key.public().to_string(),
        token: config.token.clone(),
        host_name: local_host_name(),
        relay: endpoint_home_relay(endpoint).or_else(|| config.relay.clone()),
    }
}

/// Read the iroh endpoint's currently-known home relay, if any. Pair payloads
/// prefer this over the static config so phones can dial the host even when
/// pkarr/DNS publishing is broken (e.g. IPv6-only relays + Tailscale).
pub fn endpoint_home_relay(endpoint: Option<&Endpoint>) -> Option<String> {
    endpoint?
        .addr()
        .relay_urls()
        .next()
        .map(|url| url.to_string())
}

fn local_host_name() -> Option<String> {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .map(|name| name.trim().trim_end_matches('.').to_string())
        .filter(|name| !name.is_empty())
}

fn validate_version(request: &Request) -> Result<(), String> {
    if request.version() == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(format!(
            "protocol mismatch: client={} host={}",
            request.version(),
            PROTOCOL_VERSION
        ))
    }
}

fn validate_token(request: &Request, expected_token: &str) -> Result<(), String> {
    if request.token() == expected_token {
        Ok(())
    } else {
        Err("invalid token".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AgentsConfig, HostConfig};

    #[test]
    fn pair_payload_uses_stable_node_id_and_token() {
        let secret_key = iroh::SecretKey::generate();
        let config = HostConfig {
            token: "token-1".to_string(),
            relay: Some("https://relay.example".to_string()),
            agents: AgentsConfig::default(),
            session: crate::config::SessionConfig::default(),
        };

        let payload = pair_payload(&secret_key, &config, None);

        assert_eq!(payload.v, PROTOCOL_VERSION);
        assert_eq!(payload.node_id, secret_key.public().to_string());
        assert_eq!(payload.token, "token-1");
        assert_eq!(payload.relay.as_deref(), Some("https://relay.example"));
        assert!(
            payload
                .host_name
                .as_deref()
                .is_some_and(|name| !name.is_empty())
        );
    }

    #[test]
    fn first_frame_auth_accepts_matching_token() {
        let request = Request::Connect {
            v: PROTOCOL_VERSION,
            token: "secret".to_string(),
            agent: "codex".to_string(),
            resume: None,
        };

        validate_version(&request).unwrap();
        validate_token(&request, "secret").unwrap();
    }

    #[test]
    fn first_frame_auth_rejects_protocol_mismatch() {
        let request = Request::ListAgents {
            v: PROTOCOL_VERSION + 1,
            token: "secret".to_string(),
        };

        let err = validate_version(&request).unwrap_err();
        assert!(err.contains("protocol mismatch"));
    }

    #[test]
    fn first_frame_auth_rejects_invalid_token() {
        let request = Request::ListAgents {
            v: PROTOCOL_VERSION,
            token: "wrong".to_string(),
        };

        validate_version(&request).unwrap();
        assert_eq!(
            validate_token(&request, "secret").unwrap_err(),
            "invalid token"
        );
    }
}
