//! `alleycat probe` — local debug client that connects to the daemon over iroh
//! exactly the way the phone does, runs the JSON-RPC initialize handshake
//! against an agent, and invokes a method (default `thread/list`).
//!
//! Two modes:
//! - No `--agent`: round-trip a `list_agents` over the alleycat protocol and
//!   print the agent table.
//! - With `--agent <name>`: open a `connect`-style stream, send `initialize`
//!   + `initialized` + the user-supplied method, and dump every JSON-RPC frame
//!     in/out.
//!
//! Identity: reads the daemon's local `host.toml` + `host.key` so the probe
//! authenticates with the same node id and token a phone holding the QR
//! payload would. Generates a fresh client iroh identity each run.

use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, ValueEnum};
use futures::{SinkExt, StreamExt};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::AgentWire;

use crate::cli;
use crate::daemon::control::Request as ControlRequest;
use crate::framing::{read_json_frame, write_json_frame};
use crate::host;
use crate::protocol::{ALLEYCAT_ALPN, PROTOCOL_VERSION, PairPayload, Request, Response, Resume};

#[derive(Args, Debug)]
pub struct ProbeArgs {
    /// Agent to connect to (`pi`, `opencode`, `codex`). Omit to round-trip a
    /// `list_agents` call instead.
    #[arg(long)]
    pub agent: Option<String>,
    /// JSON-RPC method to invoke after `initialize` succeeds. Ignored when
    /// `--agent` is omitted. Defaults to `thread/list`.
    #[arg(long)]
    pub method: Option<String>,
    /// JSON params for the method. Defaults to `{}`.
    #[arg(long, default_value = "{}")]
    pub params: String,
    /// Optional thread/start params. When set, probe invokes `thread/start` on
    /// the same stream before `--method` and injects the returned thread id into
    /// `--params.threadId` when that field is absent or set to `$threadId`.
    #[arg(long)]
    pub start_thread_params: Option<String>,
    /// Optional JSON-RPC method to invoke on the same stream before `--method`.
    /// Useful for attaching listeners before a streaming request.
    #[arg(long)]
    pub before_method: Option<String>,
    /// JSON params for `--before-method`. Defaults to `{}`.
    #[arg(long, default_value = "{}")]
    pub before_params: String,
    /// Wire protocol to speak after connect. `auto` discovers the agent's
    /// advertised wire from list_agents; use `websocket` for Codex-style
    /// app-server transports.
    #[arg(long, value_enum, default_value_t = ProbeWire::Auto)]
    pub wire: ProbeWire,
    /// Override the node id to dial. Defaults to the local daemon's node id
    /// (read from `host.key`). Useful for probing a remote alleycat.
    #[arg(long)]
    pub node_id: Option<String>,
    /// Override the auth token. Defaults to the local daemon's token (read
    /// from `host.toml`). Pair this with `--node-id` to probe a remote.
    #[arg(long)]
    pub token: Option<String>,
    /// Override the relay URL. By default local probes use the daemon's live
    /// pair payload relay, matching the QR path used by mobile clients.
    #[arg(long)]
    pub relay: Option<String>,
    /// How long to wait for additional JSON-RPC frames after the method
    /// response before exiting, in seconds. Streaming methods may push
    /// notifications; raise this to capture them.
    #[arg(long, default_value_t = 5)]
    pub linger_secs: u64,
    /// Timeout for the JSON-RPC method response, in seconds.
    #[arg(long, default_value_t = 30)]
    pub timeout_secs: u64,
    /// During the linger window, exit early after seeing this notification
    /// method. Useful for streaming methods such as `turn/start` where the
    /// response arrives before `turn/completed`.
    #[arg(long)]
    pub until_method: Option<String>,
    /// Send an explicit alleycat resume cursor on connect. Useful for
    /// debugging reconnect/replay behavior; clients normally use the highest
    /// `_alleycat_seq` they observed before reconnecting.
    #[arg(long)]
    pub resume_from: Option<u64>,
    /// After the first probe finishes, open a second connect stream on the
    /// same iroh connection with this resume cursor. This simulates a client
    /// reconnect from the same endpoint identity, so the host can attach the
    /// existing session and exercise replay/drift paths.
    #[arg(long)]
    pub repeat_resume_from: Option<u64>,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum ProbeWire {
    Auto,
    Jsonl,
    Websocket,
}

pub async fn run(args: ProbeArgs) -> anyhow::Result<()> {
    if args.node_id.is_none() {
        cli::ensure_current_daemon().await?;
    }

    let cfg = crate::config::load_or_init().await?;
    let server_secret = crate::state::load_or_create_secret_key().await?;
    let local_payload = load_local_pair_payload(&server_secret, &cfg, args.node_id.is_none()).await;

    let token = match &args.token {
        Some(t) => t.clone(),
        None => local_payload.token.clone(),
    };
    let node_id: PublicKey = match &args.node_id {
        Some(s) => s
            .parse()
            .with_context(|| format!("parsing --node-id {s:?} as iroh public key"))?,
        None => local_payload
            .node_id
            .parse()
            .with_context(|| format!("parsing pair payload node_id {:?}", local_payload.node_id))?,
    };
    let relay = match (&args.relay, &args.node_id) {
        (Some(relay), _) => Some(relay.clone()),
        (None, None) => local_payload.relay.clone(),
        (None, Some(_)) => None,
    };

    eprintln!(
        "probe: dialing node_id={} token={} relay={}",
        node_id,
        short_token(&token),
        relay.as_deref().unwrap_or("<iroh default>")
    );

    let endpoint = build_client_endpoint().await?;
    let result = probe_with_endpoint(&endpoint, node_id, relay.as_deref(), &token, &args).await;
    endpoint.close().await;
    result
}

async fn load_local_pair_payload(
    server_secret: &SecretKey,
    cfg: &crate::config::HostConfig,
    prefer_daemon: bool,
) -> PairPayload {
    if prefer_daemon
        && let Ok(resp) = cli::send(ControlRequest::Pair).await
        && let Ok(payload) = cli::decode_data::<PairPayload>(resp)
    {
        return payload;
    }

    host::pair_payload(server_secret, cfg, None)
}

async fn probe_with_endpoint(
    endpoint: &Endpoint,
    node_id: PublicKey,
    relay: Option<&str>,
    token: &str,
    args: &ProbeArgs,
) -> anyhow::Result<()> {
    let _ = tokio::time::timeout(Duration::from_secs(8), endpoint.online()).await;

    let addr = endpoint_addr(node_id, relay)?;
    let conn = endpoint
        .connect(addr, ALLEYCAT_ALPN)
        .await
        .with_context(|| format!("dialing alleycat node {node_id}"))?;
    eprintln!("probe: iroh connection established");

    let result = match args.agent.as_deref() {
        None => list_agents(&conn, token).await.map(|_| ()),
        Some(agent) => {
            let wire = resolve_probe_wire(&conn, token, agent, &args.wire).await?;
            probe_agent(&conn, token, agent, args, args.resume_from, wire.clone()).await?;
            if let Some(resume_from) = args.repeat_resume_from {
                eprintln!("probe: opening second connect stream with resume_from={resume_from}");
                probe_agent(&conn, token, agent, args, Some(resume_from), wire.clone()).await?;
            }
            Ok(())
        }
    };
    conn.close(iroh::endpoint::VarInt::from_u32(0), b"probe complete");
    result
}

fn endpoint_addr(node_id: PublicKey, relay: Option<&str>) -> anyhow::Result<EndpointAddr> {
    let mut addr = EndpointAddr::new(node_id);
    if let Some(relay) = relay {
        let relay_url = relay
            .parse()
            .with_context(|| format!("parsing relay URL {relay:?}"))?;
        addr = addr.with_relay_url(relay_url);
    }
    Ok(addr)
}

async fn list_agents(conn: &iroh::endpoint::Connection, token: &str) -> anyhow::Result<Response> {
    let resp = fetch_agents(conn, token).await?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(resp)
}

async fn fetch_agents(conn: &iroh::endpoint::Connection, token: &str) -> anyhow::Result<Response> {
    let (mut send, mut recv) = conn.open_bi().await.context("opening list_agents stream")?;
    write_json_frame(
        &mut send,
        &Request::ListAgents {
            v: PROTOCOL_VERSION,
            token: token.to_string(),
        },
    )
    .await?;
    send.finish().ok();
    read_json_frame(&mut recv).await
}

async fn resolve_probe_wire(
    conn: &iroh::endpoint::Connection,
    token: &str,
    agent: &str,
    requested: &ProbeWire,
) -> anyhow::Result<AgentWire> {
    match requested {
        ProbeWire::Jsonl => Ok(AgentWire::Jsonl),
        ProbeWire::Websocket => Ok(AgentWire::Websocket),
        ProbeWire::Auto => {
            let resp = fetch_agents(conn, token).await?;
            let wire = resp
                .agents
                .as_deref()
                .and_then(|agents| agents.iter().find(|info| info.name == agent))
                .map(|info| info.wire.clone())
                .unwrap_or(AgentWire::Jsonl);
            eprintln!(
                "probe: auto-selected wire={} for agent={agent}",
                wire.as_str()
            );
            Ok(wire)
        }
    }
}

async fn probe_agent(
    conn: &iroh::endpoint::Connection,
    token: &str,
    agent: &str,
    args: &ProbeArgs,
    resume_from: Option<u64>,
    wire: AgentWire,
) -> anyhow::Result<()> {
    let method = args
        .method
        .clone()
        .unwrap_or_else(|| "thread/list".to_string());
    let params: Value = serde_json::from_str(&args.params)
        .with_context(|| format!("parsing --params {:?} as JSON", args.params))?;
    let start_thread_params = match args.start_thread_params.as_ref() {
        Some(raw) => Some(
            serde_json::from_str(raw)
                .with_context(|| format!("parsing --start-thread-params {raw:?} as JSON"))?,
        ),
        None => None,
    };
    let before = match args.before_method.as_ref() {
        Some(method) => Some((
            method.clone(),
            serde_json::from_str(&args.before_params).with_context(|| {
                format!("parsing --before-params {:?} as JSON", args.before_params)
            })?,
        )),
        None => None,
    };
    let request = AgentProbeRequest {
        args,
        resume_from,
        start_thread_params,
        before,
        method,
        params,
    };

    match wire {
        AgentWire::Jsonl => probe_agent_jsonl(conn, token, agent, request).await,
        AgentWire::Websocket => probe_agent_websocket(conn, token, agent, request).await,
    }
}

struct AgentProbeRequest<'a> {
    args: &'a ProbeArgs,
    resume_from: Option<u64>,
    start_thread_params: Option<Value>,
    before: Option<(String, Value)>,
    method: String,
    params: Value,
}

async fn open_agent_stream(
    conn: &iroh::endpoint::Connection,
    token: &str,
    agent: &str,
    resume_from: Option<u64>,
    wire_label: &str,
) -> anyhow::Result<(iroh::endpoint::SendStream, iroh::endpoint::RecvStream)> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .with_context(|| format!("opening connect stream for agent `{agent}`"))?;
    write_json_frame(
        &mut send,
        &Request::Connect {
            v: PROTOCOL_VERSION,
            token: token.to_string(),
            agent: agent.to_string(),
            resume: resume_from.map(|last_seq| Resume { last_seq }),
        },
    )
    .await?;

    let resp: Response = read_json_frame(&mut recv).await?;
    if !resp.ok {
        anyhow::bail!(
            "connect rejected: {}",
            resp.error.unwrap_or_else(|| "<no error>".to_string())
        );
    }
    if let Some(session) = resp.session.as_ref() {
        eprintln!(
            "probe: connect ok agent={agent} attached={:?} current_seq={} floor_seq={} resume_from={:?}; switching to {wire_label}",
            session.attached, session.current_seq, session.floor_seq, resume_from
        );
    } else {
        eprintln!(
            "probe: connect ok agent={agent} resume_from={:?}; switching to {wire_label}",
            resume_from
        );
    }

    Ok((send, recv))
}

async fn probe_agent_jsonl(
    conn: &iroh::endpoint::Connection,
    token: &str,
    agent: &str,
    request: AgentProbeRequest<'_>,
) -> anyhow::Result<()> {
    let AgentProbeRequest {
        args,
        resume_from,
        start_thread_params,
        before,
        method,
        mut params,
    } = request;
    let (mut send, recv) = open_agent_stream(conn, token, agent, resume_from, "JSONL").await?;
    let mut reader = BufReader::new(recv);

    let init = initialize_request();
    print_outbound(&init);
    write_jsonl(&mut send, &init).await?;

    loop {
        let frame = read_jsonl_with_timeout(&mut reader, Duration::from_secs(args.timeout_secs))
            .await
            .context("reading initialize response")?;
        print_inbound(&frame);
        if frame.get("id").is_some() {
            break;
        }
    }

    let initialized = initialized_notification();
    print_outbound(&initialized);
    write_jsonl(&mut send, &initialized).await?;

    if let Some(thread_params) = start_thread_params {
        let thread_req = method_request_with_id(100, "thread/start", thread_params);
        print_outbound(&thread_req);
        write_jsonl(&mut send, &thread_req).await?;
        if let Some(response) = drain_jsonl_response(&mut reader, args, "thread/start", 100).await
            && let Some(thread_id) = extract_thread_id(&response)
        {
            inject_thread_id(&mut params, &thread_id);
        }
    }

    if let Some((before_method, before_params)) = before {
        let before_req = method_request_with_id(100, &before_method, before_params);
        print_outbound(&before_req);
        write_jsonl(&mut send, &before_req).await?;
        let _ = drain_jsonl_response(&mut reader, args, &before_method, 100).await;
    }

    let method_req = method_request(&method, params);
    print_outbound(&method_req);
    write_jsonl(&mut send, &method_req).await?;

    drain_jsonl_method(&mut reader, args, &method).await;

    let _ = send.finish();
    Ok(())
}

async fn probe_agent_websocket(
    conn: &iroh::endpoint::Connection,
    token: &str,
    agent: &str,
    request: AgentProbeRequest<'_>,
) -> anyhow::Result<()> {
    let AgentProbeRequest {
        args,
        resume_from,
        start_thread_params,
        before,
        method,
        mut params,
    } = request;
    let (send, recv) = open_agent_stream(conn, token, agent, resume_from, "WebSocket").await?;
    let stream = tokio::io::join(recv, send);
    let websocket_url = format!("ws://alleycat/{agent}");
    let (mut ws, _) = tokio_tungstenite::client_async(websocket_url.as_str(), stream)
        .await
        .context("opening WebSocket over alleycat stream")?;

    let init = initialize_request();
    write_websocket_json(&mut ws, &init).await?;

    loop {
        let frame =
            read_websocket_json_with_timeout(&mut ws, Duration::from_secs(args.timeout_secs))
                .await
                .context("reading initialize response")?;
        print_inbound(&frame);
        if frame.get("id").is_some() {
            break;
        }
    }

    let initialized = initialized_notification();
    write_websocket_json(&mut ws, &initialized).await?;

    if let Some(thread_params) = start_thread_params {
        let thread_req = method_request_with_id(100, "thread/start", thread_params);
        write_websocket_json(&mut ws, &thread_req).await?;
        if let Some(response) = drain_websocket_response(&mut ws, args, "thread/start", 100).await
            && let Some(thread_id) = extract_thread_id(&response)
        {
            inject_thread_id(&mut params, &thread_id);
        }
    }

    if let Some((before_method, before_params)) = before {
        let before_req = method_request_with_id(100, &before_method, before_params);
        write_websocket_json(&mut ws, &before_req).await?;
        let _ = drain_websocket_response(&mut ws, args, &before_method, 100).await;
    }

    let method_req = method_request(&method, params);
    write_websocket_json(&mut ws, &method_req).await?;

    drain_websocket_method(&mut ws, args, &method).await;

    let _ = ws.close(None).await;
    Ok(())
}

fn initialize_request() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {
                "name": "alleycat-probe",
                "version": env!("CARGO_PKG_VERSION"),
                "title": "alleycat-probe"
            },
            "capabilities": { "experimentalApi": true }
        }
    })
}

fn initialized_notification() -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "initialized",
        "params": {}
    })
}

fn method_request(method: &str, params: Value) -> Value {
    method_request_with_id(2, method, params)
}

fn method_request_with_id(id: u64, method: &str, params: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

fn extract_thread_id(response: &Value) -> Option<String> {
    response
        .get("result")
        .and_then(|result| result.get("thread"))
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn inject_thread_id(params: &mut Value, thread_id: &str) {
    let Some(object) = params.as_object_mut() else {
        return;
    };
    let should_set = match object.get("threadId") {
        Some(Value::String(existing)) => existing == "$threadId",
        Some(_) => false,
        None => true,
    };
    if should_set {
        object.insert("threadId".to_string(), Value::String(thread_id.to_string()));
    }
}

async fn drain_jsonl_response<R>(
    reader: &mut BufReader<R>,
    args: &ProbeArgs,
    method: &str,
    response_id: u64,
) -> Option<Value>
where
    R: AsyncRead + Unpin,
{
    let response_deadline = tokio::time::Instant::now() + Duration::from_secs(args.timeout_secs);
    while tokio::time::Instant::now() < response_deadline {
        match read_jsonl_with_timeout(
            reader,
            response_deadline.saturating_duration_since(tokio::time::Instant::now()),
        )
        .await
        {
            Ok(frame) => {
                let is_response = frame.get("id") == Some(&json!(response_id));
                print_inbound(&frame);
                if is_response {
                    return Some(frame);
                }
            }
            Err(error) => {
                eprintln!("probe: read error: {error:#}");
                break;
            }
        }
    }
    eprintln!(
        "probe: did not receive response to id={response_id} ({method}) within {}s",
        args.timeout_secs
    );
    None
}

async fn drain_websocket_response<S>(
    ws: &mut WebSocketStream<S>,
    args: &ProbeArgs,
    method: &str,
    response_id: u64,
) -> Option<Value>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let response_deadline = tokio::time::Instant::now() + Duration::from_secs(args.timeout_secs);
    while tokio::time::Instant::now() < response_deadline {
        match read_websocket_json_with_timeout(
            ws,
            response_deadline.saturating_duration_since(tokio::time::Instant::now()),
        )
        .await
        {
            Ok(frame) => {
                let is_response = frame.get("id") == Some(&json!(response_id));
                print_inbound(&frame);
                if is_response {
                    return Some(frame);
                }
            }
            Err(error) => {
                eprintln!("probe: websocket read error: {error:#}");
                break;
            }
        }
    }
    eprintln!(
        "probe: did not receive response to id={response_id} ({method}) within {}s",
        args.timeout_secs
    );
    None
}

async fn drain_jsonl_method<R>(reader: &mut BufReader<R>, args: &ProbeArgs, method: &str)
where
    R: AsyncRead + Unpin,
{
    let _ = drain_jsonl_response(reader, args, method, 2).await;

    if args.linger_secs > 0 {
        eprintln!("probe: lingering {}s for trailing frames", args.linger_secs);
        let linger_deadline = tokio::time::Instant::now() + Duration::from_secs(args.linger_secs);
        while tokio::time::Instant::now() < linger_deadline {
            match read_jsonl_with_timeout(
                reader,
                linger_deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            {
                Ok(frame) => {
                    let reached_until = reached_until_method(&frame, args.until_method.as_deref());
                    print_inbound(&frame);
                    if reached_until {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
}

async fn drain_websocket_method<S>(ws: &mut WebSocketStream<S>, args: &ProbeArgs, method: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _ = drain_websocket_response(ws, args, method, 2).await;

    if args.linger_secs > 0 {
        eprintln!("probe: lingering {}s for trailing frames", args.linger_secs);
        let linger_deadline = tokio::time::Instant::now() + Duration::from_secs(args.linger_secs);
        while tokio::time::Instant::now() < linger_deadline {
            match read_websocket_json_with_timeout(
                ws,
                linger_deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            {
                Ok(frame) => {
                    let reached_until = reached_until_method(&frame, args.until_method.as_deref());
                    print_inbound(&frame);
                    if reached_until {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
}

async fn write_jsonl(stream: &mut iroh::endpoint::SendStream, value: &Value) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_jsonl_with_timeout<R>(
    reader: &mut BufReader<R>,
    timeout: Duration,
) -> anyhow::Result<Value>
where
    R: AsyncRead + Unpin,
{
    let mut line = String::new();
    let n = tokio::time::timeout(timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| anyhow!("timed out waiting for JSON line"))??;
    if n == 0 {
        return Err(anyhow!("stream closed by peer"));
    }
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty JSON line"));
    }
    serde_json::from_str(trimmed).with_context(|| format!("decoding JSON-RPC line: {trimmed}"))
}

async fn write_websocket_json<S>(ws: &mut WebSocketStream<S>, value: &Value) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    print_outbound(value);
    ws.send(Message::Text(serde_json::to_string(value)?.into()))
        .await
        .context("writing WebSocket JSON-RPC frame")
}

async fn read_websocket_json_with_timeout<S>(
    ws: &mut WebSocketStream<S>,
    timeout: Duration,
) -> anyhow::Result<Value>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = tokio::time::timeout(timeout, ws.next())
            .await
            .map_err(|_| anyhow!("timed out waiting for WebSocket JSON frame"))?
            .ok_or_else(|| anyhow!("WebSocket stream closed by peer"))?
            .context("reading WebSocket frame")?;

        match message {
            Message::Text(text) => {
                return serde_json::from_str(text.as_ref())
                    .with_context(|| format!("decoding WebSocket JSON-RPC text: {text}"));
            }
            Message::Binary(bytes) => {
                let text = std::str::from_utf8(bytes.as_ref())
                    .context("decoding WebSocket binary frame as UTF-8")?;
                return serde_json::from_str(text)
                    .with_context(|| format!("decoding WebSocket JSON-RPC binary text: {text}"));
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(frame) => {
                anyhow::bail!("WebSocket closed by peer: {frame:?}");
            }
        }
    }
}

fn reached_until_method(frame: &Value, until_method: Option<&str>) -> bool {
    until_method.is_some_and(|method| frame.get("method").and_then(Value::as_str) == Some(method))
}

fn print_outbound(value: &Value) {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    eprintln!("→ {pretty}");
}

fn print_inbound(value: &Value) {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    println!("← {pretty}");
}

async fn build_client_endpoint() -> anyhow::Result<Endpoint> {
    let secret = SecretKey::generate();
    Endpoint::builder(presets::N0)
        .secret_key(secret)
        .alpns(vec![ALLEYCAT_ALPN.to_vec()])
        .bind()
        .await
        .context("binding probe client endpoint")
}

fn short_token(token: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(&Sha256::digest(token.as_bytes())[..4])
}
