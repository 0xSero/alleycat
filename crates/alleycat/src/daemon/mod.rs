//! Long-running daemon process for `alleycat serve`.
//!
//! Owns the single-instance file lock, the persistent iroh secret + token,
//! the iroh endpoint serving agent streams, and the IPC control listener
//! used by the CLI subcommands (`status`/`pair`/`rotate`/`reload`/`stop`/
//! `agents`).

pub mod control;
pub mod logging;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, anyhow};
use arc_swap::ArcSwap;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;

use crate::agents::AgentManager;
use crate::config::HostConfig;
use crate::framing::{read_json_frame, write_json_frame};
use crate::host;
use crate::ipc::{ControlListener, ControlStream};
use crate::paths;
use crate::state;

use self::control::{Request, Response, RotateResult, StatusInfo, token_fingerprint};

/// Entry point for `alleycat serve`. Initializes file logging, acquires the
/// single-instance lock, binds the iroh endpoint + control IPC, and runs
/// until SIGTERM / SIGINT / control `Stop`.
pub async fn run() -> anyhow::Result<()> {
    let log_dir = paths::log_dir().context("locating log directory")?;
    let _log_guard: WorkerGuard =
        logging::init("info", &log_dir).context("initializing logging")?;

    let mut lock = state::acquire_lock().await.context("acquiring lock file")?;
    let _lock_guard = lock.try_write().map_err(|_| {
        let pid_hint = state::read_pid_file().unwrap_or(None);
        let name = crate::binary_name();
        match pid_hint {
            Some(pid) => anyhow!(
                "another {name} daemon is already running (pid {pid}). use `{name} stop` first."
            ),
            None => {
                anyhow!("another {name} daemon is already running. use `{name} stop` first.")
            }
        }
    })?;

    let pid_path = state::write_pid_file().context("writing pid file")?;
    let _pid_cleanup = RemoveOnDrop(pid_path);

    let initial_config = crate::config::load_or_init()
        .await
        .context("loading host config")?;
    let config = Arc::new(ArcSwap::from_pointee(initial_config));

    let secret_key = state::load_or_create_secret_key()
        .await
        .context("loading host secret key")?;
    let node_id = secret_key.public().to_string();
    info!(node_id = %node_id, "loaded persistent identity");

    let endpoint = host::bind_endpoint(secret_key.clone()).await?;
    let agents = AgentManager::new(Arc::clone(&config))
        .await
        .context("initializing agent manager")?;

    let started_at = Instant::now();
    let shutdown = Arc::new(Notify::new());

    let serve_task = {
        let endpoint = endpoint.clone();
        let agents = agents.clone();
        let config = Arc::clone(&config);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            if let Err(error) = host::accept_loop(endpoint, agents, config, shutdown).await {
                error!("iroh accept loop ended: {error:#}");
            }
        })
    };

    let listener = ControlListener::bind()
        .await
        .context("binding control listener")?;
    info!("control socket listening");

    let daemon = Arc::new(DaemonState {
        config: Arc::clone(&config),
        agents,
        secret_key,
        endpoint: endpoint.clone(),
        node_id,
        started_at,
        shutdown: Arc::clone(&shutdown),
    });

    let accept_task = tokio::spawn(accept_loop(Arc::clone(&daemon), listener));

    wait_for_signal(Arc::clone(&shutdown)).await;
    info!("shutdown initiated");

    accept_task.abort();
    let _ = accept_task.await;

    let serve_abort = serve_task.abort_handle();
    match tokio::time::timeout(std::time::Duration::from_secs(5), serve_task).await {
        Ok(_) => {}
        Err(_) => {
            warn!("iroh shutdown did not complete within 5s; aborting");
            serve_abort.abort();
        }
    }

    // Kill agent child processes (ACP, claude, …) deterministically.
    // Without this, only tokio's `kill_on_drop` keeps them honest, and
    // that's racy on process exit — between restarts we'd accumulate
    // orphaned `devin acp` / `grok agent stdio` children until manual `pkill`.
    info!("shutting down bridges");
    daemon.agents.shutdown().await;

    info!("daemon exited cleanly");
    Ok(())
}

struct DaemonState {
    config: Arc<ArcSwap<HostConfig>>,
    agents: AgentManager,
    secret_key: iroh::SecretKey,
    endpoint: iroh::Endpoint,
    node_id: String,
    started_at: Instant,
    shutdown: Arc<Notify>,
}

async fn accept_loop(daemon: Arc<DaemonState>, mut listener: ControlListener) {
    loop {
        match listener.accept().await {
            Ok(stream) => {
                let daemon = Arc::clone(&daemon);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(daemon, stream).await {
                        debug!("control connection ended: {error:#}");
                    }
                });
            }
            Err(error) => {
                warn!("control accept error: {error:#}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_connection(
    daemon: Arc<DaemonState>,
    mut stream: Box<dyn ControlStream>,
) -> anyhow::Result<()> {
    let request: Request = read_json_frame(&mut stream)
        .await
        .context("reading request")?;
    debug!(?request, "control request");
    let (response, after) = dispatch(Arc::clone(&daemon), request).await;
    write_json_frame(&mut stream, &response)
        .await
        .context("writing response")?;
    drop(stream);
    if let Some(PostResponse::Shutdown) = after {
        daemon.shutdown.notify_waiters();
    }
    Ok(())
}

enum PostResponse {
    Shutdown,
}

async fn dispatch(daemon: Arc<DaemonState>, request: Request) -> (Response, Option<PostResponse>) {
    match request {
        Request::Status => (handle_status(&daemon).await, None),
        Request::Pair => (handle_pair(&daemon).await, None),
        Request::Rotate => (handle_rotate(&daemon).await, None),
        Request::Reload => (handle_reload(&daemon).await, None),
        Request::AgentsList => (handle_agents_list(&daemon).await, None),
        Request::Stop => (Response::ok(), Some(PostResponse::Shutdown)),
    }
}

async fn handle_status(daemon: &DaemonState) -> Response {
    let cfg = daemon.config.load();
    let info = StatusInfo {
        pid: std::process::id(),
        node_id: daemon.node_id.clone(),
        token_short: token_fingerprint(&cfg.token),
        relay: host::endpoint_home_relay(Some(&daemon.endpoint)).or_else(|| cfg.relay.clone()),
        config_path: paths::host_config_file()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "<unknown>".to_string()),
        uptime_secs: daemon.started_at.elapsed().as_secs(),
        agents: daemon.agents.list_agents().await,
        version: Some(crate::binary_version().to_string()),
    };
    Response::ok_with(&info).unwrap_or_else(|e| Response::err(e.to_string()))
}

async fn handle_pair(daemon: &DaemonState) -> Response {
    wait_for_relay(&daemon.endpoint).await;
    let cfg = daemon.config.load();
    let payload = host::pair_payload(&daemon.secret_key, &cfg, Some(&daemon.endpoint));
    Response::ok_with(&payload).unwrap_or_else(|e| Response::err(e.to_string()))
}

async fn handle_rotate(daemon: &DaemonState) -> Response {
    wait_for_relay(&daemon.endpoint).await;
    let new_cfg = match crate::config::rotate_token().await {
        Ok(c) => c,
        Err(error) => return Response::err(format!("rotate failed: {error:#}")),
    };
    let token_short = token_fingerprint(&new_cfg.token);
    let payload = host::pair_payload(&daemon.secret_key, &new_cfg, Some(&daemon.endpoint));
    daemon.config.store(Arc::new(new_cfg));
    Response::ok_with(&RotateResult {
        token_short,
        payload,
    })
    .unwrap_or_else(|e| Response::err(e.to_string()))
}

/// Best-effort wait for the iroh endpoint to bind a home relay before
/// serializing a pair payload. Without this, a freshly-spawned daemon will
/// happily emit a QR with `relay: null` because the relay-probe task hasn't
/// completed yet — and on networks where pkarr/DNS publishing is broken
/// (Tailscale, IPv6-only relays), that QR is undialable. 8s matches the
/// existing online-probe timeout in `host::bind_endpoint`. If the network
/// truly has no relay path, we fall through and emit whatever we have.
async fn wait_for_relay(endpoint: &iroh::Endpoint) {
    let _ = tokio::time::timeout(std::time::Duration::from_secs(8), endpoint.online()).await;
}

async fn handle_reload(daemon: &DaemonState) -> Response {
    let new_cfg = match crate::config::load_or_init().await {
        Ok(c) => c,
        Err(error) => return Response::err(format!("loading config: {error:#}")),
    };
    daemon.config.store(Arc::new(new_cfg));
    Response::ok()
}

async fn handle_agents_list(daemon: &DaemonState) -> Response {
    let agents = daemon.agents.list_agents().await;
    Response::ok_with(&agents).unwrap_or_else(|e| Response::err(e.to_string()))
}

async fn wait_for_signal(shutdown: Arc<Notify>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = term.recv() => info!("received SIGTERM"),
            _ = int.recv() => info!("received SIGINT"),
            _ = shutdown.notified() => info!("received Stop request"),
        }
    }
    #[cfg(windows)]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("received Ctrl-C"),
            _ = shutdown.notified() => info!("received Stop request"),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        shutdown.notified().await;
    }
    // Fan the shutdown notification out to other tasks (notably the iroh
    // accept loop, which is also parked on this Notify). Idempotent — if
    // we returned because of a Stop request the IPC handler already called
    // notify_waiters(); calling it again here just sees an empty wait list.
    shutdown.notify_waiters();
}

struct RemoveOnDrop(PathBuf);
impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
