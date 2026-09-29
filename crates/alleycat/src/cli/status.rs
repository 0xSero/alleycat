use clap::Args;

use crate::agent_manifest::MANIFESTS;
use crate::cli;
use crate::daemon::control::{Request, StatusInfo, token_fingerprint};
use crate::ipc;
use crate::paths;
use crate::protocol::AgentInfo;

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Emit machine-readable JSON instead of the human summary.
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: StatusArgs) -> anyhow::Result<()> {
    let info = if ipc::is_daemon_running().await {
        let resp = cli::send(Request::Status).await?;
        cli::decode_data::<StatusInfo>(resp)?
    } else {
        offline_status().await?
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&info)?);
        return Ok(());
    }

    println!("{} daemon", crate::binary_name());
    println!("  pid:               {}", info.pid);
    println!(
        "  version:           {}",
        info.version.as_deref().unwrap_or("<unknown>")
    );
    println!("  node id:           {}", info.node_id);
    println!("  token (sha256/16): {}", info.token_short);
    println!(
        "  relay:             {}",
        info.relay.as_deref().unwrap_or("<iroh default>")
    );
    println!("  config:            {}", info.config_path);
    if info.uptime_secs > 0 {
        println!("  uptime (s):        {}", info.uptime_secs);
    } else {
        println!("  uptime (s):        <daemon not running>");
    }
    println!("  agents:");
    for agent in &info.agents {
        println!(
            "    {} display=\"{}\" wire={} available={}",
            agent.name,
            agent.display_name,
            agent.wire.as_str(),
            agent.available
        );
    }
    Ok(())
}

/// Status when the daemon isn't running. Pid is 0 and uptime is 0 so the
/// human renderer can call out the offline state.
async fn offline_status() -> anyhow::Result<StatusInfo> {
    let cfg = crate::config::load_or_init().await?;
    let secret_key = crate::state::load_or_create_secret_key().await?;
    Ok(StatusInfo {
        pid: 0,
        node_id: secret_key.public().to_string(),
        token_short: token_fingerprint(&cfg.token),
        relay: cfg.relay.clone(),
        config_path: paths::host_config_file()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "<unknown>".to_string()),
        uptime_secs: 0,
        agents: offline_agents(),
        version: Some(crate::binary_version().to_string()),
    })
}

/// Offline means unavailable through this daemon, not uninstalled or disabled.
/// Only the running daemon can establish actual runtime availability. Status
/// must never initialize bridges, scan history, or spawn probes/prewarm workers.
/// This CLI-only snapshot does not update the persisted agents configuration.
fn offline_agents() -> Vec<AgentInfo> {
    MANIFESTS
        .iter()
        .map(|manifest| AgentInfo {
            name: manifest.name.to_owned(),
            display_name: manifest.display_name.to_owned(),
            wire: manifest.wire.clone(),
            available: false,
            presentation: Some(manifest.presentation()),
            capabilities: Some(manifest.capabilities()),
            local_studio: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_inventory_retains_capabilities_without_claiming_runtime_availability() {
        let agents = offline_agents();
        assert_eq!(agents.len(), MANIFESTS.len());
        for (agent, manifest) in agents.iter().zip(MANIFESTS) {
            assert_eq!(agent.name, manifest.name);
            assert_eq!(agent.display_name, manifest.display_name);
            assert_eq!(agent.wire, manifest.wire);
            assert!(
                !agent.available,
                "offline runtime {} is unavailable",
                agent.name
            );
            assert_eq!(agent.presentation, Some(manifest.presentation()));
            assert_eq!(agent.capabilities, Some(manifest.capabilities()));
        }
        assert!(agents.iter().any(|agent| agent.name == "omp"));
    }
}
