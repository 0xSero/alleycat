use alleycat_local_studio_proto::Capability;
use anyhow::bail;
use clap::{Args, Subcommand};

use crate::cli;
use crate::daemon::control::Request;
use crate::grants::PairedNodesDocument;

#[derive(Args, Debug)]
pub struct LocalStudioArgs {
    #[command(subcommand)]
    pub command: LocalStudioCommand,
}

#[derive(Subcommand, Debug)]
pub enum LocalStudioCommand {
    /// List paired endpoint grants without exposing gateway credentials.
    List,
    /// Grant explicit protocol-v1 capabilities to one authenticated Iroh endpoint.
    Grant {
        endpoint_id: String,
        /// Capability to grant. Repeat for more than one; defaults only to stats.read.
        #[arg(long = "capability", value_name = "CAPABILITY", value_parser = parse_capability)]
        capabilities: Vec<Capability>,
        /// Optional future UTC RFC3339 expiry, for example 2026-07-21T12:00:00Z.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// Revoke explicit protocol-v1 capabilities for the endpoint immediately.
    Revoke {
        endpoint_id: String,
        /// Capability to revoke. Repeat for more than one; defaults only to stats.read.
        #[arg(long = "capability", value_name = "CAPABILITY", value_parser = parse_capability)]
        capabilities: Vec<Capability>,
    },
}

pub async fn run(args: LocalStudioArgs) -> anyhow::Result<()> {
    cli::ensure_current_daemon().await?;
    let request = match args.command {
        LocalStudioCommand::List => Request::LocalStudioGrantsList,
        LocalStudioCommand::Grant {
            endpoint_id,
            capabilities,
            expires_at,
        } => {
            let capabilities = selected_capabilities(capabilities)?;
            if capabilities == [Capability::StatsRead] {
                Request::LocalStudioGrantStatsRead {
                    endpoint_id,
                    expires_at,
                }
            } else {
                Request::LocalStudioGrantCapabilities {
                    endpoint_id,
                    capabilities,
                    expires_at,
                }
            }
        }
        LocalStudioCommand::Revoke {
            endpoint_id,
            capabilities,
        } => {
            let capabilities = selected_capabilities(capabilities)?;
            if capabilities == [Capability::StatsRead] {
                Request::LocalStudioRevokeStatsRead { endpoint_id }
            } else {
                Request::LocalStudioRevokeCapabilities {
                    endpoint_id,
                    capabilities,
                }
            }
        }
    };
    let document: PairedNodesDocument = cli::decode_data(cli::send(request).await?)?;
    if document.nodes.is_empty() {
        println!("no Local Studio paired-node grants");
        return Ok(());
    }
    for node in document.nodes {
        let grants = node
            .grants
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let actions = node
            .actions
            .iter()
            .map(|action| action.kind().to_string())
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{endpoint}\tgrants={grants}\tactions={actions}\texpires={expires}\trevoked={revoked}",
            endpoint = node.endpoint_id,
            expires = node
                .expires_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "never".into()),
            revoked = node
                .revoked_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "no".into()),
        );
    }
    Ok(())
}

fn parse_capability(value: &str) -> Result<Capability, String> {
    match value {
        "stats.read" => Ok(Capability::StatsRead),
        "models.control" => Ok(Capability::ModelsControl),
        "sessions.read" => Ok(Capability::SessionsRead),
        "sessions.write" => Ok(Capability::SessionsWrite),
        "agent.turn" => Ok(Capability::AgentTurn),
        _ => Err(format!(
            "unknown capability {value:?}; expected one of stats.read, models.control, sessions.read, sessions.write, agent.turn"
        )),
    }
}

fn selected_capabilities(mut capabilities: Vec<Capability>) -> anyhow::Result<Vec<Capability>> {
    if capabilities.is_empty() {
        capabilities.push(Capability::StatsRead);
        return Ok(capabilities);
    }
    capabilities.sort_unstable();
    if capabilities.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("each Local Studio capability may be selected only once");
    }
    Ok(capabilities)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_capability_parser_has_no_all_or_admin_escape_hatch() {
        assert_eq!(
            parse_capability("stats.read").unwrap(),
            Capability::StatsRead
        );
        assert_eq!(
            parse_capability("sessions.read").unwrap(),
            Capability::SessionsRead
        );
        assert!(parse_capability("all").is_err());
        assert!(parse_capability("controller.admin").is_err());
    }

    #[test]
    fn omitted_capabilities_default_only_to_stats_read() {
        assert_eq!(
            selected_capabilities(Vec::new()).unwrap(),
            vec![Capability::StatsRead]
        );
        assert!(
            selected_capabilities(vec![Capability::SessionsRead, Capability::SessionsRead])
                .is_err()
        );
    }
}
