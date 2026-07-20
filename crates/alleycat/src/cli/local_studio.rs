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
    /// Grant protocol-v1 stats.read to one authenticated Iroh endpoint.
    Grant {
        endpoint_id: String,
        /// Optional future UTC RFC3339 expiry, for example 2026-07-21T12:00:00Z.
        #[arg(long)]
        expires_at: Option<String>,
    },
    /// Revoke protocol-v1 stats.read for the endpoint immediately.
    Revoke { endpoint_id: String },
}

pub async fn run(args: LocalStudioArgs) -> anyhow::Result<()> {
    cli::ensure_current_daemon().await?;
    let request = match args.command {
        LocalStudioCommand::List => Request::LocalStudioGrantsList,
        LocalStudioCommand::Grant {
            endpoint_id,
            expires_at,
        } => Request::LocalStudioGrantStatsRead {
            endpoint_id,
            expires_at,
        },
        LocalStudioCommand::Revoke { endpoint_id } => {
            Request::LocalStudioRevokeStatsRead { endpoint_id }
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
