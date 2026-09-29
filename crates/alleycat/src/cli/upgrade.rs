//! `<binary> upgrade` — bounce any running daemon onto *this* binary's
//! version. Designed to be invoked as `npx <wrapper>@latest upgrade`: npm
//! fetches the new tarball, then this subcommand stops the stale daemon
//! and respawns it from the current executable so subsequent CLI calls
//! talk to the upgraded binary.

use crate::cli;
use crate::daemon::control::{Request, StatusInfo};
use crate::ipc;

pub async fn run() -> anyhow::Result<()> {
    let cli_version = crate::binary_version();
    let cli_name = crate::binary_name();

    let daemon_version = if ipc::is_daemon_running().await {
        match cli::send(Request::Status).await {
            Ok(resp) => cli::decode_data::<StatusInfo>(resp)
                .ok()
                .and_then(|s| s.version)
                .unwrap_or_else(|| "<unknown>".to_string()),
            Err(_) => "<unreachable>".to_string(),
        }
    } else {
        println!("{cli_name}: no daemon running; starting v{cli_version}...");
        cli::restart_daemon().await?;
        println!("{cli_name}: daemon started at v{cli_version}.");
        return Ok(());
    };

    if cli::daemon_is_current_or_newer(&daemon_version, cli_version) {
        println!(
            "{cli_name}: daemon v{daemon_version} is current or newer than CLI v{cli_version}; nothing to do."
        );
        return Ok(());
    }

    println!("{cli_name}: restarting daemon v{daemon_version} -> v{cli_version}...");
    cli::restart_daemon().await?;
    println!("{cli_name}: daemon now running v{cli_version}.");
    Ok(())
}
