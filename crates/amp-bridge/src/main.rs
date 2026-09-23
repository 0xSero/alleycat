use std::path::PathBuf;

use alleycat_amp_bridge::AmpBridge;
use alleycat_bridge_core::serve_stdio;
#[cfg(unix)]
use alleycat_bridge_core::{ServerOptions, serve_unix};

enum Transport {
    Socket(PathBuf),
    Stdio,
}

fn transport_from_env_or_args() -> Transport {
    if let Some(path) = std::env::var_os("ALLEYCAT_BRIDGE_SOCKET") {
        return Transport::Socket(PathBuf::from(path));
    }
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--socket" || arg == "--listen" {
            if let Some(path) = args.next() {
                return Transport::Socket(PathBuf::from(path));
            }
        } else if arg == "--stdio" {
            return Transport::Stdio;
        }
    }
    Transport::Stdio
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let bridge = AmpBridge::builder().from_env().build().await?;

    match transport_from_env_or_args() {
        Transport::Socket(path) => {
            #[cfg(unix)]
            {
                serve_unix(
                    bridge,
                    ServerOptions {
                        socket_path: path,
                        unlink_stale: true,
                    },
                )
                .await
            }
            #[cfg(not(unix))]
            {
                let _ = bridge;
                anyhow::bail!(
                    "Unix socket transport is not supported on Windows: {}",
                    path.display()
                );
            }
        }
        Transport::Stdio => serve_stdio(bridge).await,
    }
}
