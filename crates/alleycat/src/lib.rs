//! Alleycat daemon library entry point. Binary crates (this repo's `alleycat`
//! and the `kittylitter` distribution wrapper in the litter repo) call
//! [`run`] with their CLI display name.

mod agent_manifest;
mod agents;
mod cli;
mod config;
mod daemon;
mod framing;
pub mod grants;
mod host;
mod ipc;
mod local_studio;
pub mod paths;
mod protocol;
mod service;
mod state;
mod stream;
#[cfg(test)]
mod test_support;

use std::sync::OnceLock;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

/// Branding + OS-identity supplied by the binary that drives this library.
/// The shipped `kittylitter` wrapper passes `com.sigkitten.kittylitter`
/// values; this crate's own dev `alleycat` binary passes alleycat-style
/// defaults. Tests fall through to [`App::DEFAULT`] so existing assertions
/// keep working.
#[derive(Clone, Copy, Debug)]
pub struct App {
    /// Argv[0]-style program name. Surfaced in `--help` output and in
    /// user-facing error/status strings via [`binary_name`].
    pub binary_name: &'static str,
    /// Reverse-DNS top-level qualifier (e.g. `com`).
    pub qualifier: &'static str,
    /// Reverse-DNS organization fragment as it should appear in
    /// `directories::ProjectDirs` paths (case-sensitive on macOS, e.g.
    /// `sigkitten`).
    pub organization: &'static str,
    /// Application slug (lowercase recommended). Used for state/log dir
    /// names, systemd unit, Windows .lnk, etc.
    pub application: &'static str,
    /// Reverse-DNS label used as the launchd Label, the plist filename
    /// stem, and `service::service_label()` (e.g. `com.sigkitten.kittylitter`).
    pub label: &'static str,
    /// SemVer of the *binary* (the wrapper crate, e.g. `kittylitter` 0.2.1),
    /// not of this library. Reported by the daemon over IPC so a freshly
    /// installed CLI can detect a stale long-running daemon and respawn
    /// itself transparently. Binaries should pass `env!("CARGO_PKG_VERSION")`.
    pub version: &'static str,
}

impl App {
    /// Defaults the library reverts to when no `App` has been registered —
    /// preserves the original alleycat-style identity so tests + dev
    /// invocations of bare `cargo run -p alleycat` Just Work.
    pub const DEFAULT: App = App {
        binary_name: "alleycat",
        qualifier: "dev",
        organization: "Alleycat",
        application: "alleycat",
        label: "dev.alleycat.alleycat",
        version: env!("CARGO_PKG_VERSION"),
    };

    /// Build a tokio runtime, parse CLI args using `self.binary_name` as
    /// the program name, and dispatch to the matching subcommand.
    pub fn run(self) -> anyhow::Result<()> {
        let _ = APP.set(self);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async_main())
    }
}

static APP: OnceLock<App> = OnceLock::new();

/// Currently-registered [`App`], or [`App::DEFAULT`] if none.
pub(crate) fn app() -> &'static App {
    APP.get().unwrap_or(&App::DEFAULT)
}

/// Name the user invoked the binary with. Threaded into user-facing CLI
/// strings so error/help messages reference the right command (`kittylitter`
/// in shipped builds, `alleycat` in dev).
pub fn binary_name() -> &'static str {
    app().binary_name
}

/// SemVer string of the binary that registered this `App`. Used by the
/// daemon to advertise its own version and by the CLI to compare against
/// it before mutating user-visible state.
pub fn binary_version() -> &'static str {
    app().version
}

#[derive(Parser)]
#[command(
    version,
    about = "Iroh-backed bridge that multiplexes local coding agents for paired clients"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the long-running daemon. Owns the iroh endpoint, the persistent
    /// identity, and the IPC control socket.
    Serve,
    /// Install the OS-appropriate autostart entry (launchd / systemd-user /
    /// Windows Startup folder). Does not require admin.
    Install,
    /// Remove the autostart entry.
    Uninstall,
    /// Print the running daemon's status (falls back to file-only when the
    /// daemon isn't running).
    Status(cli::status::StatusArgs),
    /// Print the stable pair payload, optionally with an ASCII QR code.
    Pair(cli::pair::PairArgs),
    /// Mint a fresh token. Node id is preserved.
    Rotate,
    /// Tail the daemon log files under `paths::log_dir()`.
    Logs(cli::logs::LogsArgs),
    /// Stop the running daemon.
    Stop,
    /// Reload `host.toml` live in the running daemon.
    Reload,
    /// Restart the daemon from this binary.
    Restart,
    /// Inspect agents.
    Agents(cli::agents::AgentsArgs),
    /// Manage caller-scoped Local Studio access on this computer.
    LocalStudio(cli::local_studio::LocalStudioArgs),
    /// Install or update the exact Local Studio controller on this computer.
    Studio(cli::studio::StudioArgs),
    /// Connect to the daemon over iroh like a phone client and run JSON-RPC
    /// methods directly. Defaults to invoking `thread/list` on the chosen agent.
    Probe(cli::probe::ProbeArgs),
    /// Restart any running daemon onto the version of *this* binary. Designed
    /// for `npx <wrapper>@latest upgrade` — npm fetches the new tarball, then
    /// this subcommand bounces a stale daemon onto it.
    Upgrade,
}

async fn async_main() -> anyhow::Result<()> {
    let matches = command_for(app()).get_matches();
    let cli = Cli::from_arg_matches(&matches)?;
    match cli.command {
        None => {
            init_cli_logging();
            cli::onboarding::run().await
        }
        Some(Command::Serve) => daemon::run().await,
        Some(Command::Install) => {
            init_cli_logging();
            service::install()?;
            println!("installed.");
            Ok(())
        }
        Some(Command::Uninstall) => {
            init_cli_logging();
            service::uninstall()?;
            println!("uninstalled.");
            Ok(())
        }
        Some(Command::Status(args)) => {
            init_cli_logging();
            cli::status::run(args).await
        }
        Some(Command::Pair(args)) => {
            init_cli_logging();
            cli::pair::run(args).await
        }
        Some(Command::Rotate) => {
            init_cli_logging();
            cli::rotate::run().await
        }
        Some(Command::Logs(args)) => {
            init_cli_logging();
            cli::logs::run(args).await
        }
        Some(Command::Stop) => {
            init_cli_logging();
            cli::stop::run().await
        }
        Some(Command::Reload) => {
            init_cli_logging();
            cli::reload::run().await
        }
        Some(Command::Restart) => {
            init_cli_logging();
            cli::restart_daemon().await?;
            println!("daemon restarted.");
            Ok(())
        }
        Some(Command::Agents(args)) => {
            init_cli_logging();
            cli::agents::run(args).await
        }
        Some(Command::LocalStudio(args)) => {
            init_cli_logging();
            cli::local_studio::run(args).await
        }
        Some(Command::Studio(args)) => {
            init_cli_logging();
            cli::studio::run(args).await
        }
        Some(Command::Probe(args)) => {
            init_cli_logging();
            cli::probe::run(args).await
        }
        Some(Command::Upgrade) => {
            init_cli_logging();
            cli::upgrade::run().await
        }
    }
}

fn command_for(app: &App) -> clap::Command {
    Cli::command().name(app.binary_name).version(app.version)
}

fn init_cli_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("warn,alleycat=info,iroh=error,noq=error,noq_udp=error,quinn=error")
        }))
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod cli_version_tests {
    use super::*;

    #[test]
    fn wrapper_version_overrides_library_package_version() {
        let wrapper = App {
            binary_name: "kittylitter",
            qualifier: "com",
            organization: "sigkitten",
            application: "kittylitter",
            label: "com.sigkitten.kittylitter",
            version: "9.8.7",
        };
        assert_eq!(
            command_for(&wrapper).render_version(),
            "kittylitter 9.8.7\n"
        );
    }
}
