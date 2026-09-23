use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum SpawnMode {
    #[default]
    SameDir,
    Worktree,
    /// Single-session/classic mode. Claude's UI also calls this `session`.
    Session,
}

impl SpawnMode {
    pub fn capacity_floor(self) -> usize {
        match self {
            Self::Session => 1,
            Self::SameDir | Self::Worktree => 1,
        }
    }

    pub fn is_single_session(self) -> bool {
        self == Self::Session
    }
}

#[derive(Debug, Clone, Parser, PartialEq, Eq, Serialize, Deserialize)]
#[command(
    name = "remote-control",
    alias = "rc",
    about = "Connect your local environment for remote-control sessions via claude.ai/code"
)]
pub struct RemoteControlArgs {
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long)]
    pub remote_control_session_name_prefix: Option<String>,
    #[arg(long)]
    pub permission_mode: Option<String>,
    #[arg(long)]
    pub debug_file: Option<PathBuf>,
    #[arg(short = 'd', long = "debug", alias = "d2e", alias = "debug-to-stderr")]
    pub debug_to_stderr: bool,
    #[arg(short, long)]
    pub verbose: bool,
    #[arg(long, value_enum)]
    pub spawn: Option<SpawnMode>,
    #[arg(long)]
    pub capacity: Option<usize>,
    #[arg(long = "create-session-in-dir", default_value_t = true, action = clap::ArgAction::Set)]
    pub create_session_in_dir: bool,
    #[arg(long)]
    pub session_id: Option<String>,
    #[arg(long = "continue")]
    pub continue_most_recent: bool,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub sandbox: bool,
}

impl RemoteControlArgs {
    pub fn parse_remote_control_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let args = args.into_iter().map(Into::into).map(normalize_no_flag);
        Self::try_parse_from(args)
    }

    pub fn resolved_spawn_mode(&self, project_default: Option<SpawnMode>) -> SpawnMode {
        if self.session_id.is_some() || self.continue_most_recent {
            SpawnMode::Session
        } else {
            self.spawn.or(project_default).unwrap_or_default()
        }
    }

    pub fn resolved_capacity(&self, project_default: Option<SpawnMode>) -> usize {
        let mode = self.resolved_spawn_mode(project_default);
        if mode.is_single_session() {
            return 1;
        }
        self.capacity.unwrap_or(1).max(mode.capacity_floor())
    }
}

fn normalize_no_flag(arg: OsString) -> OsString {
    if arg == "--no-create-session-in-dir" {
        OsString::from("--create-session-in-dir=false")
    } else if arg == "--no-sandbox" {
        OsString::from("--sandbox=false")
    } else if arg == "-d2e" {
        OsString::from("--debug")
    } else {
        arg
    }
}

pub const REMOTE_CONTROL_COMMAND_ALIASES: &[&str] =
    &["remote-control", "rc", "remote", "sync", "bridge"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_remote_control_surface() {
        let args = RemoteControlArgs::parse_remote_control_from([
            "remote-control",
            "--name",
            "Studio",
            "--remote-control-session-name-prefix",
            "host",
            "--permission-mode",
            "acceptEdits",
            "--debug-file",
            "/tmp/rc.log",
            "--debug-to-stderr",
            "--verbose",
            "--spawn",
            "worktree",
            "--capacity",
            "4",
            "--no-create-session-in-dir",
            "--no-sandbox",
        ])
        .unwrap();
        assert_eq!(args.name.as_deref(), Some("Studio"));
        assert_eq!(
            args.remote_control_session_name_prefix.as_deref(),
            Some("host")
        );
        assert_eq!(args.permission_mode.as_deref(), Some("acceptEdits"));
        assert!(args.debug_to_stderr);
        assert_eq!(args.spawn, Some(SpawnMode::Worktree));
        assert_eq!(args.resolved_capacity(None), 4);
        assert!(!args.create_session_in_dir);
        assert!(!args.sandbox);
    }

    #[test]
    fn session_id_forces_single_session() {
        let args = RemoteControlArgs::parse_remote_control_from([
            "remote-control",
            "--spawn",
            "worktree",
            "--capacity",
            "4",
            "--session-id",
            "sid",
        ])
        .unwrap();
        assert_eq!(args.resolved_spawn_mode(None), SpawnMode::Session);
        assert_eq!(args.resolved_capacity(None), 1);
    }

    #[test]
    fn supports_claude_debug_stderr_alias() {
        let args =
            RemoteControlArgs::parse_remote_control_from(["remote-control", "-d2e"]).unwrap();
        assert!(args.debug_to_stderr);
    }
}
