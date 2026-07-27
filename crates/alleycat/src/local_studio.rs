//! Local Studio runtime resolution.
//!
//! Local Studio is an Electron desktop app that bundles its own Pi agent
//! runtime under a private data directory. This module resolves that
//! directory and the Pi CLI binary so the daemon can expose Local Studio
//! as its own scoped [`PiBridge`] — separate from the user's standalone
//! `pi` runtime — reusing the same bridge infrastructure.
//!
//! The running Local Studio instance publishes `litter-bridge.json` in its
//! data directory. That file is authoritative: it carries the agent data
//! dir (`piAgentDir`) and the resolved Pi launch command (`piRuntime`) for
//! the version that is actually running. We prefer it over guessing an
//! install path, which can pick a stale `/Applications` bundle.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use reqwest::Url;
use serde::Deserialize;

const PROTOCOL_VERSION: u32 = 1;
const SECRET_HEADER: &str = "x-local-studio-litter-bridge-secret";
const METADATA_FILE: &str = "litter-bridge.json";
const MAX_METADATA_BYTES: u64 = 1 << 20;
const PI_PACKAGE_CLI: &str =
    "frontend/node_modules/@earendil-works/pi-coding-agent/dist/cli.js";

/// The resolved Pi launch command for a Local Studio install.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PiRuntimeCommand {
    pub program: PathBuf,
    pub prefix_args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetadataFile {
    protocol_version: u32,
    url: String,
    secret_header: String,
    secret: String,
    #[allow(dead_code)]
    controller_id: String,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pid: u32,
    issued_at: String,
    #[serde(default)]
    pi_agent_dir: Option<String>,
    #[serde(default)]
    pi_runtime: Option<PiRuntimeDescriptor>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PiRuntimeDescriptor {
    program: String,
    args: Vec<String>,
    #[serde(default)]
    env: std::collections::BTreeMap<String, String>,
}

/// Resolve Local Studio's Pi agent data directory. Returns `None` when no
/// Local Studio install is found or the metadata is invalid.
pub(crate) fn pi_agent_dir() -> Option<PathBuf> {
    let metadata = load_metadata().ok()?;
    if let Some(dir) = metadata.pi_agent_dir
        && PathBuf::from(&dir).join("models.json").is_file()
    {
        return Some(PathBuf::from(dir));
    }
    metadata
        .path
        .parent()
        .map(|data_dir| data_dir.join("pi-agent"))
        .filter(|dir| dir.join("models.json").is_file())
}

/// Resolve the Pi CLI binary Local Studio bundles. Returns `None` when no
/// runtime can be found.
pub(crate) fn bundled_pi_runtime() -> Option<PiRuntimeCommand> {
    let metadata = load_metadata().ok();

    // Prefer the running instance's published runtime — it is authoritative
    // for the version that actually owns the data dir.
    if let Some(m) = metadata.as_ref()
        && let Some(desc) = &m.pi_runtime
        && let Some(cmd) = runtime_from_descriptor(desc)
    {
        return Some(cmd);
    }

    // Linux fallback: resolve the running process's own bundle from its pid.
    #[cfg(target_os = "linux")]
    if let Some(m) = metadata.as_ref() {
        let proc_root = PathBuf::from(format!("/proc/{}", m.pid));
        if let (Ok(exe), Ok(cwd)) = (
            std::fs::read_link(proc_root.join("exe")),
            std::fs::read_link(proc_root.join("cwd")),
        ) {
            for root in [cwd.clone(), cwd.join("../.."), cwd.join("../../..")] {
                if let Some(cmd) = runtime_command(
                    exe.clone(),
                    root.join(PI_PACKAGE_CLI),
                    Vec::new(),
                ) {
                    return Some(cmd);
                }
            }
        }
    }

    // Last resort: scan known macOS install locations.
    let base = directories::BaseDirs::new()?;
    for root in [
        PathBuf::from("/Applications/Local Studio.app"),
        base.home_dir().join("Applications/Local Studio.app"),
        PathBuf::from("/Applications/Local Studio Dev.app"),
        base.home_dir().join("Applications/Local Studio Dev.app"),
        PathBuf::from("/Applications/vLLM Studio.app"),
        base.home_dir().join("Applications/vLLM Studio.app"),
    ] {
        if let Some(cmd) = runtime_from_app_bundle(&root) {
            return Some(cmd);
        }
    }
    None
}

fn runtime_from_app_bundle(root: &Path) -> Option<PiRuntimeCommand> {
    let cli = root
        .join("Contents/Resources/app/frontend/.next/standalone")
        .join(PI_PACKAGE_CLI);
    let stem = root.file_stem()?.to_str()?;
    for executable in [stem, "Local Studio", "vLLM Studio"] {
        if let Some(cmd) = runtime_command(
            root.join("Contents/MacOS").join(executable),
            cli.clone(),
            vec![(OsString::from("ELECTRON_RUN_AS_NODE"), OsString::from("1"))],
        ) {
            return Some(cmd);
        }
    }
    None
}

fn runtime_from_descriptor(desc: &PiRuntimeDescriptor) -> Option<PiRuntimeCommand> {
    let program = PathBuf::from(&desc.program);
    if !executable_file(&program) {
        return None;
    }
    let prefix_args: Vec<OsString> = desc.args.iter().map(OsString::from).collect();
    let env = desc
        .env
        .iter()
        .map(|(k, v)| (OsString::from(k), OsString::from(v)))
        .collect();
    Some(PiRuntimeCommand {
        program,
        prefix_args,
        env,
    })
}

fn runtime_command(
    program: PathBuf,
    cli: PathBuf,
    env: Vec<(OsString, OsString)>,
) -> Option<PiRuntimeCommand> {
    if !executable_file(&program) || !cli.is_file() {
        return None;
    }
    let cli = cli.canonicalize().ok()?;
    Some(PiRuntimeCommand {
        program,
        prefix_args: vec![cli.into_os_string()],
        env,
    })
}

fn executable_file(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        return metadata.permissions().mode() & 0o111 != 0;
    }
    #[cfg(not(unix))]
    {
        true
    }
}

struct LoadedMetadata {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pid: u32,
    pi_agent_dir: Option<String>,
    pi_runtime: Option<PiRuntimeDescriptor>,
    path: PathBuf,
}

fn load_metadata() -> anyhow::Result<LoadedMetadata> {
    let path = resolve_metadata_path()?;
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading Local Studio metadata {}", path.display()))?;
    if raw.len() as u64 > MAX_METADATA_BYTES {
        bail!("Local Studio metadata exceeds {} bytes", MAX_METADATA_BYTES);
    }
    let parsed: MetadataFile = serde_json::from_str(&raw).context("parsing Local Studio metadata")?;
    if parsed.protocol_version != PROTOCOL_VERSION {
        bail!("Local Studio metadata uses an unsupported protocol version");
    }
    if parsed.secret_header != SECRET_HEADER {
        bail!("Local Studio metadata names an unsupported secret header");
    }
    let url = Url::parse(&parsed.url).context("parsing Local Studio gateway URL")?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.path() != "/api/litter-bridge/v1"
    {
        bail!("Local Studio metadata URL must be http://127.0.0.1:<port>/api/litter-bridge/v1");
    }
    if parsed.secret.len() < 32 || parsed.secret.len() > 512 || parsed.secret.trim() != parsed.secret {
        bail!("Local Studio metadata secret is malformed");
    }
    if !is_utc_rfc3339(&parsed.issued_at) {
        bail!("Local Studio metadata issuedAt must be a UTC RFC3339 timestamp");
    }
    Ok(LoadedMetadata {
        pid: parsed.pid,
        pi_agent_dir: parsed.pi_agent_dir,
        pi_runtime: parsed.pi_runtime,
        path,
    })
}

fn resolve_metadata_path() -> anyhow::Result<PathBuf> {
    if let Some(value) = std::env::var_os("LOCAL_STUDIO_LITTER_BRIDGE_FILE") {
        return Ok(PathBuf::from(value));
    }
    if let Some(data_dir) = std::env::var_os("LOCAL_STUDIO_DATA_DIR") {
        return Ok(PathBuf::from(data_dir).join(METADATA_FILE));
    }
    let base = directories::BaseDirs::new()
        .ok_or_else(|| anyhow!("could not determine home directory"))?;
    // Prefer the running instance: the Electron app writes under its
    // Application Support dir; dev builds use a dot-directory.
    for candidate in [
        base.data_dir().join("Local Studio").join(METADATA_FILE),
        base.data_dir().join("Local Studio Dev").join(METADATA_FILE),
        base.home_dir().join(".local-studio").join(METADATA_FILE),
    ] {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(anyhow!("no Local Studio metadata file found"))
}

fn is_utc_rfc3339(value: &str) -> bool {
    value.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn app_bundle_runtime_resolves_electron_node_mode() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Local Studio.app");
        let exe = app.join("Contents/MacOS/Local Studio");
        std::fs::create_dir_all(exe.parent().unwrap()).unwrap();
        std::fs::write(&exe, "#!/bin/sh\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cli = app
            .join("Contents/Resources/app/frontend/.next/standalone")
            .join(PI_PACKAGE_CLI);
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&cli, "export {};\n").unwrap();

        let runtime = runtime_from_app_bundle(&app).unwrap();
        assert_eq!(runtime.program, exe);
        assert_eq!(
            runtime.prefix_args,
            vec![cli.canonicalize().unwrap().into_os_string()]
        );
        assert_eq!(
            runtime
                .env
                .iter()
                .find(|(k, _)| k == "ELECTRON_RUN_AS_NODE")
                .map(|(_, v)| v),
            Some(&OsString::from("1"))
        );
    }
}
