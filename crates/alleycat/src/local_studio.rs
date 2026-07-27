use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::Read;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use reqwest::header::HeaderValue;
use reqwest::Url;
use serde::Deserialize;
use url::Host;

const PI_PACKAGE_CLI: &str = "frontend/node_modules/@earendil-works/pi-coding-agent/dist/cli.js";
const GATEWAY_PATH: &str = "/api/litter-bridge/v1";
const GATEWAY_SECRET_HEADER: &str = "x-local-studio-litter-bridge-secret";
const MAX_METADATA_BYTES: u64 = 1 << 20;
const PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PiRuntimeCommand {
    pub program: PathBuf,
    pub prefix_args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayMetadataFile {
    protocol_version: u32,
    url: String,
    secret_header: String,
    secret: String,
    controller_id: String,
    pid: u32,
    issued_at: String,
    /// The running instance's own Pi agent directory. Authoritative when
    /// present — it pins the data dir to the instance that wrote this file.
    #[serde(default)]
    pi_agent_dir: Option<String>,
    /// The running instance's own resolved Pi runtime command. Authoritative
    /// when present — it pins the binary to whatever Local Studio version is
    /// actually running (the latest one launched), instead of guessing a
    /// hardcoded `/Applications` bundle that may be an older install.
    #[serde(default)]
    pi_runtime: Option<PiRuntimeDescriptor>,
}

/// A Pi launch command published by the running Local Studio instance.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PiRuntimeDescriptor {
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: std::collections::BTreeMap<String, String>,
}

struct GatewayMetadata {
    #[allow(dead_code)]
    url: Url,
    #[allow(dead_code)]
    secret: HeaderValue,
    #[allow(dead_code)]
    controller_id: String,
    /// Local Studio's agent-runtime pid. Only the Linux runtime resolver reads
    /// it, via `/proc/<pid>/{exe,cwd}`; macOS resolves through the `.app`
    /// bundle and never touches this.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pid: u32,
    issued_at: String,
    /// Present only when the running instance published it (protocol addition).
    pi_agent_dir: Option<PathBuf>,
    /// Present only when the running instance published it (protocol addition).
    pi_runtime: Option<PiRuntimeCommand>,
}

fn resolve_gateway_metadata_path() -> anyhow::Result<PathBuf> {
    if let Some(path) = env_path("LOCAL_STUDIO_LITTER_BRIDGE_FILE")? {
        return require_absolute(path, "LOCAL_STUDIO_LITTER_BRIDGE_FILE");
    }
    if let Some(data_dir) = env_path("LOCAL_STUDIO_DATA_DIR")? {
        return Ok(require_absolute(data_dir, "LOCAL_STUDIO_DATA_DIR")?.join("litter-bridge.json"));
    }

    let base = directories::BaseDirs::new()
        .ok_or_else(|| anyhow!("could not determine the current user's home directory"))?;
    #[cfg(target_os = "macos")]
    {
        let application_support = base.home_dir().join("Library/Application Support");
        let mut app_candidates = std::fs::read_dir(&application_support)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name == "Local Studio" || name.starts_with("Local Studio "))
            })
            .map(|entry| entry.path().join("litter-bridge.json"))
            .collect::<Vec<_>>();
        app_candidates.sort();

        // "Local Studio" means the running Electron product, not a standalone
        // development sidecar. When more than one app channel is open, follow
        // the newest live descriptor; only fall back to ~/.local-studio when
        // no Electron-owned gateway is active.
        if let Some(path) = newest_live_gateway(app_candidates.iter()) {
            return Ok(path);
        }
        let standalone = base.home_dir().join(".local-studio/litter-bridge.json");
        if gateway_process_is_live(&standalone) {
            return Ok(standalone);
        }
        if let Some(path) = app_candidates.into_iter().find(|path| path.exists()) {
            return Ok(path);
        }
        return Ok(standalone);
    }
    #[cfg(not(target_os = "macos"))]
    {
        let candidates = vec![base.home_dir().join(".local-studio/litter-bridge.json")];
        for candidate in &candidates {
            if candidate.exists() {
                return Ok(candidate.clone());
            }
        }
        candidates
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no Local Studio metadata location is supported"))
    }
}

#[cfg(target_os = "macos")]
fn newest_live_gateway<'a>(paths: impl Iterator<Item = &'a PathBuf>) -> Option<PathBuf> {
    paths
        .filter_map(|path| {
            let metadata = load_gateway_metadata_from_path(path).ok()?;
            process_is_live(metadata.pid).then_some((metadata.issued_at, path.clone()))
        })
        .max_by(|left, right| left.0.cmp(&right.0))
        .map(|(_, path)| path)
}

#[cfg(target_os = "macos")]
fn gateway_process_is_live(path: &Path) -> bool {
    load_gateway_metadata_from_path(path).is_ok_and(|metadata| process_is_live(metadata.pid))
}

#[cfg(unix)]
fn process_is_live(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_live(_pid: u32) -> bool {
    true
}

/// Return the exact Pi agent directory generated by Local Studio when it is
/// present. The models file is the readiness marker: merely having a Local
/// Studio data directory must not redirect a user's standalone Pi install.
pub(crate) fn pi_agent_dir() -> Option<PathBuf> {
    // Prefer the agent dir the running instance published — it is authoritative
    // for the version that actually owns the data — and only fall back to
    // deriving it from the descriptor's location for older builds.
    if let Ok(metadata) = load_gateway_metadata()
        && let Some(agent_dir) = metadata.pi_agent_dir
        && agent_dir.join("models.json").is_file()
    {
        return Some(agent_dir);
    }

    let data_dir = resolve_gateway_metadata_path()
        .ok()?
        .parent()?
        .to_path_buf();
    let agent_dir = data_dir.join("pi-agent");
    agent_dir.join("models.json").is_file().then_some(agent_dir)
}

/// Resolve the Pi CLI shipped with Local Studio. The desktop bundle runs the
/// package through Electron's Node mode; Linux controller installs run the
/// same package through the node process that owns the agent-runtime sidecar.
/// An explicit override is useful for non-standard/package-manager layouts.
pub(crate) fn bundled_pi_runtime() -> Option<PiRuntimeCommand> {
    if let (Some(program), Some(cli)) = (
        absolute_env_path("LOCAL_STUDIO_NODE_BIN"),
        absolute_env_path("LOCAL_STUDIO_PI_CLI"),
    ) {
        return runtime_command(program, cli, Vec::new());
    }
    if let Some(program) = absolute_env_path("LOCAL_STUDIO_PI_BIN")
        && executable_file(&program)
    {
        return Some(PiRuntimeCommand {
            program,
            prefix_args: Vec::new(),
            env: Vec::new(),
        });
    }

    // Prefer the running instance's own published runtime. This binds the pi
    // binary to whatever Local Studio version is actually running — the latest
    // one the user launched — and keeps it consistent with the data dir that
    // same instance owns, instead of a `/Applications` bundle that may be an
    // older, split-brain install.
    let metadata = load_gateway_metadata().ok();
    if let Some(runtime) = metadata.as_ref().and_then(|m| m.pi_runtime.clone()) {
        return Some(runtime);
    }

    // Linux fallback for older descriptors that don't publish `piRuntime`:
    // resolve the running process's own bundle from its pid.
    #[cfg(target_os = "linux")]
    if let Some(metadata) = metadata.as_ref() {
        let proc_root = PathBuf::from(format!("/proc/{}", metadata.pid));
        if let (Ok(exe), Ok(cwd)) = (
            std::fs::read_link(proc_root.join("exe")),
            std::fs::read_link(proc_root.join("cwd")),
        ) && let Some(runtime) = runtime_from_process(&exe, &cwd)
        {
            return Some(runtime);
        }
    }

    // Last resort for when no instance is running or an old build published no
    // runtime: scan the known install locations. This is the path that can pick
    // a stale version, so it runs only after the live instance has been tried.
    let base = directories::BaseDirs::new()?;
    let mut roots = Vec::new();
    if let Some(app_name) = metadata
        .as_ref()
        .and_then(|value| value.pi_agent_dir.as_ref())
        .and_then(|agent_dir| agent_dir.parent())
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .filter(|name| *name == "Local Studio" || name.starts_with("Local Studio "))
    {
        roots.push(PathBuf::from(format!("/Applications/{app_name}.app")));
        roots.push(
            base.home_dir()
                .join("Applications")
                .join(format!("{app_name}.app")),
        );
    }
    roots.extend([
        PathBuf::from("/Applications/Local Studio.app"),
        base.home_dir().join("Applications/Local Studio.app"),
        PathBuf::from("/Applications/vLLM Studio.app"),
        base.home_dir().join("Applications/vLLM Studio.app"),
    ]);
    roots.dedup();
    for root in roots {
        if let Some(runtime) = runtime_from_app_bundle(&root) {
            return Some(runtime);
        }
    }

    None
}

fn runtime_from_app_bundle(root: &Path) -> Option<PiRuntimeCommand> {
    let cli = root
        .join("Contents/Resources/app/frontend/.next/standalone")
        .join(PI_PACKAGE_CLI);
    let inferred_executable = root.file_stem()?.to_str()?;
    for executable in [inferred_executable, "Local Studio", "vLLM Studio"] {
        if let Some(runtime) = runtime_command(
            root.join("Contents/MacOS").join(executable),
            cli.clone(),
            vec![(OsString::from("ELECTRON_RUN_AS_NODE"), OsString::from("1"))],
        ) {
            return Some(runtime);
        }
    }
    None
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn runtime_from_process(program: &Path, cwd: &Path) -> Option<PiRuntimeCommand> {
    for root in [cwd.to_path_buf(), cwd.join("../.."), cwd.join("../../..")] {
        if let Some(runtime) =
            runtime_command(program.to_path_buf(), root.join(PI_PACKAGE_CLI), Vec::new())
        {
            return Some(runtime);
        }
    }
    None
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

fn absolute_env_path(name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os(name)?);
    path.is_absolute().then_some(path)
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
    true
}

fn env_path(name: &str) -> anyhow::Result<Option<PathBuf>> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(None);
    };
    if value.is_empty() {
        bail!("{name} is set but empty");
    }
    Ok(Some(PathBuf::from(value)))
}

fn require_absolute(path: PathBuf, source: &str) -> anyhow::Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{source} must be an absolute path");
    }
    Ok(path)
}

fn load_gateway_metadata() -> anyhow::Result<GatewayMetadata> {
    let path = resolve_gateway_metadata_path()?;
    load_gateway_metadata_from_path(&path)
}

fn load_gateway_metadata_from_path(path: &Path) -> anyhow::Result<GatewayMetadata> {
    let file = open_metadata_file(&path)?;
    let metadata = file
        .metadata()
        .with_context(|| format!("reading metadata for {}", path.display()))?;
    validate_metadata_file(&path, &metadata)?;
    if metadata.len() > MAX_METADATA_BYTES {
        bail!("Local Studio gateway metadata exceeds the size limit");
    }
    let mut raw = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut raw)
        .with_context(|| format!("reading {}", path.display()))?;
    if raw.len() as u64 > MAX_METADATA_BYTES {
        bail!("Local Studio gateway metadata exceeds the size limit");
    }
    let parsed: GatewayMetadataFile =
        serde_json::from_slice(&raw).context("parsing Local Studio gateway metadata")?;
    validate_gateway_metadata(parsed)
}

fn open_metadata_file(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .with_context(|| format!("opening Local Studio gateway metadata {}", path.display()))
}

fn validate_metadata_file(path: &Path, metadata: &std::fs::Metadata) -> anyhow::Result<()> {
    if !metadata.file_type().is_file() {
        bail!("Local Studio gateway metadata is not a regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            bail!("Local Studio gateway metadata must have mode 0600");
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("Local Studio gateway metadata is not owned by the current user");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("secure Local Studio metadata permissions are unsupported on this platform");
    }
    let _ = path;
    Ok(())
}

fn validate_gateway_metadata(parsed: GatewayMetadataFile) -> anyhow::Result<GatewayMetadata> {
    if parsed.protocol_version != PROTOCOL_VERSION {
        bail!("Local Studio gateway metadata uses an unsupported protocol version");
    }
    if parsed.secret_header != GATEWAY_SECRET_HEADER {
        bail!("Local Studio gateway metadata names an unsupported secret header");
    }
    if parsed.secret.len() < 32
        || parsed.secret.len() > 512
        || parsed.secret.trim() != parsed.secret
    {
        bail!("Local Studio gateway metadata contains an invalid secret");
    }
    if parsed.controller_id.is_empty()
        || parsed.controller_id.trim() != parsed.controller_id
        || parsed.controller_id.len() > 512
    {
        bail!("Local Studio gateway metadata contains an invalid controller ID");
    }
    if parsed.pid == 0 {
        bail!("Local Studio gateway metadata contains an invalid process ID");
    }
    if !is_utc_timestamp(&parsed.issued_at) {
        bail!("Local Studio gateway metadata contains an invalid timestamp");
    }

    let url = Url::parse(&parsed.url).context("parsing Local Studio gateway URL")?;
    validate_gateway_url(&url)?;
    let mut secret = HeaderValue::from_str(&parsed.secret)
        .map_err(|_| anyhow!("Local Studio gateway metadata contains an invalid secret"))?;
    secret.set_sensitive(true);
    // The published data dir and runtime are trusted at the same level as the
    // rest of this file (validated 0600, owned by the current user). Still
    // require an absolute program path and reject empty values so a malformed
    // descriptor falls back to discovery rather than launching something odd.
    let pi_agent_dir = parsed.pi_agent_dir.and_then(|dir| {
        let path = PathBuf::from(dir);
        path.is_absolute().then_some(path)
    });
    let pi_runtime = parsed.pi_runtime.and_then(runtime_from_descriptor);

    Ok(GatewayMetadata {
        url,
        secret,
        controller_id: parsed.controller_id,
        pid: parsed.pid,
        issued_at: parsed.issued_at,
        pi_agent_dir,
        pi_runtime,
    })
}

/// Convert a published runtime descriptor into an executable command, or `None`
/// if the program is not an absolute executable file.
fn runtime_from_descriptor(descriptor: PiRuntimeDescriptor) -> Option<PiRuntimeCommand> {
    let program = PathBuf::from(&descriptor.program);
    if !program.is_absolute() || !executable_file(&program) {
        return None;
    }
    Some(PiRuntimeCommand {
        program,
        prefix_args: descriptor.args.into_iter().map(OsString::from).collect(),
        env: descriptor
            .env
            .into_iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value)))
            .collect(),
    })
}

fn validate_gateway_url(url: &Url) -> anyhow::Result<()> {
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != GATEWAY_PATH
        || url.port().is_none()
    {
        bail!("Local Studio gateway URL does not match the fixed loopback route");
    }
    let ip = match url.host() {
        Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
        _ => bail!("Local Studio gateway URL must use a numeric loopback address"),
    };
    if !ip.is_loopback() {
        bail!("Local Studio gateway URL must use a numeric loopback address");
    }
    Ok(())
}

fn is_utc_timestamp(value: &str) -> bool {
    value.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(value).is_ok()
}
#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use serde_json::json;

    use super::*;
    use crate::test_support::TempHome;

    const TEST_SECRET: &str = "test-secret-that-is-at-least-thirty-two-bytes-long";
    const CONTROLLER_ID: &str = "controller-test";

    fn configure_metadata_path(home: &mut TempHome) -> PathBuf {
        let path = home.path().join("litter-bridge.json");
        home.override_env(&[("LOCAL_STUDIO_LITTER_BRIDGE_FILE", path.to_str().unwrap())]);
        path
    }

    fn write_metadata(path: &Path, url: &str) {
        write_metadata_with_issued_at(path, url, "2026-07-20T18:30:00.000Z");
    }

    fn write_metadata_with_issued_at(path: &Path, url: &str, issued_at: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "protocolVersion": 1,
                "url": url,
                "secretHeader": GATEWAY_SECRET_HEADER,
                "secret": TEST_SECRET,
                "controllerId": CONTROLLER_ID,
                "pid": std::process::id(),
                "issuedAt": issued_at
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn executable(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn app_bundle_runtime_uses_electron_node_mode_and_bundled_cli() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Local Studio.app");
        executable(&app.join("Contents/MacOS/Local Studio"));
        let cli = app
            .join("Contents/Resources/app/frontend/.next/standalone")
            .join(PI_PACKAGE_CLI);
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&cli, "export {};\n").unwrap();

        let runtime = runtime_from_app_bundle(&app).unwrap();
        assert_eq!(runtime.program, app.join("Contents/MacOS/Local Studio"));
        assert_eq!(
            runtime.prefix_args,
            vec![cli.canonicalize().unwrap().into_os_string()]
        );
        assert_eq!(
            runtime.env,
            vec![(OsString::from("ELECTRON_RUN_AS_NODE"), OsString::from("1"))]
        );
    }

    #[test]
    fn development_app_bundle_uses_its_matching_executable() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("Local Studio Dev.app");
        executable(&app.join("Contents/MacOS/Local Studio Dev"));
        let cli = app
            .join("Contents/Resources/app/frontend/.next/standalone")
            .join(PI_PACKAGE_CLI);
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&cli, "export {};\n").unwrap();

        let runtime = runtime_from_app_bundle(&app).unwrap();
        assert_eq!(runtime.program, app.join("Contents/MacOS/Local Studio Dev"));
    }

    #[test]
    fn published_runtime_descriptor_is_preferred_over_app_bundle_scan() {
        // A running instance publishes its own runtime. That must win over the
        // static /Applications scan so the binary tracks the version actually
        // running, not a possibly-stale install.
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);

        // The published program: a distinct executable, unrelated to any
        // /Applications bundle.
        let program = home.path().join("running-instance/pi-runner");
        executable(&program);
        let agent_dir = home.path().join("running-instance/pi-agent");
        std::fs::create_dir_all(&agent_dir).unwrap();
        std::fs::write(agent_dir.join("models.json"), "{}").unwrap();

        std::fs::write(
            &metadata,
            serde_json::to_vec_pretty(&json!({
                "protocolVersion": 1,
                "url": "http://127.0.0.1:8081/api/litter-bridge/v1",
                "secretHeader": GATEWAY_SECRET_HEADER,
                "secret": TEST_SECRET,
                "controllerId": CONTROLLER_ID,
                "pid": std::process::id(),
                "issuedAt": "2026-07-20T18:30:00.000Z",
                "piAgentDir": agent_dir.to_str().unwrap(),
                "piRuntime": {
                    "program": program.to_str().unwrap(),
                    "args": ["/opt/pi/dist/cli.js"],
                    "env": { "ELECTRON_RUN_AS_NODE": "1" }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o600)).unwrap();

        let runtime = bundled_pi_runtime().expect("descriptor runtime should resolve");
        assert_eq!(runtime.program, program);
        assert_eq!(
            runtime.prefix_args,
            vec![OsString::from("/opt/pi/dist/cli.js")]
        );
        assert_eq!(
            runtime.env,
            vec![(OsString::from("ELECTRON_RUN_AS_NODE"), OsString::from("1"))]
        );

        // The published agent dir is authoritative too.
        assert_eq!(pi_agent_dir(), Some(agent_dir));
    }

    #[test]
    fn runtime_descriptor_with_relative_or_missing_program_is_ignored() {
        // Relative program → rejected (falls back to discovery).
        assert!(
            runtime_from_descriptor(PiRuntimeDescriptor {
                program: "pi-runner".into(),
                args: vec![],
                env: Default::default(),
            })
            .is_none()
        );
        // Absolute but non-existent → rejected.
        assert!(
            runtime_from_descriptor(PiRuntimeDescriptor {
                program: "/no/such/pi-runner".into(),
                args: vec![],
                env: Default::default(),
            })
            .is_none()
        );
    }

    #[test]
    fn linux_sidecar_layout_resolves_node_and_frontend_cli() {
        let temp = tempfile::tempdir().unwrap();
        let program = temp.path().join("bin/node");
        executable(&program);
        let cwd = temp.path().join("project/services/agent-runtime");
        std::fs::create_dir_all(&cwd).unwrap();
        let cli = temp.path().join("project").join(PI_PACKAGE_CLI);
        std::fs::create_dir_all(cli.parent().unwrap()).unwrap();
        std::fs::write(&cli, "export {};\n").unwrap();

        let runtime = runtime_from_process(&program, &cwd).unwrap();
        assert_eq!(runtime.program, program);
        assert_eq!(
            runtime.prefix_args,
            vec![cli.canonicalize().unwrap().into_os_string()]
        );
        assert!(runtime.env.is_empty());
    }


    #[test]
    fn malformed_or_insecure_metadata_fails_closed() {
        let mut home = TempHome::new();
        let metadata = configure_metadata_path(&mut home);

        write_metadata(&metadata, "http://localhost:54321/api/litter-bridge/v1");
        assert!(load_gateway_metadata().is_err());

        let mut extra =
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(&metadata).unwrap()).unwrap();
        extra["token"] = json!("unexpected");
        std::fs::write(&metadata, serde_json::to_vec(&extra).unwrap()).unwrap();
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(load_gateway_metadata().is_err());

        write_metadata(&metadata, "http://127.0.0.1:54321/api/litter-bridge/v1");
        std::fs::set_permissions(&metadata, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_gateway_metadata().is_err());

        std::fs::remove_file(&metadata).unwrap();
        std::os::unix::fs::symlink(home.path().join("missing-real-metadata"), &metadata).unwrap();
        assert!(load_gateway_metadata().is_err());
    }

    #[test]
    fn explicit_metadata_file_has_precedence_over_data_directory() {
        let mut home = TempHome::new();
        let explicit = home.path().join("explicit.json");
        let data_dir = home.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        home.override_env(&[
            (
                "LOCAL_STUDIO_LITTER_BRIDGE_FILE",
                explicit.to_str().unwrap(),
            ),
            ("LOCAL_STUDIO_DATA_DIR", data_dir.to_str().unwrap()),
        ]);
        assert_eq!(resolve_gateway_metadata_path().unwrap(), explicit);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn running_electron_gateway_wins_over_newer_standalone_sidecar() {
        let home = TempHome::new();
        let electron = home
            .path()
            .join("Library/Application Support/Local Studio Dev/litter-bridge.json");
        let standalone = home.path().join(".local-studio/litter-bridge.json");
        write_metadata_with_issued_at(
            &electron,
            "http://127.0.0.1:54321/api/litter-bridge/v1",
            "2026-07-20T18:30:00.000Z",
        );
        write_metadata_with_issued_at(
            &standalone,
            "http://127.0.0.1:54322/api/litter-bridge/v1",
            "2026-07-20T18:31:00.000Z",
        );

        assert_eq!(resolve_gateway_metadata_path().unwrap(), electron);
    }

}
