//! `<binary> studio` — delegate controller installation to Local Studio's
//! own installer. KittyLitter intentionally does not carry a second copy of
//! controller setup policy: this command downloads and executes the script
//! shipped by the Local Studio repository.

use std::process::Stdio;

use anyhow::{Context, bail};
use clap::Args;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const INSTALLER_BASE: &str = "https://raw.githubusercontent.com/sybil-solutions/local-studio";
const MAX_INSTALLER_BYTES: usize = 1024 * 1024;

#[derive(Debug, Args)]
pub struct StudioArgs {
    /// Local Studio git ref to install from. Defaults to the app's main branch.
    #[arg(long, default_value = "main")]
    pub r#ref: String,
}

pub async fn run(args: StudioArgs) -> anyhow::Result<()> {
    validate_ref(&args.r#ref)?;
    let url = installer_url(&args.r#ref);
    eprintln!("downloading Local Studio's controller installer from {url}");

    let response = reqwest::get(&url)
        .await
        .with_context(|| format!("downloading {url}"))?
        .error_for_status()
        .with_context(|| format!("downloading {url}"))?;
    let bytes = response
        .bytes()
        .await
        .context("reading Local Studio controller installer")?;
    if bytes.is_empty() || bytes.len() > MAX_INSTALLER_BYTES {
        bail!(
            "Local Studio controller installer has an invalid size ({} bytes)",
            bytes.len()
        );
    }

    #[cfg(unix)]
    {
        let mut child = Command::new("/bin/bash")
            .arg("-s")
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("starting Local Studio controller installer")?;
        child
            .stdin
            .take()
            .context("opening installer stdin")?
            .write_all(&bytes)
            .await
            .context("sending Local Studio installer to bash")?;
        let status = child
            .wait()
            .await
            .context("waiting for Local Studio controller installer")?;
        if !status.success() {
            bail!("Local Studio controller installer exited with {status}");
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _ = bytes;
        bail!("`studio` currently requires macOS or Linux")
    }
}

fn installer_url(reference: &str) -> String {
    format!("{INSTALLER_BASE}/{reference}/scripts/install-controller.sh")
}

fn validate_ref(reference: &str) -> anyhow::Result<()> {
    if reference.is_empty()
        || reference.len() > 128
        || !reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("Local Studio ref must contain only letters, digits, '.', '_' or '-'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installer_url_points_at_local_studio_source() {
        assert_eq!(
            installer_url("v2.1.0"),
            "https://raw.githubusercontent.com/sybil-solutions/local-studio/v2.1.0/scripts/install-controller.sh"
        );
    }

    #[test]
    fn installer_ref_rejects_paths_and_urls() {
        for value in ["", "../main", "feature/branch", "https://example.com/x"] {
            assert!(validate_ref(value).is_err(), "accepted {value}");
        }
        assert!(validate_ref("main").is_ok());
        assert!(validate_ref("v2.1.0").is_ok());
    }
}
