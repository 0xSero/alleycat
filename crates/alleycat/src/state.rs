use std::path::Path;

use anyhow::{Context, anyhow};
use fd_lock::RwLock;
use iroh::SecretKey;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::paths;

pub async fn load_or_create_secret_key() -> anyhow::Result<SecretKey> {
    let path = paths::host_key_file()?;
    load_or_create_secret_key_at(&path).await
}

/// Load a stable iroh identity from `path`, creating it with owner-only
/// permissions when it does not exist. The daemon uses the standard host-key
/// path; diagnostic clients may opt into their own persistent identity so a
/// reconnect is authenticated as the same device.
pub(crate) async fn load_or_create_secret_key_at(path: &Path) -> anyhow::Result<SecretKey> {
    match fs::read_to_string(path).await {
        Ok(raw) => parse_secret_key(raw.trim())
            .with_context(|| format!("parsing secret key {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            write_secret_key(path, &key).await?;
            Ok(key)
        }
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

pub async fn acquire_lock() -> anyhow::Result<RwLock<std::fs::File>> {
    let path = paths::host_lock_file()?;
    tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(RwLock::new(file))
    })
    .await
    .context("joining lock open task")?
}

/// Write the daemon's PID atomically. Used to surface a friendly hint when
/// `acquire_lock()` finds the lock already held.
pub fn write_pid_file() -> anyhow::Result<std::path::PathBuf> {
    let path = paths::daemon_pid_file()?;
    std::fs::write(&path, format!("{}\n", std::process::id()))
        .with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(path)
}

/// Read the PID written by `write_pid_file`. Returns `None` if the file is
/// missing or unparseable.
pub fn read_pid_file() -> anyhow::Result<Option<u32>> {
    let path = paths::daemon_pid_file()?;
    match std::fs::read_to_string(&path) {
        Ok(raw) => Ok(raw.trim().parse::<u32>().ok()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn parse_secret_key(raw: &str) -> anyhow::Result<SecretKey> {
    let bytes = hex::decode(raw).context("host key must be 32 bytes of lowercase hex")?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow!("host key must decode to exactly 32 bytes"))?;
    Ok(SecretKey::from_bytes(&bytes))
}

async fn write_secret_key(path: &Path, key: &SecretKey) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = tmp_path(path);
    {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .with_context(|| format!("opening {}", tmp.display()))?;
        file.write_all(hex::encode(key.to_bytes()).as_bytes())
            .await
            .with_context(|| format!("writing {}", tmp.display()))?;
        file.write_all(b"\n").await.ok();
        file.flush().await.ok();
        file.sync_all().await.ok();
    }
    set_mode_0600(&tmp)?;
    fs::rename(&tmp, path)
        .await
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
    set_mode_0600(path)?;
    Ok(())
}

fn tmp_path(path: &Path) -> std::path::PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    tmp.into()
}

#[cfg(unix)]
fn set_mode_0600(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))
}

#[cfg(not(unix))]
fn set_mode_0600(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_secret_key_requires_32_bytes() {
        let err = parse_secret_key("abcd").unwrap_err().to_string();
        assert!(err.contains("exactly 32 bytes"));
    }

    #[test]
    fn parse_secret_key_round_trips_hex() {
        let key = SecretKey::generate();
        let parsed = parse_secret_key(&hex::encode(key.to_bytes())).unwrap();
        assert_eq!(parsed.public(), key.public());
    }

    #[tokio::test]
    async fn path_loader_reuses_the_same_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe.key");

        let first = load_or_create_secret_key_at(&path).await.unwrap();
        let second = load_or_create_secret_key_at(&path).await.unwrap();

        assert_eq!(first.public(), second.public());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
