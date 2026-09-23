use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cli::SpawnMode;
use crate::error::RemoteControlError;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DaemonConfig {
    pub remote_control: Vec<RemoteControlDaemonEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteControlDaemonEntry {
    pub dir: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_mode: Option<SpawnMode>,
}

impl DaemonConfig {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self, RemoteControlError> {
        match tokio::fs::read(path.as_ref()).await {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(err) => Err(RemoteControlError::Protocol(format!(
                "failed to read daemon config {}: {err}",
                path.as_ref().display()
            ))),
        }
    }

    pub async fn save(&self, path: impl AsRef<Path>) -> Result<(), RemoteControlError> {
        if let Some(parent) = path.as_ref().parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                RemoteControlError::Protocol(format!(
                    "failed to create daemon config parent {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        tokio::fs::write(path.as_ref(), bytes).await.map_err(|err| {
            RemoteControlError::Protocol(format!(
                "failed to write daemon config {}: {err}",
                path.as_ref().display()
            ))
        })
    }

    pub fn find_by_dir(&self, dir: &Path) -> Option<&RemoteControlDaemonEntry> {
        self.remote_control.iter().find(|entry| entry.dir == dir)
    }

    pub fn upsert(&mut self, entry: RemoteControlDaemonEntry) {
        if let Some(existing) = self
            .remote_control
            .iter_mut()
            .find(|existing| existing.dir == entry.dir)
        {
            *existing = entry;
        } else {
            self.remote_control.push(entry);
        }
    }

    pub fn remove_by_name_or_dir(&mut self, name_or_dir: &str) -> Option<RemoteControlDaemonEntry> {
        let idx = self.remote_control.iter().position(|entry| {
            entry.dir == Path::new(name_or_dir) || entry.name.as_deref() == Some(name_or_dir)
        })?;
        Some(self.remote_control.remove(idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_replaces_matching_dir() {
        let mut cfg = DaemonConfig::default();
        cfg.upsert(RemoteControlDaemonEntry {
            dir: PathBuf::from("/repo"),
            name: Some("old".to_string()),
            spawn_mode: Some(SpawnMode::SameDir),
        });
        cfg.upsert(RemoteControlDaemonEntry {
            dir: PathBuf::from("/repo"),
            name: Some("new".to_string()),
            spawn_mode: Some(SpawnMode::Worktree),
        });
        assert_eq!(cfg.remote_control.len(), 1);
        assert_eq!(cfg.remote_control[0].name.as_deref(), Some("new"));
        assert_eq!(cfg.remote_control[0].spawn_mode, Some(SpawnMode::Worktree));
    }
}
