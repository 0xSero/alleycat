use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use alleycat_local_studio_proto::{
    Capability, ControllerAction, ControllerActionKind, ProtocolVersion,
};
use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Utc};
use iroh::EndpointId;
use serde::{Deserialize, Deserializer, Serialize};

use crate::paths;

pub const PAIRED_NODES_STORE_VERSION: u32 = 1;
const MAX_GRANT_STORE_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PairedNodesDocument {
    pub version: u32,
    pub nodes: Vec<PairedNodeGrant>,
}

impl Default for PairedNodesDocument {
    fn default() -> Self {
        Self {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PairedNodeGrant {
    pub endpoint_id: String,
    pub protocol_version: ProtocolVersion,
    pub grants: Vec<Capability>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub revoked_at: Option<DateTime<Utc>>,
    pub actions: Vec<ActionTargetGrant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActionTargetGrant {
    StartRecipe { targets: Vec<String> },
    CancelLaunch { targets: Vec<String> },
    EvictModel { targets: Vec<String> },
}

impl ActionTargetGrant {
    pub const fn kind(&self) -> ControllerActionKind {
        match self {
            Self::StartRecipe { .. } => ControllerActionKind::StartRecipe,
            Self::CancelLaunch { .. } => ControllerActionKind::CancelLaunch,
            Self::EvictModel { .. } => ControllerActionKind::EvictModel,
        }
    }

    pub fn targets(&self) -> &[String] {
        match self {
            Self::StartRecipe { targets }
            | Self::CancelLaunch { targets }
            | Self::EvictModel { targets } => targets,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveGrant {
    pub endpoint_id: String,
    pub grants: BTreeSet<Capability>,
    pub actions: BTreeMap<ControllerActionKind, BTreeSet<String>>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl EffectiveGrant {
    pub fn allows_capability(&self, capability: Capability) -> bool {
        self.grants.contains(&capability)
    }

    pub fn allows_action(&self, action: &ControllerAction) -> bool {
        self.allows_capability(Capability::ModelsControl)
            && self
                .actions
                .get(&action.kind())
                .is_some_and(|targets| targets.contains(action.target()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct GrantStore {
    nodes: BTreeMap<String, PairedNodeGrant>,
}

impl GrantStore {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_document(document: PairedNodesDocument) -> anyhow::Result<Self> {
        if document.version != PAIRED_NODES_STORE_VERSION {
            bail!(
                "unsupported paired-nodes store version {}",
                document.version
            );
        }
        let mut nodes = BTreeMap::new();
        for grant in document.nodes {
            validate_grant(&grant)?;
            let endpoint_id = normalize_endpoint_id(&grant.endpoint_id)?;
            if nodes.insert(endpoint_id.clone(), grant).is_some() {
                bail!("duplicate paired-node grant for endpoint {endpoint_id}");
            }
        }
        Ok(Self { nodes })
    }

    pub fn from_json(raw: &[u8]) -> anyhow::Result<Self> {
        let document: PairedNodesDocument =
            serde_json::from_slice(raw).context("parsing paired-nodes grant store")?;
        Self::from_document(document)
    }

    pub fn load() -> anyhow::Result<Self> {
        Self::load_from(&paths::paired_nodes_file()?)
    }

    pub fn load_from(path: &Path) -> anyhow::Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::empty());
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("opening paired-node grants from {}", path.display())
                });
            }
        };
        let metadata = file
            .metadata()
            .with_context(|| format!("reading metadata for {}", path.display()))?;
        validate_private_store_file(&metadata)?;
        if metadata.len() > MAX_GRANT_STORE_BYTES {
            bail!("paired-node grant store exceeds the size limit");
        }
        let mut raw = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_GRANT_STORE_BYTES + 1)
            .read_to_end(&mut raw)
            .with_context(|| format!("reading paired-node grants from {}", path.display()))?;
        if raw.len() as u64 > MAX_GRANT_STORE_BYTES {
            bail!("paired-node grant store exceeds the size limit");
        }
        Self::from_json(&raw)
            .with_context(|| format!("loading paired-node grants from {}", path.display()))
    }

    pub fn document(&self) -> PairedNodesDocument {
        PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: self.nodes.values().cloned().collect(),
        }
    }

    pub fn save(&self) -> anyhow::Result<PathBuf> {
        let path = paths::paired_nodes_file()?;
        self.save_to(&path)?;
        Ok(path)
    }

    pub fn save_to(&self, path: &Path) -> anyhow::Result<()> {
        let encoded = serde_json::to_vec_pretty(&self.document())
            .context("serializing paired-nodes grant store")?;
        atomic_write_0600(path, &encoded)
    }

    pub fn replace(&mut self, grant: PairedNodeGrant) -> anyhow::Result<()> {
        validate_grant(&grant)?;
        let endpoint_id = normalize_endpoint_id(&grant.endpoint_id)?;
        self.nodes.insert(endpoint_id, grant);
        Ok(())
    }

    pub fn grant_stats_read(
        &mut self,
        endpoint_id: EndpointId,
        expires_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        self.grant_capabilities(endpoint_id, &[Capability::StatsRead], expires_at)
    }

    pub fn grant_capabilities(
        &mut self,
        endpoint_id: EndpointId,
        capabilities: &[Capability],
        expires_at: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        let requested = validate_capability_selection(capabilities)?;
        let key = endpoint_id.to_string();
        let mut grant = self.nodes.remove(&key).unwrap_or(PairedNodeGrant {
            endpoint_id: key,
            protocol_version: ProtocolVersion,
            grants: Vec::new(),
            expires_at: None,
            revoked_at: None,
            actions: Vec::new(),
        });
        for capability in requested {
            if !grant.grants.contains(&capability) {
                grant.grants.push(capability);
            }
        }
        grant.grants.sort_unstable();
        grant.expires_at = expires_at;
        grant.revoked_at = None;
        self.replace(grant)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &PairedNodeGrant> {
        self.nodes.values()
    }

    pub fn revoke(&mut self, endpoint_id: &EndpointId, revoked_at: DateTime<Utc>) -> bool {
        let Some(grant) = self.nodes.get_mut(&endpoint_id.to_string()) else {
            return false;
        };
        grant.revoked_at = Some(revoked_at);
        true
    }

    pub fn revoke_stats_read(&mut self, endpoint_id: &EndpointId) -> bool {
        self.revoke_capabilities(endpoint_id, &[Capability::StatsRead])
            .unwrap_or(false)
    }

    pub fn revoke_capabilities(
        &mut self,
        endpoint_id: &EndpointId,
        capabilities: &[Capability],
    ) -> anyhow::Result<bool> {
        let requested = validate_capability_selection(capabilities)?;
        let Some(grant) = self.nodes.get_mut(&endpoint_id.to_string()) else {
            return Ok(false);
        };
        let before = grant.grants.len();
        grant
            .grants
            .retain(|capability| !requested.contains(capability));
        if requested.contains(&Capability::ModelsControl) {
            grant.actions.clear();
        }
        Ok(grant.grants.len() != before)
    }

    pub fn effective(
        &self,
        endpoint_id: &EndpointId,
        now: DateTime<Utc>,
    ) -> Option<EffectiveGrant> {
        let endpoint_id = endpoint_id.to_string();
        let grant = self.nodes.get(&endpoint_id)?;
        if grant.revoked_at.is_some() || grant.expires_at.is_some_and(|expiry| now >= expiry) {
            return None;
        }

        Some(EffectiveGrant {
            endpoint_id,
            grants: grant.grants.iter().copied().collect(),
            actions: grant
                .actions
                .iter()
                .map(|action| {
                    (
                        action.kind(),
                        action.targets().iter().cloned().collect::<BTreeSet<_>>(),
                    )
                })
                .collect(),
            expires_at: grant.expires_at,
        })
    }

    pub fn allows_capability(
        &self,
        endpoint_id: &EndpointId,
        capability: Capability,
        now: DateTime<Utc>,
    ) -> bool {
        self.effective(endpoint_id, now)
            .is_some_and(|grant| grant.allows_capability(capability))
    }

    pub fn allows_action(
        &self,
        endpoint_id: &EndpointId,
        action: &ControllerAction,
        now: DateTime<Utc>,
    ) -> bool {
        self.effective(endpoint_id, now)
            .is_some_and(|grant| grant.allows_action(action))
    }
}

fn validate_capability_selection(
    capabilities: &[Capability],
) -> anyhow::Result<BTreeSet<Capability>> {
    if capabilities.is_empty() {
        bail!("at least one explicit Local Studio capability is required");
    }
    let requested = capabilities.iter().copied().collect::<BTreeSet<_>>();
    if requested.len() != capabilities.len() {
        bail!("duplicate Local Studio capability selection");
    }
    Ok(requested)
}

fn validate_grant(grant: &PairedNodeGrant) -> anyhow::Result<()> {
    normalize_endpoint_id(&grant.endpoint_id)?;

    let mut grants = HashSet::with_capacity(grant.grants.len());
    for capability in &grant.grants {
        if !grants.insert(*capability) {
            bail!(
                "duplicate capability {capability} for endpoint {}",
                grant.endpoint_id
            );
        }
    }

    let mut actions = HashSet::with_capacity(grant.actions.len());
    for action in &grant.actions {
        if !actions.insert(action.kind()) {
            bail!(
                "duplicate action {} for endpoint {}",
                action.kind(),
                grant.endpoint_id
            );
        }
        let mut targets = HashSet::with_capacity(action.targets().len());
        for target in action.targets() {
            if target.is_empty() || target.trim() != target || target.len() > 512 {
                bail!(
                    "invalid target for action {} and endpoint {}",
                    action.kind(),
                    grant.endpoint_id
                );
            }
            if !targets.insert(target) {
                bail!(
                    "duplicate target {target} for action {} and endpoint {}",
                    action.kind(),
                    grant.endpoint_id
                );
            }
        }
    }
    if !grant.actions.is_empty() && !grants.contains(&Capability::ModelsControl) {
        bail!(
            "controller action targets require models.control for endpoint {}",
            grant.endpoint_id
        );
    }
    Ok(())
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn normalize_endpoint_id(raw: &str) -> anyhow::Result<String> {
    let endpoint_id: EndpointId = raw
        .parse()
        .with_context(|| format!("invalid Iroh endpoint ID {raw:?}"))?;
    let normalized = endpoint_id.to_string();
    if raw != normalized {
        bail!("Iroh endpoint ID must use canonical encoding");
    }
    Ok(normalized)
}

fn atomic_write_0600(target: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("paired-nodes store has no parent directory"))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating paired-nodes directory {}", parent.display()))?;
    set_mode(parent, 0o700)?;

    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("paired-nodes store has an invalid file name"))?;
    let mut temporary = None;
    for _ in 0..8 {
        let candidate = parent.join(format!(
            ".{file_name}.tmp-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        match open_private_new(&candidate) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("creating temporary store in {}", parent.display()));
            }
        }
    }
    let (temporary_path, mut file) = temporary
        .ok_or_else(|| anyhow!("could not allocate a unique paired-nodes temporary file"))?;

    let write_result = (|| -> anyhow::Result<()> {
        file.write_all(contents)
            .with_context(|| format!("writing {}", temporary_path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("writing {}", temporary_path.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", temporary_path.display()))?;
        drop(file);
        std::fs::rename(&temporary_path, target).with_context(|| {
            format!(
                "renaming {} to {}",
                temporary_path.display(),
                target.display()
            )
        })?;
        set_mode(target, 0o600)?;
        sync_directory(parent)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary_path);
    }
    write_result
}

fn open_private_new(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn validate_private_store_file(metadata: &std::fs::Metadata) -> anyhow::Result<()> {
    if !metadata.file_type().is_file() {
        bail!("paired-node grant store is not a regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o777 != 0o600 {
            bail!("paired-node grant store must have mode 0600");
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("paired-node grant store is not owned by the current user");
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("chmod {mode:o} {}", path.display()))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("syncing directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn endpoint() -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    fn grant(endpoint_id: &EndpointId) -> PairedNodeGrant {
        PairedNodeGrant {
            endpoint_id: endpoint_id.to_string(),
            protocol_version: ProtocolVersion,
            grants: vec![Capability::StatsRead, Capability::ModelsControl],
            expires_at: None,
            revoked_at: None,
            actions: vec![ActionTargetGrant::StartRecipe {
                targets: vec!["recipe-allowed".into()],
            }],
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 20, 12, 0, 0).unwrap()
    }

    #[test]
    fn unknown_nodes_are_default_deny_and_grants_are_isolated() {
        let allowed = endpoint();
        let other = endpoint();
        let store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![grant(&allowed)],
        })
        .unwrap();

        assert!(store.allows_capability(&allowed, Capability::StatsRead, now()));
        assert!(!store.allows_capability(&other, Capability::StatsRead, now()));
        assert!(store.effective(&other, now()).is_none());
    }

    #[test]
    fn expiry_and_revocation_fail_closed() {
        let expired_id = endpoint();
        let revoked_id = endpoint();
        let mut expired = grant(&expired_id);
        expired.expires_at = Some(now());
        let mut revoked = grant(&revoked_id);
        revoked.revoked_at = Some(now() - chrono::Duration::seconds(1));
        let store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![expired, revoked],
        })
        .unwrap();

        assert!(store.effective(&expired_id, now()).is_none());
        assert!(store.effective(&revoked_id, now()).is_none());
    }

    #[test]
    fn runtime_revoke_removes_existing_authority() {
        let id = endpoint();
        let mut store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![grant(&id)],
        })
        .unwrap();
        assert!(store.allows_capability(&id, Capability::StatsRead, now()));
        assert!(store.revoke(&id, now()));
        assert!(!store.allows_capability(&id, Capability::StatsRead, now()));
    }

    #[test]
    fn stats_revoke_preserves_unrelated_future_capabilities() {
        let id = endpoint();
        let mut entry = grant(&id);
        entry.grants.push(Capability::SessionsRead);
        let mut store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![entry],
        })
        .unwrap();
        assert!(store.revoke_stats_read(&id));
        assert!(!store.allows_capability(&id, Capability::StatsRead, now()));
        assert!(store.allows_capability(&id, Capability::SessionsRead, now()));
    }

    #[test]
    fn explicit_capability_grants_and_revocations_are_narrow() {
        let id = endpoint();
        let mut store = GrantStore::empty();
        store
            .grant_capabilities(id, &[Capability::StatsRead, Capability::SessionsRead], None)
            .unwrap();

        assert!(store.allows_capability(&id, Capability::StatsRead, now()));
        assert!(store.allows_capability(&id, Capability::SessionsRead, now()));
        assert!(!store.allows_capability(&id, Capability::SessionsWrite, now()));
        assert!(!store.allows_capability(&id, Capability::AgentTurn, now()));

        assert!(
            store
                .revoke_capabilities(&id, &[Capability::SessionsRead])
                .unwrap()
        );
        assert!(store.allows_capability(&id, Capability::StatsRead, now()));
        assert!(!store.allows_capability(&id, Capability::SessionsRead, now()));
    }

    #[test]
    fn capability_selection_rejects_empty_or_duplicate_requests() {
        let id = endpoint();
        let mut store = GrantStore::empty();
        assert!(store.grant_capabilities(id, &[], None).is_err());
        assert!(
            store
                .grant_capabilities(
                    id,
                    &[Capability::SessionsRead, Capability::SessionsRead],
                    None,
                )
                .is_err()
        );
        assert!(store.nodes().next().is_none());
    }

    #[test]
    fn revoking_models_control_also_revokes_action_targets() {
        let id = endpoint();
        let mut store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![grant(&id)],
        })
        .unwrap();

        assert!(
            store
                .revoke_capabilities(&id, &[Capability::ModelsControl])
                .unwrap()
        );
        let stored = store.nodes().next().unwrap();
        assert!(!stored.grants.contains(&Capability::ModelsControl));
        assert!(stored.actions.is_empty());
    }

    #[test]
    fn model_control_requires_exact_action_and_target() {
        let id = endpoint();
        let store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![grant(&id)],
        })
        .unwrap();

        assert!(store.allows_action(
            &id,
            &ControllerAction::StartRecipe {
                recipe_id: "recipe-allowed".into()
            },
            now()
        ));
        assert!(!store.allows_action(
            &id,
            &ControllerAction::StartRecipe {
                recipe_id: "recipe-other".into()
            },
            now()
        ));
        assert!(!store.allows_action(
            &id,
            &ControllerAction::EvictModel {
                model_id: "recipe-allowed".into()
            },
            now()
        ));
    }

    #[test]
    fn malformed_duplicate_and_unknown_entries_are_rejected() {
        let id = endpoint();
        let mut duplicate_capability = grant(&id);
        duplicate_capability.grants.push(Capability::StatsRead);
        assert!(
            GrantStore::from_document(PairedNodesDocument {
                version: PAIRED_NODES_STORE_VERSION,
                nodes: vec![duplicate_capability]
            })
            .is_err()
        );
        assert!(
            GrantStore::from_document(PairedNodesDocument {
                version: PAIRED_NODES_STORE_VERSION,
                nodes: vec![grant(&id), grant(&id)]
            })
            .is_err()
        );
        assert!(
            GrantStore::from_json(
                br#"{"version":1,"nodes":[{"endpoint_id":"bad","protocol_version":1,"grants":["stats.read"],"expires_at":null,"revoked_at":null,"actions":[],"token":"secret"}]}"#
            )
            .is_err()
        );
        assert!(
            GrantStore::from_json(
                format!(
                    r#"{{"version":1,"nodes":[{{"endpoint_id":"{}","protocol_version":1,"grants":["controller.admin"],"expires_at":null,"revoked_at":null,"actions":[]}}]}}"#,
                    id
                )
                .as_bytes()
            )
            .is_err()
        );
        assert!(
            GrantStore::from_json(
                format!(
                    r#"{{"version":1,"nodes":[{{"endpoint_id":"{}","protocol_version":1,"grants":["stats.read"],"revoked_at":null,"actions":[]}}]}}"#,
                    id
                )
                .as_bytes()
            )
            .is_err()
        );
        assert!(
            GrantStore::from_json(
                format!(
                    r#"{{"version":1,"nodes":[{{"endpoint_id":"{}","protocol_version":1,"grants":["models.control"],"expires_at":null,"revoked_at":null,"actions":[{{"action":"restart_controller","targets":["controller"]}}]}}]}}"#,
                    id
                )
                .as_bytes()
            )
            .is_err()
        );
    }

    #[test]
    fn load_rejects_malformed_store_instead_of_falling_back() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("paired-nodes.json");
        std::fs::write(&path, b"not-json").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(GrantStore::load_from(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn load_rejects_insecure_or_symlinked_store() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("paired-nodes.json");
        let link = directory.path().join("paired-nodes-link.json");
        let id = endpoint();
        let store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![grant(&id)],
        })
        .unwrap();
        store.save_to(&real).unwrap();

        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(GrantStore::load_from(&real).is_err());

        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(GrantStore::load_from(&link).is_err());
    }

    #[test]
    fn persistence_is_atomic_and_private() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("paired-nodes.json");
        let id = endpoint();
        let store = GrantStore::from_document(PairedNodesDocument {
            version: PAIRED_NODES_STORE_VERSION,
            nodes: vec![grant(&id)],
        })
        .unwrap();

        store.save_to(&path).unwrap();
        let loaded = GrantStore::load_from(&path).unwrap();
        assert!(loaded.allows_capability(&id, Capability::StatsRead, now()));
        assert_eq!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .count(),
            1
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
