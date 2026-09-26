//! Last-known-good `model/list` catalogs.
//!
//! Native model discovery (spawning `claude`, `amp`, `droid`, … to ask for
//! their catalogs) is slow and flaky on a loaded host. [`CachedCatalogBridge`]
//! wraps any [`Bridge`] and remembers the last successful `model/list`
//! response per request shape, in memory and on disk. When fresh discovery
//! fails or exceeds its deadline, the cached catalog is served instead and a
//! background refresh is kicked off so the next call is fresh again. With no
//! cache the original error is returned unchanged.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value};
use tracing::{debug, warn};

use crate::envelope::JsonRpcError;
use crate::server::{Bridge, Conn};

pub const MODEL_LIST_METHOD: &str = "model/list";

/// Outer deadline applied to fresh discovery only when a cached catalog can
/// be served instead. Without a cache the bridge's own deadlines apply.
pub const DEFAULT_FRESH_DEADLINE_WITH_CACHE: Duration = Duration::from_secs(20);

/// Per-agent catalog store. `dir = None` keeps the cache in memory only.
pub struct ModelCatalogCache {
    agent: String,
    path: Option<PathBuf>,
    entries: Mutex<HashMap<String, Value>>,
    refreshing: AtomicBool,
}

impl ModelCatalogCache {
    pub fn new(agent: impl Into<String>, dir: Option<PathBuf>) -> Arc<Self> {
        let agent = agent.into();
        let path = dir.map(|dir| dir.join(format!("{}.json", sanitize(&agent))));
        let entries = path
            .as_ref()
            .and_then(|path| std::fs::read(path).ok())
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|value| value.get("entries").cloned())
            .and_then(|entries| serde_json::from_value::<HashMap<String, Value>>(entries).ok())
            .unwrap_or_default();
        Arc::new(Self {
            agent,
            path,
            entries: Mutex::new(entries),
            refreshing: AtomicBool::new(false),
        })
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn get(&self, params: &Value) -> Option<Value> {
        self.entries.lock().unwrap().get(&cache_key(params)).cloned()
    }

    pub fn store(&self, params: &Value, catalog: &Value) {
        let snapshot = {
            let mut entries = self.entries.lock().unwrap();
            let key = cache_key(params);
            if entries.get(&key) == Some(catalog) {
                return;
            }
            entries.insert(key, catalog.clone());
            entries.clone()
        };
        let Some(path) = self.path.as_ref() else {
            return;
        };
        let body = serde_json::json!({ "version": 1, "agent": self.agent, "entries": snapshot });
        if let Err(error) = write_atomic(path, &body) {
            warn!(agent = %self.agent, path = %path.display(), "persisting model catalog failed: {error}");
        }
    }
}

fn write_atomic(path: &std::path::Path, body: &Value) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("json.tmp{}", std::process::id()));
    std::fs::write(&tmp, serde_json::to_vec(body)?)?;
    std::fs::rename(&tmp, path)
}

fn sanitize(agent: &str) -> String {
    agent
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// Canonical request-shape key: nulls dropped, `Null` params == `{}`.
fn cache_key(params: &Value) -> String {
    fn strip(value: &Value) -> Value {
        match value {
            Value::Object(map) => Value::Object(
                map.iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k.clone(), strip(v)))
                    .collect::<Map<_, _>>(),
            ),
            other => other.clone(),
        }
    }
    match params {
        Value::Null => "{}".to_owned(),
        other => strip(other).to_string(),
    }
}

/// Wraps a bridge so `model/list` falls back to the last good catalog.
pub struct CachedCatalogBridge {
    inner: Arc<dyn Bridge>,
    cache: Arc<ModelCatalogCache>,
    fresh_deadline: Duration,
}

impl CachedCatalogBridge {
    pub fn new(inner: Arc<dyn Bridge>, cache: Arc<ModelCatalogCache>) -> Self {
        Self {
            inner,
            cache,
            fresh_deadline: DEFAULT_FRESH_DEADLINE_WITH_CACHE,
        }
    }

    pub fn with_fresh_deadline(mut self, deadline: Duration) -> Self {
        self.fresh_deadline = deadline;
        self
    }

    pub fn cache(&self) -> &Arc<ModelCatalogCache> {
        &self.cache
    }

    async fn model_list(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let cached = self.cache.get(&params);
        let fresh = if cached.is_some() {
            match tokio::time::timeout(
                self.fresh_deadline,
                self.inner.dispatch(ctx, MODEL_LIST_METHOD, params.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(JsonRpcError::internal(format!(
                    "model discovery exceeded {}s",
                    self.fresh_deadline.as_secs()
                ))),
            }
        } else {
            self.inner.dispatch(ctx, MODEL_LIST_METHOD, params.clone()).await
        };
        match (fresh, cached) {
            (Ok(catalog), _) => {
                self.cache.store(&params, &catalog);
                Ok(catalog)
            }
            (Err(error), Some(catalog)) => {
                warn!(
                    agent = %self.cache.agent(),
                    error = %error.message,
                    "model/list discovery failed; serving last-known-good catalog"
                );
                self.spawn_refresh(ctx.clone(), params);
                Ok(catalog)
            }
            (Err(error), None) => Err(error),
        }
    }

    fn spawn_refresh(&self, ctx: Conn, params: Value) {
        if self.cache.refreshing.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = Arc::clone(&self.inner);
        let cache = Arc::clone(&self.cache);
        tokio::spawn(async move {
            match inner.dispatch(&ctx, MODEL_LIST_METHOD, params.clone()).await {
                Ok(catalog) => cache.store(&params, &catalog),
                Err(error) => {
                    debug!(agent = %cache.agent(), "background model/list refresh failed: {}", error.message)
                }
            }
            cache.refreshing.store(false, Ordering::Release);
        });
    }
}

#[async_trait]
impl Bridge for CachedCatalogBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        self.inner.initialize(ctx, params).await
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        if method == MODEL_LIST_METHOD {
            return self.model_list(ctx, params).await;
        }
        self.inner.dispatch(ctx, method, params).await
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        self.inner.notification(ctx, method, params).await
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{SessionRegistry, SessionRegistryConfig};
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    /// Returns `catalog-N` for call N while `ok`, errors or hangs otherwise.
    struct Fake {
        mode: Mutex<&'static str>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Bridge for Fake {
        async fn initialize(&self, _: &Conn, _: Value) -> Result<Value, JsonRpcError> {
            Ok(json!({}))
        }
        async fn dispatch(&self, _: &Conn, method: &str, _: Value) -> Result<Value, JsonRpcError> {
            assert_eq!(method, MODEL_LIST_METHOD);
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let mode = *self.mode.lock().unwrap();
            match mode {
                "ok" => Ok(json!({ "data": [{ "id": format!("catalog-{n}") }] })),
                "hang" => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    unreachable!()
                }
                _ => Err(JsonRpcError::internal("Claude catalog initialization timed out")),
            }
        }
    }

    fn setup(dir: Option<PathBuf>) -> (Arc<Fake>, CachedCatalogBridge, Conn) {
        let fake = Arc::new(Fake { mode: Mutex::new("ok"), calls: AtomicUsize::new(0) });
        let bridge = CachedCatalogBridge::new(
            fake.clone() as Arc<dyn Bridge>,
            ModelCatalogCache::new("claude", dir),
        )
        .with_fresh_deadline(Duration::from_millis(200));
        let registry = SessionRegistry::new(SessionRegistryConfig::default());
        let conn = Conn::from_session(registry.get_or_create("test".into(), "claude"));
        (fake, bridge, conn)
    }

    #[tokio::test]
    async fn error_without_cache_is_preserved() {
        let (fake, bridge, conn) = setup(None);
        *fake.mode.lock().unwrap() = "err";
        let err = bridge.dispatch(&conn, MODEL_LIST_METHOD, json!({})).await.unwrap_err();
        assert!(err.message.contains("timed out"));
    }

    #[tokio::test]
    async fn cache_hit_on_error_and_timeout_then_background_refresh() {
        let (fake, bridge, conn) = setup(None);
        let first = bridge.dispatch(&conn, MODEL_LIST_METHOD, Value::Null).await.unwrap();
        assert_eq!(first["data"][0]["id"], "catalog-0");

        *fake.mode.lock().unwrap() = "err";
        let served = bridge.dispatch(&conn, MODEL_LIST_METHOD, json!({})).await.unwrap();
        assert_eq!(served, first);

        *fake.mode.lock().unwrap() = "hang";
        let served = bridge.dispatch(&conn, MODEL_LIST_METHOD, json!({"cursor": null})).await.unwrap();
        assert_eq!(served, first);

        // Background refresh lands once discovery recovers.
        *fake.mode.lock().unwrap() = "ok";
        bridge.cache().refreshing.store(false, Ordering::SeqCst);
        bridge.spawn_refresh(conn.clone(), json!({}));
        for _ in 0..50 {
            if bridge.cache().get(&json!({})) != Some(first.clone()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_ne!(bridge.cache().get(&json!({})), Some(first));
    }

    #[tokio::test]
    async fn fresh_overrides_cache() {
        let (_fake, bridge, conn) = setup(None);
        let a = bridge.dispatch(&conn, MODEL_LIST_METHOD, json!({})).await.unwrap();
        let b = bridge.dispatch(&conn, MODEL_LIST_METHOD, json!({})).await.unwrap();
        assert_ne!(a, b);
        assert_eq!(bridge.cache().get(&json!({})), Some(b));
    }

    #[tokio::test]
    async fn persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (_fake, bridge, conn) = setup(Some(dir.path().to_path_buf()));
        let fresh = bridge.dispatch(&conn, MODEL_LIST_METHOD, json!({"includeHidden": true})).await.unwrap();
        assert!(dir.path().join("claude.json").is_file());

        // A new daemon process: fresh discovery fails, disk cache serves.
        let (fake2, bridge2, conn2) = setup(Some(dir.path().to_path_buf()));
        *fake2.mode.lock().unwrap() = "err";
        let served = bridge2
            .dispatch(&conn2, MODEL_LIST_METHOD, json!({"includeHidden": true}))
            .await
            .unwrap();
        assert_eq!(served, fresh);
        // Different request shape has no cache entry.
        assert!(bridge2.dispatch(&conn2, MODEL_LIST_METHOD, json!({})).await.is_err());
    }
}
