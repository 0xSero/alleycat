//! `PiPool` — owns the set of live pi-coding-agent subprocesses and routes
//! codex thread ids to the right process.
//!
//! Per the design (`~/.claude/plans/i-wanna-design-a-smooth-horizon.md`,
//! "Multi-cwd / multi-process pool"):
//!
//! - **One pi process per codex thread.** Pi binds `process.cwd` per-process
//!   via `process.chdir`, and a single pi process holds exactly one active
//!   session at a time. So even when two codex threads share a `cwd`, each
//!   gets its own pi child.
//! - **Idle reaping.** A thread with no in-flight turn for the configured
//!   idle TTL is reaped: stdin is closed (pi exits cleanly, JSONL persists
//!   for resume).
//! - **Bounded.** A capacity cap LRU-evicts the least-recently-active idle
//!   thread when a new acquire would exceed it. Active threads (turn in
//!   progress) are never evicted — over-cap acquires fail with
//!   [`PoolError::Capacity`] in that case.
//!
//! The bookkeeping lives in [`alleycat_bridge_core::pool::ProcessPool`]; this
//! module wraps it with pi-specific spawn config so callers don't have to
//! re-implement the eviction / capacity loop.

pub mod pi_protocol;
pub mod process;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::pool::ProcessPool;
pub use alleycat_bridge_core::pool::{
    DEFAULT_IDLE_TTL, DEFAULT_MAX_PROCESSES, PoolError, ThreadId,
};
use alleycat_bridge_core::{LocalLauncher, ProcessLauncher};
use uuid::Uuid;

pub use pi_protocol::*;
pub use process::{PiProcessError, PiProcessHandle};

/// Thread-safe pool of pi processes.
#[derive(Clone)]
pub struct PiPool {
    inner: ProcessPool<PiProcessHandle>,
    pi_bin: PathBuf,
    launcher: Arc<dyn ProcessLauncher>,
}

impl std::fmt::Debug for PiPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PiPool")
            .field("pi_bin", &self.pi_bin)
            .finish_non_exhaustive()
    }
}

impl PiPool {
    /// Compatibility constructor: build a pool that uses `LocalLauncher`. Kept
    /// so the daemon's existing `agents.rs` callsite (`PiPool::new(bin)`)
    /// keeps compiling unchanged through the A2 → A5 sequence; A5 will
    /// migrate that callsite to `PiBridge::builder().launcher(...)` and
    /// drop this shim.
    pub fn new(pi_bin: impl Into<PathBuf>) -> Self {
        Self::with_launcher(pi_bin, Arc::new(LocalLauncher))
    }

    /// Build a pool that launches `pi-coding-agent` through `launcher` with
    /// the default cap + idle TTL. Daemon path uses `Arc::new(LocalLauncher)`;
    /// Litter substitutes `Arc::new(SshLauncher::new(...))`.
    pub fn with_launcher(pi_bin: impl Into<PathBuf>, launcher: Arc<dyn ProcessLauncher>) -> Self {
        Self::with_launcher_and_limits(pi_bin, launcher, DEFAULT_MAX_PROCESSES, DEFAULT_IDLE_TTL)
    }

    pub fn with_limits(
        pi_bin: impl Into<PathBuf>,
        max_processes: usize,
        idle_ttl: Duration,
    ) -> Self {
        Self::with_launcher_and_limits(pi_bin, Arc::new(LocalLauncher), max_processes, idle_ttl)
    }

    pub fn with_launcher_and_limits(
        pi_bin: impl Into<PathBuf>,
        launcher: Arc<dyn ProcessLauncher>,
        max_processes: usize,
        idle_ttl: Duration,
    ) -> Self {
        Self {
            inner: ProcessPool::new(max_processes, idle_ttl),
            pi_bin: pi_bin.into(),
            launcher,
        }
    }

    /// Path of the pi binary this pool spawns.
    pub fn pi_bin(&self) -> &Path {
        &self.pi_bin
    }

    /// Process launcher used to spawn pi children.
    pub fn launcher(&self) -> &Arc<dyn ProcessLauncher> {
        &self.launcher
    }

    /// Spawn a fresh pi process for a brand-new codex thread, mint a thread
    /// id, and return both. The handler is responsible for sending pi the
    /// `new_session` (and any `set_model`/`set_thinking_level` overrides)
    /// before the first `prompt`.
    pub async fn acquire_for_new_thread(
        &self,
        cwd: impl AsRef<Path>,
    ) -> Result<(ThreadId, Arc<PiProcessHandle>), PoolError> {
        let thread_id = Uuid::now_v7().to_string();
        let handle = if let Some((_previous_id, handle)) = self
            .inner
            .reassign_lru_idle_with_prefix_for_cwd(thread_id.clone(), cwd.as_ref(), "utility_")
            .await?
        {
            handle
        } else {
            self.spawn_with_capacity_check(thread_id.clone(), cwd.as_ref())
                .await?
        };
        self.schedule_prewarm(cwd.as_ref().to_path_buf());
        Ok((thread_id, handle))
    }

    fn schedule_prewarm(&self, cwd: PathBuf) {
        let pool = self.clone();
        tokio::spawn(async move {
            let mut hasher = DefaultHasher::new();
            cwd.hash(&mut hasher);
            let utility_id = format!("utility_warm_{:x}", hasher.finish());
            if pool.inner.get(&utility_id).await.is_some() {
                return;
            }
            let Ok(handle) =
                PiProcessHandle::launch_with(pool.launcher.as_ref(), &cwd, &pool.pi_bin).await
            else {
                return;
            };
            let handle = Arc::new(handle);
            if pool
                .inner
                .track_new_if_below_capacity(utility_id.clone(), cwd, handle.clone(), true)
                .await
                .is_err()
            {
                handle.shutdown().await;
                return;
            }
            let warmed = handle
                .send_request(pi_protocol::RpcCommand::GetState(
                    pi_protocol::BareCmd::default(),
                ))
                .await
                .is_ok();
            if !warmed {
                pool.inner.release(&utility_id).await;
            } else {
                pool.inner.mark_idle(&utility_id).await;
            }
        });
    }

    /// Spawn a fresh pi process bound to `cwd` for an explicit `thread_id`,
    /// e.g. when resuming a thread that already exists in the bridge index.
    /// Errors if the pool already tracks `thread_id` — callers should `get`
    /// first and only fall back to acquire if the existing process exited.
    pub async fn acquire_for_resume(
        &self,
        thread_id: ThreadId,
        cwd: impl AsRef<Path>,
    ) -> Result<Arc<PiProcessHandle>, PoolError> {
        self.spawn_with_capacity_check(thread_id, cwd.as_ref())
            .await
    }

    /// Borrow a pi process for a one-shot, connection-scoped query
    /// (`model/list`, `skills/list`). Pi processes one command at a time on
    /// stdin, so utility queries serialize behind any in-flight turn on the
    /// same handle — but `get_available_models` / `get_commands` are fast and
    /// non-blocking on pi's side, so the wait is bounded.
    ///
    /// Reuse strategy:
    /// 1. If `cwd` is supplied and a thread-bound pi process exists for that
    ///    cwd, return its handle (no spawn). This matches `skills/list`
    ///    semantics — the catalog is per-cwd.
    /// 2. Otherwise, if any thread-bound pi process exists, return the
    ///    least-recently-active one (suitable for `model/list` which is
    ///    cwd-independent).
    /// 3. Otherwise, spawn a fresh pi process tagged with a synthetic
    ///    `utility_<uuid>` thread id. The handle rides the normal idle TTL.
    ///
    /// `cwd` defaults to the bridge process's current directory when `None`
    /// and a fresh spawn is required, since pi binds `process.chdir` at
    /// startup and `model/list` doesn't care which cwd it sees.
    pub async fn acquire_utility(
        &self,
        cwd: Option<&Path>,
    ) -> Result<Arc<PiProcessHandle>, PoolError> {
        if let Some(handle) = self.inner.try_reuse_for_utility(cwd).await {
            return Ok(handle);
        }
        let cwd = cwd
            .map(Path::to_path_buf)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        let synthetic_id = format!("utility_{}", Uuid::now_v7());
        self.spawn_with_capacity_check(synthetic_id, &cwd).await
    }

    /// Look up the pi process that owns `thread_id`, refreshing its
    /// last-active timestamp so the reaper won't pick it up immediately.
    pub async fn get(&self, thread_id: &str) -> Option<Arc<PiProcessHandle>> {
        self.inner.get(thread_id).await
    }

    /// Mark a thread as currently driving a turn (or any other long-running
    /// operation). Active threads are not eligible for LRU eviction or idle
    /// reaping until [`Self::mark_idle`] is called.
    pub async fn mark_active(&self, thread_id: &str) {
        self.inner.mark_active(thread_id).await
    }

    /// Inverse of [`Self::mark_active`]; refreshes `last_active`.
    pub async fn mark_idle(&self, thread_id: &str) {
        self.inner.mark_idle(thread_id).await
    }

    /// Explicitly release a thread's pi process (e.g. user closed the
    /// thread). Sends EOF on stdin and reaps the child. No-op if the
    /// thread isn't in the pool.
    pub async fn release(&self, thread_id: &str) {
        self.inner.release(thread_id).await
    }

    /// All thread ids currently tracked by the pool.
    pub async fn loaded_thread_ids(&self) -> Vec<ThreadId> {
        self.inner.loaded_thread_ids().await
    }

    /// Thread ids running in the given `cwd`.
    pub async fn threads_for_cwd(&self, cwd: impl AsRef<Path>) -> Vec<ThreadId> {
        self.inner.threads_for_cwd(cwd.as_ref()).await
    }

    /// Count of live pi processes (== number of tracked threads).
    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    /// Returns true when the pool has no live processes.
    pub async fn is_empty(&self) -> bool {
        self.inner.is_empty().await
    }

    /// Sweep idle threads whose `last_active` is older than `idle_ttl`.
    /// Returns the thread ids that were reaped. Callers may run this on a
    /// timer; it's also called opportunistically before each new acquire.
    pub async fn reap_idle(&self) -> Vec<ThreadId> {
        self.inner.reap_idle().await
    }

    /// Spawn `pi-coding-agent --mode rpc` for the given thread/cwd. Performs
    /// idle-reaping and at-cap LRU eviction first; bails with
    /// [`PoolError::Capacity`] only if every tracked thread is currently
    /// active.
    async fn spawn_with_capacity_check(
        &self,
        thread_id: ThreadId,
        cwd: &Path,
    ) -> Result<Arc<PiProcessHandle>, PoolError> {
        self.inner.ensure_capacity_for(&thread_id).await?;

        let handle = PiProcessHandle::launch_with(self.launcher.as_ref(), cwd, &self.pi_bin)
            .await
            .map_err(PoolError::Spawn)?;
        let handle = Arc::new(handle);
        match self
            .inner
            .track_new(thread_id, cwd.to_path_buf(), handle.clone())
            .await
        {
            Ok(()) => Ok(handle),
            Err(err) => {
                handle.shutdown().await;
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_pi_pool(max: usize, ttl: Duration) -> PiPool {
        PiPool::with_limits(PathBuf::from("/usr/bin/false"), max, ttl)
    }

    async fn track_dummy(pool: &PiPool, id: &str, cwd: &str) -> Arc<PiProcessHandle> {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::unbounded_channel();
        let (events_tx, _) = tokio::sync::broadcast::channel(1);
        let handle = PiProcessHandle::__test_dangling(writer_tx, events_tx, PathBuf::from(cwd));
        let handle = Arc::new(handle);
        pool.inner
            .track_new(id.into(), PathBuf::from(cwd), handle.clone())
            .await
            .expect("track");
        handle
    }

    #[tokio::test]
    async fn loaded_thread_ids_and_len() {
        let pool = fake_pi_pool(8, Duration::from_secs(60));
        assert_eq!(pool.len().await, 0);
        assert!(pool.is_empty().await);
        track_dummy(&pool, "alpha", "/a").await;
        track_dummy(&pool, "beta", "/b").await;
        assert_eq!(pool.len().await, 2);
        let mut ids = pool.loaded_thread_ids().await;
        ids.sort();
        assert_eq!(ids, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[tokio::test]
    async fn threads_for_cwd_indexes_correctly() {
        let pool = fake_pi_pool(8, Duration::from_secs(60));
        track_dummy(&pool, "t1", "/x").await;
        track_dummy(&pool, "t2", "/x").await;
        track_dummy(&pool, "t3", "/y").await;
        let mut x = pool.threads_for_cwd("/x").await;
        x.sort();
        assert_eq!(x, vec!["t1".to_string(), "t2".to_string()]);
        assert_eq!(pool.threads_for_cwd("/y").await, vec!["t3".to_string()]);
    }

    #[tokio::test]
    async fn mark_active_blocks_lru_pick_via_ensure_capacity() {
        let pool = fake_pi_pool(1, Duration::from_secs(60));
        track_dummy(&pool, "only", "/a").await;
        pool.mark_active("only").await;
        let err = pool.inner.ensure_capacity_for("new").await.unwrap_err();
        assert!(matches!(err, PoolError::Capacity(1)));
        pool.mark_idle("only").await;
        pool.inner.ensure_capacity_for("new").await.expect("ok");
    }

    #[tokio::test]
    async fn try_reuse_for_utility_prefers_cwd_match() {
        let pool = fake_pi_pool(8, Duration::from_secs(60));
        let target_handle = track_dummy(&pool, "t1", "/repo").await;
        track_dummy(&pool, "t2", "/other").await;
        let handle = pool
            .inner
            .try_reuse_for_utility(Some(Path::new("/repo")))
            .await
            .expect("utility");
        assert!(Arc::ptr_eq(&handle, &target_handle));
    }

    #[tokio::test]
    async fn new_thread_reuses_idle_process_in_same_cwd() {
        let pool = fake_pi_pool(8, Duration::from_secs(60));
        let old = track_dummy(&pool, "utility_old", "/repo").await;
        let (new_id, handle) = pool
            .acquire_for_new_thread("/repo")
            .await
            .expect("warm acquire");
        assert!(Arc::ptr_eq(&handle, &old));
        assert!(pool.get("utility_old").await.is_none());
        assert!(Arc::ptr_eq(&pool.get(&new_id).await.expect("new"), &old));
    }
}
