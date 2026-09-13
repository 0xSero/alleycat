//! Focused lifecycle coverage for adopting Pi's initial RPC session.

mod support;

use std::ffi::OsString;
use std::sync::Arc;

use alleycat_bridge_core::{ChildProcess, LocalLauncher, ProcessLauncher, ProcessSpec};
use alleycat_pi_bridge::codex_proto as p;
use alleycat_pi_bridge::handlers;
use alleycat_pi_bridge::pool::PiPool;
use alleycat_pi_bridge::state::{ConnectionState, ThreadDefaults};
use tempfile::TempDir;

use support::fake_pi_path;
use support::thread_index_stub::NoopThreadIndex;

#[derive(Clone, Copy)]
struct DirtyInitialSessionLauncher;

impl ProcessLauncher for DirtyInitialSessionLauncher {
    fn launch(
        &self,
        mut spec: ProcessSpec,
    ) -> futures::future::BoxFuture<'_, std::io::Result<Box<dyn ChildProcess>>> {
        spec.env.push((
            OsString::from("FAKE_PI_INITIAL_MESSAGE_COUNT"),
            OsString::from("1"),
        ));
        LocalLauncher.launch(spec)
    }
}

#[tokio::test]
async fn dirty_initial_sessions_are_rejected_and_released() {
    let pool = Arc::new(PiPool::with_launcher(
        fake_pi_path(),
        Arc::new(DirtyInitialSessionLauncher),
    ));
    let (state, _notifications) = ConnectionState::for_test(
        Arc::clone(&pool),
        Arc::new(NoopThreadIndex),
        ThreadDefaults::default(),
    );
    let cwd = TempDir::new().unwrap();

    let error = handlers::thread::handle_thread_start(
        &state,
        p::ThreadStartParams {
            cwd: Some(cwd.path().to_string_lossy().into_owned()),
            ..Default::default()
        },
    )
    .await
    .expect_err("dirty initial sessions must not become user threads");

    assert!(
        error.to_string().contains("not clean after 2 attempts"),
        "unexpected error: {error}"
    );
    assert!(
        pool.is_empty().await,
        "every rejected claim must be removed from the process pool"
    );
}
