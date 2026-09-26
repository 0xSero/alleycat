pub mod codex_resolver;
pub mod envelope;
pub mod framing;
pub mod git_info;
pub mod launch_environment;
pub mod launcher;
pub mod model_catalog;
pub mod notify;
pub mod pool;
pub mod server;
pub mod session;
pub mod state;
pub mod thread_index;

pub use envelope::{
    InboundMessage, JsonRpcError, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse, JsonRpcVersion, RequestId, error_codes,
};
pub use git_info::{git_info_for_cwd, map_entries_with_git_info};
pub use launch_environment::{
    LaunchEnvironment, LaunchEnvironmentPolicy, LaunchEnvironmentResolver, UserEnvironmentLauncher,
};
pub use launcher::{
    ChildProcess, ChildStderr, ChildStdin, ChildStdout, LocalLauncher, ProcessLauncher,
    ProcessRole, ProcessSpec, StdioMode,
};
pub use model_catalog::{CachedCatalogBridge, ModelCatalogCache};
pub use notify::NotificationSender;
pub use server::{Bridge, Conn, serve_stdio, serve_stream, serve_stream_with_session};
#[cfg(unix)]
pub use server::{ServerOptions, serve_unix};
pub use session::{AttachKind, AttachOutcome, Session, SessionRegistry, SessionRegistryConfig};
pub use thread_index::{
    DEFAULT_LIST_LIMIT, Hydrator, IndexEntry, ListFilter, ListPage, ListSort, MAX_LIST_LIMIT,
    ThreadIndex, ThreadIndexHandle, encode_backwards_cursor, resolve_list_limit,
};

pub mod settings;
