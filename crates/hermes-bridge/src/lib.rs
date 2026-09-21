//! Hermes bridge for Alleycat.
//!
//! Implements the `alleycat_bridge_core::Bridge` trait to expose a Hermes Agent
//! backend (via its gateway API or CLI fallback) as a codex app-server
//! compatible JSON-RPC endpoint.
//!
//! # Modes
//!
//! - **API mode**: Connects to a running Hermes gateway server at a configured
//!   loopback address. Uses `/v1/runs` for turn creation and
//!   `/v1/runs/{id}/events` (SSE) for streaming output.
//!
//! - **CLI fallback mode**: Spawns a `hermes` binary from PATH when the API
//!   server is unavailable or disabled in config.
//!
//! - **Auto mode** (default): Attempts API first, falls back to CLI.

mod api_client;
mod bridge;
mod catalog;
mod cli_adapter;
mod config;
mod index;
mod sse;
mod state;

pub use bridge::HermesBridge;
pub use config::{HermesBridgeConfig, HermesMode};
