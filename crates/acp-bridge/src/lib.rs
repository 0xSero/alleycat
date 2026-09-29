//! `alleycat-acp-bridge` — codex app-server façade over ACP-compliant agents.

pub mod acp_client;
pub mod bridge;
pub mod catalog;
pub mod config;
pub mod handlers;
pub mod persistence;
pub mod pool;
pub mod streaming;
pub mod translate;
pub mod translator;

pub use bridge::{AcpBridge, AcpBridgeBuilder};
