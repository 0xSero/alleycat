//! Native settings must reach their selected runtime, never the bridge-only file.
#![cfg(unix)]
mod support;
use alleycat_bridge_core::{LocalLauncher, session::Session};
use alleycat_pi_bridge::{
    codex_proto as p,
    handlers::config,
    pool::PiPool,
    state::{ConnectionState, ThreadDefaults},
};
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
};
use support::thread_index_stub::NoopThreadIndex;

#[tokio::test]
async fn omp_defaults_and_writes_use_native_command_even_for_remote_files() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("fake-omp");
    std::fs::write(&bin,r#"#!/bin/sh
case "$2" in
 list) value=true; test ! -f "$PI_CODING_AGENT_DIR/value" || value=$(cat "$PI_CODING_AGENT_DIR/value"); printf '{"autoResume":{"value":%s,"type":"boolean"},"auth.broker.token":{"redacted":true}}' "$value";;
 set) test "$3" = autoResume || exit 2; printf '%s' "$4" > "$PI_CODING_AGENT_DIR/value"; printf '{"key":"autoResume","value":%s}' "$4";;
 *) exit 2;;
esac
"#).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.path().join("config.yml");
    // A remote path must never be opened on the device. This invalid local
    // lookalike would fail if the settings handler used the local filesystem.
    std::fs::write(&path, "[invalid local configuration").unwrap();
    let launcher = Arc::new(LocalLauncher);
    let state = Arc::new(
        ConnectionState::new(
            Arc::new(Session::new("omp", "test".into(), 64, 1 << 20)),
            Arc::new(PiPool::with_launcher(bin, launcher.clone())),
            Arc::new(NoopThreadIndex),
            Arc::new(Mutex::new(ThreadDefaults::default())),
            launcher,
            true,
            Arc::new(vec![]),
            None,
            None,
        )
        .with_native_settings_path(Some(path.clone())),
    );
    let before = config::handle_config_read(&state, dir.path(), p::ConfigReadParams::default())
        .await
        .unwrap();
    let descriptors = before.config["_litterSettings"].as_array().unwrap();
    assert!(
        descriptors
            .iter()
            .any(|v| v["key"] == "autoResume" && v["valueJson"] == "true")
    );
    assert!(!descriptors.iter().any(|v| v["key"] == "auth.broker.token"));
    let params = serde_json::from_value(
        json!({"keyPath":"autoResume","value":false,"mergeStrategy":"replace"}),
    )
    .unwrap();
    config::handle_config_value_write(&state, dir.path(), params)
        .await
        .unwrap();
    let after = config::handle_config_read(&state, dir.path(), p::ConfigReadParams::default())
        .await
        .unwrap();
    assert!(
        after.config["_litterSettings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["key"] == "autoResume" && v["valueJson"] == "false")
    );
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "[invalid local configuration"
    );
}

#[tokio::test]
#[ignore = "requires an installed native OMP; set LITTER_NATIVE_OMP_BIN"]
async fn installed_omp_lists_unset_defaults_and_persists_native_settings() {
    let bin = std::env::var_os("LITTER_NATIVE_OMP_BIN").expect("LITTER_NATIVE_OMP_BIN");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yml");
    std::fs::write(
        &path,
        "autoResume: false\nauth:\n  broker:\n    token: TEST_ONLY_OMP_CREDENTIAL\n",
    )
    .unwrap();
    let launcher = Arc::new(LocalLauncher);
    let state = Arc::new(
        ConnectionState::new(
            Arc::new(Session::new(
                "omp",
                "native-settings-probe".into(),
                64,
                1 << 20,
            )),
            Arc::new(PiPool::with_launcher(bin, launcher.clone())),
            Arc::new(NoopThreadIndex),
            Arc::new(Mutex::new(ThreadDefaults::default())),
            launcher,
            false,
            Arc::new(vec![]),
            None,
            None,
        )
        .with_native_settings_path(Some(path.clone())),
    );
    let before = config::handle_config_read(&state, dir.path(), p::ConfigReadParams::default())
        .await
        .unwrap();
    let descriptors = before.config["_litterSettings"].as_array().unwrap();
    assert!(
        descriptors.len() > 100,
        "native schema should include unconfigured defaults"
    );
    assert!(descriptors.iter().any(|v| v["key"] == "compaction.enabled"));
    assert!(
        !before
            .config
            .to_string()
            .contains("TEST_ONLY_OMP_CREDENTIAL")
    );
    let params = serde_json::from_value(
        json!({"keyPath":"autoResume","value":true,"mergeStrategy":"replace"}),
    )
    .unwrap();
    config::handle_config_value_write(&state, dir.path(), params)
        .await
        .unwrap();
    let after = config::handle_config_read(&state, dir.path(), p::ConfigReadParams::default())
        .await
        .unwrap();
    assert!(
        after.config["_litterSettings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["key"] == "autoResume" && v["valueJson"] == "true")
    );
    let persisted = std::fs::read_to_string(path).unwrap();
    assert!(persisted.contains("TEST_ONLY_OMP_CREDENTIAL"));
    println!(
        "native OMP descriptors: {}; real setter and authoritative readback passed",
        descriptors.len()
    );
}

#[tokio::test]
#[ignore = "requires an installed Pi npm package; set LITTER_NATIVE_PI_BIN"]
async fn installed_pi_declares_unset_fields_and_preserves_native_overrides() {
    let bin = std::env::var_os("LITTER_NATIVE_PI_BIN").expect("LITTER_NATIVE_PI_BIN");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("settings.json");
    std::fs::write(
        &path,
        r#"{"compaction":{"enabled":false},"apiKey":"TEST_ONLY_PI_CREDENTIAL"}"#,
    )
    .unwrap();
    let launcher = Arc::new(LocalLauncher);
    let state = Arc::new(
        ConnectionState::new(
            Arc::new(Session::new("pi", "settings-probe".into(), 64, 1 << 20)),
            Arc::new(PiPool::with_launcher(bin, launcher.clone())),
            Arc::new(NoopThreadIndex),
            Arc::new(Mutex::new(ThreadDefaults::default())),
            launcher,
            false,
            Arc::new(vec![]),
            None,
            None,
        )
        .with_native_settings_path(Some(path.clone())),
    );
    let before = config::handle_config_read(&state, dir.path(), p::ConfigReadParams::default())
        .await
        .unwrap();
    let rows = before.config["_litterSettings"].as_array().unwrap();
    assert!(rows.len() > 40);
    assert_eq!(
        rows.iter()
            .filter(|v| v["key"] == "compaction.enabled")
            .count(),
        1
    );
    assert!(
        rows.iter()
            .any(|v| v["key"] == "retry.provider.maxRetries" && v["valueJson"] == "null")
    );
    assert!(
        !before
            .config
            .to_string()
            .contains("TEST_ONLY_PI_CREDENTIAL")
    );
    config::handle_config_value_write(
        &state,
        dir.path(),
        serde_json::from_value(json!({
            "keyPath":"retry.provider.maxRetries", "value":4, "mergeStrategy":"replace"
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let after = config::handle_config_read(&state, dir.path(), p::ConfigReadParams::default())
        .await
        .unwrap();
    assert_eq!(after.config["retry"]["provider"]["maxRetries"], 4);
    assert_eq!(after.config["compaction"]["enabled"], false);
    assert!(
        std::fs::read_to_string(path)
            .unwrap()
            .contains("TEST_ONLY_PI_CREDENTIAL")
    );
    println!(
        "installed Pi descriptors: {}; unset fields, native persistence, and credential preservation passed",
        rows.len()
    );
}
