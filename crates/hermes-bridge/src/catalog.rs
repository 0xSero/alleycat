//! Query the installed Hermes picker inventory without a gateway or chat session.
use alleycat_bridge_core::{LocalLauncher, ProcessLauncher, ProcessSpec, StdioMode};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio::io::AsyncReadExt;

// Call the native inventory beneath /api/model/options, without its unused
// pricing/featured decoration. Export only identifiers; native provider rows may
// contain endpoint/configuration metadata.
const INVENTORY_SCRIPT: &str = r#"
import contextlib, inspect, json, logging, os, threading, time
logging.disable(logging.CRITICAL)
with open(os.devnull, 'w') as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
    from hermes_cli.env_loader import load_hermes_dotenv
    load_hermes_dotenv()
    from hermes_cli.inventory import build_models_payload, load_picker_context
    # Match the native GUI read path: cached catalogs refresh in the background,
    # and offline saved endpoints must not block unrelated provider rows.
    options = dict(non_blocking_catalogs=True, probe_custom_providers=False,
                   probe_current_custom_provider=True)
    parameters = inspect.signature(build_models_payload).parameters
    options = {key: value for key, value in options.items() if key in parameters}
    payload = build_models_payload(load_picker_context(), **options)
    # Native stale-while-revalidate workers are daemons. Let healthy refreshes
    # persist before this short-lived interpreter exits; failures retry next open.
    refresh_deadline = time.monotonic() + 5
    for worker in threading.enumerate():
        if worker.name.startswith('model-cache-swr-'):
            worker.join(max(0, refresh_deadline - time.monotonic()))
    providers = [{k: row[k] for k in ('slug', 'models', 'authenticated') if k in row}
                 for row in payload['providers']]
print(json.dumps({'providers': providers, 'provider': payload['provider'], 'model': payload['model']}))
"#;

pub(crate) async fn discover(bin: Option<&str>) -> Result<Value> {
    let installed = which::which(bin.unwrap_or("hermes"))
        .context("Cannot resolve installed Hermes for model discovery")?
        .canonicalize()
        .context("Cannot resolve Hermes installation")?;
    let interpreter = installed
        .parent()
        .context("Invalid Hermes installation path")?
        .join(if cfg!(windows) {
            "python.exe"
        } else {
            "python"
        });
    query(&interpreter, Duration::from_secs(14)).await
}

async fn query(interpreter: &Path, timeout: Duration) -> Result<Value> {
    let mut spec = ProcessSpec::new(interpreter);
    spec.args = vec!["-c".into(), INVENTORY_SCRIPT.into()];
    spec.stdin = StdioMode::Null;
    spec.stderr = StdioMode::Null;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut child = tokio::time::timeout_at(deadline, LocalLauncher.launch(spec))
        .await
        .context("Hermes model inventory launch timed out")?
        .context("Hermes model inventory could not start")?;
    let result = tokio::time::timeout_at(deadline, async {
        const LIMIT: u64 = 1024 * 1024;
        let mut stdout = child
            .take_stdout()
            .context("Hermes model inventory has no stdout")?
            .take(LIMIT + 1);
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).await?;
        ensure!(
            bytes.len() <= LIMIT as usize,
            "Hermes model inventory exceeded output limit"
        );
        ensure!(
            child.wait().await?.success(),
            "Installed Hermes could not expose its native model inventory"
        );
        serde_json::from_slice(&bytes).context("Invalid Hermes model inventory")
    })
    .await
    .context("Hermes model inventory timed out");
    // Kill also reaps the process on timeout/error; LocalLauncher kills on drop
    // if the caller cancels the discovery future.
    let _ = tokio::time::timeout(Duration::from_secs(1), child.kill()).await;
    result?
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn inventory_refreshes_and_rejects_failure_and_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("python");
        let fixture = dir.path().join("inventory.json");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\ntest \"$1\" = '-c' || exit 2\ncat '{}'\n",
                fixture.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        for model in ["old-model", "new-model"] {
            let payload = serde_json::json!({"provider":"custom:local", "model":model,
                "providers":[{"slug":"custom:local","authenticated":true,"models":[model]}]});
            std::fs::write(&fixture, payload.to_string()).unwrap();
            assert_eq!(
                query(&executable, Duration::from_secs(2)).await.unwrap(),
                payload
            );
        }
        std::fs::write(&executable, "#!/bin/sh\nprintf '{}'\nexit 1\n").unwrap();
        assert!(query(&executable, Duration::from_secs(2)).await.is_err());
        std::fs::write(&executable, "#!/bin/sh\nexec sleep 60\n").unwrap();
        assert!(
            query(&executable, Duration::from_millis(20))
                .await
                .unwrap_err()
                .to_string()
                .contains("timed out")
        );
    }

    #[test]
    fn native_gui_options_preserve_custom_routes_and_support_older_signatures() {
        let dir = tempfile::tempdir().unwrap();
        let package = dir.path().join("hermes_cli");
        std::fs::create_dir(&package).unwrap();
        std::fs::write(package.join("__init__.py"), "").unwrap();
        std::fs::write(
            package.join("env_loader.py"),
            "def load_hermes_dotenv(): pass\n",
        )
        .unwrap();
        let payload = serde_json::json!({
            "provider": "custom:local", "model": "latest-local-model",
            "providers": [{"slug": "custom:local", "authenticated": true,
                "models": ["latest-local-model", "custom/model-id"],
                "endpoint": "must-not-be-exported"}]
        });
        let definitions = [
            "def build_models_payload(ctx, *, non_blocking_catalogs=False, probe_custom_providers=True, probe_current_custom_provider=False):\n    assert non_blocking_catalogs is True\n    assert probe_custom_providers is False\n    assert probe_current_custom_provider is True\n",
            "def build_models_payload(ctx):\n",
        ];
        for definition in definitions {
            std::fs::write(
                package.join("inventory.py"),
                format!(
                    "import json\ndef load_picker_context(): return 'native-context'\n{definition}    assert ctx == 'native-context'\n    return json.loads({:?})\n",
                    payload.to_string()
                ),
            )
            .unwrap();
            let output = std::process::Command::new("python3")
                .args(["-B", "-c", INVENTORY_SCRIPT])
                .env("PYTHONPATH", dir.path())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(actual["provider"], payload["provider"]);
            assert_eq!(actual["model"], payload["model"]);
            assert_eq!(
                actual["providers"][0]["models"],
                payload["providers"][0]["models"]
            );
            assert_eq!(actual["providers"][0]["authenticated"], true);
            assert!(actual["providers"][0].get("endpoint").is_none());
        }
    }

    #[tokio::test]
    #[ignore = "requires an installed, configured Hermes and provider network access"]
    async fn installed_native_inventory_has_selectable_routes() {
        let payload = discover(None).await.unwrap();
        let models = crate::bridge::gateway_model_ids(&payload).unwrap();
        eprintln!(
            "Native Hermes inventory: {} models across {} providers",
            models.len(),
            payload["providers"].as_array().unwrap().len()
        );
    }
}
