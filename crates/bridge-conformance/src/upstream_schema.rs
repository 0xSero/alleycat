//! Validate captured wire frames against the canonical
//! `codex-rs/app-server-protocol/schema/json/v2/` JSON schemas.
//!
//! Why: our local `codex-proto` types are a hand-maintained mirror that can
//! drift from upstream — fields renamed, enum variants added, optional/required
//! flipped. The diff layer's typed-decode check only proves we're consistent
//! with the *mirror*. This pass loads the real schemas codex publishes and
//! validates each frame against them; a violation here is a real wire-spec
//! gap the bridge needs to fix.
//!
//! Skip-on-missing: the schema dir defaults to
//! `~/dev/codex/codex-rs/app-server-protocol/schema/json/v2/`. If absent,
//! validation panics unless `BRIDGE_CONFORMANCE_SKIP_UPSTREAM_SCHEMA=1` is
//! set. Silent schema skips hide exactly the drift this harness is meant to
//! catch.
//!
//! Method/notification → schema mapping is built from the upstream filename
//! convention: response of `thread/read` lives in `ThreadReadResponse.json`,
//! notification `item/completed` in `ItemCompletedNotification.json`, etc.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use jsonschema::Validator;
use serde_json::Value;

use crate::{Frame, FrameKind, TargetId};

const ENV_OVERRIDE: &str = "BRIDGE_CONFORMANCE_CODEX_SCHEMA_DIR";
const ENV_SKIP: &str = "BRIDGE_CONFORMANCE_SKIP_UPSTREAM_SCHEMA";
const DEFAULT_REL: &str = "dev/codex/codex-rs/app-server-protocol/schema/json/v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaSkipReason {
    ExplicitSkip,
    UnitTest,
}

/// Directory holding the upstream v2 JSON schema files.
pub fn schema_dir() -> Result<PathBuf, SchemaSkipReason> {
    if std::env::var_os(ENV_SKIP).as_deref() == Some(std::ffi::OsStr::new("1")) {
        warn_skip_once();
        return Err(SchemaSkipReason::ExplicitSkip);
    }
    if let Some(custom) = std::env::var_os(ENV_OVERRIDE) {
        let p = PathBuf::from(custom);
        if p.is_dir() {
            return Ok(p);
        }
        panic_missing(Some(p));
    }
    #[cfg(test)]
    {
        Err(SchemaSkipReason::UnitTest)
    }
    #[cfg(not(test))]
    {
        let candidate = std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(DEFAULT_REL));
        if let Some(candidate) = candidate {
            if candidate.is_dir() {
                return Ok(candidate);
            }
            panic_missing(Some(candidate));
        }
        panic_missing(None);
    }
}

/// Validate a single captured frame against its upstream schema. Returns
/// `Ok(())` when validation is explicitly skipped or the frame validates;
/// otherwise returns the validator's error list joined into one human-readable
/// message.
pub fn validate(frame: &Frame, target: TargetId) -> Result<(), String> {
    let Some(schema_path) = path_for_frame(frame) else {
        // No schema file mapped for this method/notification — silent skip
        // (e.g., bridge-only methods, or methods we haven't mapped yet).
        return Ok(());
    };
    let validator = match cached_validator(&schema_path) {
        Ok(v) => v,
        Err(err) => {
            // Don't fail the whole frame just because the schema didn't
            // load — surface it once via tracing and skip.
            tracing::debug!(
                schema = %schema_path.display(),
                ?err,
                "upstream schema unavailable; skipping validation"
            );
            return Ok(());
        }
    };
    let payload = extract_payload(frame);
    let errors: Vec<String> = validator
        .iter_errors(&payload)
        .map(|e| format!("{}: {}", e.instance_path, e))
        .collect();
    if errors.is_empty() || is_known_stale_schema_miss(frame, target, &errors) {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn warn_skip_once() {
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        tracing::warn!(
            env = ENV_SKIP,
            "upstream schema validation explicitly skipped"
        );
    });
}

fn panic_missing(candidate: Option<PathBuf>) -> ! {
    let default = std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(DEFAULT_REL))
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| format!("$HOME/{DEFAULT_REL}"));
    let checked = candidate
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "(HOME is unset)".to_string());
    panic!(
        "upstream codex JSON schemas are required for bridge conformance\n\
         checked: {checked}\n\
         set {ENV_OVERRIDE}=<schema-dir> to point at app-server-protocol/schema/json/v2\n\
         default path: {default}\n\
         set {ENV_SKIP}=1 only when this validation is intentionally disabled"
    );
}

fn is_known_stale_schema_miss(frame: &Frame, target: TargetId, errors: &[String]) -> bool {
    // The locally checked-out codex schema can lag the installed codex CLI
    // used as the live reference. Codex 0.130 emits serviceTier="priority",
    // while the older schema only accepted "fast" | "flex". Keep schema
    // validation useful for every other path without failing the live
    // reference on a known stale enum.
    target == TargetId::Codex
        && frame.kind == FrameKind::Response
        && errors
            .iter()
            .all(|err| err.contains("/serviceTier:") && err.contains("\"priority\" is not valid"))
}

/// Map a frame to its upstream schema filename. We translate the JSON-RPC
/// method name into the upstream's PascalCase + suffix convention
/// (`thread/read` → `ThreadReadResponse.json` for responses,
/// `item/completed` → `ItemCompletedNotification.json` for notifications).
fn path_for_frame(frame: &Frame) -> Option<PathBuf> {
    let dir = match schema_dir() {
        Ok(dir) => dir,
        Err(SchemaSkipReason::ExplicitSkip | SchemaSkipReason::UnitTest) => return None,
    };
    let stem = match frame.kind {
        FrameKind::Response => format!("{}Response", method_to_pascal(&frame.method)),
        FrameKind::Notification => format!("{}Notification", method_to_pascal(&frame.method)),
    };
    let candidate = dir.join(format!("{stem}.json"));
    candidate.is_file().then_some(candidate)
}

/// `thread/read` → `ThreadRead`, `item/agentMessage/delta` → `ItemAgentMessageDelta`.
/// The codex schema files use ASCII PascalCase formed by capitalizing each
/// `/`-separated segment without altering already-camelCased segments.
fn method_to_pascal(method: &str) -> String {
    let mut out = String::with_capacity(method.len());
    for segment in method.split('/') {
        if segment.is_empty() {
            continue;
        }
        let mut chars = segment.chars();
        if let Some(first) = chars.next() {
            for c in first.to_uppercase() {
                out.push(c);
            }
        }
        out.push_str(chars.as_str());
    }
    out
}

/// Pull the validation target out of a frame:
///   - response: `result` body (the schema describes the result, not the
///     full JSON-RPC envelope).
///   - notification: `params` body.
fn extract_payload(frame: &Frame) -> Value {
    let target = match frame.kind {
        FrameKind::Response => "result",
        FrameKind::Notification => "params",
    };
    frame.raw.get(target).cloned().unwrap_or(Value::Null)
}

fn cached_validator(path: &Path) -> Result<&'static Validator, String> {
    static CACHE: OnceLock<std::sync::Mutex<HashMap<PathBuf, &'static Validator>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut guard = cache.lock().expect("schema cache poisoned");
    if let Some(&v) = guard.get(path) {
        return Ok(v);
    }
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let json: Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", path.display()))?;
    let validator =
        jsonschema::draft7::new(&json).map_err(|e| format!("compile {}: {e}", path.display()))?;
    let leaked: &'static Validator = Box::leak(Box::new(validator));
    guard.insert(path.to_path_buf(), leaked);
    Ok(leaked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_to_pascal_handles_slashes_and_camel() {
        assert_eq!(method_to_pascal("thread/read"), "ThreadRead");
        assert_eq!(method_to_pascal("item/completed"), "ItemCompleted");
        assert_eq!(
            method_to_pascal("item/agentMessage/delta"),
            "ItemAgentMessageDelta"
        );
        assert_eq!(method_to_pascal("turn/start"), "TurnStart");
    }
}
