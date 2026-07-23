//! `model/list` and the surrounding capability-listing methods.
//!
//! `model/list` translates pi's `get_available_models` response into codex's
//! `Model` shape. The handler reaches a live pi process through
//! [`PiPool::acquire_utility(None)`] — pi-runtime's three-tier fallback
//! first reuses any existing thread-bound pi (model catalog is
//! cwd-agnostic), and only spawns a fresh utility pi when the bridge has
//! no live processes at all. The utility spawn is short-lived: callers
//! must not `mark_active` it, and the next idle reap sweeps it.
//!
//! Returns an empty list when pi spawn fails — codex clients tolerate
//! empty `model/list` responses (no model picker contents; they fall back
//! to the thread's active model).
//!
//! `experimentalFeature/list`, `collaborationMode/list`, and
//! `mock/experimentalMethod` are inlined in `main.rs`'s dispatcher; this
//! module deliberately does not duplicate them.

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use serde_json::json;

use crate::codex_proto as p;
use crate::pool::pi_protocol as pi;
use crate::state::ConnectionState;

const PI_SETTINGS_PATH_ENV: &str = "PI_AGENT_SETTINGS_PATH";
const THINKING_SUFFIXES: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh"];

pub async fn handle_model_list(
    state: &Arc<ConnectionState>,
    _params: p::ModelListParams,
) -> p::ModelListResponse {
    let pi_models = fetch_models_via_pool(state).await;
    let data = pi_models
        .into_iter()
        .enumerate()
        .map(|(idx, model)| translate_pi_model(&model, idx == 0))
        .collect();
    p::ModelListResponse {
        data,
        next_cursor: None,
    }
}

/// Fetch `Model[]` via the pool. Returns `Vec::new()` on any spawn or RPC
/// failure — the codex client interprets an empty list as "no model
/// picker today" and falls back to the thread's active model. Spawn
/// failure is logged at WARN so it's visible in bridge logs.
async fn fetch_models_via_pool(state: &Arc<ConnectionState>) -> Vec<PiAvailableModel> {
    let handle = match state.pi_pool().acquire_utility(None).await {
        Ok(h) => h,
        Err(err) => {
            tracing::warn!(%err, "model/list: failed to acquire utility pi handle");
            return Vec::new();
        }
    };
    match fetch_models_from_handle(&handle).await {
        Ok(models) => filter_models_by_enabled_models(filter_models_by_provider(
            models,
            state.model_provider_prefixes(),
        )),
        Err(err) => {
            tracing::warn!(%err, "model/list: get_available_models RPC failed");
            Vec::new()
        }
    }
}

fn filter_models_by_provider(
    models: Vec<PiAvailableModel>,
    prefixes: &[String],
) -> Vec<PiAvailableModel> {
    if prefixes.is_empty() {
        return models;
    }
    models
        .into_iter()
        .filter(|model| {
            let provider = model.provider.as_deref().unwrap_or_default();
            prefixes
                .iter()
                .any(|prefix| provider == prefix || provider.starts_with(&format!("{prefix}-")))
        })
        .collect()
}

/// Translate one pi `Model<any>` into codex `Model`. Pi's catalog is loose
/// JSON so we work through the [`PiAvailableModel`] sieve, taking only what
/// codex needs.
fn translate_pi_model(model: &PiAvailableModel, is_default: bool) -> p::Model {
    let provider = model.provider.as_deref().unwrap_or("pi");
    let model_id = model
        .model_id
        .as_deref()
        .or(model.id.as_deref())
        .unwrap_or("unknown");
    let id = format!("{provider}/{model_id}");
    let base_display_name = model
        .display_name
        .clone()
        .or_else(|| model.label.clone())
        .unwrap_or_else(|| model_id.to_string());
    let display_name = display_name_with_provider(provider, &base_display_name);
    let description = model.description.clone().unwrap_or_default();

    // Codex contract: `supported_reasoning_efforts` is a list of
    // `{ reasoning_effort, description }` pairs. Pi's `ThinkingLevel`
    // vocabulary is wider than codex's `ReasoningEffort` (it includes "off"
    // and "xhigh" which codex doesn't expose), so we always advertise the
    // full codex set and let the bridge map at `set_thinking_level` time.
    let supported_reasoning_efforts = vec![
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Minimal,
            description: "Lowest latency, no extended thinking".to_string(),
        },
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Low,
            description: "Brief reasoning".to_string(),
        },
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::Medium,
            description: "Default depth of reasoning".to_string(),
        },
        p::ReasoningEffortOption {
            reasoning_effort: p::ReasoningEffort::High,
            description: "Maximum reasoning effort".to_string(),
        },
    ];

    p::Model {
        id,
        model: model_id.to_string(),
        upgrade: None,
        upgrade_info: None,
        availability_nux: None,
        display_name,
        description,
        hidden: false,
        supported_reasoning_efforts,
        default_reasoning_effort: p::ReasoningEffort::Medium,
        input_modalities: model
            .input_modalities
            .clone()
            .unwrap_or_else(|| vec![json!("text")]),
        supports_personality: false,
        additional_speed_tiers: Vec::new(),
        service_tiers: standard_service_tiers(),
        is_default,
    }
}

fn filter_models_by_enabled_models(models: Vec<PiAvailableModel>) -> Vec<PiAvailableModel> {
    let Some(patterns) = enabled_model_patterns_from_settings() else {
        return models;
    };
    filter_models_with_patterns(models, &patterns)
}

fn filter_models_with_patterns(
    models: Vec<PiAvailableModel>,
    patterns: &[String],
) -> Vec<PiAvailableModel> {
    if patterns.is_empty() {
        return models;
    }
    let filtered: Vec<PiAvailableModel> = models
        .iter()
        .filter(|model| model_matches_enabled_patterns(model, &patterns))
        .cloned()
        .collect();
    tracing::info!(
        before = models.len(),
        after = filtered.len(),
        patterns = patterns.len(),
        "model/list: applied pi enabledModels filter"
    );
    filtered
}

fn enabled_model_patterns_from_settings() -> Option<Vec<String>> {
    let path = pi_settings_path()?;
    let bytes = std::fs::read_to_string(&path).ok()?;
    let value: Value = serde_json::from_str(&bytes).ok()?;
    let patterns = value.get("enabledModels")?.as_array()?;
    Some(
        patterns
            .iter()
            .filter_map(|pattern| pattern.as_str())
            .map(str::trim)
            .filter(|pattern| !pattern.is_empty())
            .map(strip_thinking_suffix)
            .collect(),
    )
}

fn pi_settings_path() -> Option<PathBuf> {
    let path = std::env::var(PI_SETTINGS_PATH_ENV).ok()?;
    let trimmed = path.trim();
    (!trimmed.is_empty()).then(|| PathBuf::from(trimmed))
}

fn strip_thinking_suffix(pattern: &str) -> String {
    let Some((head, suffix)) = pattern.rsplit_once(':') else {
        return pattern.to_string();
    };
    if THINKING_SUFFIXES
        .iter()
        .any(|known| suffix.eq_ignore_ascii_case(known))
    {
        head.to_string()
    } else {
        pattern.to_string()
    }
}

fn model_matches_enabled_patterns(model: &PiAvailableModel, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| model_matches_enabled_pattern(model, pattern))
}

fn model_matches_enabled_pattern(model: &PiAvailableModel, pattern: &str) -> bool {
    let normalized = pattern.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return false;
    }
    let refs = model_reference_candidates(model);
    if normalized.contains('*') || normalized.contains('?') {
        refs.iter()
            .any(|candidate| wildcard_match(&normalized, candidate))
    } else {
        refs.iter().any(|candidate| candidate == &normalized)
    }
}

fn model_reference_candidates(model: &PiAvailableModel) -> Vec<String> {
    let provider = model.provider.as_deref().unwrap_or("pi");
    let mut refs = Vec::new();
    for id in [model.model_id.as_deref(), model.id.as_deref()]
        .into_iter()
        .flatten()
    {
        push_ref(&mut refs, &format!("{provider}/{id}"));
        if !id.contains('/') {
            push_ref(&mut refs, id);
        }
        if let Some(tail) = id.rsplit('/').next() {
            push_ref(&mut refs, &format!("{provider}/{tail}"));
            push_ref(&mut refs, tail);
        }
    }
    refs
}

fn push_ref(refs: &mut Vec<String>, value: &str) {
    let normalized = value.trim().to_ascii_lowercase();
    if !normalized.is_empty() && !refs.contains(&normalized) {
        refs.push(normalized);
    }
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    wildcard_match_bytes(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_match_bytes(pattern: &[u8], value: &[u8]) -> bool {
    let (mut p, mut v) = (0, 0);
    let mut star = None;
    let mut star_match = 0;
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == value[v]) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            star_match = v;
        } else if let Some(star_idx) = star {
            p = star_idx + 1;
            star_match += 1;
            v = star_match;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

fn standard_service_tiers() -> Vec<p::ModelServiceTier> {
    vec![p::ModelServiceTier {
        id: "standard".to_string(),
        name: "Standard".to_string(),
        description: "Default bridge service tier".to_string(),
    }]
}

fn display_name_with_provider(provider: &str, display_name: &str) -> String {
    let display_name = display_name.trim();
    let display_name = if display_name.is_empty() {
        "unknown"
    } else {
        display_name
    };
    let provider = provider.trim();
    if provider.is_empty() || provider == "pi" {
        return display_name.to_string();
    }
    if display_name
        .to_ascii_lowercase()
        .contains(&provider.to_ascii_lowercase())
    {
        return display_name.to_string();
    }
    format!("{display_name} ({provider})")
}

/// Lossy view over pi's `Model<any>` shape (`pi-mono/packages/ai/src/types.ts`).
/// Pi treats most fields as opaque-by-provider, so we sieve only what codex
/// `model/list` needs.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PiAvailableModel {
    /// Provider key (e.g. "openai", "anthropic", "groq").
    #[serde(default)]
    provider: Option<String>,
    /// Provider-specific model id (`gpt-5-codex`, `claude-sonnet-4-6`, ...).
    #[serde(default)]
    id: Option<String>,
    /// Some pi providers spell the same field as `modelId`. Pi internals use
    /// both depending on registry source.
    #[serde(default)]
    model_id: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    /// Free-form modalities list pi exposes (`text`, `image`, etc.). We
    /// pass through verbatim and let codex pick what it understands.
    #[serde(default)]
    input_modalities: Option<Vec<Value>>,
}

fn parse_pi_models_response(value: &Value) -> Vec<PiAvailableModel> {
    let Some(models) = value.get("models").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    models
        .iter()
        .filter_map(|m| serde_json::from_value(m.clone()).ok())
        .collect()
}

/// Drive `RpcCommand::GetAvailableModels` against a single pi handle and
/// return the parsed catalog. Free function so tests can exercise the
/// unpacking layer without standing up the full pool.
async fn fetch_models_from_handle(
    handle: &crate::pool::PiProcessHandle,
) -> Result<Vec<PiAvailableModel>, crate::pool::PiProcessError> {
    let resp = handle
        .send_request(pi::RpcCommand::GetAvailableModels(pi::BareCmd::default()))
        .await?;
    let value = resp.data.unwrap_or(Value::Null);
    Ok(parse_pi_models_response(&value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_pi_model_synthesizes_id_and_efforts() {
        let pi = PiAvailableModel {
            provider: Some("openai".into()),
            model_id: Some("gpt-5".into()),
            display_name: Some("GPT-5".into()),
            description: Some("Codex flagship model".into()),
            input_modalities: Some(vec![json!("text"), json!("image")]),
            ..Default::default()
        };
        let m = translate_pi_model(&pi, true);
        assert_eq!(m.id, "openai/gpt-5");
        assert_eq!(m.model, "gpt-5");
        assert_eq!(m.display_name, "GPT-5 (openai)");
        assert!(m.is_default);
        assert_eq!(m.supported_reasoning_efforts.len(), 4);
        assert!(matches!(
            m.default_reasoning_effort,
            p::ReasoningEffort::Medium
        ));
        assert_eq!(m.input_modalities.len(), 2);
    }

    #[test]
    fn translate_pi_model_falls_back_to_id_when_model_id_missing() {
        let pi = PiAvailableModel {
            provider: None,
            id: Some("haiku".into()),
            ..Default::default()
        };
        let m = translate_pi_model(&pi, false);
        assert_eq!(m.id, "pi/haiku");
        assert_eq!(m.model, "haiku");
        assert_eq!(m.display_name, "haiku");
        assert!(!m.is_default);
    }

    #[test]
    fn translate_pi_model_does_not_duplicate_provider_in_display_name() {
        let pi = PiAvailableModel {
            provider: Some("openai".into()),
            model_id: Some("gpt-5".into()),
            display_name: Some("OpenAI GPT-5".into()),
            ..Default::default()
        };
        let m = translate_pi_model(&pi, false);
        assert_eq!(m.display_name, "OpenAI GPT-5");
    }

    #[test]
    fn parse_pi_models_response_extracts_array() {
        let raw = json!({
            "models": [
                { "provider": "anthropic", "modelId": "claude-sonnet-4-6", "displayName": "Sonnet 4.6" },
                { "provider": "openai", "id": "gpt-5", "displayName": "GPT-5" },
            ]
        });
        let parsed = parse_pi_models_response(&raw);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].provider.as_deref(), Some("anthropic"));
        assert_eq!(parsed[0].model_id.as_deref(), Some("claude-sonnet-4-6"));
        assert_eq!(parsed[1].id.as_deref(), Some("gpt-5"));
    }

    #[test]
    fn enabled_model_filter_matches_full_ids_suffixes_and_globs() {
        let model = PiAvailableModel {
            provider: Some("fireworks".into()),
            model_id: Some("accounts/fireworks/models/deepseek-v4-pro".into()),
            ..Default::default()
        };
        assert!(model_matches_enabled_pattern(
            &model,
            "fireworks/accounts/fireworks/models/deepseek-v4-pro"
        ));
        assert!(!model_matches_enabled_pattern(
            &model,
            "opencode-go/deepseek-v4-pro"
        ));
        assert!(model_matches_enabled_pattern(
            &model,
            "fireworks/*deepseek-v4*"
        ));
        assert!(!model_matches_enabled_pattern(
            &model,
            "accounts/fireworks/models/deepseek-v4-pro"
        ));
        assert!(!model_matches_enabled_pattern(&model, "openai/gpt-5"));
    }

    #[test]
    fn strip_thinking_suffix_leaves_colon_model_ids_alone() {
        assert_eq!(
            strip_thinking_suffix("anthropic/sonnet:high"),
            "anthropic/sonnet"
        );
        assert_eq!(
            strip_thinking_suffix("openrouter/model:exacto"),
            "openrouter/model:exacto"
        );
    }

    #[test]
    fn parse_pi_models_response_empty_on_missing_field() {
        let raw = json!({ "other": [] });
        assert!(parse_pi_models_response(&raw).is_empty());
    }

    #[test]
    fn empty_enabled_models_keeps_the_full_catalog() {
        let models = vec![PiAvailableModel {
            provider: Some("local-studio".into()),
            model_id: Some("glm-5.2".into()),
            ..Default::default()
        }];
        let filtered = filter_models_with_patterns(models, &[]);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn provider_filter_keeps_only_controller_variants() {
        let models = [
            "local-studio",
            "local-studio-spark",
            "openrouter",
            "user-pi-local",
        ]
        .into_iter()
        .map(|provider| PiAvailableModel {
            provider: Some(provider.into()),
            model_id: Some("model".into()),
            ..Default::default()
        })
        .collect();
        let filtered = filter_models_by_provider(models, &["local-studio".into()]);
        assert_eq!(
            filtered
                .iter()
                .filter_map(|model| model.provider.as_deref())
                .collect::<Vec<_>>(),
            vec!["local-studio", "local-studio-spark"]
        );
    }

    #[tokio::test]
    async fn handle_model_list_returns_empty_when_pi_spawn_fails() {
        // The dummy pool's `pi_bin` is `/dev/null`, so `acquire_utility`
        // tries to spawn `/dev/null --mode rpc` and fails. The handler
        // contract is to log + return an empty list, never propagate the
        // spawn error to the codex client.
        let dir = tempfile::tempdir().unwrap();
        let index = crate::index::ThreadIndex::open_at(dir.path().join("threads.json"))
            .await
            .unwrap();
        std::mem::forget(dir);
        let (state, _rx) = ConnectionState::for_test(
            Arc::new(crate::pool::PiPool::new("/dev/null")),
            index,
            Default::default(),
        );
        let resp = handle_model_list(&state, p::ModelListParams::default()).await;
        assert!(resp.data.is_empty());
        assert!(resp.next_cursor.is_none());
    }
}
