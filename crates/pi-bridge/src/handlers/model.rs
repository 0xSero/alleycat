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
//! Discovery errors propagate so clients can retain their last good catalog and retry.
//!
//! `experimentalFeature/list`, `collaborationMode/list`, and
//! `mock/experimentalMethod` are inlined in `main.rs`'s dispatcher; this
//! module deliberately does not duplicate them.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use serde_json::json;

use crate::codex_proto as p;
use crate::pool::pi_protocol as pi;
use crate::state::ConnectionState;

const PI_SETTINGS_PATH_ENV: &str = "PI_AGENT_SETTINGS_PATH";
const MODEL_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_MODEL_CATALOG_BYTES: usize = 8 * 1024 * 1024;
const THINKING_SUFFIXES: &[&str] = &["off", "minimal", "low", "medium", "high", "xhigh", "max"];

pub async fn handle_model_list(
    state: &Arc<ConnectionState>,
    _params: p::ModelListParams,
) -> anyhow::Result<p::ModelListResponse> {
    let pi_models = fetch_models_via_pool(state).await?;
    let omp = state
        .native_settings_path()
        .and_then(Path::extension)
        .is_some_and(|ext| ext == "yml" || ext == "yaml");
    let default_index = pi_models
        .iter()
        .position(|model| model.active == Some(true));
    let data = pi_models
        .into_iter()
        .enumerate()
        .map(|(idx, model)| {
            translate_pi_model(
                &model,
                default_index.map_or(idx == 0, |default| idx == default),
                !omp,
            )
        })
        .collect();
    Ok(p::ModelListResponse {
        data,
        next_cursor: None,
    })
}

/// Return the controller's currently active model as a provider-qualified id.
///
/// A prewarmed Pi process can outlive a model switch in Local Studio. New
/// threads that omit an explicit model must therefore consult the controller
/// catalog instead of inheriting the stale model captured when that process
/// started.
pub(crate) async fn active_controller_model(state: &ConnectionState) -> Option<String> {
    let path = state.model_catalog_path()?;
    let models = fetch_models_from_catalog(path, state.model_provider_prefixes())
        .await
        .ok()?;
    qualified_active_model(&models)
}

fn qualified_active_model(models: &[PiAvailableModel]) -> Option<String> {
    let model = models.iter().find(|model| model.active == Some(true))?;
    let provider = model.provider.as_deref()?.trim();
    let model_id = model.model_id.as_deref().or(model.id.as_deref())?.trim();
    (!provider.is_empty() && !model_id.is_empty()).then(|| format!("{provider}/{model_id}"))
}

/// Fetch the authoritative catalog, propagating failures so callers can retry
/// without replacing their last successful catalog with an empty list.
async fn fetch_models_via_pool(
    state: &Arc<ConnectionState>,
) -> anyhow::Result<Vec<PiAvailableModel>> {
    if let Some(path) = state.model_catalog_path() {
        match fetch_models_from_catalog(path, state.model_provider_prefixes()).await {
            Ok(models) => return Ok(filter_models_by_enabled_models(state, models).await),
            Err(err) => {
                tracing::warn!(%err, path = %path.display(), "model/list: controller catalog unavailable; falling back to pi RPC");
            }
        }
    }

    let models = tokio::time::timeout(MODEL_RPC_TIMEOUT, fetch_models_via_rpc(state))
        .await
        .map_err(|_| anyhow::anyhow!("Pi model discovery timed out"))??;
    Ok(filter_models_by_enabled_models(
        state,
        filter_models_by_provider(models, state.model_provider_prefixes()),
    )
    .await)
}

async fn fetch_models_via_rpc(
    state: &Arc<ConnectionState>,
) -> anyhow::Result<Vec<PiAvailableModel>> {
    let handle = state.pi_pool().acquire_utility(None).await?;
    Ok(fetch_models_from_handle(&handle).await?)
}

async fn fetch_models_from_catalog(
    path: &Path,
    prefixes: &[String],
) -> anyhow::Result<Vec<PiAvailableModel>> {
    let bytes = tokio::fs::read(path).await?;
    anyhow::ensure!(
        bytes.len() <= MAX_MODEL_CATALOG_BYTES,
        "controller model catalog exceeds size limit"
    );
    let value: Value = serde_json::from_slice(&bytes)?;
    Ok(parse_pi_models_catalog(&value, prefixes))
}

fn parse_pi_models_catalog(value: &Value, prefixes: &[String]) -> Vec<PiAvailableModel> {
    let Some(providers) = value.get("providers").and_then(Value::as_object) else {
        return Vec::new();
    };
    providers
        .iter()
        .filter(|(provider, _)| {
            prefixes.is_empty()
                || prefixes.iter().any(|prefix| {
                    provider.as_str() == prefix || provider.starts_with(&format!("{prefix}-"))
                })
        })
        .flat_map(|(provider, config)| {
            config
                .get("models")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(move |model| {
                    let mut model = model.clone();
                    model
                        .as_object_mut()?
                        .insert("provider".to_string(), Value::String(provider.clone()));
                    serde_json::from_value(model).ok()
                })
        })
        .collect()
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
fn translate_pi_model(model: &PiAvailableModel, is_default: bool, pi_defaults: bool) -> p::Model {
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

    // OMP advertises exact efforts. Pi uses its native thinkingLevelMap rules:
    // explicit null disables a level, and xhigh/max require an explicit mapping.
    let levels: Vec<String> = if model.reasoning != Some(true) {
        Vec::new()
    } else if let Some(thinking) = &model.thinking {
        thinking.efforts.clone()
    } else if pi_defaults {
        ["minimal", "low", "medium", "high", "xhigh", "max"]
            .into_iter()
            .filter(|level| match model.thinking_level_map.get(*level) {
                Some(Value::Null) => false,
                Some(_) => true,
                None => !matches!(*level, "xhigh" | "max"),
            })
            .map(str::to_owned)
            .collect()
    } else {
        Vec::new()
    };
    let supported_reasoning_efforts: Vec<p::ReasoningEffortOption> = levels
        .into_iter()
        .filter_map(|level| {
            serde_json::from_value(json!(level))
                .ok()
                .map(|reasoning_effort| p::ReasoningEffortOption {
                    reasoning_effort,
                    description: format!("{level} reasoning"),
                })
        })
        .collect();
    let default_reasoning_effort = model
        .thinking
        .as_ref()
        .and_then(|thinking| thinking.default_level.as_ref())
        .and_then(|level| serde_json::from_value(json!(level)).ok())
        .filter(|effort| {
            supported_reasoning_efforts
                .iter()
                .any(|option| option.reasoning_effort == *effort)
        })
        .or_else(|| {
            supported_reasoning_efforts
                .iter()
                .find(|option| option.reasoning_effort == p::ReasoningEffort::Medium)
                .map(|option| option.reasoning_effort)
        })
        .or_else(|| {
            supported_reasoning_efforts
                .first()
                .map(|option| option.reasoning_effort)
        })
        .unwrap_or(p::ReasoningEffort::None);

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
        default_reasoning_effort,
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

async fn filter_models_by_enabled_models(
    state: &ConnectionState,
    models: Vec<PiAvailableModel>,
) -> Vec<PiAvailableModel> {
    let Some(patterns) = enabled_model_patterns_from_settings(state).await else {
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

async fn enabled_model_patterns_from_settings(state: &ConnectionState) -> Option<Vec<String>> {
    // Remote/native catalogs must never be filtered by files on the client device.
    if state.trust_persisted_cwd() {
        return None;
    }
    let path = state
        .native_settings_path()
        .map(Path::to_path_buf)
        .or_else(|| state.model_catalog_path().and_then(|_| pi_settings_path()))?;
    if path
        .extension()
        .is_some_and(|ext| ext == "yml" || ext == "yaml")
    {
        return None;
    }
    let bytes = tokio::fs::read_to_string(&path).await.ok()?;
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
    #[serde(default)]
    reasoning: Option<bool>,
    #[serde(default)]
    thinking_level_map: std::collections::HashMap<String, Value>,
    #[serde(default)]
    thinking: Option<PiThinkingConfig>,
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
    #[serde(alias = "name")]
    display_name: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    active: Option<bool>,
    /// Free-form modalities list pi exposes (`text`, `image`, etc.). We
    /// pass through verbatim and let codex pick what it understands.
    #[serde(default, alias = "input")]
    input_modalities: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PiThinkingConfig {
    #[serde(default)]
    efforts: Vec<String>,
    default_level: Option<String>,
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
    fn native_capabilities_do_not_advertise_unsupported_efforts() {
        let native: PiAvailableModel = serde_json::from_value(json!({
            "id": "native", "reasoning": true,
            "thinkingLevelMap": {"minimal": null, "high": null, "max": "max"}
        }))
        .unwrap();
        let model = translate_pi_model(&native, true, true);
        assert_eq!(
            model
                .supported_reasoning_efforts
                .iter()
                .map(|option| option.reasoning_effort)
                .collect::<Vec<_>>(),
            vec![
                p::ReasoningEffort::Low,
                p::ReasoningEffort::Medium,
                p::ReasoningEffort::Max
            ]
        );
        let non_reasoning: PiAvailableModel = serde_json::from_value(json!({
            "id": "fast", "reasoning": false, "thinkingLevelMap": {"max": "max"}
        }))
        .unwrap();
        let model = translate_pi_model(&non_reasoning, false, true);
        assert!(model.supported_reasoning_efforts.is_empty());
        assert_eq!(model.default_reasoning_effort, p::ReasoningEffort::None);
    }

    #[test]
    fn omp_uses_explicit_efforts_and_native_default() {
        let native: PiAvailableModel = serde_json::from_value(json!({
            "id": "native", "reasoning": true,
            "thinking": {"efforts": ["low", "xhigh", "max"], "defaultLevel": "xhigh"}
        }))
        .unwrap();
        let model = translate_pi_model(&native, true, false);
        assert_eq!(model.supported_reasoning_efforts.len(), 3);
        assert_eq!(model.default_reasoning_effort, p::ReasoningEffort::XHigh);
        let native: PiAvailableModel =
            serde_json::from_value(json!({"id": "fixed", "reasoning": true})).unwrap();
        assert!(
            translate_pi_model(&native, false, false)
                .supported_reasoning_efforts
                .is_empty()
        );
    }

    #[test]
    fn translate_pi_model_preserves_declared_capabilities() {
        let pi = PiAvailableModel {
            provider: Some("openai".into()),
            model_id: Some("gpt-5".into()),
            display_name: Some("GPT-5".into()),
            description: Some("Codex flagship model".into()),
            reasoning: Some(true),
            input_modalities: Some(vec![json!("text"), json!("image")]),
            ..Default::default()
        };
        let m = translate_pi_model(&pi, true, true);
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
        let m = translate_pi_model(&pi, false, true);
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
        let m = translate_pi_model(&pi, false, true);
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
    fn active_catalog_model_becomes_the_default() {
        let models = vec![
            PiAvailableModel {
                id: Some("inactive".into()),
                active: Some(false),
                ..Default::default()
            },
            PiAvailableModel {
                id: Some("running".into()),
                active: Some(true),
                ..Default::default()
            },
        ];
        let default_index = models.iter().position(|model| model.active == Some(true));
        let translated = models
            .iter()
            .enumerate()
            .map(|(index, model)| {
                translate_pi_model(
                    model,
                    default_index.map_or(index == 0, |default| index == default),
                    true,
                )
            })
            .collect::<Vec<_>>();
        assert!(!translated[0].is_default);
        assert!(translated[1].is_default);
    }

    #[test]
    fn active_catalog_model_is_provider_qualified_for_thread_start() {
        let models = vec![
            PiAvailableModel {
                provider: Some("local-studio".into()),
                id: Some("stale".into()),
                active: Some(false),
                ..Default::default()
            },
            PiAvailableModel {
                provider: Some("local-studio".into()),
                model_id: Some("glm-5.2".into()),
                active: Some(true),
                ..Default::default()
            },
        ];
        assert_eq!(
            qualified_active_model(&models).as_deref(),
            Some("local-studio/glm-5.2")
        );
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

    #[test]
    fn controller_catalog_reads_only_scoped_provider() {
        let catalog = json!({
            "providers": {
                "local-studio": {
                    "models": [
                        {"id": "glm-5.2", "name": "GLM 5.2"}
                    ]
                },
                "openrouter": {
                    "models": [
                        {"id": "other", "name": "Other"}
                    ]
                }
            }
        });
        let models = parse_pi_models_catalog(&catalog, &["local-studio".to_string()]);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].provider.as_deref(), Some("local-studio"));
        assert_eq!(models[0].id.as_deref(), Some("glm-5.2"));
        assert_eq!(models[0].display_name.as_deref(), Some("GLM 5.2"));
    }

    #[tokio::test]
    async fn handle_model_list_propagates_discovery_failure() {
        // Failed discovery must not erase a previously successful client catalog.
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
        assert!(resp.is_err());
    }
}
