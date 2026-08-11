//! Compare a target transcript against the codex (reference) transcript.
//!
//! Three layers of comparison:
//!  - Schema: every frame must round-trip through the typed `codex-proto`
//!    structs (delegated to [`crate::schema`]).
//!  - Method-presence: methods that succeed on codex must succeed on the
//!    target — except those documented in [`KnownDivergence`].
//!  - Notification pattern: for each step, the *kinds* of notifications
//!    emitted by the target must match codex's (modulo allowlist), and
//!    where both emit a given kind, their key-fingerprints must match.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::schema;
use crate::{Frame, FrameKind, TargetId, Transcript};

/// Documented gaps where a target legitimately diverges from codex.
///
/// Bootstrapped from the manual exploration of each bridge's dispatcher.
/// Adding support to a bridge means *removing* the corresponding entry —
/// silent passes are not allowed.
#[derive(Debug, Clone)]
pub struct KnownDivergence {
    pub target: TargetId,
    /// Methods that may return an error (any code) on this target but
    /// succeed on codex. Also skips key-fingerprint comparison entirely
    /// for these methods — use only for methods whose response shape is
    /// fundamentally different (e.g., bridges proxy a different agent's
    /// config tree).
    pub skipped_methods: &'static [&'static str],
    /// Whole scenario steps that are allowed to diverge. Use this only when
    /// the same request method is still checked elsewhere with different
    /// params; for example buffered `command/exec` is required even on
    /// targets that cannot support the streaming `command/exec.streaming`
    /// probe.
    pub skipped_steps: &'static [&'static str],
    /// Notification kinds whose presence/absence/shape may differ in
    /// either direction.
    pub skipped_notifications: &'static [&'static str],
    /// Per-method field-path allowlist: paths in this list are excluded
    /// from the missing/extra key fingerprint diff. Use this for fields
    /// like `permissionProfile`, `agentNickname`, `phase` that bridges
    /// don't populate but the rest of the response shape *does* match
    /// codex — `skipped_methods` would silence too much.
    pub field_path_divergences: &'static [(&'static str, &'static [&'static str])],
    /// Methods allowed to fail the stricter cross-frame streaming checks
    /// during staged bridge rollout.
    pub streaming_relaxed_methods: &'static [&'static str],
    /// Methods allowed to fail semantic content checks during staged bridge
    /// rollout.
    pub semantic_relaxed_methods: &'static [&'static str],
}

impl KnownDivergence {
    pub fn for_target(target: TargetId) -> Self {
        // Bridges proxy *different* coding agents than codex. Methods listed
        // here legitimately diverge in response shape (codex populates
        // fields the underlying agent has no equivalent for) but are not
        // bugs:
        //
        //  - `account/read` — codex tracks a chatgpt account; bridges don't.
        //  - `collaborationMode/list`, `experimentalFeature/list`,
        //    `mcpServerStatus/list` — codex-specific feature surfaces.
        //  - `config/read` — each agent has its own (much smaller) config
        //    tree.
        //  - `model/list`, `skills/list` — codex enriches each entry with
        //    upgrade/availability/icon metadata the bridges don't track.
        //  - `thread/start` / `thread/list` / `thread/read` — bridges
        //    don't synthesize codex-specific defaults like
        //    `permissionProfile`, `serviceTier`, `reasoningEffort` when
        //    the upstream agent doesn't expose them.
        // Methods whose entire response is fundamentally different between
        // codex and the bridge (different agent's config tree, codex-only
        // marketplaces/feature flags, etc.). thread/start/read/list/resume
        // are *not* here — those are field-level allowlisted below so we
        // still compare the parts that do match.
        // `account/read` and `model/list` were previously here but are now
        // handled by the bridges (Account::ApiKey and per-provider model
        // catalogs respectively); they're compared field-by-field instead.
        const SHAPE_DIVERGENT_RESPONSES: &[&str] = &[
            "collaborationMode/list",
            "experimentalFeature/list",
            "mcpServerStatus/list",
            "config/read",
            "skills/list",
        ];

        // Field paths bridges may legitimately not populate. Applied to
        // missing/extra both ways. Keys derive from `schema::fingerprint`
        // (dotted, with `[]` for array elements).
        //
        // Common across all three bridges:
        //  - `permissionProfile`/`reasoningEffort`/`serviceTier` are
        //    Option<T> on `ThreadResumeResponse` etc.; bridges return None
        //    when the underlying agent has no equivalent.
        //  - `thread.agentNickname` / `thread.agentRole` are codex-only.
        //  - `thread.turns[].items[].phase` is codex's per-message phase
        //    metadata; bridges don't track it.
        // Field paths bridges legitimately have empty/null content for —
        // codex populates them from its own (chatgpt-only) feature surfaces.
        // The fingerprint walker treats `null` and missing identically (it
        // skips both), so these are *content* divergences, not schema
        // divergences. The fields ARE present on every bridge response,
        // emitted as `null`; codex just fills them with content.
        //
        //  - `permissionProfile` / `permissionProfile.type`: codex has named
        //    permission profiles ("disabled", per-tool overrides) the bridges
        //    don't model.
        //  - `reasoningEffort`: codex defaults "high" on every thread; the
        //    underlying agents (pi/claude/opencode) expose this differently
        //    (or not at all) and bridges don't synthesize a default.
        //  - `serviceTier`: codex's chatgpt-tier metadata.
        const COMMON_FIELD_DIVERGENCES: &[(&str, &[&str])] = &[
            (
                "thread/start",
                &[
                    "instructionSources",
                    "instructionSources[]",
                    "permissionProfile",
                    "permissionProfile.type",
                    "reasoningEffort",
                    "serviceTier",
                    "thread.agentNickname",
                    "thread.agentRole",
                    "thread.name",
                    "thread.path",
                    "thread.turns[].items[].phase",
                ],
            ),
            (
                "thread/fork",
                &[
                    "instructionSources",
                    "instructionSources[]",
                    "permissionProfile",
                    "permissionProfile.type",
                    "reasoningEffort",
                    "serviceTier",
                    "thread.forkedFromId",
                    "thread.gitInfo",
                    "thread.gitInfo.branch",
                    "thread.gitInfo.originUrl",
                    "thread.gitInfo.sha",
                    "thread.name",
                    "thread.path",
                    "thread.turns[].completedAt",
                    "thread.turns[].durationMs",
                    "thread.turns[].id",
                    "thread.turns[].items",
                    "thread.turns[].itemsView",
                    "thread.turns[].items[]",
                    "thread.turns[].items[].content",
                    "thread.turns[].items[].content[]",
                    "thread.turns[].items[].content[].text",
                    "thread.turns[].items[].content[].text_elements",
                    "thread.turns[].items[].content[].text_elements[]",
                    "thread.turns[].items[].content[].type",
                    "thread.turns[].items[].id",
                    "thread.turns[].items[].phase",
                    "thread.turns[].items[].text",
                    "thread.turns[].items[].type",
                    "thread.turns[].startedAt",
                    "thread.turns[].status",
                ],
            ),
            (
                "thread/resume",
                &[
                    // `serviceTier` is the OpenAI account tier (upstream
                    // schema: `"fast" | "flex" | null`). Codex emits the
                    // user's actual tier; bridges legitimately have no
                    // concept and emit null. Real content gap, valid
                    // wire shape (won't fail upstream-schema check).
                    "serviceTier",
                    //codex's per-message phase metadata; bridges have no
                    // analogue. claude's stream-json doesn't carry a
                    // turn-internal phase field; pi/opencode similarly.
                    "thread.turns[].items[].phase",
                    "thread.agentNickname",
                    "thread.agentRole",
                    "thread.name",
                    "thread.path",
                    "thread.turns[].durationMs",
                    "thread.turns[].items[].aggregatedOutput",
                    "thread.turns[].items[].arguments",
                    "thread.turns[].items[].arguments.cmd",
                    "thread.turns[].items[].command",
                    "thread.turns[].items[].commandActions",
                    "thread.turns[].items[].commandActions[]",
                    "thread.turns[].items[].commandActions[].command",
                    "thread.turns[].items[].commandActions[].name",
                    "thread.turns[].items[].commandActions[].path",
                    "thread.turns[].items[].commandActions[].type",
                    "thread.turns[].items[].contentItems",
                    "thread.turns[].items[].contentItems[]",
                    "thread.turns[].items[].contentItems[].text",
                    "thread.turns[].items[].contentItems[].type",
                    "thread.turns[].items[].cwd",
                    "thread.turns[].items[].durationMs",
                    "thread.turns[].items[].exitCode",
                    "thread.turns[].items[].namespace",
                    "thread.turns[].items[].source",
                    "thread.turns[].items[].status",
                    "thread.turns[].items[].success",
                    "thread.turns[].items[].summary",
                    "thread.turns[].items[].summary[]",
                    "thread.turns[].items[].tool",
                    "permissionProfile",
                    "permissionProfile.type",
                    "thread.turns[].error",
                    "thread.turns[].error.message",
                ],
            ),
            (
                "thread/read",
                &[
                    "thread.agentNickname",
                    "thread.agentRole",
                    "thread.path",
                    "thread.name",
                    "thread.turns[].durationMs",
                    "thread.turns[].error",
                    "thread.turns[].error.message",
                    "thread.turns[].items[].arguments",
                    "thread.turns[].items[].arguments.cmd",
                    "thread.turns[].items[].phase",
                    "thread.turns[].items[].aggregatedOutput",
                    "thread.turns[].items[].command",
                    "thread.turns[].items[].commandActions",
                    "thread.turns[].items[].commandActions[]",
                    "thread.turns[].items[].commandActions[].command",
                    "thread.turns[].items[].commandActions[].name",
                    "thread.turns[].items[].commandActions[].path",
                    "thread.turns[].items[].commandActions[].type",
                    "thread.turns[].items[].contentItems",
                    "thread.turns[].items[].contentItems[]",
                    "thread.turns[].items[].contentItems[].text",
                    "thread.turns[].items[].contentItems[].type",
                    "thread.turns[].items[].cwd",
                    "thread.turns[].items[].durationMs",
                    "thread.turns[].items[].exitCode",
                    "thread.turns[].items[].namespace",
                    "thread.turns[].items[].source",
                    "thread.turns[].items[].status",
                    "thread.turns[].items[].success",
                    "thread.turns[].items[].summary",
                    "thread.turns[].items[].summary[]",
                    "thread.turns[].items[].tool",
                ],
            ),
            (
                "thread/rollback",
                &[
                    "thread.name",
                    "thread.turns[].completedAt",
                    "thread.turns[].durationMs",
                    "thread.turns[].id",
                    "thread.turns[].items",
                    "thread.turns[].itemsView",
                    "thread.turns[].items[]",
                    "thread.turns[].items[].content",
                    "thread.turns[].items[].content[]",
                    "thread.turns[].items[].content[].text",
                    "thread.turns[].items[].content[].text_elements",
                    "thread.turns[].items[].content[].text_elements[]",
                    "thread.turns[].items[].content[].type",
                    "thread.turns[].items[].id",
                    "thread.turns[].items[].phase",
                    "thread.turns[].items[].summary",
                    "thread.turns[].items[].summary[]",
                    "thread.turns[].items[].text",
                    "thread.turns[].items[].type",
                    "thread.turns[].startedAt",
                    "thread.turns[].status",
                ],
            ),
            (
                "thread/unarchive",
                &[
                    "thread.agentNickname",
                    "thread.agentRole",
                    "thread.name",
                    "thread.path",
                ],
            ),
            (
                "account/read",
                &[
                    // Populated only on the `Chatgpt` Account variant. Bridges
                    // return `Account::ApiKey {}` which carries no identity
                    // metadata.
                    "account.email",
                    "account.planType",
                ],
            ),
            (
                "model/list",
                &[
                    // codex announces newly-shipped models via these fields
                    // (e.g. the GPT-5.5 availability nux, upgrade
                    // suggestions for older models). Bridges proxy other
                    // agents that don't ship marketing copy.
                    "data[].availabilityNux",
                    "data[].availabilityNux.message",
                    "data[].serviceTiers[]",
                    "data[].serviceTiers[].description",
                    "data[].serviceTiers[].id",
                    "data[].serviceTiers[].name",
                    "data[].upgrade",
                    "data[].upgradeInfo",
                    "data[].upgradeInfo.model",
                    "data[].upgradeInfo.upgradeCopy",
                    "data[].upgradeInfo.modelLink",
                    "data[].upgradeInfo.migrationMarkdown",
                ],
            ),
            (
                "thread/list",
                &[
                    "data[].cliVersion",
                    "data[].createdAt",
                    "data[].cwd",
                    "data[].ephemeral",
                    "data[].forkedFromId",
                    "data[].id",
                    "data[].modelProvider",
                    // Thread titles are content, not shape. Codex and
                    // opencode can auto-title threads; claude/pi may only
                    // have an explicit name after `thread/name/set`.
                    "data[].name",
                    "data[].path",
                    "data[].preview",
                    "data[].sessionId",
                    "data[].source",
                    "data[].status",
                    "data[].status.type",
                    "data[].turns",
                    "data[].turns[]",
                    "data[].updatedAt",
                    "data[].agentNickname",
                    "data[].agentRole",
                    // codex paginates at 25 entries; bridges return all
                    // matching threads in one page so the cursors are null
                    // (and `skip_serializing_if`-omitted from the wire).
                    "nextCursor",
                    "backwardsCursor",
                ],
            ),
            (
                "thread/turns/list",
                &[
                    "backwardsCursor",
                    "data[].error",
                    "data[].error.message",
                    "data[].durationMs",
                    "data[].items[].phase",
                    "data[].items[].summary",
                    "data[].items[].summary[]",
                ],
            ),
            (
                "turn/start",
                &[
                    "turn.completedAt",
                    "turn.durationMs",
                    "turn.items[].aggregatedOutput",
                    "turn.items[].command",
                    "turn.items[].commandActions",
                    "turn.items[].commandActions[]",
                    "turn.items[].content",
                    "turn.items[].contentItems",
                    "turn.items[].contentItems[]",
                    "turn.items[].contentItems[].text",
                    "turn.items[].contentItems[].type",
                    "turn.items[].content[]",
                    "turn.items[].content[].text",
                    "turn.items[].content[].text_elements",
                    "turn.items[].content[].text_elements[]",
                    "turn.items[].content[].type",
                    "turn.items[].cwd",
                    "turn.items[].durationMs",
                    "turn.items[].id",
                    "turn.items[].source",
                    "turn.items[].status",
                    "turn.items[].success",
                    "turn.items[].summary",
                    "turn.items[].summary[]",
                    "turn.items[].text",
                    "turn.items[].tool",
                    "turn.items[].type",
                    "turn.startedAt",
                ],
            ),
        ];
        // codex emits these notifications around its own MCP/account/
        // session lifecycle; the bridges have no equivalent so they never
        // fire (or have nothing to report when they do).
        const SHAPE_DIVERGENT_NOTIFICATIONS: &[&str] = &[
            "mcpServer/startupStatus/updated",
            "account/rateLimits/updated",
            // Codex app-server emits these for its own host-side remote
            // control / persisted-goal subsystems. The bridges do not own
            // those Codex-local controllers.
            "remoteControl/status/changed",
            "thread/goal/cleared",
            // `tokenUsage` differs because bridges report their own
            // token counts which often lack `modelContextWindow`.
            "thread/tokenUsage/updated",
            // `item/started`/`item/completed` payload shapes diverge for
            // items codex enriches (e.g., `item.phase`); the streaming
            // check in `crate::streaming` still validates the lifecycle.
            "item/started",
            "item/completed",
            // codex emits `thread/status/changed` whenever it transitions
            // the session between idle and active; bridges that don't
            // model that state machine just stay implicit.
            "thread/status/changed",
            // Thread and turn lifecycle notifications are timing-sensitive:
            // some bridges surface the same state in the response or final
            // history load, while others emit live notifications. The
            // standalone semantic and streaming checks still validate that
            // turns actually produce useful item lifecycles.
            "thread/started",
            "thread/archived",
            "thread/unarchived",
            "thread/compacted",
            "turn/started",
            "turn/completed",
            // Codex emits hook lifecycle notifications for local hook runs.
            // Bridges may not have hook execution at all.
            "hook/started",
            "hook/completed",
            // codex surfaces startup warnings from its own MCP/sandbox
            // boot path; bridges don't have these warning sources.
            "warning",
            // claude/pi stream bash stdout incrementally as
            // `item/commandExecution/outputDelta`; codex's unifiedExec path
            // emits begin+end with the full aggregatedOutput on the end
            // event (no deltas). Per-byte vs final-blob is a streaming
            // mechanism choice, not a wire-shape bug.
            "item/commandExecution/outputDelta",
            // pi/opencode stream model reasoning incrementally; codex's
            // gpt-5.5 doesn't reason (or reasoning is invisible). Whether
            // a reasoning event fires is a per-model decision, not a
            // wire-shape bug.
            "item/reasoning/textDelta",
            "item/reasoning/summaryTextDelta",
            "item/reasoning/summaryPartAdded",
        ];
        const INTERRUPTED_TURN_STREAMING_RELAXED: &[&str] = &["turn/start.interruptible"];
        const NO_STREAMING_EXEC_STEPS: &[&str] = &[
            "command/exec.streaming",
            "command/exec/write",
            "command/exec/resize",
        ];
        const TURN_STEER_STEP: &[&str] = &["turn/steer"];

        match target {
            TargetId::Codex => Self {
                target,
                skipped_methods: &[],
                skipped_steps: &[],
                skipped_notifications: &[],
                field_path_divergences: &[],
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[],
            },
            TargetId::Pi => Self {
                target,
                // Pi: review/start unimplemented + all architectural
                // divergences from the const lists above.
                skipped_methods: concat_static(
                    &["review/start", "command/exec/write", "command/exec/resize"],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: concat_static(NO_STREAMING_EXEC_STEPS, TURN_STEER_STEP),
                // Pi additionally emits a one-off `configWarning` advising
                // clients that pi-bridge v1 doesn't proxy MCP servers; codex
                // never emits this and there's no equivalent on the codex
                // side, so allowlist the extra.
                skipped_notifications: concat_static(
                    &["configWarning"],
                    SHAPE_DIVERGENT_NOTIFICATIONS,
                ),
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[],
            },
            TargetId::Claude => Self {
                target,
                skipped_methods: concat_static(
                    &[
                        "review/start",
                        "thread/rollback",
                        "command/exec/write",
                        "command/exec/resize",
                    ],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: concat_static(NO_STREAMING_EXEC_STEPS, TURN_STEER_STEP),
                skipped_notifications: concat_static(&["error"], SHAPE_DIVERGENT_NOTIFICATIONS),
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[],
            },
            TargetId::Amp => Self {
                target,
                skipped_methods: concat_static(
                    &[
                        "thread/fork",
                        "thread/rollback",
                        "mcpServer/oauth/login",
                        // Amp turns can finish before the harness gets a
                        // deterministic interrupt window, especially on the
                        // short live prompt used here. The bridge still
                        // implements interrupt for an active process; the
                        // live conformance scenario cannot force Amp to stay
                        // busy long enough every run.
                        "turn/interrupt",
                        "review/start",
                        "command/exec/write",
                        "command/exec/resize",
                    ],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: concat_static(
                    concat_static(NO_STREAMING_EXEC_STEPS, TURN_STEER_STEP),
                    &["turn/start.tool", "turn/start.interruptible"],
                ),
                skipped_notifications: SHAPE_DIVERGENT_NOTIFICATIONS,
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[
                    // Amp's live continued-thread CLI can refuse or avoid the
                    // exact shell-tool prompt after prior context, so the
                    // commandExecution item is not deterministic enough for a
                    // hard semantic assertion. The transcript still records
                    // the final assistant turn and history shape.
                    "turn/start.tool",
                ],
            },
            TargetId::Opencode => Self {
                target,
                skipped_methods: concat_static(
                    &[
                        "account/login/start",
                        "account/login/cancel",
                        "account/logout",
                        "mcpServer/oauth/login",
                        "skills/config/write",
                        "configRequirements/read",
                        "thread/turns/list",
                        "review/start",
                    ],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: TURN_STEER_STEP,
                skipped_notifications: concat_static(
                    &[
                        "error",
                        "thread/started",
                        "thread/closed",
                        "skills/changed",
                        "turn/diff/updated",
                        "turn/plan/updated",
                        // Opencode reasoning is wired (see translate/events.rs)
                        // but only fires when the underlying model reasons.
                        // Codex's call may not reason in the same turn, so
                        // textDelta on opencode and not codex is fine.
                        "item/reasoning/textDelta",
                        "item/reasoning/summaryTextDelta",
                        "item/reasoning/summaryPartAdded",
                        "item/mcpToolCall/progress",
                        "item/dynamicToolCall/argumentsDelta",
                        // Opencode auto-generates a thread title during the
                        // first turn; codex never does.
                        "thread/name/updated",
                        "model/rerouted",
                        "configWarning",
                        "deprecationNotice",
                        "serverRequest/resolved",
                    ],
                    SHAPE_DIVERGENT_NOTIFICATIONS,
                ),
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[
                    // Opencode persists bash tool calls into history, but its
                    // SSE stream can complete the turn with only reasoning and
                    // final text visible. `thread/read.afterTool` still
                    // proves the commandExecution item and marker output.
                    "turn/start.tool",
                ],
            },

            TargetId::Hermes => Self {
                target,
                skipped_methods: concat_static(
                    &[
                        "account/login/start",
                        "account/login/cancel",
                        "account/logout",
                        "mcpServer/oauth/login",
                        "skills/config/write",
                        "thread/fork",
                        "thread/compact/start",
                        "thread/rollback",
                        "review/start",
                    ],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: concat_static(NO_STREAMING_EXEC_STEPS, TURN_STEER_STEP),
                skipped_notifications: concat_static(
                    &[
                        "thread/name/updated",
                        "turn/diff/updated",
                        "turn/plan/updated",
                        "item/mcpToolCall/progress",
                        "item/dynamicToolCall/argumentsDelta",
                        "model/rerouted",
                        "configWarning",
                        "deprecationNotice",
                        "serverRequest/resolved",
                    ],
                    SHAPE_DIVERGENT_NOTIFICATIONS,
                ),
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[
                    // Hermes CLI oneshot exposes final text only, not the
                    // backend's internal tool-call item lifecycle. The
                    // marker remains checked in `thread/read.afterTool`.
                    "turn/start.tool",
                ],
            },
            TargetId::Droid => Self {
                target,
                skipped_methods: concat_static(
                    &[
                        "mcpServer/oauth/login",
                        "thread/fork",
                        "thread/archive",
                        "thread/unarchive",
                        "thread/rollback",
                        "review/start",
                        "command/exec/write",
                        "command/exec/resize",
                    ],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: concat_static(NO_STREAMING_EXEC_STEPS, TURN_STEER_STEP),
                skipped_notifications: concat_static(
                    &["thread/name/updated"],
                    SHAPE_DIVERGENT_NOTIFICATIONS,
                ),
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[
                    // ACP agents can satisfy the marker check through
                    // non-command tools (for example fs/read_text_file)
                    // rather than a Codex commandExecution item. History
                    // still has to contain the marker.
                    "turn/start.tool",
                ],
            },
            TargetId::Acp => Self {
                target,
                // ACP: methods that return METHOD_NOT_FOUND (not supported by ACP protocol)
                skipped_methods: concat_static(
                    &[
                        "account/login/start",
                        "account/login/cancel",
                        "account/logout",
                        "feedback/upload",
                        "mcpServer/oauth/login",
                        "config/value/write",
                        "config/batchWrite",
                        "config/mcpServer/reload",
                        "mock/experimentalMethod",
                        "skills/remote/list",
                        "skills/remote/export",
                        "skills/config/write",
                        "thread/resume",
                        "thread/compact/start",
                        "thread/rollback",
                        "thread/archive",
                        "thread/unarchive",
                        "thread/turns/list",
                        "thread/loaded/list",
                        "thread/backgroundTerminals/clean",
                        "review/start",
                        "command/exec",
                        "command/exec/write",
                        "command/exec/resize",
                        "turn/steer",
                    ],
                    SHAPE_DIVERGENT_RESPONSES,
                ),
                skipped_steps: concat_static(NO_STREAMING_EXEC_STEPS, TURN_STEER_STEP),
                skipped_notifications: concat_static(
                    &[
                        // thread/name/updated is now implemented
                    ],
                    SHAPE_DIVERGENT_NOTIFICATIONS,
                ),
                field_path_divergences: COMMON_FIELD_DIVERGENCES,
                streaming_relaxed_methods: INTERRUPTED_TURN_STREAMING_RELAXED,
                semantic_relaxed_methods: &[
                    // ACP agents can satisfy the marker check through
                    // non-command tools (for example fs/read_text_file)
                    // rather than a Codex commandExecution item. History
                    // still has to contain the marker.
                    "turn/start.tool",
                ],
            },
        }
    }

    pub fn skips_step(&self, step: &str) -> bool {
        self.skipped_steps.contains(&step)
    }

    pub fn skips_response(&self, frame: &Frame) -> bool {
        self.skipped_methods.contains(&frame.method.as_str()) || self.skips_step(&frame.step)
    }

    pub fn relaxes_streaming(&self, finding: &Finding) -> bool {
        method_relaxed(finding, self.streaming_relaxed_methods)
    }

    pub fn relaxes_semantic(&self, finding: &Finding) -> bool {
        method_relaxed(finding, self.semantic_relaxed_methods)
    }
}

fn method_relaxed(finding: &Finding, methods: &[&str]) -> bool {
    let method = finding.method();
    let step = finding.step();
    let step_method = finding.step().split('.').next().unwrap_or(finding.step());
    methods.contains(&method) || methods.contains(&step) || methods.contains(&step_method)
}

/// Const-friendly concatenation of two `&[&'static str]` slices. Returns a
/// leaked static slice so the result satisfies `&'static [&'static str]`. The
/// allocation happens once per process.
fn concat_static(
    a: &'static [&'static str],
    b: &'static [&'static str],
) -> &'static [&'static str] {
    // SAFETY: leaking a Vec produces a slice that lives for the rest of the
    // process — exactly what we want for these allowlist tables.
    let mut out = Vec::with_capacity(a.len() + b.len());
    out.extend_from_slice(a);
    out.extend_from_slice(b);
    Box::leak(out.into_boxed_slice())
}

#[derive(Debug, Clone)]
pub struct ConformanceReport {
    pub target: TargetId,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone)]
pub enum Finding {
    /// Frame failed typed deserialize.
    SchemaError {
        step: String,
        method: String,
        kind: FrameKind,
        message: String,
    },
    /// Frame failed validation against the upstream codex-rs JSON schema.
    /// Independent verification: a violation here is a real wire-spec gap,
    /// not just a drift between bridge output and our `codex-proto` mirror.
    UpstreamSchemaError {
        step: String,
        method: String,
        kind: FrameKind,
        message: String,
    },
    /// Frame passed schema checks but violated a method-specific semantic
    /// contract (for example an empty `model/list` catalog).
    SemanticViolation {
        step: String,
        method: String,
        contract: String,
        detail: String,
    },
    /// Method returned an error on the target but succeeded on codex (and the
    /// method is not in the per-target allowlist).
    UnexpectedError {
        step: String,
        method: String,
        code: i64,
        message: String,
    },
    /// Codex emitted a notification kind during a step that the target did
    /// not emit.
    MissingNotification { step: String, method: String },
    /// Target emitted a notification kind that codex did not emit during the
    /// same step.
    ExtraNotification { step: String, method: String },
    /// Both codex and target emitted a frame for the same step+method but
    /// the populated key set differs. `missing` is keys present in codex but
    /// absent on the target; `extra` is keys present on the target but
    /// absent on codex.
    KeyDifference {
        step: String,
        method: String,
        kind: FrameKind,
        missing: BTreeSet<String>,
        extra: BTreeSet<String>,
    },
}

impl Finding {
    pub fn step(&self) -> &str {
        match self {
            Finding::SchemaError { step, .. }
            | Finding::UpstreamSchemaError { step, .. }
            | Finding::SemanticViolation { step, .. }
            | Finding::UnexpectedError { step, .. }
            | Finding::MissingNotification { step, .. }
            | Finding::ExtraNotification { step, .. }
            | Finding::KeyDifference { step, .. } => step,
        }
    }

    pub fn method(&self) -> &str {
        match self {
            Finding::SchemaError { method, .. }
            | Finding::UpstreamSchemaError { method, .. }
            | Finding::SemanticViolation { method, .. }
            | Finding::UnexpectedError { method, .. }
            | Finding::MissingNotification { method, .. }
            | Finding::ExtraNotification { method, .. }
            | Finding::KeyDifference { method, .. } => method,
        }
    }
}

impl ConformanceReport {
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }
}

impl fmt::Display for ConformanceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[conformance:{}] {} finding(s)",
            self.target,
            self.findings.len()
        )?;
        if self.findings.is_empty() {
            writeln!(f, "\nmethod | status | findings")?;
            writeln!(f, "--- | --- | ---")?;
            writeln!(f, "(all) | PASS | 0")?;
            return Ok(());
        }

        let mut by_method: BTreeMap<&str, Vec<&Finding>> = BTreeMap::new();
        for finding in &self.findings {
            by_method.entry(finding.method()).or_default().push(finding);
        }

        writeln!(f, "\nmethod | status | findings")?;
        writeln!(f, "--- | --- | ---")?;
        for (method, findings) in &by_method {
            writeln!(
                f,
                "{method} | FAIL({}) | {}",
                findings.len(),
                findings.len()
            )?;
        }

        writeln!(f, "\ndetailed findings")?;
        for (method, findings) in by_method {
            writeln!(f, "\n{method}")?;
            for (i, finding) in findings.iter().enumerate() {
                write!(f, "  #{i}: ")?;
                write_finding_detail(f, finding)?;
            }
        }
        Ok(())
    }
}

fn write_finding_detail(f: &mut fmt::Formatter<'_>, finding: &Finding) -> fmt::Result {
    match finding {
        Finding::SchemaError {
            step,
            method,
            kind,
            message,
        } => {
            writeln!(
                f,
                "schema error in {kind:?} {method} (step={step}): {message}"
            )
        }
        Finding::UpstreamSchemaError {
            step,
            method,
            kind,
            message,
        } => {
            writeln!(
                f,
                "upstream-schema violation in {kind:?} {method} (step={step}): {message}"
            )
        }
        Finding::SemanticViolation {
            step,
            method,
            contract,
            detail,
        } => {
            writeln!(
                f,
                "semantic violation for {method}.{contract} (step={step}): {detail}"
            )
        }
        Finding::UnexpectedError {
            step,
            method,
            code,
            message,
        } => {
            writeln!(
                f,
                "unexpected error response for {method} (step={step}, code={code}): {message}"
            )
        }
        Finding::MissingNotification { step, method } => {
            writeln!(f, "missing notification {method} in step {step}")
        }
        Finding::ExtraNotification { step, method } => {
            writeln!(f, "unexpected extra notification {method} in step {step}")
        }
        Finding::KeyDifference {
            step,
            method,
            kind,
            missing,
            extra,
        } => {
            writeln!(
                f,
                "key fingerprint diff for {kind:?} {method} (step={step}):"
            )?;
            if !missing.is_empty() {
                writeln!(f, "      missing on target: {missing:?}")?;
            }
            if !extra.is_empty() {
                writeln!(f, "      extra on target:   {extra:?}")?;
            }
            Ok(())
        }
    }
}

/// Run all three layers of the conformance check on `target` against
/// `reference`.
pub fn compare(reference: &Transcript, target: &Transcript) -> ConformanceReport {
    let mut report = ConformanceReport {
        target: target.target,
        findings: Vec::new(),
    };
    let div = KnownDivergence::for_target(target.target);

    // Layer 1: schema for every frame on the target.
    for frame in &target.frames {
        let chk = schema::check(frame);
        // Schema deserialize errors are always findings, regardless of the
        // divergence allowlist — a stub returning `null` is the bug we want
        // to surface.
        if let Some(err) = chk.deserialize_error.clone() {
            report.findings.push(Finding::SchemaError {
                step: frame.step.clone(),
                method: frame.method.clone(),
                kind: frame.kind,
                message: err,
            });
        }
        // Independent layer: validate against the upstream codex-rs JSON
        // schemas (skipped silently when the schema dir isn't present).
        // Error frames are exempt — their `result` is null and the schema
        // expects a populated payload.
        if !chk.is_error_response()
            && let Err(err) = crate::upstream_schema::validate(frame, target.target)
        {
            report.findings.push(Finding::UpstreamSchemaError {
                step: frame.step.clone(),
                method: frame.method.clone(),
                kind: frame.kind,
                message: err,
            });
        }
        // Error responses for non-allowlisted methods → UnexpectedError.
        if frame.kind == FrameKind::Response
            && let (Some(code), Some(msg)) = (chk.error_code, chk.error_message)
            && !div.skips_response(frame)
        {
            report.findings.push(Finding::UnexpectedError {
                step: frame.step.clone(),
                method: frame.method.clone(),
                code,
                message: msg,
            });
        }
    }

    // Layer 2 + 3: per-step comparison against codex.
    let ref_by_step = group_by_step(reference);
    let tgt_by_step = group_by_step(target);
    let all_steps: BTreeSet<&str> = ref_by_step
        .keys()
        .chain(tgt_by_step.keys())
        .copied()
        .collect();
    for step in all_steps {
        if div.skips_step(step) {
            continue;
        }
        let ref_frames = ref_by_step.get(step).copied().unwrap_or(&[][..]);
        let tgt_frames = tgt_by_step.get(step).copied().unwrap_or(&[][..]);

        // Notifications: which kinds appeared on each side?
        let ref_notif_kinds: BTreeSet<&str> = ref_frames
            .iter()
            .filter(|f| f.kind == FrameKind::Notification)
            .map(|f| f.method.as_str())
            .collect();
        let tgt_notif_kinds: BTreeSet<&str> = tgt_frames
            .iter()
            .filter(|f| f.kind == FrameKind::Notification)
            .map(|f| f.method.as_str())
            .collect();
        for kind in ref_notif_kinds.difference(&tgt_notif_kinds) {
            if !div.skipped_notifications.contains(kind) {
                report.findings.push(Finding::MissingNotification {
                    step: step.to_string(),
                    method: kind.to_string(),
                });
            }
        }
        for kind in tgt_notif_kinds.difference(&ref_notif_kinds) {
            // `skipped_notifications` documents notification kinds whose
            // presence/absence is allowed to differ in either direction
            // — codex may emit them and the bridge not, or vice versa.
            // Extras outside the allowlist are still surfaced.
            if div.skipped_notifications.contains(kind) {
                continue;
            }
            report.findings.push(Finding::ExtraNotification {
                step: step.to_string(),
                method: kind.to_string(),
            });
        }

        // Per-method/per-kind key fingerprint comparison. We union all
        // fingerprints for a given (method, FrameKind) within the step on
        // each side and compare the unions — captures optional fields that
        // appear on only some entries.
        let ref_fp = group_fingerprints(ref_frames);
        let tgt_fp = group_fingerprints(tgt_frames);
        let mut keys: BTreeSet<&(String, FrameKind)> = BTreeSet::new();
        keys.extend(ref_fp.keys());
        keys.extend(tgt_fp.keys());
        for key in keys {
            let (method, kind) = key;
            let r = ref_fp.get(key).cloned().unwrap_or_default();
            let t = tgt_fp.get(key).cloned().unwrap_or_default();
            if r.is_empty() && t.is_empty() {
                continue;
            }
            // Skip key-set comparison entirely when:
            //  - the method is in the per-target skipped_methods list
            //    (response frames) — we already either filtered it via
            //    UnexpectedError or accepted it.
            //  - the method (for notifications) is in skipped_notifications.
            let allow_skip = match kind {
                FrameKind::Response => div.skipped_methods.contains(&method.as_str()),
                FrameKind::Notification => div.skipped_notifications.contains(&method.as_str()),
            };
            if allow_skip {
                continue;
            }
            // Field paths the divergence allowlist marks as expected for
            // this method (codex-specific fields the bridge doesn't carry,
            // or vice versa). Subtract from both sides before reporting.
            let allowed_paths: BTreeSet<&str> = div
                .field_path_divergences
                .iter()
                .filter(|(m, _)| *m == method.as_str())
                .flat_map(|(_, paths)| paths.iter().copied())
                .collect();
            let missing: BTreeSet<String> = r
                .difference(&t)
                .filter(|p| !allowed_paths.contains(p.as_str()))
                .cloned()
                .collect();
            let extra: BTreeSet<String> = t
                .difference(&r)
                .filter(|p| !allowed_paths.contains(p.as_str()))
                .cloned()
                .collect();
            if !missing.is_empty() || !extra.is_empty() {
                report.findings.push(Finding::KeyDifference {
                    step: step.to_string(),
                    method: method.clone(),
                    kind: *kind,
                    missing,
                    extra,
                });
            }
        }
    }

    report
}

fn group_by_step(t: &Transcript) -> BTreeMap<&str, &[Frame]> {
    let mut out = BTreeMap::new();
    let mut current: &str = "";
    let mut start = 0usize;
    for (i, f) in t.frames.iter().enumerate() {
        if f.step != current {
            if !current.is_empty() {
                out.insert(current, &t.frames[start..i]);
            }
            current = f.step.as_str();
            start = i;
        }
    }
    if !current.is_empty() {
        out.insert(current, &t.frames[start..]);
    }
    out
}

fn group_fingerprints(frames: &[Frame]) -> BTreeMap<(String, FrameKind), BTreeSet<String>> {
    let mut out: BTreeMap<(String, FrameKind), BTreeSet<String>> = BTreeMap::new();
    for f in frames {
        let chk = schema::check(f);
        if !chk.fingerprint.is_empty() {
            let key = (f.method.clone(), f.kind);
            out.entry(key).or_default().extend(chk.fingerprint);
        }
    }
    out
}

/// Run schema validation in isolation (no reference required). Used when a
/// target is the only one available — still surfaces deserialize failures.
pub fn schema_only(transcript: &Transcript) -> Vec<Finding> {
    let mut findings = Vec::new();
    let div = KnownDivergence::for_target(transcript.target);
    for frame in &transcript.frames {
        let chk = schema::check(frame);
        if let Some(err) = chk.deserialize_error.clone() {
            findings.push(Finding::SchemaError {
                step: frame.step.clone(),
                method: frame.method.clone(),
                kind: frame.kind,
                message: err,
            });
        }
        if !chk.is_error_response()
            && let Err(err) = crate::upstream_schema::validate(frame, transcript.target)
        {
            findings.push(Finding::UpstreamSchemaError {
                step: frame.step.clone(),
                method: frame.method.clone(),
                kind: frame.kind,
                message: err,
            });
        }
        if frame.kind == FrameKind::Response
            && let (Some(code), Some(msg)) = (chk.error_code, chk.error_message.clone())
            && !div.skips_response(frame)
        {
            findings.push(Finding::UnexpectedError {
                step: frame.step.clone(),
                method: frame.method.clone(),
                code,
                message: msg,
            });
        }
    }
    findings
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame(step: &str, method: &str, kind: FrameKind, raw: serde_json::Value) -> Frame {
        Frame {
            step: step.to_string(),
            method: method.to_string(),
            kind,
            raw,
        }
    }

    #[test]
    fn identical_transcripts_have_no_findings() {
        let mut a = Transcript::new(TargetId::Codex);
        let mut b = Transcript::new(TargetId::Pi);
        let resp = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "userAgent": "x", "codexHome": "/tmp",
                "platformFamily": "unix", "platformOs": "linux"
            }
        });
        a.push(frame(
            "initialize",
            "initialize",
            FrameKind::Response,
            resp.clone(),
        ));
        b.push(frame("initialize", "initialize", FrameKind::Response, resp));
        let report = compare(&a, &b);
        assert!(report.is_clean(), "{report}");
    }

    #[test]
    fn missing_field_is_flagged() {
        let mut a = Transcript::new(TargetId::Codex);
        let mut b = Transcript::new(TargetId::Pi);
        a.push(frame("step", "initialize", FrameKind::Response, json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"userAgent": "x", "codexHome": "/h", "platformFamily": "u", "platformOs": "l"}
        })));
        b.push(frame(
            "step",
            "initialize",
            FrameKind::Response,
            json!({
                "jsonrpc": "2.0", "id": 1,
                "result": {"userAgent": "x", "platformFamily": "u", "platformOs": "l"}
            }),
        ));
        let report = compare(&a, &b);
        // Pi's response is missing "codexHome" -> fingerprint diff *and*
        // schema (typed) failure.
        assert!(!report.is_clean());
        let has_key_diff = report.findings.iter().any(|f| {
            matches!(
                f, Finding::KeyDifference { missing, .. } if missing.contains("codexHome")
            )
        });
        assert!(has_key_diff, "{report}");
    }

    #[test]
    fn opencode_known_divergence_is_quiet() {
        let mut a = Transcript::new(TargetId::Codex);
        let b = Transcript::new(TargetId::Opencode);
        a.push(frame(
            "turn/start",
            "thread/started",
            FrameKind::Notification,
            json!({
                "jsonrpc": "2.0", "method": "thread/started",
                "params": {"thread": {}}
            }),
        ));
        // Opencode does not emit thread/started — that's allowlisted.
        let report = compare(&a, &b);
        // We do still get a SchemaError for codex's empty thread struct, but
        // crucially no MissingNotification on opencode.
        assert!(!report
            .findings
            .iter()
            .any(|f| matches!(f, Finding::MissingNotification { method, .. } if method == "thread/started")));
    }
}
