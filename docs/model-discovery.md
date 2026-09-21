# Native model discovery

Verified 2026-09-21 against the installed harnesses. Catalogs are runtime data;
versioned model names belong in native responses and test fixtures, not fallbacks.
A failed refresh returns an error so clients can retain their last successful catalog.

| Harness | Authoritative source | Verification |
| --- | --- | --- |
| Claude Code 2.1.278 | No-prompt SDK `initialize.models` | Five native aliases; low/medium/high/xhigh/max preserved. Native `apply_flag_settings` controls effort; legacy budgets are reset before adaptive effort. |
| Droid 0.223.0 | No-session `droid.list_models` JSON-RPC | Rebuilt bridge returned 94 models, including 51 custom IDs. Per-model efforts and image support come from native metadata. Native session defaults are read from initialization; turn overrides use `droid.update_session_settings`. |
| Devin 3000.10.31 | `devin models list --format json` | Family variants retain exact `model_uid` values, including context/effort/speed variants. No session is created. |
| Grok 1.0.40 | `grok models` | Native configured IDs and default marker, including custom providers. No session is created. |
| Amp 0.0.1789984754-g4af04f | Current built-in modes plus `amp plugins list` active `agent mode:` rows | 34 plugin mode rows observed; duplicate keys removed. Agent names are not substituted for mode keys. External-agent admin enumeration was permission-denied on this account. |
| OpenCode 1.18.31 | `/config/providers` | Provider-qualified models and only advertised enabled variant keys; selected variants forward to native prompt `variant`. |
| Hermes 0.21.3 | Gateway `/api/model/options`, older gateway `/v1/models` | Provider-qualified selection survives model IDs containing `/`. CLI-only mode reports discovery unavailable rather than inventing a catalog. |
| Generic ACP | Session `configOptions` / legacy `availableModels` | Grouped options and subsequent catalog updates supported. Generic agents without native discovery explain that a session must first be opened. |

Discovery uses the configured headless launcher, bounded waits and bounded output.
Transient catalog children are killed/reaped. Droid/Claude receive no prompt or
session-initialization request during catalog discovery. Provider credentials are
not included in catalog projections; HTTP catalog errors omit request URLs.

Regression coverage includes current native schema fixtures, fresh replacement
catalogs, failed refreshes, provider routing, native effort/variant forwarding,
legacy-budget reset, and cold discovery without creating conversation sessions.

Primary references: [Claude model configuration](https://code.claude.com/docs/en/model-config),
[ACP configuration options](https://agentclientprotocol.com/protocol/v1/session-config-options),
[OpenCode server API](https://opencode.ai/docs/server/),
[OpenCode model variants](https://opencode.ai/docs/models/#variants), and installed
CLI `--help` / no-prompt native protocol responses.
