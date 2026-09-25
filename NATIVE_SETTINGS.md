# Native runtime settings

`config/read` includes runtime-scoped descriptors in `config._litterSettings`.
Configured values come from each runtime's native storage/API. `$native` edits
the complete visible user override object, including settings introduced after
this bridge was released. Credentials are excluded and preserved when writing.
A successful write requires actual persistence; the mobile client then reads
back the value from the selected runtime. Unsupported operations return errors.

| Runtime | Discovery and editable scope |
|---|---|
| Codex | Native app-server configuration, origins and policy requirements. |
| OMP | Installed `omp config list --json` schema, including effective defaults; native `config set` setters. User file is `~/.omp/agent/config.yml`, isolated from Pi. |
| Pi | User `settings.json` plus optional fields from the selected installed package's `dist/core/settings-manager.d.ts`. Fields absent from the user file are explicitly **unset**, not inferred effective defaults. Native imported/structured types remain JSON. |
| Claude Code | Native user `settings.json` plus unset fields from the published JSON Schema explicitly linked by [Claude's official docs](https://code.claude.com/docs/en/settings#edit-a-settings-file). Nested objects, string choices, and local schema references are discovered dynamically. Managed-only and global-config-file fields are read-only; unknown configured keys remain available through native JSON editing. |
| Droid | Native `~/.factory/settings.json`, configured fields and the full JSON editor, plus explicitly typed personal-setting properties from the [official user settings reference](https://docs.factory.ai/droid-cli/settings.md). Enterprise controls and Factory App-only preferences are excluded from user-file discovery. Untyped tables are not interpreted as schemas. |
| Amp | User settings and documented unset keys from the installed CLI's Settings reference (`--help`). `AMP_SETTINGS_FILE` is honored; dotted native keys remain literal. No defaults are guessed. |
| Hermes CLI | Installed `hermes_cli.config.load_config_readonly()` effective defaults and overrides. `$native` edits only the user override file, not a copy of every default. Managed configuration is read-only. |
| Hermes gateway | Read-only when the selected gateway has no settings API. No unrelated local file is presented as gateway settings. |
| OpenCode | Native `/config` GET/PATCH. |
| Devin | Native user `~/.config/devin/config.json` (JSON comments supported) and separate `mcp_config.json`. Configured fields, full native JSON editors, and unset fields from the [official configuration reference](https://docs.devin.ai/cli/reference/configuration/config-file.md). ACP session settings remain separately read-only. |
| Grok | Native user `$GROK_HOME/config.toml` (default `~/.grok/config.toml`), configured fields/full JSON editor, and unset fields from the [official TOML reference](https://docs.x.ai/build/settings/reference.md). Separate `sandbox.toml` profile JSON editing and read-only managed-source rows are included. Requirements pins are read-only and writes touching pinned settings are rejected. ACP session settings remain separately read-only. |
| ACP | Native session `configOptions`, current values and choices; read-only because there is no global settings setter. |

Pi package discovery follows the configured executable on the selected host;
it does not use a mobile device's installed packages for an SSH runtime. If an
installation does not ship declarations, native configured fields and JSON
editing still work. Changes to the installed declarations appear on the next
read without a bundled option list. This is not a claim that every runtime
publishes every unset default.

The user-file view does not claim to represent project-local or managed layers
unless the native runtime supplies them. Native runtimes decide when persisted
changes apply to existing sessions. Direct SSH Pi/Claude JSON operations run
headlessly through the existing launcher and require Python 3 on that host.

Claude's schema source is
[SchemaStore's published Claude Code schema](https://json.schemastore.org/claude-code-settings.json).
The official documentation warns it may lag the installed CLI. Discovery never
substitutes schema defaults for native current values, restricts already-set
values to an older enum, or treats a schema's default as effective configuration.
An absent field is **unset** until the user writes an override. The fetch is
limited to two seconds and one MiB, uses conditional ETag requests, and caches
successes for ten minutes. Failed fetches retain the last good schema and all
configured native values, with a one-minute retry backoff. External schema
references are not fetched. The full native JSON editor remains available
whether schema retrieval succeeds or fails.

Factory discovery reads only typed `Property` metadata before the official
reference's enterprise-settings section. The raw Markdown URL is published in
[Factory's documentation index](https://docs.factory.ai/llms.txt). Its retrieval
uses the same size, timeout, cache and failure bounds as Claude's schema. Prose
such as "Inherits" or "Product default" is not turned into a value. If the
personal/enterprise boundary is missing or the metadata format changes,
discovery falls back to the last good metadata and configured native fields.
Factory's separate OpenAPI managed-settings schema is not used to advertise
writable user settings.

Devin/Grok public references use the same bounded cache as other public metadata.
Only documented option/section identifiers are discovered; examples and default
values are never populated as current settings. Grok environment-variable rows
and its separate `sandbox.toml` profile schema are not treated as `config.toml`
keys. Native JSON/TOML writes preserve unknown fields, value types, symlinks and
hidden credential values, but normalize formatting and omit source comments.
Invalid input files fail closed; they are never replaced with empty defaults.

Published primitive types and closed string choices select the existing typed
mobile controls, while the current value remains `null` for an unset override.
Editors label it Unset and require an explicit edit before saving; document
examples/defaults never become persisted values. TOML date/time values have no
lossless JSON representation here: files containing them require the native
editor and fail before any write, preserving the original contents.
