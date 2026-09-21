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
| Claude Code | Native user `settings.json`, configured fields and the full JSON editor. The installed CLI has no supported global settings-schema enumeration command. `--json-schema` describes response output, not settings. Session initialize metadata is not a global settings schema. |
| Droid | Native `~/.factory/settings.json`, configured fields and the full JSON editor. Initialize returns selected session settings and policy-filtered autonomy levels; it does not enumerate the global user-file schema. Private minified Zod objects in the executable are not a stable schema API. |
| Amp | User settings and documented unset keys from the installed CLI's Settings reference (`--help`). `AMP_SETTINGS_FILE` is honored; dotted native keys remain literal. No defaults are guessed. |
| Hermes CLI | Installed `hermes_cli.config.load_config_readonly()` effective defaults and overrides. `$native` edits only the user override file, not a copy of every default. Managed configuration is read-only. |
| Hermes gateway | Read-only when the selected gateway has no settings API. No unrelated local file is presented as gateway settings. |
| OpenCode | Native `/config` GET/PATCH. |
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
