# Alleycat ACP Bridge

`alleycat-acp-bridge` presents an ACP-compliant coding agent through the Codex
app-server JSON-RPC surface used by Alleycat clients. The implementation is
agent-neutral; the defaults launch `devin acp`, while Grok and other ACP agents
can be selected through configuration.

## Runtime flow

```text
Codex-shaped client
       │ JSON-RPC
       ▼
AcpBridge / typed handlers
       │ ACP JSON-RPC over stdio
       ▼
ACP agent process
```

- `acp_client.rs` owns the child stdio reader/writer, request correlation, and
  notification fan-out.
- `pool.rs` owns bounded agent processes and idle eviction.
- `handlers.rs` maps Codex requests to ACP sessions and bridge-owned state.
- `streaming.rs` converts live `session/update` notifications into ordered
  Codex item/turn lifecycle notifications.
- `translator.rs` normalizes ACP content, tool calls, plans, and permission
  events into Codex-shaped values.
- `persistence.rs` stores bridge-owned session history when a state directory
  is configured.

## Supported behavior

The bridge negotiates ACP capabilities, creates/resumes/loads sessions, sends
prompts, streams assistant/reasoning/tool content, cancels active prompts, and
reconstructs thread reads from ACP replay plus bridge-owned history.

The Codex-facing surface includes:

- initialization and synthesized account/config/model/capability reads;
- thread list, start, resume, read, name, and a new-session fork projection;
- turn start with live item and turn lifecycle notifications;
- turn interrupt via ACP `session/cancel`;
- ACP file/tool/permission/plan updates rendered as typed Codex items; and
- stable persisted turn/item identifiers across stream, read, and resume.

Methods without a truthful ACP mapping return JSON-RPC `METHOD_NOT_FOUND`.
They are not successful no-ops.

## Explicit limitations

- Direct Codex `command/exec` cannot be routed because its request has no
  thread/session id, while ACP terminal methods require a session id.
- `command/exec/terminate` has the same missing-session problem.
- ACP does not expose streaming terminal stdin or PTY resize for
  `command/exec/write` and `command/exec/resize`.
- Thread rollback/archive/unarchive and Codex review mode have no ACP
  equivalent.
- Turn steering has no ACP primitive; turn interruption is supported.
- MCP startup state, Codex rate limits, collaboration modes, skills, token
  usage, and Codex-only reasoning events may be synthesized, empty, or absent.
- The fork handler creates a new ACP session and a Codex-shaped result; it does
  not clone the source session's complete server-side history.

The live conformance harness records intentional protocol differences. Do not
add a divergence merely to make a failing test green: first prove that the ACP
protocol cannot represent the Codex behavior.

## Configuration

The standalone binary uses stdio by default. `--socket <path>` (or
`ALLEYCAT_BRIDGE_SOCKET`) selects a Unix socket.

```bash
# Devin (defaults)
ACP_BRIDGE_AGENT_BIN=devin \
ACP_BRIDGE_AGENT_ARGS=acp \
cargo run -p alleycat-acp-bridge

# Grok
ACP_BRIDGE_AGENT_BIN=grok \
ACP_BRIDGE_AGENT_ARGS='agent stdio' \
cargo run -p alleycat-acp-bridge
```

Optional runtime settings:

- `ACP_BRIDGE_STATE_DIR`
- `ACP_BRIDGE_POOL_CAPACITY`
- `ACP_BRIDGE_IDLE_TTL_SECS`
- `ACP_BRIDGE_REQUEST_TIMEOUT_SECS`
- `ACP_BRIDGE_MAX_RETRIES`
- `ACP_BRIDGE_RETRY_BACKOFF_MS`

`ACP_BRIDGE_AGENT_ARGS` is whitespace-split. It cannot currently represent an
argument containing embedded spaces.

## Verification

```bash
cargo test -p alleycat-acp-bridge
cargo clippy -p alleycat-acp-bridge --all-targets -- -D warnings

# Live, opt-in: requires a configured ACP agent on PATH.
cargo test -p alleycat-bridge-conformance --test conformance \
  conformance_acp -- --ignored --nocapture
```

Live conformance is intentionally ignored in the default workspace test run.
It exercises an external agent and must never be mistaken for a hermetic unit
test.
