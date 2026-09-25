# Host storage retention

Daemon diagnostics and saved conversation history have different policies.

## Bounded daemon diagnostics

`daemon.log.YYYY-MM-DD` uses UTC dates. The single daemon writer retains at most
seven exact dated regular files, each at most 8 MiB (56 MiB total file contents).
At the per-file limit it truncates its owned append descriptor, writes a reset
marker, then keeps newer records. An oversized record is replaced by a short
marker, rather than partially writing UTF-8 or exceeding the bound. Existing
oversized retained logs are reset when opened; older dated files are removed.
The tracing queue remains nonblocking and may drop records under overload.
Diagnostics are therefore intentionally lossy, not an audit trail.

Rotation/capping is performed by one background writer after the daemon acquires
its single-instance lock. New files are private on Unix. Symlinks, directories,
unrecognized filenames and unrelated files are not retention candidates. The
bound assumes successful filesystem operations and no external writer modifying
these owned files; failures are propagated rather than silently claiming a cap.

`logs --follow` detects a file getting shorter and resets its read position. If
a cap reset and refill past the old position both occur between polls, it may
skip part of the new beginning. Reading `logs` again shows the retained content.

## Raw service output is a separate remaining limit

The macOS LaunchAgent writes inherited stdout/stderr to `service-startup.log`.
This preserves bootstrap errors and panic diagnostics, and avoids the previous
collision between a live `daemon.log` descriptor and dated-log prefix cleanup.
It does **not** impose a byte limit on raw service output. Older installations
may also leave a bare `daemon.log`; it is not silently deleted. Normal daemon
tracing is not mirrored to this sink because service stderr is not a TTY.

At the audited shipping revision, OpenCode's child server inherits stderr and
can write here throughout its lifetime; this is a concrete residual growth
path, not evidence that all diagnostic storage is bounded. Other inspected
agent pools use piped or null stderr. Linux service stderr follows the user's
systemd journal policy, not this app's file cap.

## Durable session state is not a disposable log cache

Bridge `threads.json` indexes replace their prior contents (core indexes use a
single `.json.tmp` plus rename). Repeated listing does not append another index.
They have no byte/row quota and retain known sessions, including archived rows;
source-file deletion does not currently guarantee stale-row pruning. Retention
must preserve stable IDs, names, archive state and fork links before removing
rows. Temporary space during an atomic update can approach two index copies.

Amp `amp-transcripts/<thread-id>.jsonl` appends completed turns and is read back
to restore conversation history. It has no automatic byte/age quota. Native
agent session stores are owned by those agents. Automatically removing any of
these histories to satisfy a cache quota would risk user data loss; this patch
does not do that. The host grant/config/key files are persistent state, not
per-request diagnostic spools.

The isolated 1,000-Pi-session acceptance on the original 0.3.11 candidate found
15,872 bytes of dated diagnostic log growth per later 140-request cycle, with
the index unchanged. That short check validates the source of growth, not a
long-running leak-free claim. The focused writer tests exercise the cap,
restart, day change, pruning, concurrent producers and filesystem safety.
