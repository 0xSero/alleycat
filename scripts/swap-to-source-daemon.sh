#!/usr/bin/env bash
# swap-to-source-daemon.sh — run the locally-built `alleycat` source binary as
# the system daemon, in place of the npm-packaged `kittylitter` one, without a
# reboot and without breaking the mobile app's pairing.
#
# Why you'd want this: the mobile app (Litter/kittylitter) is paired to the
# iroh identity stored in host.key. The source `alleycat` binary uses a
# different application name ("alleycat" vs "kittylitter") and therefore
# different config/state dirs, so naively running `alleycat serve` boots a
# *fresh* identity and the app can't connect. This script shares the identity
# via symlinks and repoints the systemd unit's ExecStart at the source binary.
#
# It also handles low-RAM boxes: if a release build is needed, it stops the
# daemon first to free memory (the iroh stack is heavy and can OOM the daemon
# on machines with <4G RAM), then builds, then starts the source binary.
#
# Prerequisites (one-time setup):
#   ln -s ~/.config/kittylitter ~/.config/alleycat
#   ln -s ~/.local/state/kittylitter ~/.local/state/alleycat
#
# Usage (MUST be detached, because stopping the daemon kills any pi session
# running under it — including, potentially, the one driving this script):
#
#   systemd-run --user --unit=alleycat-swap --collect \
#       scripts/swap-to-source-daemon.sh
#
# Then monitor: tail -f /tmp/alleycat-swap.log
#
# Rollback: scripts/revert-to-npm-daemon.sh   (or: systemctl --user revert kittylitter.service)
set -euo pipefail

# Resolve the repo root from the script's own location (scripts/..) so this
# works regardless of where it's invoked from.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$SCRIPT_DIR/.." && pwd)
BIN="$REPO/target/release/alleycat"
UNIT=kittylitter.service
OVERRIDE_DIR="$HOME/.config/systemd/user/$UNIT.d"
LOG=/tmp/alleycat-swap.log
BUILD_LOG=/tmp/alleycat-build.log

exec >>"$LOG" 2>&1
echo "=========================================================="
echo "swap-to-source-daemon starting at $(date -Is)"
echo "pid=$$ user=$(whoami) repo=$REPO"

# Locate cargo: prefer $PATH, fall back to the rustup shim location.
CARGO=$(command -v cargo 2>/dev/null || true)
if [ -z "$CARGO" ]; then
  CARGO="$HOME/.cargo/bin/cargo"
fi
if [ ! -x "$CARGO" ]; then
  echo "ERROR: cargo not found (tried PATH and $CARGO). Aborting." >&2
  exit 1
fi

# 1. If a release binary already exists, skip the build. Otherwise we need to
#    build, and building may need all available RAM, so stop the npm daemon
#    first (it frees ~1-1.5G).
HAVE_BINARY=no
if [ -x "$BIN" ]; then
  HAVE_BINARY=yes
  echo "binary already present: $BIN — skipping build"
fi

if [ "$HAVE_BINARY" = "no" ]; then
  echo "stopping $UNIT to free RAM for build..."
  systemctl --user stop "$UNIT"
  echo "stopped $UNIT at $(date -Is)"
  sleep 3
  echo "free RAM after stop: $(free -h | awk '/^Mem:/ {print $7}')"

  echo "starting build at $(date -Is)..."
  cd "$REPO"
  # Minimal-RAM flags: serial, single codegen unit, opt-level=1, no LTO.
  # On a ~2.8G box this keeps a single rustc under the available ceiling; on
  # roomier boxes you can drop these env vars for a faster parallel build.
  if ! CARGO_BUILD_JOBS=1 \
          CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
          CARGO_PROFILE_RELEASE_LTO=false \
          CARGO_PROFILE_RELEASE_OPT_LEVEL=1 \
          "$CARGO" build --release -j1 -p alleycat 2>>"$BUILD_LOG"; then
    echo "ERROR: build failed (see $BUILD_LOG). Restarting npm daemon and aborting." >&2
    systemctl --user start "$UNIT"
    exit 1
  fi
  echo "build finished at $(date -Is)"
fi

# 2. Verify the binary exists and is executable.
if [ ! -x "$BIN" ]; then
  echo "ERROR: $BIN missing or not executable. Aborting." >&2
  [ "$HAVE_BINARY" = "no" ] && systemctl --user start "$UNIT"
  exit 1
fi
echo "binary OK: $BIN ($("$BIN" --version 2>&1 | head -1))"

# 3. Sanity: the identity symlinks must be in place (else we'd boot a fresh
#    key and break the mobile pairing).
for link in "$HOME/.config/alleycat" "$HOME/.local/state/alleycat"; do
  if [ ! -L "$link" ]; then
    cat >&2 <<EOF
ERROR: $link is not a symlink — identity not shared; aborting.
Fix with:
  ln -s ~/.config/kittylitter ~/.config/alleycat
  ln -s ~/.local/state/kittylitter ~/.local/state/alleycat
EOF
    [ "$HAVE_BINARY" = "no" ] && systemctl --user start "$UNIT"
    exit 1
  fi
done
echo "identity symlinks OK"

# 4. Install the systemd drop-in that repoints ExecStart at the source binary.
mkdir -p "$OVERRIDE_DIR"
cat > "$OVERRIDE_DIR/exec-start.conf" <<EOF
[Service]
# Clear the upstream ExecStart and point at the locally-built source binary.
# Installed by scripts/swap-to-source-daemon.sh — remove to revert.
ExecStart=
ExecStart=$BIN serve
EOF
echo "wrote override: $OVERRIDE_DIR/exec-start.conf"

# 5. Reload so systemd picks up the drop-in.
systemctl --user daemon-reload
echo "daemon-reload done"

# 6. Start the unit — now running the source binary. If the npm daemon is
#    still running (binary-was-already-built path), stop it first.
if systemctl --user is-active --quiet "$UNIT"; then
  echo "stopping npm $UNIT before starting source binary..."
  systemctl --user stop "$UNIT"
fi

systemctl --user start "$UNIT"
echo "started $UNIT at $(date -Is)"

# 7. Give it a few seconds to bind, then report status.
sleep 5
if systemctl --user is-active --quiet "$UNIT"; then
  echo "RESULT: $UNIT active with source binary at $(date -Is)"
  echo "running pid: $(systemctl --user show -p MainPID --value "$UNIT")"
  echo "cmdline: $(cat /proc/$(systemctl --user show -p MainPID --value "$UNIT")/cmdline | tr '\0' ' ')"
else
  echo "RESULT: $UNIT FAILED to start"
  systemctl --user status "$UNIT" --no-pager | tail -30
  exit 1
fi

echo
echo "rollback: systemctl --user revert $UNIT  # restores npm binary"
echo "swap complete at $(date -Is)"
