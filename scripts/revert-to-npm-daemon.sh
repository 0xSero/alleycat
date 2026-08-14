#!/usr/bin/env bash
# revert-to-npm-daemon.sh — undo the source-daemon swap.
#
# Removes the ExecStart override so `kittylitter.service` runs the npm-packaged
# binary again, then restarts the unit. Safe to run from any session (stopping
# the daemon will still kill any pi session running under it — that's expected,
# the conversation resumes when the daemon comes back).
#
# Usage: scripts/revert-to-npm-daemon.sh
set -euo pipefail

UNIT=kittylitter.service
OVERRIDE_DIR="$HOME/.config/systemd/user/$UNIT.d"

echo "reverting $UNIT to npm binary at $(date -Is)"

if [ -d "$OVERRIDE_DIR" ]; then
  rm -rf "$OVERRIDE_DIR"
  echo "removed override dir $OVERRIDE_DIR"
else
  echo "no override dir present — nothing to revert"
fi

systemctl --user daemon-reload
systemctl --user restart "$UNIT"
sleep 3
if systemctl --user is-active --quiet "$UNIT"; then
  echo "RESULT: $UNIT active with npm (original) binary at $(date -Is)"
  echo "running pid: $(systemctl --user show -p MainPID --value "$UNIT")"
else
  echo "RESULT: $UNIT FAILED to start"
  systemctl --user status "$UNIT" --no-pager | tail -30
  exit 1
fi
