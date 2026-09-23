#!/usr/bin/env python3
"""Drive an alleycat bridge binary in socket mode and call one method.

Spawns one of `alleycat-pi-bridge` / `alleycat-claude-bridge` /
`alleycat-opencode-bridge` listening on a temp Unix socket, connects to
it, performs the JSON-RPC `initialize` handshake, sends one request,
prints any notifications + the response, then exits.

All three bridges support `--socket <path>`; opencode-bridge only
supports socket mode, so we use it uniformly for all three.

Examples:
  bridge-call.py thread/list
  bridge-call.py thread/read 0193f0...           # auto-shorthand: {threadId, includeTurns:true}
  bridge-call.py -b claude thread/list
  bridge-call.py -b opencode thread/read ses_2296...
  bridge-call.py turn/start 0193f0... "summarize the README"
  bridge-call.py model/list
  bridge-call.py --watch turn/start 0193f0... "what does fn foo do?"
  bridge-call.py thread/list '{"cwd":"/Users/me/dev/proj"}'  # raw JSON params

Builds the bridge binary with `cargo build` on first run if it isn't
already built. Use --release to use the release binary instead.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

BRIDGES = {
    "pi": "alleycat-pi-bridge",
    "claude": "alleycat-claude-bridge",
    "opencode": "alleycat-opencode-bridge",
}


# Shorthand: positional args -> JSON-RPC params object. Returns None to fall
# back to either an empty object or raw JSON parsing of the first arg.
def shorthand_params(method: str, args: list[str]) -> dict[str, Any] | None:
    match method:
        case "thread/read":
            if len(args) != 1:
                return None
            return {"threadId": args[0], "includeTurns": True}
        case "thread/list":
            if not args:
                return {}
            if len(args) == 1:
                return {"cwd": args[0]}
            return None
        case "thread/start":
            if not args:
                return {}
            if len(args) == 1:
                return {"cwd": args[0]}
            return None
        case "thread/resume":
            if len(args) != 1:
                return None
            return {"threadId": args[0]}
        case "thread/archive" | "thread/unarchive":
            if len(args) != 1:
                return None
            return {"threadId": args[0]}
        case "thread/turns/list":
            if len(args) != 1:
                return None
            return {"threadId": args[0]}
        case "turn/start":
            if len(args) != 2:
                return None
            return {
                "threadId": args[0],
                "input": [{"type": "text", "text": args[1]}],
            }
        case "turn/interrupt":
            if len(args) != 1:
                return None
            return {"threadId": args[0]}
        case "skills/list":
            if not args:
                return {"cwds": []}
            return {"cwds": list(args)}
        case "model/list" | "account/read" | "account/rateLimits/read":
            if not args:
                return {}
            return None
        case _:
            return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bridge-call",
        description="Invoke a JSON-RPC method on an alleycat bridge over a temp Unix socket.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-b",
        "--bridge",
        choices=BRIDGES.keys(),
        default="pi",
        help="Which bridge binary to drive (default: pi).",
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="Use target/release/<bridge> instead of target/debug.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help=(
            "After the response arrives, keep reading notifications for"
            " --watch-for seconds (useful for turn/start)."
        ),
    )
    parser.add_argument(
        "--watch-for",
        type=float,
        default=120.0,
        help="Seconds to keep reading notifications after the response (default 120).",
    )
    parser.add_argument(
        "--quiet-init",
        action="store_true",
        help="Suppress the initialize handshake and any notifications, only print the final response.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for the response to the requested method (default 30).",
    )
    parser.add_argument("method", help="JSON-RPC method, e.g. thread/read.")
    parser.add_argument(
        "args",
        nargs="*",
        help="Either positional shorthand args or one raw JSON params object.",
    )
    # `parse_intermixed_args` lets the user put `-b claude` after the method.
    return parser.parse_intermixed_args()


def build_params(method: str, args: list[str]) -> dict[str, Any]:
    if len(args) == 1 and args[0].lstrip().startswith(("{", "[")):
        return json.loads(args[0])
    shorthand = shorthand_params(method, args)
    if shorthand is not None:
        return shorthand
    if not args:
        return {}
    raise SystemExit(
        f"unrecognized shorthand for {method!r} with {len(args)} args; "
        "pass a single JSON object instead"
    )


def ensure_bridge_built(bridge: str, release: bool) -> Path:
    bin_name = BRIDGES[bridge]
    profile_dir = "release" if release else "debug"
    path = REPO_ROOT / "target" / profile_dir / bin_name
    if path.exists():
        return path
    profile_flags = ["--release"] if release else []
    print(f"bridge-call: building {bin_name}...", file=sys.stderr)
    subprocess.run(
        ["cargo", "build", "-p", bin_name, *profile_flags],
        cwd=REPO_ROOT,
        check=True,
    )
    if not path.exists():
        raise SystemExit(f"build succeeded but {path} not found")
    return path


def wait_for_socket(path: Path, deadline: float) -> None:
    while time.monotonic() < deadline:
        if path.exists():
            # Sometimes the file appears before bind() finishes; try to
            # connect with a tiny timeout to confirm it's accepting.
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe.settimeout(0.2)
            try:
                probe.connect(str(path))
                probe.close()
                return
            except OSError:
                probe.close()
        time.sleep(0.05)
    raise SystemExit(f"bridge socket {path} never appeared")


def write_frame(sock_file, frame: dict[str, Any]) -> None:
    line = json.dumps(frame, separators=(",", ":")) + "\n"
    sock_file.write(line.encode("utf-8"))
    sock_file.flush()


def read_frame_with_deadline(
    sock: socket.socket, sock_file, deadline: float
) -> dict[str, Any] | None:
    """Read one JSON-RPC frame, returning None on EOF or deadline expiry."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return None
    previous_timeout = sock.gettimeout()
    sock.settimeout(remaining)
    try:
        line = sock_file.readline()
    except TimeoutError:
        return None
    finally:
        sock.settimeout(previous_timeout)
    if not line:
        return None
    return json.loads(line)


def main() -> int:
    ns = parse_args()
    params = build_params(ns.method, ns.args)
    bin_path = ensure_bridge_built(ns.bridge, ns.release)

    socket_dir = Path(tempfile.mkdtemp(prefix="bridge-call-"))
    socket_path = socket_dir / f"{ns.bridge}-{uuid.uuid4().hex[:8]}.sock"

    env = os.environ.copy()
    env.setdefault("RUST_LOG", env.get("RUST_LOG", "warn"))

    proc = subprocess.Popen(
        [str(bin_path), "--socket", str(socket_path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=sys.stderr,
        env=env,
    )

    sock = None
    sock_file = None
    try:
        wait_for_socket(socket_path, time.monotonic() + 5.0)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(str(socket_path))
        # makefile lets us use line-oriented reads (readline) and a single
        # blocking write/flush API; underlying socket stays the same.
        sock_file = sock.makefile("rwb", buffering=0)

        # 1. initialize handshake.
        init_id = 1
        write_frame(
            sock_file,
            {
                "jsonrpc": "2.0",
                "id": init_id,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "bridge-call",
                        "title": "bridge-call CLI",
                        "version": "0.1.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            },
        )
        deadline = time.monotonic() + ns.timeout
        while True:
            frame = read_frame_with_deadline(sock, sock_file, deadline)
            if frame is None:
                raise SystemExit("bridge closed before responding to initialize")
            if frame.get("id") == init_id:
                if not ns.quiet_init:
                    print(json.dumps({"_init_response": frame}, indent=2))
                break
            if not ns.quiet_init:
                print(json.dumps(frame, indent=2))

        # 2. The actual method.
        request_id = 2
        write_frame(
            sock_file,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": ns.method,
                "params": params,
            },
        )
        deadline = time.monotonic() + ns.timeout
        response: dict[str, Any] | None = None
        while True:
            frame = read_frame_with_deadline(sock, sock_file, deadline)
            if frame is None:
                if response is None:
                    print(
                        f"bridge-call: timed out waiting {ns.timeout}s for {ns.method} response",
                        file=sys.stderr,
                    )
                    return 1
                break
            if frame.get("id") == request_id:
                response = frame
                if ns.watch:
                    print(json.dumps(response, indent=2))
                    watch_deadline = time.monotonic() + ns.watch_for
                    while True:
                        more = read_frame_with_deadline(sock, sock_file, watch_deadline)
                        if more is None:
                            break
                        print(json.dumps(more, indent=2))
                    return 0
                print(json.dumps(response, indent=2))
                return 0
            print(json.dumps(frame, indent=2))
    finally:
        try:
            if sock_file is not None:
                sock_file.close()
        except Exception:
            pass
        try:
            if sock is not None:
                sock.close()
        except Exception:
            pass
        # Bridge listens forever on the socket — we have to kill it.
        try:
            proc.terminate()
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        try:
            socket_path.unlink(missing_ok=True)
            socket_dir.rmdir()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
