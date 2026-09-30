"""Run with python3 -m unittest discover -s scripts -p 'test_*.py'."""

import argparse
import contextlib
import importlib.util
import io
import json
import socket
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "bridge_call", Path(__file__).with_name("bridge-call.py")
)
bridge_call = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge_call)


@unittest.skipUnless(hasattr(socket, "socketpair"), "requires socketpair")
class RpcExitStatus(unittest.TestCase):
    def invoke(self, *, error=False, init_error=False, watch=False):
        client, server = socket.socketpair()
        requests = []

        def respond():
            with server, server.makefile("rwb", buffering=0) as stream:
                while line := stream.readline():
                    request = json.loads(line)
                    requests.append(request)
                    failed = init_error if request["id"] == 1 else error
                    response = {"jsonrpc": "2.0", "id": request["id"]}
                    if failed:
                        response["error"] = {
                            "code": -32601,
                            "message": "Unknown method",
                        }
                    else:
                        response["result"] = {}
                    stream.write((json.dumps(response) + "\n").encode())
                    if request["id"] == 2:
                        return

        worker = threading.Thread(target=respond, daemon=True)
        worker.start()
        ns = argparse.Namespace(
            method="model/list",
            args=[],
            bridge="pi",
            release=False,
            quiet_init=True,
            timeout=1,
            watch=watch,
            watch_for=0.05,
        )
        output = io.StringIO()
        # main owns cleanup; use the real socket stream without connecting to
        # a daemon or building/launching any coding agent.
        with patch.object(bridge_call, "parse_args", return_value=ns), patch.object(
            bridge_call, "ensure_bridge_built", return_value=Path("unused")
        ), patch.object(bridge_call, "wait_for_socket"), patch.object(
            bridge_call.subprocess, "Popen"
        ), patch.object(
            bridge_call.socket, "socket"
        ) as socket_factory, contextlib.redirect_stdout(
            output
        ):
            wrapper = socket_factory.return_value
            wrapper.makefile.side_effect = client.makefile
            wrapper.gettimeout.side_effect = client.gettimeout
            wrapper.settimeout.side_effect = client.settimeout
            wrapper.close.side_effect = client.close
            code = bridge_call.main()
        worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        return code, output.getvalue(), requests

    def test_method_error_is_failure_with_and_without_watch(self):
        for watch in (False, True):
            with self.subTest(watch=watch):
                code, output, requests = self.invoke(error=True, watch=watch)
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(output)["error"]["code"], -32601)
                self.assertEqual(len(requests), 2)

    def test_success_remains_success_with_and_without_watch(self):
        for watch in (False, True):
            with self.subTest(watch=watch):
                code, output, requests = self.invoke(watch=watch)
                self.assertEqual(code, 0)
                self.assertEqual(json.loads(output)["result"], {})
                self.assertEqual(len(requests), 2)

    def test_initialization_error_stops_before_method_even_when_quiet(self):
        code, output, requests = self.invoke(init_error=True)
        self.assertEqual(code, 1)
        self.assertEqual(len(requests), 1)
        self.assertEqual(json.loads(output)["_init_response"]["error"]["code"], -32601)


if __name__ == "__main__":
    unittest.main()
