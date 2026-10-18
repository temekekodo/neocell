"""Local test UI: a developer dashboard for watching the PPE work.

This is a debugging and teaching tool, not part of the compute
architecture. The PPE/SPE boundary remains process pipes; nothing in the
job path knows this server exists.

It binds to 127.0.0.1 only. The listen socket exists so a browser can
render a page, which is a different concern from the architecture having a
network transport, and it is deliberately unreachable from another
machine.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from neocell.ui.runner import RunManager
from neocell.ui.scenarios import as_dicts, describe_limits
from neocell.ui.scenarios import RunRequest
from neocell.version import VERSION

STATIC = Path(__file__).with_name("static")


class UiHandler(BaseHTTPRequestHandler):
    """Tiny JSON API plus server-sent events. No framework needed."""

    manager: RunManager

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        return  # keep the console clean; the UI is the interface

    # -- helpers -------------------------------------------------------
    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, name: str, content_type: str) -> None:
        path = STATIC / name
        body = path.read_bytes() if path.exists() else b"not found"
        self.send_response(200 if path.exists() else 404)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- routes --------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        route = self.path.split("?", 1)[0]
        if route == "/":
            self._send_file("index.html", "text/html; charset=utf-8")
        elif route == "/app.js":
            self._send_file("app.js", "text/javascript; charset=utf-8")
        elif route == "/style.css":
            self._send_file("style.css", "text/css; charset=utf-8")
        elif route == "/api/config":
            self._send_json(
                {
                    "version": VERSION,
                    "scenarios": as_dicts(),
                    "limits": describe_limits(),
                }
            )
        elif route == "/api/state":
            self._send_json(self.manager.snapshot())
        elif route == "/api/stream":
            self._stream()
        else:
            self._send_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        route = self.path.split("?", 1)[0]
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw or b"{}")
        except ValueError:
            self._send_json({"error": "invalid JSON"}, status=400)
            return

        if route == "/api/run":
            try:
                request = RunRequest.from_dict(payload)
                run_id = self.manager.start(request)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            except RuntimeError as exc:
                self._send_json({"error": str(exc)}, status=409)
                return
            self._send_json({"run_id": run_id})
        elif route == "/api/stop":
            self.manager.stop()
            self._send_json({"status": "stopping"})
        else:
            self._send_json({"error": "not found"}, status=404)

    def _stream(self) -> None:
        """Server-sent events; the browser reconnects on its own if dropped."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            self._write_event({"type": "state", "state": self.manager.snapshot()})
            idle = 0
            while True:
                updates = self.manager.drain(timeout=1.0)
                for update in updates:
                    self._write_event(update)
                if not updates:
                    idle += 1
                    if idle % 15 == 0:
                        self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def _write_event(self, payload: Any) -> None:
        body = json.dumps(payload)
        self.wfile.write(f"data: {body}\n\n".encode("utf-8"))
        self.wfile.flush()


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    """Run the UI until interrupted."""
    manager = RunManager()
    handler = type("BoundUiHandler", (UiHandler,), {"manager": manager})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    url = f"http://{host}:{port}/"
    print(f"neocell test UI on {url}")
    print("  local only; the PPE/SPE path uses process pipes, not HTTP")
    print("  Ctrl-C to stop")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        httpd.shutdown()
        httpd.server_close()


def start_background(host: str = "127.0.0.1", port: int = 0):
    """Start the server on a thread and return ``(httpd, thread, manager)``.

    Used by tests, which need a live server without blocking.
    """
    manager = RunManager()
    handler = type("BoundUiHandler", (UiHandler,), {"manager": manager})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, manager