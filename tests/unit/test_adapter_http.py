from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import TCPServer
from typing import Tuple

import pandas as pd

from skewsentry.adapters.http_adapter import HTTPAdapter


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        rows = json.loads(body.decode("utf-8"))
        # Echo back with feature 'z' = a + b
        out = [{"id": r["id"], "z": r["a"] + r["b"]} for r in rows]
        resp = json.dumps(out).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)

    def log_message(self, *args, **kwargs):  # silence
        return


def _run_server(server: HTTPServer):
    server.serve_forever()


def _start_server() -> Tuple[HTTPServer, str]:
    TCPServer.allow_reuse_address = True
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=_run_server, args=(server,), daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def test_http_adapter_round_trip():
    server, url = _start_server()
    try:
        adapter = HTTPAdapter(url=url, batch_size=2, timeout=2.0, retries=1)
        df = pd.DataFrame({"id": [1, 2, 3], "a": [10, 20, 30], "b": [1, 2, 3]})
        out = adapter.get_features(df)
        assert list(out.columns) == ["id", "z"]
        assert out["z"].tolist() == [11, 22, 33]
    finally:
        server.shutdown()

