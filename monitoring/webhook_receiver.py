#!/usr/bin/env python3
"""Récepteur webhook minimal pour conserver les notifications Alertmanager."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

HOST = "0.0.0.0"
PORT = 5001
LOG_PATH = Path(os.environ.get("ALERT_LOG", "/data/monitoring-alerts.jsonl"))


class Handler(BaseHTTPRequestHandler):
    server_version = "VelibAlertWebhook/1.0"

    def _send(self, status: int, body: str, content_type: str = "text/plain") -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send(200, "ok\n")
            return
        self._send(404, "not found\n")

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/alerts":
            self._send(404, "not found\n")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            data: Any = json.loads(raw.decode("utf-8"))
            record = {
                "received_at": datetime.now(timezone.utc).isoformat(),
                "payload": data,
            }
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with LOG_PATH.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

            alerts = data.get("alerts", []) if isinstance(data, dict) else []
            summary = [
                f"{item.get('status', '?')}:{item.get('labels', {}).get('alertname', '?')}"
                for item in alerts
                if isinstance(item, dict)
            ]
            print("ALERT_WEBHOOK " + ", ".join(summary), flush=True)
            self._send(200, "stored\n")
        except (ValueError, json.JSONDecodeError, OSError) as exc:
            self._send(400, f"invalid payload: {exc}\n")

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"WEBHOOK {self.address_string()} {fmt % args}", flush=True)


if __name__ == "__main__":
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Webhook listening on {HOST}:{PORT}; log={LOG_PATH}", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
