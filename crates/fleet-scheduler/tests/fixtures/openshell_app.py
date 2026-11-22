"""Stdlib-only HTTP/SSE workload for the isolated native Docker acceptance."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import base64
import hashlib
import time


class Application(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    healthy = True

    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.path.startswith("/health"):
            if self.path == "/health/unready":
                Application.healthy = False
            elif self.path == "/health/ready":
                Application.healthy = True
            self.send_response(200 if Application.healthy else 503)
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self.path == "/ws":
            # Minimal test peer: only short, final, masked client frames. This
            # exercises real WS upgrades/binary bytes, not a production server.
            key = self.headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", base64.b64encode(hashlib.sha1(key.encode()).digest()).decode())
            self.end_headers()
            while True:
                header = self.rfile.read(2)
                if len(header) != 2:
                    return
                opcode, size = header
                if opcode & 15 == 8:
                    return
                assert opcode in (0x81, 0x82) and size & 128 and size & 127 < 126
                mask = self.rfile.read(4)
                payload = self.rfile.read(size & 127)
                body = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
                self.wfile.write(bytes((opcode, len(body))) + body)
                self.wfile.flush()
        elif self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            try:
                while True:
                    self.wfile.write(b"data: event\n\n")
                    self.wfile.flush()
                    time.sleep(0.05)
            except (BrokenPipeError, ConnectionResetError):
                pass
        else:
            assert "X-Trusted-Access" not in self.headers
            body = self.headers.get("Authorization", "missing").encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


ThreadingHTTPServer(("127.0.0.1", 8090), Application).serve_forever()
