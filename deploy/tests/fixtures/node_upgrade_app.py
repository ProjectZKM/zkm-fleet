
import base64, hashlib, json, struct, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
jobs = []
class App(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def reply(self, data):
        data=json.dumps(data).encode(); self.send_response(200)
        self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length',0)))
        if self.headers.get('Authorization') != 'Bearer business-key': self.send_error(403);return
        jobs.append(len(jobs)+1);self.reply(jobs)
    def do_GET(self):
        if self.path == '/health': self.reply('ok');return
        if self.headers.get('Authorization') != 'Bearer business-key': self.send_error(403);return
        if self.path == '/events':
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
            try:
                i=0
                while True:
                    self.wfile.write(('data: %d\n\n'%i).encode());self.wfile.flush();i+=1;time.sleep(.05)
            except (BrokenPipeError,ConnectionResetError): pass
        elif self.path == '/ws':
            key=self.headers['Sec-WebSocket-Key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
            self.send_response(101);self.send_header('Upgrade','websocket');self.send_header('Connection','Upgrade')
            self.send_header('Sec-WebSocket-Accept',base64.b64encode(hashlib.sha1(key.encode()).digest()).decode());self.end_headers()
            while True:
                header=self.rfile.read(2)
                if len(header)!=2:break
                size=header[1]&127
                if header[0]&15==8:break
                if size>=126:break
                mask=self.rfile.read(4);data=self.rfile.read(size)
                data=bytes(c^mask[i%4] for i,c in enumerate(data))
                self.wfile.write(bytes([129,len(data)])+data);self.wfile.flush()
        else:self.reply(jobs)
ThreadingHTTPServer(('0.0.0.0',8080),App).serve_forever()
