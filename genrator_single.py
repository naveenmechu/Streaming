import argparse
import asyncio
import json
import random
import string
from datetime import datetime
import socket

def random_id(n=8):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def make_event():
    return {
        "id": random_id(),
        "ts": datetime.utcnow().isoformat() + "Z",
        "value": round(random.uniform(0, 100), 3),
        "type": random.choice(["click", "view", "purchase"])
    }

class SimpleTCPServer:
    def __init__(self, host='127.0.0.1', port=9999):
        self.host = host
        self.port = port
        self.clients = set()
        self.server = None

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        self.clients.add(writer)
        try:
            while not reader.at_eof():
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            self.clients.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        return self.server

    async def broadcast(self, line: str):
        dead = []
        for w in list(self.clients):
            try:
                w.write((line + "\n").encode())
                await w.drain()
            except Exception:
                dead.append(w)
        for d in dead:
            self.clients.discard(d)

async def produce(rate, console, socket_server):
    interval = 4.0 / rate if rate > 0 else 0
    while True:
        ev = make_event()
        line = json.dumps(ev)
        if console:
            print(line, flush=True)
        if socket_server:
            await socket_server.broadcast(line)
        if interval:
            await asyncio.sleep(interval)
        else:
            await asyncio.sleep(0.001)

def parse_args():
    p = argparse.ArgumentParser(description="Simple streaming data generator")
    p.add_argument("--rate", type=float, default=2.0, help="events per second")
    p.add_argument("--no-console", dest="console", action="store_false", help="disable console output")
    p.set_defaults(console=True)
    p.add_argument("--socket", action="store_true", help="enable TCP socket server")
    p.add_argument("--host", default="127.0.0.1", help="socket host")
    p.add_argument("--port", type=int, default=9999, help="socket port")
    return p.parse_args()

async def main():
    args = parse_args()
    server = None
    if args.socket:
        server = SimpleTCPServer(args.host, args.port)
        srv = await server.start()
        print(f"TCP server listening on {args.host}:{args.port}")
    try:
        await produce(args.rate, args.console, server)
    except asyncio.CancelledError:
        pass
    finally:
        if server and server.server:
            server.server.close()
            await server.server.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopped by user")