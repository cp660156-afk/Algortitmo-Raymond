"""Observador: solo registra BEGIN/END y acusa solapamientos.
No concede permisos, no bloquea a nadie y NO guarda ningún dato de R."""
import asyncio
import json

from common import Log, OBSERVER_PORT, LOCAL

log = Log("observer")
state = {"active": 0}


async def handle(reader, writer):
    while True:
        line = await reader.readline()
        if not line:
            break
        m = json.loads(line)
        if m["t"] == "BEGIN":
            state["active"] += 1
            log("BEGIN", node=m["node"], rid=m["rid"], active=state["active"])
            if state["active"] > 1:
                log("VIOLATION", node=m["node"], rid=m["rid"], active=state["active"])
        elif m["t"] == "END":
            state["active"] -= 1
            log("END", node=m["node"], rid=m["rid"], active=state["active"])
        writer.write(b'{"ok": 1}\n')
        await writer.drain()


async def main():
    host = "127.0.0.1" if LOCAL else "0.0.0.0"
    server = await asyncio.start_server(handle, host, OBSERVER_PORT)
    log("START")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
