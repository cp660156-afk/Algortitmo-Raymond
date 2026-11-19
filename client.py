"""Cliente: conoce un solo nodo, pide R ACCESSES veces con request_id = c<id>-<n>."""
import argparse
import asyncio
import json
import os
import random
import time

from common import Log, node_addr

ACCESSES = int(os.environ.get("ACCESSES", 10))          # el TP pide entre 10 y 50
THINK_MIN = float(os.environ.get("THINK_MIN", 1.0))     # 0 y 0 = alta contención
THINK_MAX = float(os.environ.get("THINK_MAX", 5.0))
START_DELAY = float(os.environ.get("START_DELAY", 3.0))


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--node", type=int, help="nodo al que se conecta (por defecto, el mismo id)")
    a = ap.parse_args()
    cid = a.id
    rng = random.Random(int(os.environ.get("SEED", 1)) * 100 + cid)
    log = Log(f"client{cid}")
    await asyncio.sleep(START_DELAY)       # que todos los procesos estén listos
    while True:
        try:
            reader, writer = await asyncio.open_connection(*node_addr(a.node or cid))
            break
        except OSError:
            await asyncio.sleep(0.5)
    for n in range(1, ACCESSES + 1):
        rid = f"c{cid}-{n}"
        log("REQUEST", rid=rid)
        t0 = time.monotonic()
        writer.write((json.dumps({"t": "ACQUIRE", "rid": rid}) + "\n").encode())
        await writer.drain()
        m = json.loads(await reader.readline())
        assert m["t"] == "COMMITTED" and m["rid"] == rid
        log("COMMITTED", rid=rid, latency=round(time.monotonic() - t0, 4))
        if n < ACCESSES:
            await asyncio.sleep(rng.uniform(THINK_MIN, THINK_MAX))
    log("DONE", total=ACCESSES)


if __name__ == "__main__":
    asyncio.run(main())
