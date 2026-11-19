"""Nodo del Cluster Sync: Raymond (exclusión mutua) + réplica local del dato R.

- Cada nodo tiene su PROPIA base de datos SQLite (DATA_DIR/node<id>.db).
- El token lleva `seq`, el último número de secuencia de escritura: da un orden total.
- El escritor propaga su escritura (UPDATE) por los enlaces del árbol ANTES de soltar
  el token; cada nodo reenvía a sus vecinos. Ningún nodo habla con quien no es vecino.
"""
import argparse
import asyncio
import json
import os
import random
import sqlite3
import time

from common import (LOCAL, TOPOLOGY, Log, build_tree, load_topology, node_addr,
                    observer_addr)
from core import RaymondCore

CS_MIN = float(os.environ.get("CS_MIN", 0.2))   # duración de la sección crítica
CS_MAX = float(os.environ.get("CS_MAX", 1.0))
DATA_DIR = os.environ.get("DATA_DIR", "data")


def enc(obj):
    return (json.dumps(obj) + "\n").encode()


class Store:
    """Base de datos local del nodo: estado actual + registro de escrituras aplicadas."""

    def __init__(self, nid):
        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, f"node{nid}.db")
        for suf in ("", "-journal", "-wal", "-shm"):
            if os.path.exists(path + suf):
                os.remove(path + suf)
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE state (k TEXT PRIMARY KEY, v INTEGER)")
        self.db.execute("INSERT INTO state VALUES ('value', 0)")
        self.db.execute("CREATE TABLE oplog (n INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "seq INTEGER, rid TEXT, node INTEGER, v INTEGER)")
        self.db.commit()

    def apply(self, op):
        self.db.execute("INSERT INTO oplog(seq, rid, node, v) VALUES (?,?,?,?)",
                        (op["seq"], op["rid"], op["node"], op["v"]))
        self.db.execute("UPDATE state SET v=? WHERE k='value'", (op["v"],))
        self.db.commit()


class Node:
    def __init__(self, nid, nosync):
        self.id = nid
        self.nosync = nosync
        neighbors, holders = build_tree(load_topology())
        self.nbrs = neighbors[nid]
        self.core = RaymondCore(nid, holders[nid])
        self.outq = {n: asyncio.Queue() for n in self.nbrs}  # cola FIFO por vecino
        self.clients = {}       # request_id -> writer del cliente
        self.t0 = {}            # request_id -> instante (monotónico) del pedido
        self.tasks = set()
        self.rng = random.Random(int(os.environ.get("SEED", 1)) * 1000 + nid)
        self.log = Log(f"node{nid}")
        self.obs_lock = asyncio.Lock()
        self.orr = self.ow = None
        # réplica local
        self.store = Store(nid)
        self.value = 0
        self.applied_seq = 0    # última escritura aplicada en orden
        self.token_seq = 0      # seq que viaja en el token (válido si tengo el token)
        self.pending = {}       # escrituras recibidas adelantadas

    def st(self):
        q = [x if isinstance(x, int) else f"self:{x[1]}" for x in self.core.queue]
        return {"holder": self.core.holder, "queue": q}

    def spawn(self, coro):
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    # ---- envío a vecinos: cola FIFO + un emisor por vecino (canal ordenado) ----
    def send(self, to, msg):
        self.outq[to].put_nowait(msg)

    async def sender(self, peer):
        w = None
        while True:
            msg = await self.outq[peer].get()
            while w is None:
                try:
                    _, w = await asyncio.open_connection(*node_addr(peer))
                except OSError:
                    await asyncio.sleep(0.3)
            w.write(enc(msg))
            await w.drain()

    # ---- ejecutar acciones del núcleo ----
    def run(self, acts):
        for kind, arg in acts:
            if kind == "ENTER":
                self.spawn(self.critical(arg))
            else:
                msg = {"t": kind, "from": self.id}
                if kind == "TOKEN":
                    msg["seq"] = self.token_seq     # el token lleva el orden de escritura
                self.send(arg, msg)
                self.log("SEND_" + kind, to=arg, **self.st())

    # ---- réplica local: propagación y aplicación ordenada ----
    def broadcast(self, op, exclude=None):
        for n in self.nbrs:
            if n != exclude:
                self.send(n, {"t": "UPDATE", "from": self.id, **op})
                self.log("SEND_UPDATE", to=n, seq=op["seq"], rid=op["rid"])

    def receive_op(self, op):
        if self.nosync:                      # contraste: se aplica en orden de llegada
            self.apply(op)
        else:                                # con protocolo: estrictamente por seq
            self.pending[op["seq"]] = op
            while self.applied_seq + 1 in self.pending:
                self.apply(self.pending.pop(self.applied_seq + 1))

    def apply(self, op):
        conflict = op["seq"] != self.applied_seq + 1
        self.value = op["v"]
        self.applied_seq = max(self.applied_seq, op["seq"])
        self.store.apply(op)
        self.log("APPLY", seq=op["seq"], rid=op["rid"], v=op["v"], conflict=conflict)

    # ---- recepción ----
    async def handle(self, reader, writer):
        while True:
            line = await reader.readline()
            if not line:
                break
            m = json.loads(line)
            t = m["t"]
            if t == "REQUEST":
                acts = self.core.on_request(m["from"])
                self.log("RECV_REQUEST", frm=m["from"], **self.st())
                self.run(acts)
            elif t == "TOKEN":
                self.token_seq = m["seq"]
                acts = self.core.on_token()
                self.log("RECV_TOKEN", frm=m["from"], seq=m["seq"], **self.st())
                self.run(acts)
            elif t == "UPDATE":
                op = {k: m[k] for k in ("seq", "rid", "node", "v")}
                self.broadcast(op, exclude=m["from"])   # reenviar a los demás vecinos
                self.receive_op(op)
            elif t == "ACQUIRE":              # pedido de un cliente local
                rid = m["rid"]
                self.clients[rid] = writer
                self.t0[rid] = time.monotonic()
                self.log("REQUEST", rid=rid, **self.st())
                if self.nosync:               # corrida de contraste: sin protocolo
                    self.spawn(self.critical(rid))
                else:
                    self.run(self.core.local_request(rid))

    # ---- observador ----
    async def obs(self, msg):
        async with self.obs_lock:
            while self.ow is None:
                try:
                    self.orr, self.ow = await asyncio.open_connection(*observer_addr())
                except OSError:
                    await asyncio.sleep(0.3)
            self.ow.write(enc(msg))
            await self.ow.drain()
            return json.loads(await self.orr.readline())

    # ---- sección crítica: lee la réplica LOCAL, escribe y propaga ----
    async def critical(self, rid):
        wait = time.monotonic() - self.t0[rid]
        self.log("ACQUIRE", rid=rid, wait=round(wait, 4), **self.st())
        await self.obs({"t": "BEGIN", "node": self.id, "rid": rid})  # antes del sleep
        self.log("ENTER", rid=rid)
        if not self.nosync:                  # mi réplica debe tener todo lo anterior
            while self.applied_seq < self.token_seq:
                await asyncio.sleep(0.005)
        v = self.value                       # lectura de la copia local
        await asyncio.sleep(self.rng.uniform(CS_MIN, CS_MAX))
        if self.nosync:
            seq = self.applied_seq + 1
        else:
            seq = self.token_seq + 1
            self.token_seq = seq
        op = {"seq": seq, "rid": rid, "node": self.id, "v": v + 1}
        self.broadcast(op)                   # antes de soltar el token (canales FIFO)
        self.receive_op(op)
        await self.obs({"t": "END", "node": self.id, "rid": rid})
        self.log("EXIT", rid=rid)
        self.log("RELEASE", rid=rid)
        if not self.nosync:
            self.run(self.core.on_exit())    # el TOKEN sale detrás del UPDATE
        w = self.clients.pop(rid)
        w.write(enc({"t": "COMMITTED", "rid": rid}))
        self.log("COMMITTED", rid=rid)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--no-sync", action="store_true",
                    default=os.environ.get("NOSYNC") == "1")
    a = ap.parse_args()
    node = Node(a.id, a.no_sync)
    for n in node.nbrs:
        node.spawn(node.sender(n))
    host = "127.0.0.1" if LOCAL else "0.0.0.0"
    server = await asyncio.start_server(node.handle, host, node_addr(a.id)[1])
    node.log("START", neighbors=node.nbrs, topology=TOPOLOGY, nosync=a.no_sync, **node.st())
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
