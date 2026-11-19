"""Utilidades compartidas: direcciones, topología y logs JSON-lines."""
import json
import os
import time

LOCAL = os.environ.get("LOCAL") == "1"          # 1 = procesos locales (sin Docker)
LOG_DIR = os.environ.get("LOG_DIR", "logs")
TOPOLOGY = os.environ.get("TOPOLOGY", "balanced")
OBSERVER_PORT = 5100


def node_addr(i):
    return ("127.0.0.1", 5000 + i) if LOCAL else (f"node{i}", 5000)


def observer_addr():
    return ("127.0.0.1", OBSERVER_PORT) if LOCAL else ("observer", OBSERVER_PORT)


def load_topology(name=None):
    name = name or TOPOLOGY
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "config", f"topology_{name}.json")
    with open(path) as f:
        return json.load(f)


def build_tree(topo):
    """Devuelve (vecinos, holder inicial). El token empieza en la raíz."""
    neighbors = {}
    for a, b in topo["edges"]:
        neighbors.setdefault(a, []).append(b)
        neighbors.setdefault(b, []).append(a)
    root = topo["root"]
    holder = {root: root}
    stack = [root]
    while stack:
        u = stack.pop()
        for v in neighbors[u]:
            if v not in holder:
                holder[v] = u
                stack.append(v)
    return neighbors, holder


class Log:
    """Una línea JSON por evento, en LOG_DIR/<name>.jsonl y en stdout."""

    def __init__(self, name):
        os.makedirs(LOG_DIR, exist_ok=True)
        self.name = name
        self.f = open(os.path.join(LOG_DIR, f"{name}.jsonl"), "w", buffering=1)

    def __call__(self, ev, **kw):
        line = json.dumps({"ts": round(time.time(), 4), "who": self.name,
                           "ev": ev, **kw})
        self.f.write(line + "\n")
        print(line, flush=True)
