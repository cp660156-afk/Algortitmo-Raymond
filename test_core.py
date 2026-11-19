"""Prueba del núcleo sin red: red simulada (canales FIFO, entrega en orden aleatorio
entre canales) + invariantes de Raymond. Uso: python test_core.py"""
import random
from collections import deque

from common import build_tree, load_topology
from core import RaymondCore


def simulate(topo, seed, total=60):
    rng = random.Random(seed)
    neighbors, holder = build_tree(load_topology(topo))
    nodes = {i: RaymondCore(i, holder[i]) for i in neighbors}
    chans = {(a, b): deque() for a in neighbors for b in neighbors[a]}
    in_cs, msgs, issued, served = set(), 0, 0, 0

    def apply(i, acts):
        nonlocal msgs
        for kind, arg in acts:
            if kind == "ENTER":
                assert not in_cs, "dos nodos en la sección crítica"
                in_cs.add(i)
            else:
                chans[(i, arg)].append(kind)
                msgs += 1

    while served < total:
        opts = []
        if issued < total:
            opts.append("issue")
        if in_cs:
            opts.append("exit")
        full = [c for c, q in chans.items() if q]
        if full:
            opts.append("deliver")
        act = rng.choice(opts)
        if act == "issue":
            i = rng.choice(list(nodes))
            issued += 1
            apply(i, nodes[i].local_request(f"r{issued}"))
        elif act == "exit":
            i = in_cs.pop()
            served += 1
            apply(i, nodes[i].on_exit())
        else:
            src, dst = rng.choice(full)
            kind = chans[(src, dst)].popleft()
            apply(dst, nodes[dst].on_request(src) if kind == "REQUEST"
                  else nodes[dst].on_token())
        # invariante: exactamente un token (en un nodo o en tránsito)
        tokens = sum(n.holder == n.me for n in nodes.values()) + \
            sum(m == "TOKEN" for q in chans.values() for m in q)
        assert tokens == 1, f"hay {tokens} tokens"
    assert all(not n.queue and not n.asked for n in nodes.values()), "quedaron pedidos"
    return msgs / total


if __name__ == "__main__":
    for topo in ("chain", "balanced", "star"):
        res = [simulate(topo, s) for s in range(200)]
        print(f"{topo:9s} 200 corridas OK | mensajes/entrada (promedio): {sum(res)/len(res):.2f}")
    print("Todas las invariantes se cumplieron.")
