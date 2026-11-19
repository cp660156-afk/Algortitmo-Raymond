"""Verificador automático: lee logs y bases locales y comprueba las propiedades.
Uso: python verify.py [logdir] [--data DIR] [--expect-violations]"""
import argparse
import glob
import hashlib
import json
import os
import sqlite3
import statistics
import sys

from common import load_topology


def load(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def read_db(path):
    db = sqlite3.connect(path)
    value = db.execute("SELECT v FROM state WHERE k='value'").fetchone()[0]
    rows = db.execute("SELECT seq, rid, v FROM oplog ORDER BY n").fetchall()
    db.close()
    digest = hashlib.sha256(json.dumps([value, rows]).encode()).hexdigest()[:12]
    return {"value": value, "last_seq": max([r[0] for r in rows], default=0),
            "writes": len(rows), "hash": digest}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("logdir", nargs="?", default="logs")
    ap.add_argument("--data", default="data")
    ap.add_argument("--expect-violations", action="store_true")
    a = ap.parse_args()
    d = a.logdir

    # --- observador ---
    active = max_active = viol = 0
    for e in load(os.path.join(d, "observer.jsonl")):
        if e["ev"] == "BEGIN":
            active += 1
            max_active = max(max_active, active)
        elif e["ev"] == "END":
            active -= 1
        elif e["ev"] == "VIOLATION":
            viol += 1

    # --- clientes ---
    requested, committed, per_client = set(), set(), {}
    for p in sorted(glob.glob(os.path.join(d, "client*.jsonl"))):
        for e in load(p):
            if e["ev"] == "REQUEST":
                requested.add(e["rid"])
            elif e["ev"] == "COMMITTED":
                committed.add(e["rid"])
                per_client[e["who"]] = per_client.get(e["who"], 0) + 1

    # --- nodos ---
    waits, entries = [], 0
    msgs = {"REQUEST": 0, "TOKEN": 0, "UPDATE": 0}
    applies, conflicts, bad_links, topo_name = {}, 0, [], None
    sends = []
    for p in sorted(glob.glob(os.path.join(d, "node*.jsonl"))):
        for e in load(p):
            nid = int(e["who"][4:])
            if e["ev"] == "START":
                topo_name = e["topology"]
            elif e["ev"] == "ACQUIRE":
                waits.append(e["wait"])
            elif e["ev"] == "ENTER":
                entries += 1
            elif e["ev"].startswith("SEND_"):
                msgs[e["ev"][5:]] += 1
                sends.append((nid, e["to"], e["ev"][5:]))
            elif e["ev"] == "APPLY":
                applies.setdefault(nid, []).append((e["seq"], e["rid"], e["v"]))
                conflicts += bool(e["conflict"])
    edges = set()
    if topo_name:
        for x, y in load_topology(topo_name)["edges"]:
            edges |= {(x, y), (y, x)}
    bad_links = [s for s in sends if (s[0], s[1]) not in edges]

    # --- bases de datos locales ---
    dbs = {}
    for p in sorted(glob.glob(os.path.join(a.data, "**", "node*.db"), recursive=True)):
        dbs[int(os.path.basename(p)[4:-3])] = (p, read_db(p))

    n_commit = len(committed)
    print("=== Resultados ===")
    print(f"Pedidos: {len(requested)} | COMMITTED: {n_commit} | topología: {topo_name}")
    print(f"Accesos por cliente: {dict(sorted(per_client.items()))}")
    print(f"Máx. accesos simultáneos a R (observador): {max_active} | violaciones: {viol}")
    if waits:
        print(f"Espera mediana: {statistics.median(waits):.3f} s | máx: {max(waits):.3f} s")
    if entries and not a.expect_violations:
        proto = msgs["REQUEST"] + msgs["TOKEN"]
        print(f"Mensajes del protocolo: {msgs['REQUEST']} REQUEST + {msgs['TOKEN']} TOKEN "
              f"-> {proto / entries:.2f} por entrada")
    if entries:
        print(f"Mensajes de datos (UPDATE): {msgs['UPDATE']} -> {msgs['UPDATE'] / entries:.2f} por entrada")
    print("Réplicas locales:")
    for nid, (p, r) in sorted(dbs.items()):
        print(f"  nodo {nid}: valor={r['value']} escrituras={r['writes']} "
              f"última seq={r['last_seq']} hash={r['hash']}  ({p})")

    hashes = {r["hash"] for _, r in dbs.values()}
    converged = len(dbs) > 0 and len(hashes) == 1 and \
        all(r["value"] == n_commit for _, r in dbs.values())
    seqs = [[s for s, _, _ in v] for v in applies.values()]
    same_order = len(applies) > 0 and all(v == list(applies.values())[0] for v in applies.values()) \
        and all(s == list(range(1, n_commit + 1)) for s in seqs)
    diverged = (not converged) or conflicts > 0 or not same_order

    checks = []
    checks.append(("todos los pedidos terminaron en COMMITTED",
                   requested == committed and n_commit > 0))
    checks.append((f"cada nodo tiene su propia BD local ({len(dbs)} archivos distintos)",
                   len(dbs) >= 2 and len({p for p, _ in dbs.values()}) == len(dbs)))
    checks.append(("toda comunicación del protocolo y de datos fue entre vecinos del árbol"
                   + (f" (violan: {bad_links[:3]})" if bad_links else ""), not bad_links))
    if a.expect_violations:
        checks.append(("el observador DETECTA solapamientos (corrida sin protocolo)", viol > 0))
        checks.append((f"las réplicas DIVERGEN o se pierden escrituras (conflictos de seq: {conflicts})",
                       diverged))
    else:
        checks.append(("exclusión mutua: nunca más de 1 acceso simultáneo",
                       max_active <= 1 and viol == 0))
        checks.append(("convergencia: todas las réplicas idénticas y valor = nº de accesos",
                       converged))
        checks.append(("mismo orden: todos aplicaron la misma secuencia 1..N sin conflictos",
                       same_order and conflicts == 0))
    print()
    ok = True
    for name, res in checks:
        print(("[OK]   " if res else "[FALLO] ") + name)
        ok &= res
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
