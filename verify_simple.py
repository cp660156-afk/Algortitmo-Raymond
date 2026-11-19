"""Validador simple para logs de la versión básica (sin BDs ni eventos START/topología).

Uso: python verify_simple.py [logdir]
"""
import glob
import json
import os
import statistics
import sys


def load(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else "logs"

    # --- observador ---
    active = max_active = viol = 0
    final_value = 0
    for e in load(os.path.join(d, "observer.jsonl")):
        if e["ev"] == "BEGIN":
            active += 1
            max_active = max(max_active, active)
        elif e["ev"] == "END":
            active -= 1
        elif e["ev"] == "VIOLATION":
            viol += 1
        elif e["ev"] == "WRITE":
            final_value = e["v"]

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
    for p in sorted(glob.glob(os.path.join(d, "node*.jsonl"))):
        for e in load(p):
            if e["ev"] == "ACQUIRE":
                waits.append(e["wait"])
            elif e["ev"] == "ENTER":
                entries += 1

    print("=== Resultados ===")
    print(f"Pedidos: {len(requested)} | COMMITTED: {len(committed)}")
    print(f"Accesos por cliente: {dict(sorted(per_client.items()))}")
    print(f"Máx. accesos simultáneos a R (observador): {max_active} | violaciones: {viol}")
    print(f"Valor final del contador R: {final_value} (esperado {len(committed)})")
    if waits:
        print(f"Espera mediana: {statistics.median(waits):.3f} s | máx: {max(waits):.3f} s")
    if entries:
        print(f"Entradas a sección crítica: {entries}")

    checks = [
        ("exclusión mutua: nunca más de 1 acceso simultáneo",
         max_active <= 1 and viol == 0),
        ("el contador de R coincide con los accesos (sin pérdidas)",
         final_value == len(committed)),
        ("todos los pedidos terminaron en COMMITTED",
         requested == committed and len(requested) > 0),
    ]
    print()
    ok = True
    for name, res in checks:
        print(("[OK]   " if res else "[FALLO] ") + name)
        ok &= res
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()