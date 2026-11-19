"""Ejecuta todo en esta máquina SIN Docker (procesos independientes por TCP local).
Ejemplos:
  python run_local.py --fast
  python run_local.py --topology chain --accesses 10
  python run_local.py --nosync --fast          # corrida de contraste
  python run_local.py --contention --fast      # alta contención"""
import argparse
import os
import shutil
import subprocess
import sys
import time

from common import build_tree, load_topology

ap = argparse.ArgumentParser()
ap.add_argument("--topology", default="balanced", choices=["balanced", "chain", "star"])
ap.add_argument("--accesses", type=int, default=10)
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--nosync", action="store_true")
ap.add_argument("--contention", action="store_true", help="sin espera entre pedidos")
ap.add_argument("--fast", action="store_true", help="tiempos cortos solo para probar")
ap.add_argument("--logdir", default="logs_local")
a = ap.parse_args()

env = {**os.environ, "LOCAL": "1", "LOG_DIR": a.logdir, "TOPOLOGY": a.topology,
       "ACCESSES": str(a.accesses), "SEED": str(a.seed), "START_DELAY": "1", "DATA_DIR": os.path.join(a.logdir, "data"),
       "NOSYNC": "1" if a.nosync else "0"}
if a.fast:
    env.update(CS_MIN="0.05", CS_MAX="0.15", THINK_MIN="0.05", THINK_MAX="0.3")
if a.contention:
    env.update(THINK_MIN="0", THINK_MAX="0")

shutil.rmtree(a.logdir, ignore_errors=True)
os.makedirs(a.logdir)
ids = sorted(build_tree(load_topology(a.topology))[0])
here = os.path.dirname(os.path.abspath(__file__))


def start(*cmd):
    return subprocess.Popen([sys.executable, *cmd], cwd=here, env=env,
                            stdout=subprocess.DEVNULL)


obs = start("observer.py")
nodes = [start("node.py", "--id", str(i)) for i in ids]
clients = [start("client.py", "--id", str(i)) for i in ids]
t0 = time.monotonic()
for c in clients:
    c.wait()
print(f"Clientes terminados en {time.monotonic() - t0:.1f} s")
time.sleep(0.5)
for p in nodes + [obs]:
    p.terminate()
cmd = [sys.executable, "verify.py", a.logdir, "--data", os.path.join(a.logdir, "data")] + (["--expect-violations"] if a.nosync else [])
sys.exit(subprocess.call(cmd, cwd=here))
