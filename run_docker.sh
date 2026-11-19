#!/usr/bin/env bash
# Uso: ./run_docker.sh            (normal)
#      NOSYNC=1 ./run_docker.sh   (contraste: debe detectar solapamientos y divergencia)
#      TOPOLOGY=chain THINK_MIN=0 THINK_MAX=0 ./run_docker.sh
set -e
docker compose down -v 2>/dev/null || true
mkdir -p logs data/node1 data/node2 data/node3 data/node4 data/node5
rm -f logs/*.jsonl 2>/dev/null || true
docker compose up -d --build
docker wait $(docker compose ps -aq client1 client2 client3 client4 client5) >/dev/null
sleep 2
docker compose down
if [ "$NOSYNC" = "1" ]; then python3 verify.py logs --data data --expect-violations
else python3 verify.py logs --data data; fi
