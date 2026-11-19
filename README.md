# TP2 - Raymond (token + árbol lógico) con réplica local por nodo

Requisitos: Python 3.10+ (solo biblioteca estándar, SQLite incluido). Docker es opcional.

## Qué hace
- **Exclusión mutua** con el algoritmo de Raymond (`core.py`, `node.py`).
- **Dato local por nodo**: cada nodo tiene su propia base SQLite (`DATA_DIR/node<id>.db`).
  El observador NO guarda ningún dato de R.
- **Orden total**: el token lleva `seq` (último número de escritura). El escritor propaga
  su escritura por los enlaces del árbol antes de soltar el token; cada nodo la reenvía
  solo a sus vecinos y la aplica por orden de `seq`.

## Archivos
- `core.py` lógica pura de Raymond | `node.py` nodo (red + Raymond + réplica local)
- `client.py` cliente (un solo nodo, request_id = c<id>-<n>)
- `observer.py` solo registra BEGIN/END y acusa solapamientos
- `verify.py` verificador automático (logs + bases locales)
- `test_core.py` prueba del núcleo sin red | `config/topology_*.json` balanced, chain, star

## Ejecutar
1. Núcleo sin red:           `python test_core.py`
2. Sin Docker (rápido):      `python run_local.py --fast`
3. Contraste sin protocolo:  `python run_local.py --fast --nosync`
4. Cadena + alta contención: `python run_local.py --fast --contention --topology chain`
5. Docker (5 nodos):         `./run_docker.sh`
   - contraste: `NOSYNC=1 ./run_docker.sh`
   - `TOPOLOGY=chain THINK_MIN=0 THINK_MAX=0 ./run_docker.sh`, `ACCESSES=30 ./run_docker.sh`

Sin `--fast` se usan los tiempos del enunciado (sección crítica 0,2-1 s; espera 1-5 s).

## Qué verifica `verify.py`
1. Todos los pedidos terminan en COMMITTED.
2. Cada nodo tiene su propia BD (archivos distintos).
3. Todo mensaje (REQUEST, TOKEN, UPDATE) viajó entre vecinos del árbol.
4. Exclusión mutua según el observador.
5. Convergencia: hashes idénticos en todas las réplicas y valor = nº de accesos.
6. Mismo orden: todos aplicaron la secuencia 1..N sin conflictos.
En `--nosync` se espera lo contrario: solapamientos detectados y réplicas divergentes.

## Alcance
Operación normal sin fallos (como pide el TP2). La replicación es un extra: no mide
tolerancia a fallos ni reemplaza al Cluster Store del TP3.
