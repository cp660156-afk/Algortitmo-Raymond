"""Núcleo del algoritmo de Raymond. Sin red: recibe eventos y devuelve acciones.

Acciones devueltas (en orden):
  ("ENTER", request_id)  -> este nodo puede entrar a la sección crítica
  ("TOKEN", vecino)      -> enviar el token a ese vecino
  ("REQUEST", vecino)    -> enviar REQUEST a ese vecino
"""
from collections import deque


class RaymondCore:
    def __init__(self, me, holder):
        self.me = me
        self.holder = holder        # vecino hacia el token (o yo mismo si lo tengo)
        self.queue = deque()        # int = vecino por el que llegó el pedido
        self.using = False          # ("SELF", rid) = pedido de este nodo
        self.asked = False          # ya envié REQUEST a mi holder

    # ---- eventos ----
    def local_request(self, rid):
        self.queue.append(("SELF", rid))
        return self._step()

    def on_request(self, sender):
        self.queue.append(sender)
        return self._step()

    def on_token(self):
        self.holder = self.me
        self.asked = False
        return self._step()

    def on_exit(self):
        self.using = False
        return self._step()

    # ---- las dos reglas de Raymond ----
    def _step(self):
        out = []
        # 1) asignar el token
        if self.holder == self.me and not self.using and self.queue:
            nxt = self.queue.popleft()
            if isinstance(nxt, tuple):          # pedido propio: entro
                self.using = True
                out.append(("ENTER", nxt[1]))
            else:                               # pedido de un vecino: cedo el token
                self.holder = nxt
                self.asked = False
                out.append(("TOKEN", nxt))
        # 2) pedir el token (también cubre "ceder y aún quedan pedidos")
        if self.holder != self.me and self.queue and not self.asked:
            self.asked = True
            out.append(("REQUEST", self.holder))
        return out
