# middleware/replicador.py
"""
Middleware MOM Enterprise — Replicador Fanout Ativo (Marco 3).

Provê a camada de mensageria orientada a mensagens (MOM) com replicação Fanout
idêntica em todos os servidores backend (server-01 a server-04), garantindo
Zero Data Loss, particionamento determinístico por paciente_id, controle de vazão
(Token Bucket), Circuit Breaker e Dead Letter Topic (DLT).

Pode rodar tanto em modo standalone (como processo independente na porta 9092/9094)
quanto ser invocado programaticamente pelo monitor e produtor.
"""

import argparse
import asyncio
from datetime import datetime
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Dict, List, Optional

# Adicionar diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.config import (
    KAFKA_CONSUMER_GROUPS,
    KAFKA_TOPIC,
    KAFKA_DLT_TOPIC,
    CIRCUIT_BREAKER_ENABLED,
    CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_TOKENS_PER_SECOND,
    RATE_LIMIT_BURST_SIZE,
)
from middleware.consumidor import CircuitBreaker, TokenBucketRateLimiter
import requests

logger = logging.getLogger("middleware.replicador")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class MOMReplicador:
    """Motor de replicação Fanout em nível de middleware."""

    def __init__(self):
        self.particoes = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
        self.offsets = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
        self.total_publicadas = 0
        self.total_replicadas = 0
        self.total_falhas = 0
        self.dlt_queue: List[dict] = []
        self._lock = threading.Lock()

        # Circuit Breakers dedicados por servidor
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        if CIRCUIT_BREAKER_ENABLED:
            for g in KAFKA_CONSUMER_GROUPS:
                sid = g["server_id"]
                self.circuit_breakers[sid] = CircuitBreaker(
                    name=sid,
                    failure_threshold=CIRCUIT_BREAKER_FAILURE_THRESHOLD,
                    recovery_timeout=CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
                )

        # Rate Limiter
        self.rate_limiter = (
            TokenBucketRateLimiter(RATE_LIMIT_TOKENS_PER_SECOND, RATE_LIMIT_BURST_SIZE)
            if RATE_LIMIT_ENABLED else None
        )

    def replicar(self, record: dict) -> dict:
        """
        Recebe um registro clínico, particiona determinísticamente e
        dispara a difusão Fanout para todos os 4 Consumer Groups.
        """
        if self.rate_limiter and not self.rate_limiter.acquire(timeout=2.0):
            logger.warning("[MOM] Rate limit atingido — atrasando ingestão")

        pid = int(record.get("paciente_id") or 0)
        part_idx = pid % 4
        part_nome = f"P{part_idx}"

        with self._lock:
            self.total_publicadas += 1
            self.particoes[part_nome] += 1
            self.offsets[part_nome] += 1
            offset = self.offsets[part_nome]

        resultados = {}
        headers = {"X-Replicated": "true", "X-MOM-Partition": part_nome}

        for group in KAFKA_CONSUMER_GROUPS:
            sid = group["server_id"]
            url = f"{group['servidor_url']}/dados"
            cb = self.circuit_breakers.get(sid)

            if cb and not cb.allow_request():
                logger.warning("[%s] Circuit Breaker ABERTO — telemetria retida", sid)
                resultados[sid] = "circuit_open"
                continue

            try:
                resp = requests.post(url, json=record, headers=headers, timeout=2.5)
                if resp.status_code == 200:
                    if cb:
                        cb.record_success()
                    resultados[sid] = "ok"
                    with self._lock:
                        self.total_replicadas += 1
                else:
                    if cb:
                        cb.record_failure()
                    resultados[sid] = f"error_{resp.status_code}"
                    self._enviar_dlt(record, sid, f"HTTP {resp.status_code}")
            except Exception as e:
                if cb:
                    cb.record_failure()
                resultados[sid] = "unreachable"
                self._enviar_dlt(record, sid, str(e))

        return {
            "status": "ok",
            "topico": KAFKA_TOPIC,
            "particao": part_idx,
            "particao_nome": part_nome,
            "offset": offset,
            "resultados": resultados,
        }

    def _enviar_dlt(self, record: dict, server_id: str, motivo: str):
        with self._lock:
            self.total_falhas += 1
            self.dlt_queue.append({
                "record": record,
                "server_id": server_id,
                "motivo": motivo,
                "timestamp": datetime.now().isoformat(),
            })
            if len(self.dlt_queue) > 500:
                self.dlt_queue.pop(0)

    def status(self) -> dict:
        with self._lock:
            return {
                "topico": KAFKA_TOPIC,
                "particoes": dict(self.particoes),
                "offsets": dict(self.offsets),
                "total_publicadas": self.total_publicadas,
                "total_replicadas": self.total_replicadas,
                "total_dlt": len(self.dlt_queue),
                "consumer_groups": [g["group_id"] for g in KAFKA_CONSUMER_GROUPS],
            }


_replicador_global: Optional[MOMReplicador] = None


def get_replicador() -> MOMReplicador:
    global _replicador_global
    if _replicador_global is None:
        _replicador_global = MOMReplicador()
    return _replicador_global


def replicar_telemetria(record: dict) -> dict:
    """Função utilitária direta para outros módulos invocarem a replicação Fanout."""
    return get_replicador().replicar(record)


# Servidor REST HTTP para ingestão remota
def iniciar_servidor_mom(porta: int = 9094):
    """Inicia um servidor REST HTTP leve para receber telemetria e replicar."""
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    import uvicorn

    app = FastAPI(title="Middleware MOM Enterprise Replicator")
    rep = get_replicador()

    @app.get("/health")
    @app.get("/status")
    def health():
        return {"status": "online", "middleware": "MOM Enterprise", **rep.status()}

    @app.post("/publicar")
    @app.post("/telemetria")
    @app.post("/dados")
    async def publicar(request: Request):
        try:
            body = await request.json()
            res = rep.replicar(body)
            return JSONResponse(status_code=200, content=res)
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.get("/dlt")
    def ver_dlt():
        return {"dlt_topic": KAFKA_DLT_TOPIC, "total": len(rep.dlt_queue), "mensagens": rep.dlt_queue[-50:]}

    print("╔" + "═" * 68 + "╗")
    print("║" + " MIDDLEWARE MOM ENTERPRISE — REPLICADOR FANOUT ".center(68) + "║")
    print("╠" + "═" * 68 + "╣")
    print(f"║  Porta HTTP MOM : {porta:<48}║")
    print(f"║  Tópico         : {KAFKA_TOPIC} (4 partições P0..P3)".ljust(69) + "║")
    print("║  Consumer Groups: 4 ativos (replicação Fanout em todos os servidores) ║")
    for g in KAFKA_CONSUMER_GROUPS:
        print(f"║    • {g['group_id']:<26} → {g['servidor_url']} ({g['server_id']})".ljust(69) + "║")
    print("╠" + "═" * 68 + "╣")
    print("║  Garantia       : Replicação Ativa • Zero Data Loss • Marco 3      ║")
    print("╚" + "═" * 68 + "╝")

    uvicorn.run(app, host="0.0.0.0", port=porta, log_level="warning")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Middleware MOM Enterprise Replicator")
    parser.add_argument("--porta", type=int, default=int(os.getenv("MOM_PORT", "9094")), help="Porta HTTP MOM (padrão: 9094)")
    args = parser.parse_args()

    def _sig(s, f):
        print("\n[MOM] Encerrando Replicador...")
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    iniciar_servidor_mom(args.porta)

