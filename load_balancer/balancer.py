# load_balancer/balancer.py
"""
Balanceador de carga Round Robin para o sistema distribuído.

Recebe requisições na porta 8000 e distribui entre os servidores
utilizando o algoritmo Round Robin.

Uso:
    python load_balancer/balancer.py
    python load_balancer/balancer.py --port 8000 --servidores http://localhost:8001,http://localhost:8002,http://localhost:8003
"""

import argparse
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import httpx


# ============================================================
# Configuração padrão
# ============================================================

SERVIDORES_PADRAO = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]


class RoundRobinBalancer:
    """
    Balanceador de carga com algoritmo Round Robin.

    Alterna sequencialmente entre os servidores registrados,
    distribuindo as requisições de forma uniforme.
    """

    def __init__(self, servidores: list[str]):
        self.servidores = servidores
        self.indice_atual = 0
        self.total_requisicoes = 0

    def proximo_servidor(self) -> str:
        """
        Retorna o próximo servidor na rotação Round Robin.

        Returns:
            URL base do próximo servidor.
        """
        servidor = self.servidores[self.indice_atual]
        self.indice_atual = (self.indice_atual + 1) % len(self.servidores)
        self.total_requisicoes += 1
        return servidor


def criar_balanceador(servidores: list[str] = None, port: int = 8000) -> FastAPI:
    """
    Cria e configura o balanceador de carga.

    Args:
        servidores: Lista de URLs dos servidores backend.
        port: Porta do balanceador (apenas para exibição).

    Returns:
        Instância configurada do FastAPI.
    """
    _servidores = servidores or SERVIDORES_PADRAO
    balancer = RoundRobinBalancer(_servidores)

    # Cliente HTTP para encaminhar requisições
    client = httpx.AsyncClient(timeout=10.0)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        """Ciclo de vida do balanceador."""
        print(f"[BALANCEADOR] Iniciado na porta {port}")
        print(f"[BALANCEADOR] Servidores registrados:")
        for i, s in enumerate(_servidores):
            print(f"  [{i + 1}] {s}")
        print()
        yield
        await client.aclose()

    app = FastAPI(
        title="Balanceador de Carga - Sistema Hospitalar",
        description="Distribui requisições entre servidores usando Round Robin",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.post("/dados")
    async def encaminhar_post_dados(request: Request):
        """
        Encaminha POST /dados para o próximo servidor via Round Robin.
        """
        servidor = balancer.proximo_servidor()
        body = await request.json()

        print(
            f"[BALANCEADOR] Requisição {balancer.total_requisicoes} → {servidor}"
        )

        try:
            response = await client.post(f"{servidor}/dados", json=body)
            return JSONResponse(
                status_code=response.status_code,
                content=response.json(),
            )
        except httpx.ConnectError:
            print(f"[BALANCEADOR] ERRO: Não foi possível conectar a {servidor}")
            return JSONResponse(
                status_code=502,
                content={
                    "detail": f"Servidor indisponível: {servidor}",
                    "servidor": servidor,
                },
            )
        except Exception as e:
            print(f"[BALANCEADOR] ERRO: {e}")
            return JSONResponse(
                status_code=500,
                content={"detail": str(e)},
            )

    @app.get("/dados")
    async def encaminhar_get_dados(request: Request):
        """
        Encaminha GET /dados para o próximo servidor via Round Robin.
        """
        servidor = balancer.proximo_servidor()
        query_string = str(request.url.query)
        url = f"{servidor}/dados"
        if query_string:
            url += f"?{query_string}"

        print(
            f"[BALANCEADOR] Requisição {balancer.total_requisicoes} (GET /dados) → {servidor}"
        )

        try:
            response = await client.get(url)
            return JSONResponse(
                status_code=response.status_code,
                content=response.json(),
            )
        except httpx.ConnectError:
            return JSONResponse(
                status_code=502,
                content={"detail": f"Servidor indisponível: {servidor}"},
            )

    @app.get("/dados/{paciente_id}")
    async def encaminhar_get_paciente(paciente_id: int):
        """
        Encaminha GET /dados/{paciente_id} para o próximo servidor via Round Robin.
        """
        servidor = balancer.proximo_servidor()

        print(
            f"[BALANCEADOR] Requisição {balancer.total_requisicoes} "
            f"(GET /dados/{paciente_id}) → {servidor}"
        )

        try:
            response = await client.get(f"{servidor}/dados/{paciente_id}")
            return JSONResponse(
                status_code=response.status_code,
                content=response.json(),
            )
        except httpx.ConnectError:
            return JSONResponse(
                status_code=502,
                content={"detail": f"Servidor indisponível: {servidor}"},
            )

    @app.get("/status")
    async def status():
        """
        Retorna o status do balanceador: servidores registrados e
        total de requisições processadas.
        """
        return {
            "servidores": _servidores,
            "total_requisicoes": balancer.total_requisicoes,
            "proximo_servidor_indice": balancer.indice_atual,
        }

    return app


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(
        description="Balanceador de carga Round Robin"
    )
    parser.add_argument(
        "--port", type=int, default=8000,
        help="Porta do balanceador (padrão: 8000)"
    )
    parser.add_argument(
        "--servidores", type=str,
        default="http://localhost:8001,http://localhost:8002,http://localhost:8003",
        help="Lista de servidores separados por vírgula "
             "(padrão: http://localhost:8001,http://localhost:8002,http://localhost:8003)"
    )

    args = parser.parse_args()
    servidores = [s.strip() for s in args.servidores.split(",")]

    print("=" * 60)
    print("  BALANCEADOR DE CARGA — Round Robin")
    print("=" * 60)
    print(f"  Porta: {args.port}")
    print(f"  Servidores:")
    for i, s in enumerate(servidores):
        print(f"    [{i + 1}] {s}")
    print("=" * 60)
    print()

    app = criar_balanceador(servidores, args.port)
    uvicorn.run(app, host="0.0.0.0", port=args.port)
