# load_balancer/balancer.py
"""
Balanceador de carga Round Robin para o sistema distribuído hospitalar.

Recebe requisições na porta 8000 e distribui entre os servidores backend
utilizando o algoritmo Round Robin.

Recursos avançados:
- Suporte a pool de servidores ativos e servidores reserva (Warm Standby / Burst Scaling)
- Adição e remoção dinâmica de servidores via API (/servidores/adicionar, /servidores/remover)
- Ativação/Desativação de reserva (/servidores/ativar-reserva, /servidores/desativar-reserva)
- Tolerância a falhas (Failover automático caso um nó esteja temporariamente indisponível)
- Endpoint de monitoramento (/status) e verificação de integridade (/health)

Uso:
    python load_balancer/balancer.py
    python load_balancer/balancer.py --port 8000 \
        --servidores http://localhost:8001,http://localhost:8002,http://localhost:8003 \
        --servidores-reserva http://localhost:8004
"""

import argparse
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import httpx


# ============================================================
# Configurações padrão
# ============================================================

SERVIDORES_PADRAO = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

SERVIDORES_RESERVA_PADRAO = [
    "http://localhost:8004",
]


class RoundRobinBalancer:
    """
    Balanceador de carga com algoritmo Round Robin e suporte a elasticidade.

    Alterna sequencialmente entre os servidores ativos registrados,
    distribuindo as requisições de forma uniforme e permitindo expansão
    com servidores reservas sob demanda.
    """

    def __init__(self, servidores: list[str], servidores_reserva: list[str] = None):
        self.servidores = list(servidores)
        self.servidores_reserva = list(servidores_reserva or [])
        self.indice_atual = 0
        self.total_requisicoes = 0

    def proximo_servidor(self) -> str:
        """
        Retorna o próximo servidor na rotação Round Robin.

        Returns:
            URL base do próximo servidor.
        """
        if not self.servidores:
            raise RuntimeError("Nenhum servidor ativo disponível no balanceador")

        self.indice_atual = self.indice_atual % len(self.servidores)
        servidor = self.servidores[self.indice_atual]
        self.indice_atual = (self.indice_atual + 1) % len(self.servidores)
        self.total_requisicoes += 1
        return servidor

    def adicionar_servidor(self, url: str) -> bool:
        """Adiciona um servidor à lista de servidores ativos."""
        url = url.rstrip("/")
        if url not in self.servidores:
            self.servidores.append(url)
            if url in self.servidores_reserva:
                self.servidores_reserva.remove(url)
            return True
        return False

    def remover_servidor(self, url: str) -> bool:
        """Remove um servidor da lista de ativos."""
        url = url.rstrip("/")
        if url in self.servidores:
            self.servidores.remove(url)
            if self.indice_atual >= len(self.servidores) and self.servidores:
                self.indice_atual = 0
            return True
        return False

    def ativar_reserva(self, url: str = None) -> str | None:
        """
        Ativa um servidor reserva no pool de servidores ativos (Scale-Out).
        Se url não for informado, pega o primeiro da lista de reserva.
        """
        if url:
            alvo = url.rstrip("/")
            if alvo in self.servidores_reserva:
                self.servidores_reserva.remove(alvo)
                if alvo not in self.servidores:
                    self.servidores.append(alvo)
                return alvo
        elif self.servidores_reserva:
            alvo = self.servidores_reserva.pop(0)
            if alvo not in self.servidores:
                self.servidores.append(alvo)
            return alvo
        return None

    def desativar_reserva(self, url: str = None) -> str | None:
        """
        Desativa um servidor da rota ativa e o devolve ao pool de reserva (Scale-In).
        """
        if url:
            alvo = url.rstrip("/")
            if alvo in self.servidores:
                self.servidores.remove(alvo)
                if alvo not in self.servidores_reserva:
                    self.servidores_reserva.append(alvo)
                if self.indice_atual >= len(self.servidores) and self.servidores:
                    self.indice_atual = 0
                return alvo
        elif len(self.servidores) > 3:
            # Por padrão retira o último nó se houver mais de 3
            alvo = self.servidores.pop()
            if alvo not in self.servidores_reserva:
                self.servidores_reserva.append(alvo)
            if self.indice_atual >= len(self.servidores) and self.servidores:
                self.indice_atual = 0
            return alvo
        return None


class ServidorPayload(BaseModel):
    url: str


def criar_balanceador(
    servidores: list[str] = None,
    port: int = 8000,
    servidores_reserva: list[str] = None,
) -> FastAPI:
    """
    Cria e configura o balanceador de carga.

    Args:
        servidores: Lista de URLs dos servidores backend ativos.
        port: Porta do balanceador.
        servidores_reserva: Lista de URLs dos servidores reservas.

    Returns:
        Instância configurada do FastAPI.
    """
    if isinstance(port, list):
        # Caso chamada posicional criar_balanceador(servidores, reservas)
        servidores_reserva = port
        port = 8000

    _servidores = servidores or SERVIDORES_PADRAO
    _reservas = servidores_reserva if servidores_reserva is not None else SERVIDORES_RESERVA_PADRAO
    balancer = RoundRobinBalancer(_servidores, _reservas)

    # Cliente HTTP para encaminhar requisições
    client = httpx.AsyncClient(timeout=10.0)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        """Ciclo de vida do balanceador."""
        print(f"[BALANCEADOR] Iniciado na porta {port}")
        print(f"[BALANCEADOR] Servidores ativos registrados:")
        for i, s in enumerate(balancer.servidores):
            print(f"  [{i + 1}] {s}")
        if balancer.servidores_reserva:
            print(f"[BALANCEADOR] Servidores reserva (Warm Standby):")
            for i, s in enumerate(balancer.servidores_reserva):
                print(f"  [R{i + 1}] {s}")
        print()
        yield
        await client.aclose()

    app = FastAPI(
        title="Balanceador de Carga - Sistema Hospitalar",
        description="Distribui requisições com Round Robin, Failover e Elasticidade",
        version="2.0.0",
        lifespan=lifespan,
    )

    @app.post("/dados")
    async def encaminhar_post_dados(request: Request):
        """
        Encaminha POST /dados via Round Robin com Failover transparente.
        """
        if not balancer.servidores:
            return JSONResponse(status_code=503, content={"detail": "Nenhum servidor backend configurado"})

        body = await request.json()
        tentativas = len(balancer.servidores)

        for tentativa in range(tentativas):
            servidor = balancer.proximo_servidor()
            print(f"[BALANCEADOR] POST /dados [{tentativa + 1}/{tentativas}] → {servidor}")
            try:
                response = await client.post(f"{servidor}/dados", json=body)
                return JSONResponse(
                    status_code=response.status_code,
                    content=response.json(),
                )
            except (httpx.ConnectError, httpx.TimeoutException):
                print(f"[BALANCEADOR] AVISO: Falha de conexão em {servidor}. Executando failover...")
                continue
            except Exception as e:
                print(f"[BALANCEADOR] ERRO inesperado em {servidor}: {e}")
                return JSONResponse(status_code=500, content={"detail": str(e)})

        return JSONResponse(
            status_code=502,
            content={"detail": "Todos os servidores backend falharam em responder"},
        )

    @app.get("/dados")
    async def encaminhar_get_dados(request: Request):
        """
        Encaminha GET /dados via Round Robin com Failover transparente.
        """
        if not balancer.servidores:
            return JSONResponse(status_code=503, content={"detail": "Nenhum servidor backend configurado"})

        query_string = str(request.url.query)
        tentativas = len(balancer.servidores)

        for tentativa in range(tentativas):
            servidor = balancer.proximo_servidor()
            url = f"{servidor}/dados"
            if query_string:
                url += f"?{query_string}"

            print(f"[BALANCEADOR] GET /dados [{tentativa + 1}/{tentativas}] → {servidor}")
            try:
                response = await client.get(url)
                return JSONResponse(
                    status_code=response.status_code,
                    content=response.json(),
                )
            except (httpx.ConnectError, httpx.TimeoutException):
                print(f"[BALANCEADOR] AVISO: Falha de conexão em {servidor}. Executando failover...")
                continue
            except Exception as e:
                return JSONResponse(status_code=500, content={"detail": str(e)})

        return JSONResponse(
            status_code=502,
            content={"detail": "Todos os servidores backend falharam em responder"},
        )

    @app.get("/dados/{paciente_id}")
    async def encaminhar_get_paciente(paciente_id: int):
        """
        Encaminha GET /dados/{paciente_id} via Round Robin com Failover transparente.
        """
        if not balancer.servidores:
            return JSONResponse(status_code=503, content={"detail": "Nenhum servidor backend configurado"})

        tentativas = len(balancer.servidores)

        for tentativa in range(tentativas):
            servidor = balancer.proximo_servidor()
            print(f"[BALANCEADOR] GET /dados/{paciente_id} [{tentativa + 1}/{tentativas}] → {servidor}")
            try:
                response = await client.get(f"{servidor}/dados/{paciente_id}")
                return JSONResponse(
                    status_code=response.status_code,
                    content=response.json(),
                )
            except (httpx.ConnectError, httpx.TimeoutException):
                print(f"[BALANCEADOR] AVISO: Falha de conexão em {servidor}. Executando failover...")
                continue
            except Exception as e:
                return JSONResponse(status_code=500, content={"detail": str(e)})

        return JSONResponse(
            status_code=502,
            content={"detail": "Todos os servidores backend falharam em responder"},
        )

    # ── Gestão Dinâmica de Servidores (Escalabilidade e Reservas) ──

    @app.post("/servidores/adicionar")
    async def adicionar_servidor(payload: ServidorPayload):
        """Adiciona dinamicamente um servidor à rota do balanceador."""
        adicionado = balancer.adicionar_servidor(payload.url)
        return {
            "status": "sucesso" if adicionado else "ja_existente",
            "servidor": payload.url,
            "servidores_ativos": balancer.servidores,
            "total_ativos": len(balancer.servidores),
        }

    @app.post("/servidores/remover")
    async def remover_servidor(payload: ServidorPayload):
        """Remove dinamicamente um servidor da rota do balanceador."""
        removido = balancer.remover_servidor(payload.url)
        return {
            "status": "sucesso" if removido else "nao_encontrado",
            "servidor": payload.url,
            "servidores_ativos": balancer.servidores,
            "total_ativos": len(balancer.servidores),
        }

    @app.post("/servidores/ativar-reserva")
    async def ativar_reserva():
        """Ativa um servidor reserva no pool de atendimento (Scale-Out sob demanda)."""
        alvo = balancer.ativar_reserva()
        if not alvo:
            return JSONResponse(
                status_code=400,
                content={"detail": "Nenhum servidor reserva disponível no momento."},
            )
        print(f"[BALANCEADOR] 🚀 ESCALA HORIZONTAL: Servidor reserva {alvo} ATIVADO na rota!")
        return {
            "status": "ativado",
            "servidor_ativado": alvo,
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_ativos": len(balancer.servidores),
        }

    @app.post("/servidores/desativar-reserva")
    async def desativar_reserva():
        """Desativa um servidor reserva da rota ativa (Scale-In)."""
        alvo = balancer.desativar_reserva()
        if not alvo:
            return JSONResponse(
                status_code=400,
                content={"detail": "Nenhum servidor reserva ativo para desativar."},
            )
        print(f"[BALANCEADOR] ⬇️ SCALE-IN: Servidor reserva {alvo} DESATIVADO da rota!")
        return {
            "status": "desativado",
            "servidor_desativado": alvo,
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_ativos": len(balancer.servidores),
        }

    @app.get("/health")
    async def health():
        """Verificação de integridade do balanceador."""
        return {
            "status": "healthy",
            "total_ativos": len(balancer.servidores),
            "servidores_ativos": balancer.servidores,
        }

    @app.get("/status")
    async def status():
        """
        Retorna o status completo do balanceador: servidores registrados,
        reservas, total de requisições e taxa de distribuição esperada.
        """
        n_ativos = len(balancer.servidores)
        pct = f"{(100.0 / n_ativos):.1f}%" if n_ativos > 0 else "0%"
        return {
            "servidores": balancer.servidores,
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_requisicoes": balancer.total_requisicoes,
            "proximo_servidor_indice": balancer.indice_atual,
            "total_servidores": n_ativos,
            "distribuicao_esperada_por_no": pct,
        }

    return app


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(
        description="Balanceador de Carga Round Robin com Elasticidade"
    )
    parser.add_argument(
        "--port", type=int, default=8000,
        help="Porta do balanceador (padrão: 8000)"
    )
    parser.add_argument(
        "--servidores", type=str, default="",
        help="Lista de servidores ativos separados por vírgula"
    )
    parser.add_argument(
        "--servidores-reserva", type=str, default="",
        help="Lista de servidores reservas separados por vírgula"
    )

    args = parser.parse_args()

    servidores = (
        [s.strip() for s in args.servidores.split(",") if s.strip()]
        if args.servidores
        else SERVIDORES_PADRAO
    )
    servidores_reserva = (
        [s.strip() for s in args.servidores_reserva.split(",") if s.strip()]
        if args.servidores_reserva
        else SERVIDORES_RESERVA_PADRAO
    )

    app = criar_balanceador(servidores, servidores_reserva, args.port)
    uvicorn.run(app, host="0.0.0.0", port=args.port)
