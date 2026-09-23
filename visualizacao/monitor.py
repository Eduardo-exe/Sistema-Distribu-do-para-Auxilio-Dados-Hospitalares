# visualizacao/monitor.py
"""
Monitor de Visualização — Interface visual didática para o sistema distribuído.

Servidor FastAPI que:
1. Serve a interface HTML de visualização
2. Lê registros reais do CSV
3. Envia requisições reais ao Load Balancer (porta 8000)
4. Transmite eventos via SSE para animação no browser

Uso:
    python visualizacao/monitor.py

Pré-requisitos:
    - 3 servidores rodando (portas 8001, 8002, 8003)
    - Load Balancer rodando (porta 8000)
"""

import argparse
import asyncio
import csv
import json
import os
import sys
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
import httpx
import uvicorn

# Adicionar diretório raiz ao path para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.models import CSV_COLUMN_MAP, converter_registro_csv


# ============================================================
# Estado global do monitor
# ============================================================

class MonitorState:
    """Mantém o estado da simulação de visualização."""

    def __init__(self, csv_path: str, lb_url: str):
        self.csv_path = csv_path
        self.lb_url = lb_url
        self.records: list[dict] = []
        self.current_index: int = 0
        self.running: bool = False
        self.processing: bool = False
        self.speed: float = 1.0
        self.limit: int = 12
        self.contadores = {
            "total": 0,
            "sucesso": 0,
            "erros": 0,
            "server-01": 0,
            "server-02": 0,
            "server-03": 0,
        }
        self.simulation_task = None
        self.event_queues: list[asyncio.Queue] = []
        self._load_csv()

    def _load_csv(self):
        """Carrega todos os registros do CSV."""
        if not os.path.exists(self.csv_path):
            print(f"[MONITOR] ERRO: CSV não encontrado: {self.csv_path}")
            return
        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.records.append(converter_registro_csv(row))
        print(f"[MONITOR] {len(self.records)} registros carregados do CSV")

    def reset(self):
        """Reseta o estado da simulação."""
        self.current_index = 0
        self.running = False
        self.processing = False
        self.contadores = {
            "total": 0, "sucesso": 0, "erros": 0,
            "server-01": 0, "server-02": 0, "server-03": 0,
        }
        if self.simulation_task and not self.simulation_task.done():
            self.simulation_task.cancel()
            self.simulation_task = None

    async def broadcast(self, event_type: str, data: dict):
        """Envia evento SSE para todos os clientes conectados."""
        message = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
        dead_queues = []
        for q in self.event_queues:
            try:
                await q.put(message)
            except Exception:
                dead_queues.append(q)
        for q in dead_queues:
            if q in self.event_queues:
                self.event_queues.remove(q)


# ============================================================
# Informações dos servidores
# ============================================================

SERVER_INFO = {
    "01": {"porta": 8001, "db": "server1.db"},
    "02": {"porta": 8002, "db": "server2.db"},
    "03": {"porta": 8003, "db": "server3.db"},
}


# ============================================================
# Processamento de requisições
# ============================================================

async def process_single(state: MonitorState) -> bool:
    """
    Processa um único registro do CSV e emite eventos SSE para cada etapa.

    Returns:
        True se processou com sucesso, False se não há mais registros.
    """
    if state.current_index >= len(state.records):
        await state.broadcast("finished", {
            "mensagem": "Todos os registros foram processados",
            "contadores": state.contadores,
        })
        return False

    if 0 < state.limit <= state.current_index:
        await state.broadcast("finished", {
            "mensagem": f"Limite de {state.limit} registros atingido",
            "contadores": state.contadores,
        })
        return False

    record = state.records[state.current_index]
    numero = state.current_index + 1
    delay = 0.65 / state.speed

    # --- Etapa 1: Leitura do CSV ---
    await state.broadcast("csv_read", {
        "numero": numero,
        "dados": record,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay)

    # --- Etapa 2: CSV Sender preparando ---
    await state.broadcast("sending", {
        "numero": numero,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay * 0.8)

    # --- Etapa 3: HTTP POST ao Load Balancer ---
    await state.broadcast("http_post", {
        "numero": numero,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay * 0.5)

    # --- Etapa 4: Enviar requisição REAL ao Load Balancer ---
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{state.lb_url}/dados", json=record
            )
            resp_data = response.json()

        server_id = resp_data.get("server_id", "server-01")
        server_num = server_id.split("-")[-1]  # "01", "02", "03"
        info = SERVER_INFO.get(server_num, SERVER_INFO["01"])

        # --- Etapa 5: Roteamento Round Robin ---
        await state.broadcast("lb_routing", {
            "numero": numero,
            "server_id": server_id,
            "server_num": int(server_num),
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })
        await asyncio.sleep(delay)

        # --- Etapa 6: Servidor recebeu ---
        await state.broadcast("server_received", {
            "numero": numero,
            "server_id": server_id,
            "porta": info["porta"],
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })
        await asyncio.sleep(delay * 0.7)

        # --- Etapa 7: Salvando no banco ---
        await state.broadcast("db_saving", {
            "numero": numero,
            "server_id": server_id,
            "db_name": info["db"],
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })
        await asyncio.sleep(delay * 0.5)

        # --- Etapa 8: Concluído ---
        state.contadores["total"] += 1
        state.contadores["sucesso"] += 1
        key = server_id if server_id in state.contadores else "server-01"
        state.contadores[key] += 1

        await state.broadcast("complete", {
            "numero": numero,
            "server_id": server_id,
            "resposta": resp_data,
            "contadores": state.contadores,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })

    except httpx.ConnectError:
        state.contadores["total"] += 1
        state.contadores["erros"] += 1
        await state.broadcast("error", {
            "numero": numero,
            "erro": f"Não foi possível conectar ao Load Balancer ({state.lb_url})",
            "contadores": state.contadores,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })
    except Exception as e:
        state.contadores["total"] += 1
        state.contadores["erros"] += 1
        await state.broadcast("error", {
            "numero": numero,
            "erro": str(e),
            "contadores": state.contadores,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })

    state.current_index += 1
    return True


async def run_simulation(state: MonitorState):
    """Loop de simulação automática."""
    state.running = True
    while state.running:
        has_more = await process_single(state)
        if not has_more:
            state.running = False
            break
        # Pausa entre requisições
        await asyncio.sleep(0.4 / state.speed)
    state.running = False


# ============================================================
# Aplicação FastAPI
# ============================================================

def create_app(csv_path: str, lb_url: str) -> FastAPI:
    """Cria a aplicação FastAPI do monitor."""
    state = MonitorState(csv_path, lb_url)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        print(f"[MONITOR] Interface disponível em http://localhost:5000")
        yield

    app = FastAPI(
        title="Monitor de Visualização - Sistema Hospitalar",
        lifespan=lifespan,
    )

    # ── Servir HTML ──────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def serve_index():
        """Serve a interface de visualização."""
        html_path = os.path.join(os.path.dirname(__file__), "index.html")
        with open(html_path, "r", encoding="utf-8") as f:
            return f.read()

    # ── SSE Stream ───────────────────────────────────────────

    @app.get("/api/eventos")
    async def sse_stream():
        """Stream de eventos SSE para o browser."""
        queue = asyncio.Queue()
        state.event_queues.append(queue)

        async def generate():
            try:
                # Enviar estado inicial
                yield (
                    f"event: connected\n"
                    f"data: {json.dumps({'total_records': len(state.records), 'contadores': state.contadores})}\n\n"
                )
                while True:
                    try:
                        msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                        yield msg
                    except asyncio.TimeoutError:
                        # Keepalive para manter a conexão
                        yield ": keepalive\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                if queue in state.event_queues:
                    state.event_queues.remove(queue)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── Controles ────────────────────────────────────────────

    @app.post("/api/iniciar")
    async def iniciar(limite: int = 12, velocidade: float = 1.0):
        """Inicia simulação automática."""
        if state.running or state.processing:
            return {"status": "ocupado"}
        state.speed = velocidade
        state.limit = limite
        state.simulation_task = asyncio.create_task(run_simulation(state))
        return {"status": "iniciado", "limite": limite, "velocidade": velocidade}

    @app.post("/api/proximo")
    async def proximo():
        """Processa apenas a próxima requisição (step-by-step)."""
        if state.running or state.processing:
            return {"status": "ocupado"}
        state.processing = True

        async def _process():
            try:
                await process_single(state)
            finally:
                state.processing = False

        asyncio.create_task(_process())
        return {"status": "processando"}

    @app.post("/api/pausar")
    async def pausar():
        """Pausa a simulação automática."""
        state.running = False
        if state.simulation_task and not state.simulation_task.done():
            state.simulation_task.cancel()
            state.simulation_task = None
        await state.broadcast("paused", {
            "mensagem": "Simulação pausada",
            "contadores": state.contadores,
        })
        return {"status": "pausado"}

    @app.post("/api/reiniciar")
    async def reiniciar():
        """Reinicia a simulação do zero."""
        state.reset()
        await state.broadcast("reset", {
            "mensagem": "Simulação reiniciada",
            "contadores": state.contadores,
        })
        return {"status": "reiniciado"}

    @app.post("/api/velocidade")
    async def set_velocidade(valor: float = 1.0):
        """Altera a velocidade da simulação."""
        state.speed = valor
        return {"status": "ok", "velocidade": valor}

    @app.get("/api/status")
    async def get_status():
        """Retorna o estado atual do monitor."""
        return {
            "running": state.running,
            "processing": state.processing,
            "current_index": state.current_index,
            "total_records": len(state.records),
            "limit": state.limit,
            "speed": state.speed,
            "contadores": state.contadores,
        }

    return app


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    projeto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_padrao = os.path.join(projeto_dir, "data", "dataset.csv")

    parser = argparse.ArgumentParser(
        description="Monitor de Visualização do Sistema Distribuído"
    )
    parser.add_argument(
        "--port", type=int, default=5000,
        help="Porta do monitor (padrão: 5000)"
    )
    parser.add_argument(
        "--csv", type=str, default=csv_padrao,
        help="Caminho do CSV"
    )
    parser.add_argument(
        "--lb-url", type=str, default="http://localhost:8000",
        help="URL do Load Balancer (padrão: http://localhost:8000)"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("  MONITOR DE VISUALIZAÇÃO")
    print("=" * 60)
    print(f"  Porta:           {args.port}")
    print(f"  CSV:             {args.csv}")
    print(f"  Load Balancer:   {args.lb_url}")
    print(f"  Interface:       http://localhost:{args.port}")
    print("=" * 60)
    print()

    app = create_app(args.csv, args.lb_url)
    uvicorn.run(app, host="0.0.0.0", port=args.port)
