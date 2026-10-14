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
import time
import subprocess
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
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
            "server-04": 0,
        }
        self.simulation_task = None
        self.event_queues: list[asyncio.Queue] = []
        self.fila_retencao: list[dict] = []
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
            "server-01": 0, "server-02": 0, "server-03": 0, "server-04": 0,
        }
        if self.simulation_task and not self.simulation_task.done():
            self.simulation_task.cancel()
            self.simulation_task = None
        self.fila_retencao = []

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


class TesteCargaManager:
    """Gerenciador do teste de carga integrado à interface."""

    def __init__(self, state: MonitorState):
        self.state = state
        self.running = False
        self.task = None
        self.resultados = []
        self.contadores = {"server-01": 0, "server-02": 0, "server-03": 0, "server-04": 0}
        self.erros = 0
        self.total_latencia = 0
        self.total_enviadas = 0
        self.total_alvo = 0

    async def executar(self, total: int):
        """Executa o teste de carga assíncrono com broadcasting SSE."""
        self.running = True
        self.resultados = []
        self.contadores = {"server-01": 0, "server-02": 0, "server-03": 0, "server-04": 0}
        self.erros = 0
        self.total_latencia = 0
        self.total_enviadas = 0
        self.total_alvo = total

        records = self.state.records[:total]
        lb_url = self.state.lb_url

        await self.state.broadcast("teste_carga_iniciado", {
            "total": total,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })

        for i, record in enumerate(records):
            if not self.running:
                break

            inicio = time.time()
            server_id = "?"
            http_code = 0
            erro = False

            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(f"{lb_url}/dados", json=record)
                    http_code = resp.status_code
                    if resp.status_code == 200:
                        resp_data = resp.json()
                        server_id = resp_data.get("server_id", "?")
                        if server_id in self.contadores:
                            self.contadores[server_id] += 1
                        if server_id in self.state.contadores:
                            self.state.contadores[server_id] += 1
                    else:
                        self.erros += 1
                        erro = True
            except Exception:
                self.erros += 1
                erro = True
                http_code = 0

            fim = time.time()
            latencia_ms = round((fim - inicio) * 1000)
            self.total_latencia += latencia_ms
            self.total_enviadas += 1

            media_ms = round(self.total_latencia / self.total_enviadas)
            sucesso = sum(self.contadores.values())
            distribuicao = {}
            if sucesso > 0:
                for sid, cnt in self.contadores.items():
                    distribuicao[sid] = round(cnt * 100 / sucesso, 1)

            await self.state.broadcast("teste_carga_progresso", {
                "numero": i + 1,
                "total": total,
                "server_id": server_id,
                "latencia_ms": latencia_ms,
                "http_code": http_code,
                "erro": erro,
                "media_ms": media_ms,
                "erros": self.erros,
                "contadores": self.contadores,
                "distribuicao": distribuicao,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })

            await asyncio.sleep(0.035)

        # Resultado final
        sucesso = sum(self.contadores.values())
        media_final = round(self.total_latencia / self.total_enviadas) if self.total_enviadas > 0 else 0
        distribuicao_final = {}
        if sucesso > 0:
            for sid, cnt in self.contadores.items():
                distribuicao_final[sid] = round(cnt * 100 / sucesso, 1)

        await self.state.broadcast("teste_carga_concluido", {
            "total_enviadas": self.total_enviadas,
            "total_alvo": total,
            "media_ms": media_final,
            "erros": self.erros,
            "contadores": self.contadores,
            "distribuicao": distribuicao_final,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })

        self.running = False

    def parar(self):
        """Interrompe o teste de carga em andamento."""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
            self.task = None


# ============================================================
# Informações dos servidores
# ============================================================

SERVER_INFO = {
    "01": {"porta": 8001, "db": "server1.db"},
    "02": {"porta": 8002, "db": "server2.db"},
    "03": {"porta": 8003, "db": "server3.db"},
    "04": {"porta": 8004, "db": "server4.db"},
}

SERVIDORES_CONFIG = {
    1: {"porta": 8001, "server_id": "server-01", "db": "databases/server1.db", "nome": "Servidor 01"},
    2: {"porta": 8002, "server_id": "server-02", "db": "databases/server2.db", "nome": "Servidor 02"},
    3: {"porta": 8003, "server_id": "server-03", "db": "databases/server3.db", "nome": "Servidor 03"},
    4: {"porta": 8004, "server_id": "server-04", "db": "databases/server4.db", "nome": "Servidor 04 (Reserva)"},
}


async def verificar_servidor_online(porta: int) -> bool:
    """Verifica se um servidor backend está respondendo na porta."""
    try:
        async with httpx.AsyncClient(timeout=0.6) as client:
            res = await client.get(f"http://localhost:{porta}/health")
            return res.status_code == 200
    except Exception:
        return False


def derrubar_servidor_processo(porta: int) -> bool:
    """Derruba o processo que está escutando na porta especificada."""
    try:
        subprocess.run(["fuser", "-k", "-9", f"{porta}/tcp"], capture_output=True, text=True)
        return True
    except Exception as e:
        print(f"[MONITOR] Erro ao derrubar porta {porta}: {e}")
        return False


def religar_servidor_processo(servidor_num: int) -> bool:
    """Inicia novamente o servidor backend caso esteja offline."""
    cfg = SERVIDORES_CONFIG.get(servidor_num)
    if not cfg:
        return False
    porta = cfg["porta"]
    projeto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_path = os.path.join(projeto_dir, "logs", f"server{servidor_num}.log")
    try:
        log_file = open(log_path, "a")
        subprocess.Popen(
            [
                sys.executable,
                "server/app.py",
                "--port", str(porta),
                "--server-id", cfg["server_id"],
                "--db-path", cfg["db"],
            ],
            cwd=projeto_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return True
    except Exception as e:
        print(f"[MONITOR] Erro ao religar servidor {servidor_num}: {e}")
        return False


async def publicar_rabbitmq_se_disponivel(record: dict):
    """Publica registro no RabbitMQ se o broker estiver acessível."""
    def _pub():
        try:
            import pika
            params = pika.ConnectionParameters(host="localhost", port=5672, connection_attempts=1, retry_delay=1)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.exchange_declare(exchange="telemetria_hospitalar", exchange_type="fanout", durable=True)
            ch.basic_publish(
                exchange="telemetria_hospitalar",
                routing_key="dados.paciente",
                body=json.dumps(record),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            conn.close()
        except Exception:
            pass
    try:
        await asyncio.to_thread(_pub)
    except Exception:
        pass


async def descarregar_fila_retencao(state: MonitorState):
    """Descarrega mensagens retidas na fila do Middleware assim que servidores voltam."""
    await asyncio.sleep(0.5)
    if not state.fila_retencao:
        return
    qtd = len(state.fila_retencao)
    print(f"[MONITOR] Descarregando {qtd} mensagens retidas na fila do Middleware...")

    pendentes = list(state.fila_retencao)
    state.fila_retencao.clear()

    total_pendentes = len(pendentes)
    for idx, item in enumerate(pendentes):
        num = item["numero"]
        rec = item["record"]
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/dados", json=rec)
                if res.status_code == 200:
                    resp_json = res.json()
                    sid = resp_json.get("server_id", "server-01")
                    state.contadores["sucesso"] += 1
                    key = sid if sid in state.contadores else "server-01"
                    state.contadores[key] += 1
                    restantes = (total_pendentes - 1 - idx) + len(state.fila_retencao)
                    await state.broadcast("fila_descarregada", {
                        "numero": num,
                        "server_id": sid,
                        "qtd_restante": restantes,
                        "contadores": state.contadores,
                        "mensagem": f"🚀 [MIDDLEWARE] Registro #{num} descarregado da fila e gravado com sucesso em {sid}!",
                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                    })
                    await asyncio.sleep(0.35)
                else:
                    state.fila_retencao.append(item)
        except Exception:
            state.fila_retencao.append(item)


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
            try:
                resp_data = response.json()
            except Exception:
                resp_data = {"detail": response.text}

        # Publicar também no RabbitMQ se disponível
        await publicar_rabbitmq_se_disponivel(record)

        # Se o Load Balancer retornou erro (ex: 502 quando todos os servidores falharam):
        # O Middleware retém a mensagem na fila temporária para não perder dados!
        if response.status_code != 200:
            detalhe = resp_data.get("detail", f"Erro HTTP {response.status_code}")
            state.fila_retencao.append({"numero": numero, "record": record})
            state.contadores["total"] += 1
            await state.broadcast("fila_retida", {
                "numero": numero,
                "qtd_fila": len(state.fila_retencao),
                "detalhe": detalhe,
                "contadores": state.contadores,
                "mensagem": f"📦 [MIDDLEWARE] Todos os nós offline! Registro #{numero} retido com segurança na fila ({len(state.fila_retencao)} retidas). Nenhuma perda de dados!",
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })
            state.current_index += 1
            return True

        server_id = resp_data.get("server_id", "server-01")
        server_num = server_id.split("-")[-1]  # "01", "02", "03"
        info = SERVER_INFO.get(server_num, SERVER_INFO["01"])

        # Notificar evento especial de failover caso tenha ocorrido
        if resp_data.get("failover"):
            falharam = resp_data.get("servidores_falharam", [])
            falharam_fmt = ", ".join([s.replace("http://localhost:", "Porta ") for s in falharam])
            await state.broadcast("failover", {
                "numero": numero,
                "server_id": server_id,
                "server_num": int(server_num),
                "falharam": falharam,
                "mensagem": f"⚠️ FAILOVER EXECUTADO: {falharam_fmt} inoperante! Tráfego desviado com sucesso para {server_id} (Porta {info['porta']})!",
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })
            await asyncio.sleep(0.3)

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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
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
        # Se atingiu o limite anterior ou o final do arquivo, reinicia do zero
        if (state.limit > 0 and state.current_index >= state.limit) or state.current_index >= len(state.records):
            state.reset()
        state.speed = velocidade
        state.limit = limite
        state.simulation_task = asyncio.create_task(run_simulation(state))
        return {"status": "iniciado", "limite": limite, "velocidade": velocidade}

    @app.post("/api/proximo")
    async def proximo():
        """Processa apenas a próxima requisição (step-by-step)."""
        if state.running or state.processing:
            return {"status": "ocupado"}
        # Se atingiu o limite mas não o final do arquivo, expande o limite para permitir avançar
        if 0 < state.limit <= state.current_index and state.current_index < len(state.records):
            state.limit = state.current_index + 1
        elif state.current_index >= len(state.records):
            state.reset()
            state.limit = 1
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

    # ── Escalabilidade Dinâmica ──────────────────────────────

    @app.post("/api/escala/ativar-reserva")
    async def api_ativar_reserva():
        """Ativa o servidor reserva no Load Balancer (Scale-Out)."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/servidores/ativar-reserva")
                data = res.json()
                if res.status_code == 200:
                    await state.broadcast("escala_alterada", data)
                    return JSONResponse(status_code=200, content=data)
                return JSONResponse(status_code=res.status_code, content=data)
        except Exception as e:
            return JSONResponse(status_code=500, content={"status": "erro", "detail": str(e)})

    @app.post("/api/escala/desativar-reserva")
    async def api_desativar_reserva():
        """Desativa o servidor reserva no Load Balancer (Scale-In)."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/servidores/desativar-reserva")
                data = res.json()
                if res.status_code == 200:
                    await state.broadcast("escala_alterada", data)
                    return JSONResponse(status_code=200, content=data)
                return JSONResponse(status_code=res.status_code, content=data)
        except Exception as e:
            return JSONResponse(status_code=500, content={"status": "erro", "detail": str(e)})

    @app.get("/api/escala/status")
    async def api_escala_status():
        """Consulta status de nós ativos e reserva no Load Balancer."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(f"{state.lb_url}/status")
                return res.json()
        except Exception:
            return {
                "servidores_ativos": ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"],
                "servidores_reserva": ["http://localhost:8004"],
                "total_servidores": 3,
                "distribuicao_esperada_por_no": "33.3%",
            }

    # ── Gestão de Falhas e Resiliência (Chaos Engineering) ──

    @app.get("/api/servidores/status")
    async def api_servidores_status():
        """Retorna o estado de saúde e disponibilidade de cada servidor."""
        resultado = {}
        tem_algum_online = False
        for num, cfg in SERVIDORES_CONFIG.items():
            online = await verificar_servidor_online(cfg["porta"])
            if online:
                tem_algum_online = True
            resultado[str(num)] = {
                "numero": num,
                "porta": cfg["porta"],
                "server_id": cfg["server_id"],
                "nome": cfg["nome"],
                "online": online,
            }
        if tem_algum_online and state.fila_retencao:
            asyncio.create_task(descarregar_fila_retencao(state))
        return {"status": "ok", "servidores": resultado}

    @app.post("/api/servidores/{servidor_num}/derrubar")
    async def api_derrubar_servidor(servidor_num: int):
        """Simula a queda forçada de um servidor específico."""
        if servidor_num not in SERVIDORES_CONFIG:
            raise HTTPException(status_code=400, detail="Servidor inválido (escolha 1, 2, 3 ou 4)")
        cfg = SERVIDORES_CONFIG[servidor_num]
        porta = cfg["porta"]
        derrubar_servidor_processo(porta)
        # Breve espera para liberação do socket
        await asyncio.sleep(0.4)
        online = await verificar_servidor_online(porta)
        data = {
            "servidor_num": servidor_num,
            "server_id": cfg["server_id"],
            "porta": porta,
            "online": online,
            "acao": "derrubado",
            "mensagem": f"💥 {cfg['nome']} (:{porta}) foi derrubado manualmente!",
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }
        await state.broadcast("servidor_status", data)
        return {"status": "ok", **data}

    @app.post("/api/servidores/{servidor_num}/religar")
    async def api_religar_servidor(servidor_num: int):
        """Reinicia um servidor backend que estava offline."""
        if servidor_num not in SERVIDORES_CONFIG:
            raise HTTPException(status_code=400, detail="Servidor inválido (escolha 1, 2, 3 ou 4)")
        cfg = SERVIDORES_CONFIG[servidor_num]
        porta = cfg["porta"]
        ja_online = await verificar_servidor_online(porta)
        if not ja_online:
            religar_servidor_processo(servidor_num)
            for _ in range(8):
                await asyncio.sleep(0.25)
                if await verificar_servidor_online(porta):
                    break
        online = await verificar_servidor_online(porta)
        if online and state.fila_retencao:
            asyncio.create_task(descarregar_fila_retencao(state))
        data = {
            "servidor_num": servidor_num,
            "server_id": cfg["server_id"],
            "porta": porta,
            "online": online,
            "acao": "religado",
            "mensagem": f"✅ {cfg['nome']} (:{porta}) religado com sucesso!",
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }
        await state.broadcast("servidor_status", data)
        return {"status": "ok", **data}

    @app.post("/api/servidores/{servidor_num}/toggle")
    async def api_toggle_servidor(servidor_num: int):
        """Alterna o estado de um servidor (se online -> derruba; se offline -> religa)."""
        if servidor_num not in SERVIDORES_CONFIG:
            raise HTTPException(status_code=400, detail="Servidor inválido (escolha 1, 2, 3 ou 4)")
        cfg = SERVIDORES_CONFIG[servidor_num]
        porta = cfg["porta"]
        online = await verificar_servidor_online(porta)
        if online:
            return await api_derrubar_servidor(servidor_num)
        else:
            return await api_religar_servidor(servidor_num)

    # ── Teste de Carga Integrado ─────────────────────────────

    teste_carga = TesteCargaManager(state)

    @app.post("/api/teste-carga/iniciar")
    async def api_teste_carga_iniciar(total: int = 30):
        """Inicia o teste de carga integrado com N requisições."""
        if teste_carga.running:
            return JSONResponse(status_code=409, content={"status": "ocupado", "detail": "Teste de carga já em execução"})
        total = max(1, min(total, len(state.records)))
        teste_carga.task = asyncio.create_task(teste_carga.executar(total))
        return {"status": "iniciado", "total": total}

    @app.post("/api/teste-carga/parar")
    async def api_teste_carga_parar():
        """Interrompe o teste de carga em execução."""
        teste_carga.parar()
        return {"status": "parado"}

    @app.get("/api/teste-carga/status")
    async def api_teste_carga_status():
        """Retorna o status atual do teste de carga."""
        return {
            "running": teste_carga.running,
            "total_enviadas": teste_carga.total_enviadas,
            "total_alvo": teste_carga.total_alvo,
            "erros": teste_carga.erros,
            "contadores": teste_carga.contadores,
            "media_ms": round(teste_carga.total_latencia / teste_carga.total_enviadas) if teste_carga.total_enviadas > 0 else 0,
        }

    @app.post("/api/evento-externo")
    async def api_evento_externo(request: Request):
        """Recebe eventos de requisições externas atendidas pelo Load Balancer e transmite via SSE para o painel."""
        try:
            payload = await request.json()
            server_id = payload.get("server_id", "server-01")
            status_code = payload.get("status_code", 200)
            latencia_ms = payload.get("latencia_ms", 0)
            failover = payload.get("failover", False)

            state.contadores["total"] += 1
            if status_code == 200:
                state.contadores["sucesso"] += 1
                if server_id in state.contadores:
                    state.contadores[server_id] += 1
            else:
                state.contadores["erros"] += 1

            data = {
                "server_id": server_id,
                "status_code": status_code,
                "latencia_ms": latencia_ms,
                "failover": failover,
                "contadores": state.contadores,
                "timestamp": payload.get("timestamp", datetime.now().strftime("%H:%M:%S")),
            }
            await state.broadcast("evento_externo", data)
            return {"status": "ok"}
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

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
        "--port", type=int, default=int(os.environ.get("PORT", 5000)),
        help="Porta do monitor (padrão: 5000 ou $PORT)"
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
    uvicorn.run(app, host="0.0.0.0", port=args.port, access_log=False)
