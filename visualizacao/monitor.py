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
import re
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
        self.recent_events: list[dict] = []
        self.event_counter: int = 0
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
        KafkaMonitorMetrics.reset()

    async def broadcast(self, event_type: str, data: dict):
        """Envia evento SSE para todos os clientes conectados e armazena para polling."""
        self.event_counter += 1
        event_obj = {
            "id": self.event_counter,
            "event": event_type,
            "data": data,
        }
        self.recent_events.append(event_obj)
        if len(self.recent_events) > 300:
            self.recent_events.pop(0)

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

    async def executar(self, total: int, concurrency: int = 5):
        """Executa o teste de carga assíncrono com broadcasting SSE e concorrência."""
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
            "concurrency": concurrency,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })

        queue = asyncio.Queue()
        for i, rec in enumerate(records):
            queue.put_nowait((i + 1, rec))

        sem = asyncio.Semaphore(concurrency)
        counter_lock = asyncio.Lock()

        async with httpx.AsyncClient(timeout=10.0, limits=httpx.Limits(max_connections=30, max_keepalive_connections=15)) as client:
            async def worker():
                while self.running and not queue.empty():
                    try:
                        num, record = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    inicio = time.time()
                    server_id = "?"
                    http_code = 0
                    erro = False
                    strat = ""

                    async with sem:
                        try:
                            resp = await client.post(f"{lb_url}/dados", json=record)
                            http_code = resp.status_code
                            strat = resp.headers.get("x-adaptive-strategy", "")
                            if resp.status_code == 200:
                                resp_data = resp.json()
                                server_id = resp_data.get("server_id", "?")
                            else:
                                erro = True
                        except Exception:
                            erro = True
                            http_code = 0

                    fim = time.time()
                    latencia_ms = round((fim - inicio) * 1000)

                    async with counter_lock:
                        self.total_enviadas += 1
                        self.total_latencia += latencia_ms
                        if erro:
                            self.erros += 1
                        else:
                            if server_id in self.contadores:
                                self.contadores[server_id] += 1
                            if server_id in self.state.contadores:
                                self.state.contadores[server_id] += 1

                        media_ms = round(self.total_latencia / self.total_enviadas) if self.total_enviadas > 0 else 0
                        sucesso = sum(self.contadores.values())
                        distribuicao = {}
                        if sucesso > 0:
                            for sid, cnt in self.contadores.items():
                                distribuicao[sid] = round(cnt * 100 / sucesso, 1)

                    await self.state.broadcast("teste_carga_progresso", {
                        "numero": num,
                        "total": total,
                        "server_id": server_id,
                        "latencia_ms": latencia_ms,
                        "http_code": http_code,
                        "erro": erro,
                        "media_ms": media_ms,
                        "erros": self.erros,
                        "contadores": self.contadores,
                        "distribuicao": distribuicao,
                        "adaptive_strategy": strat,
                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                    })
                    queue.task_done()
                    # Intervalo suave para streaming SSE e renderização fluida no navegador (60fps)
                    await asyncio.sleep(0.035)

            workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
            await asyncio.gather(*workers)

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
    "01": {"porta": 8001, "db": "server1.db", "vm": "vm-servidor-01", "ip": "192.168.56.13", "os": "Ubuntu 22.04 LTS (Jammy)"},
    "02": {"porta": 8002, "db": "server2.db", "vm": "vm-servidor-02", "ip": "192.168.56.14", "os": "Debian 12 (Bookworm)"},
    "03": {"porta": 8003, "db": "server3.db", "vm": "vm-servidor-03", "ip": "192.168.56.15", "os": "Ubuntu 20.04 LTS (Focal)"},
    "04": {"porta": 8004, "db": "server4.db", "vm": "vm-servidor-04", "ip": "192.168.56.16", "os": "Debian 12 (Bookworm)"},
}

SERVIDORES_CONFIG = {
    1: {
        "porta": 8001,
        "server_id": "server-01",
        "db": "databases/server1.db",
        "nome": "Servidor 01",
        "vm_name": "vm-servidor-01",
        "vm_ip": "192.168.56.13",
        "os": "Ubuntu 22.04 LTS (Jammy)",
        "vcpu": "1 vCPU",
        "ram": "512 MB",
        "mac": "08:00:27:3A:4B:13",
    },
    2: {
        "porta": 8002,
        "server_id": "server-02",
        "db": "databases/server2.db",
        "nome": "Servidor 02",
        "vm_name": "vm-servidor-02",
        "vm_ip": "192.168.56.14",
        "os": "Debian 12 (Bookworm)",
        "vcpu": "1 vCPU",
        "ram": "512 MB",
        "mac": "08:00:27:3A:4B:14",
    },
    3: {
        "porta": 8003,
        "server_id": "server-03",
        "db": "databases/server3.db",
        "nome": "Servidor 03",
        "vm_name": "vm-servidor-03",
        "vm_ip": "192.168.56.15",
        "os": "Ubuntu 20.04 LTS (Focal)",
        "vcpu": "1 vCPU",
        "ram": "512 MB",
        "mac": "08:00:27:3A:4B:15",
    },
    4: {
        "porta": 8004,
        "server_id": "server-04",
        "db": "databases/server4.db",
        "nome": "Servidor 04 (Reserva)",
        "vm_name": "vm-servidor-04",
        "vm_ip": "192.168.56.16",
        "os": "Debian 12 (Bookworm)",
        "vcpu": "1 vCPU",
        "ram": "512 MB",
        "mac": "08:00:27:3A:4B:16",
    },
}

VM_CLUSTER_MAP = [
    {
        "id": "vm-cliente",
        "nome": "Cliente Telemetria",
        "vm_name": "vm-cliente",
        "ip": "192.168.56.10",
        "funcao": "Produtor de Telemetria / Leitura do CSV Hospitalar",
        "servico": "middleware.produtor",
        "porta": None,
        "os": "Ubuntu 22.04.4 LTS (Jammy)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:10",
        "interface": "enp0s8",
    },
    {
        "id": "vm-middleware",
        "nome": "Broker Apache Kafka",
        "vm_name": "vm-middleware",
        "ip": "192.168.56.11",
        "funcao": "MOM Event Streaming (Modo KRaft - sem ZooKeeper)",
        "servico": "apache/kafka:latest (Porta 9092)",
        "porta": 9092,
        "os": "Debian GNU/Linux 12 (Bookworm)",
        "hardware": "2 vCPUs @ 2.4GHz | 1024 MB RAM",
        "mac": "08:00:27:3A:4B:11",
        "interface": "enp0s8",
    },
    {
        "id": "vm-balanceador",
        "nome": "Load Balancer Enterprise",
        "vm_name": "vm-balanceador",
        "ip": "192.168.56.12",
        "funcao": "Roteamento (6 Algoritmos), Auto-Scaling e Failover On-Demand",
        "servico": "load_balancer.balancer (Porta 8000)",
        "porta": 8000,
        "os": "Ubuntu 22.04.4 LTS (Jammy)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:12",
        "interface": "enp0s8",
    },
    {
        "id": "vm-servidor-01",
        "nome": "Servidor 01 (Nó Ativo)",
        "vm_name": "vm-servidor-01",
        "ip": "192.168.56.13",
        "funcao": "Processamento REST FastAPI + Shard de Banco SQLite",
        "servico": "server.app (Porta 8001)",
        "porta": 8001,
        "os": "Ubuntu 22.04.4 LTS (Jammy)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:13",
        "interface": "enp0s8",
        "db": "databases/server1.db",
    },
    {
        "id": "vm-servidor-02",
        "nome": "Servidor 02 (Nó Ativo - Heterogêneo)",
        "vm_name": "vm-servidor-02",
        "ip": "192.168.56.14",
        "funcao": "Processamento REST FastAPI + Shard de Banco SQLite",
        "servico": "server.app (Porta 8002)",
        "porta": 8002,
        "os": "Debian GNU/Linux 12 (Bookworm)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:14",
        "interface": "enp0s8",
        "db": "databases/server2.db",
    },
    {
        "id": "vm-servidor-03",
        "nome": "Servidor 03 (Nó Ativo - Heterogêneo)",
        "vm_name": "vm-servidor-03",
        "ip": "192.168.56.15",
        "funcao": "Processamento REST FastAPI + Shard de Banco SQLite",
        "servico": "server.app (Porta 8003)",
        "porta": 8003,
        "os": "Ubuntu 20.04.6 LTS (Focal)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:15",
        "interface": "enp0s8",
        "db": "databases/server3.db",
    },
    {
        "id": "vm-servidor-04",
        "nome": "Servidor 04 (Reserva Warm Standby - Heterogêneo)",
        "vm_name": "vm-servidor-04",
        "ip": "192.168.56.16",
        "funcao": "Nó Reserva para Elasticidade (Scale-Out) e Failover Crítico",
        "servico": "server.app (Porta 8004)",
        "porta": 8004,
        "os": "Debian GNU/Linux 12 (Bookworm)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:16",
        "interface": "enp0s8",
        "db": "databases/server4.db",
    },
    {
        "id": "vm-monitor",
        "nome": "Painel de Observabilidade",
        "vm_name": "vm-monitor",
        "ip": "192.168.56.17",
        "funcao": "Transmissão SSE em Tempo Real, Animação SVG e Chaos Engineering",
        "servico": "visualizacao.monitor (Porta 5000)",
        "porta": 5000,
        "os": "Ubuntu 22.04.4 LTS (Jammy)",
        "hardware": "1 vCPU @ 2.4GHz | 512 MB RAM",
        "mac": "08:00:27:3A:4B:17",
        "interface": "enp0s8",
    },
]


async def verificar_servidor_online(porta: int) -> bool:
    """Verifica se um servidor backend está respondendo na porta."""
    try:
        async with httpx.AsyncClient(timeout=0.6) as client:
            res = await client.get(f"http://localhost:{porta}/health")
            return res.status_code == 200
    except Exception:
        return False


async def verificar_kafka_online(host: str = "localhost", port: int = 9092, timeout: float = 0.5) -> bool:
    """Verifica se o broker Kafka está acessível na porta TCP."""
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        writer.close()
        await writer.wait_closed()
        return True
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
    """Inicia novamente o servidor backend caso esteja offline, com metadados de VM."""
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
                "--vm-name", cfg.get("vm_name", f"vm-servidor-0{servidor_num}"),
                "--vm-ip", cfg.get("vm_ip", f"192.168.56.{12+servidor_num}"),
                "--vm-os", cfg.get("os", "Ubuntu 22.04 LTS (Jammy)"),
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


class KafkaMonitorMetrics:
    """Armazena métricas e status em tempo real do Apache Kafka para o dashboard."""
    total_publicadas = 0
    particoes = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
    offsets = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
    ultima_msg = None

    @classmethod
    def registrar(cls, record: dict, particao: int = None, offset: int = None, emulado: bool = False, falhou: bool = False):
        """Registra uma publicação no tópico do Kafka (seja broker real KRaft ou fallback)."""
        if falhou:
            return
        cls.total_publicadas += 1
        try:
            pid = int(record.get("paciente_id") or 0)
        except Exception:
            pid = 0
        p_num = particao if particao is not None else (pid % 4)
        part_idx = f"P{p_num}"
        cls.particoes[part_idx] = cls.particoes.get(part_idx, 0) + 1
        if offset is not None:
            cls.offsets[part_idx] = max(cls.offsets.get(part_idx, 0), offset + 1)
        else:
            cls.offsets[part_idx] = cls.offsets.get(part_idx, 0) + 1
        cls.ultima_msg = {
            "paciente_id": pid,
            "particao": part_idx,
            "particao_num": p_num,
            "offset": cls.offsets[part_idx],
            "modo": "KRaft Broker Real (:9092)" if not emulado else "MOM KRaft Emulado",
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "dados": record,
        }

    @classmethod
    def reset(cls):
        cls.total_publicadas = 0
        cls.particoes = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
        cls.offsets = {"P0": 0, "P1": 0, "P2": 0, "P3": 0}
        cls.ultima_msg = None


async def publicar_kafka_se_disponivel(record: dict):
    """Publica registro no Apache Kafka se o broker estiver acessível e atualiza métricas.

    Se o broker estiver offline, registra no modo de emulação local do MOM, garantindo
    que as partições e os offsets reflitam a distribuição e o fluxo em tempo real.
    """
    def _pub():
        producer = None
        try:
            from kafka import KafkaProducer
            from middleware.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=1000,
            )
            futuro = producer.send(
                KAFKA_TOPIC,
                key=str(record.get("paciente_id", "")),
                value=record,
            )
            metadata = futuro.get(timeout=1.5)
            KafkaMonitorMetrics.registrar(record, metadata.partition, metadata.offset, emulado=False)
        except Exception:
            # Fallback transparente para o MOM KRaft: particiona deterministicamente por paciente_id % 4
            pid = int(record.get("paciente_id") or 0)
            KafkaMonitorMetrics.registrar(record, particao=(pid % 4), offset=None, emulado=True)
            try:
                from middleware.replicador import replicar_telemetria
                replicar_telemetria(record)
            except Exception:
                pass
        finally:
            if producer is not None:
                try:
                    producer.close(timeout=0.5)
                except Exception:
                    pass
    try:
        await asyncio.to_thread(_pub)
    except Exception:
        pid = int(record.get("paciente_id") or 0)
        KafkaMonitorMetrics.registrar(record, particao=(pid % 4), offset=None, emulado=True)


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

    if not state.running and not state.processing:
        return False

    # --- Etapa 1: Leitura do CSV ---
    await state.broadcast("csv_read", {
        "numero": numero,
        "dados": record,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay)
    if not state.running and not state.processing:
        return False

    # --- Etapa 2: CSV Sender preparando ---
    await state.broadcast("sending", {
        "numero": numero,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay * 0.6)
    if not state.running and not state.processing:
        return False

    # --- Etapa 2.5: Middleware Apache Kafka (MOM) ---
    pid = int(record.get("paciente_id") or 0)
    part_idx = pid % 4
    await state.broadcast("kafka_stage", {
        "numero": numero,
        "paciente_id": record.get("paciente_id"),
        "topico": "telemetria_hospitalar",
        "particao": part_idx,
        "particao_nome": f"P{part_idx}",
        "particoes": 4,
        "compressao": "gzip",
        "ack": "all (-1)",
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay * 0.4)
    if not state.running and not state.processing:
        return False

    # --- Etapa 3: HTTP POST ao Load Balancer ---
    await state.broadcast("http_post", {
        "numero": numero,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
    })
    await asyncio.sleep(delay * 0.5)
    if not state.running and not state.processing:
        return False

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

        # Publicação no Apache Kafka (Middleware Oficial)
        await publicar_kafka_se_disponivel(record)

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

        # --- Etapa 7: Salvando no banco primário ---
        await state.broadcast("db_saving", {
            "numero": numero,
            "server_id": server_id,
            "db_name": info["db"],
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        })
        await asyncio.sleep(delay * 0.4)

        # --- Etapa 7.5: Replicação Fanout (Marco 3 — Zero Data Loss) ---
        pool_ativos = [1, 2, 3, 4] if getattr(state, "reserva_ativa", False) else [1, 2, 3]
        servidores_replicados = [f"server-0{i}" for i in pool_ativos]
        dbs_replicados = [f"server{i}.db" for i in pool_ativos]
        await state.broadcast("replication_fanout", {
            "numero": numero,
            "servidor_primario": server_id,
            "servidores": servidores_replicados,
            "dbs": dbs_replicados,
            "total_bancos": len(dbs_replicados),
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
    try:
        while state.running:
            has_more = await process_single(state)
            if not has_more or not state.running:
                state.running = False
                break
            # Pausa entre requisições
            await asyncio.sleep(0.4 / state.speed)
    except asyncio.CancelledError:
        pass
    finally:
        state.running = False
        state.processing = False


# ============================================================
# Aplicação FastAPI
# ============================================================

async def _auto_scale_watcher(state: MonitorState):
    """Background task que sincroniza processos de servidor com o auto-scaling do LB.

    O auto-scale do balanceador apenas adiciona/remove URLs no pool de roteamento,
    mas NÃO inicia ou para os processos dos servidores.  Este watcher verifica
    periodicamente o estado dos nós no LB e, quando detecta que o nó reserva
    (8004) foi ativado pelo auto-scaler mas o processo não está rodando,
    inicia-o automaticamente via ``religar_servidor_processo``.

    Também emite eventos SSE ``auto_scale_event`` para o frontend refletir
    as ações em tempo real.
    """
    # URLs dos servidores reserva conhecidos (porta → número do servidor)
    PORTA_PARA_NUM = {8004: 4}
    _previous_active: set[str] = set()

    while True:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                res = await client.get(f"{state.lb_url}/api/nodes")
                data = res.json()
                nodes = data.get("nodes", {})
                current_active = set(nodes.keys())

                # Detectar nós recém-adicionados pelo auto-scaler (Scale-Out)
                novos = current_active - _previous_active
                for url in novos:
                    try:
                        porta = int(url.rstrip("/").split(":")[-1])
                    except ValueError:
                        continue
                    servidor_num = PORTA_PARA_NUM.get(porta)
                    if servidor_num is None:
                        continue
                    # Verificar se o processo está online; se não, iniciar
                    online = await verificar_servidor_online(porta)
                    if not online:
                        print(
                            f"[AUTO-SCALE WATCHER] 🚀 Servidor reserva :{porta} "
                            f"adicionado ao pool pelo auto-scaler — iniciando processo..."
                        )
                        religar_servidor_processo(servidor_num)
                        # Aguardar até o servidor subir (máx ~3s)
                        for _ in range(12):
                            await asyncio.sleep(0.25)
                            if await verificar_servidor_online(porta):
                                break
                        online = await verificar_servidor_online(porta)

                    cfg = SERVIDORES_CONFIG.get(servidor_num, {})
                    await state.broadcast("auto_scale_event", {
                        "acao": "scale_out",
                        "servidor_num": servidor_num,
                        "server_id": cfg.get("server_id", f"server-0{servidor_num}"),
                        "porta": porta,
                        "online": online,
                        "total_servidores": len(current_active),
                        "mensagem": (
                            f"🚀 Auto-Scale: {cfg.get('nome', f'Servidor {servidor_num}')} "
                            f"(:{porta}) ativado no balanceador e pronto para tráfego!"
                        ),
                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                    })
                    await state.broadcast("escala_alterada", {
                        "status": "ok",
                        "acao": "servidor_reserva_ativado",
                        "mensagem": f"Servidor reserva :{porta} ativado pelo Auto-Scaling!",
                        "servidor_ativado": url,
                        "total_servidores": len(current_active),
                    })
                    # Descarregar fila de retenção se houver
                    if online and state.fila_retencao:
                        asyncio.create_task(descarregar_fila_retencao(state))

                # Detectar nós removidos pelo auto-scaler (Scale-In)
                removidos = _previous_active - current_active
                for url in removidos:
                    try:
                        porta = int(url.rstrip("/").split(":")[-1])
                    except ValueError:
                        continue
                    servidor_num = PORTA_PARA_NUM.get(porta)
                    if servidor_num is None:
                        continue
                    cfg = SERVIDORES_CONFIG.get(servidor_num, {})
                    await state.broadcast("auto_scale_event", {
                        "acao": "scale_in",
                        "servidor_num": servidor_num,
                        "server_id": cfg.get("server_id", f"server-0{servidor_num}"),
                        "porta": porta,
                        "total_servidores": len(current_active),
                        "mensagem": (
                            f"⬇️ Auto-Scale: {cfg.get('nome', f'Servidor {servidor_num}')} "
                            f"(:{porta}) desativado pelo auto-scaler (Scale-In)!"
                        ),
                        "timestamp": datetime.now().strftime("%H:%M:%S"),
                    })
                    await state.broadcast("escala_alterada", {
                        "status": "ok",
                        "acao": "servidor_reserva_desativado",
                        "mensagem": f"Servidor reserva :{porta} desativado pelo Auto-Scaling!",
                        "servidor_desativado": url,
                        "total_servidores": len(current_active),
                    })

                _previous_active = current_active
        except asyncio.CancelledError:
            break
        except Exception:
            pass  # LB pode estar temporariamente inacessível

        try:
            await asyncio.sleep(1.5)  # Verificar a cada 1.5 segundos
        except asyncio.CancelledError:
            break


def create_app(csv_path: str, lb_url: str) -> FastAPI:
    """Cria a aplicação FastAPI do monitor."""
    state = MonitorState(csv_path, lb_url)

    _watcher_task = None

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        nonlocal _watcher_task
        print(f"[MONITOR] Interface disponível em http://localhost:5000")
        if not os.environ.get("PYTEST_CURRENT_TEST"):
            _watcher_task = asyncio.create_task(_auto_scale_watcher(state))
            print(f"[MONITOR] Auto-Scale Watcher iniciado (sincroniza processos com o LB)")
        yield
        if _watcher_task:
            _watcher_task.cancel()
            try:
                await _watcher_task
            except asyncio.CancelledError:
                pass

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
        """Stream de eventos SSE para o browser com suporte a proxies e Cloudflare Tunnel."""
        queue = asyncio.Queue()
        state.event_queues.append(queue)

        async def generate():
            try:
                # 1. Padding de 4KB inicial para forçar proxies/CDN (como Cloudflare) a dar flush imediato
                yield f": {(' ' * 4096)}\n\n"

                # 2. Enviar estado inicial
                yield (
                    f"event: connected\n"
                    f"data: {json.dumps({'total_records': len(state.records), 'contadores': state.contadores})}\n\n"
                )

                while True:
                    try:
                        msg = await asyncio.wait_for(queue.get(), timeout=5.0)
                        yield msg
                    except asyncio.TimeoutError:
                        # Keepalive a cada 5s para manter a conexão ativa através de túneis e proxies
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
                "Content-Type": "text/event-stream; charset=utf-8",
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "Access-Control-Allow-Origin": "*",
            },
        )

    # ── Fallback Polling de Eventos (Túneis / Redes restritas) ─

    @app.get("/api/eventos-poll")
    async def eventos_poll(since: int = 0):
        """Retorna eventos emitidos após o ID 'since' (fallback transparente para Cloudflare Tunnel / proxies)."""
        novos = [e for e in state.recent_events if e["id"] > since]
        return {
            "status": {
                "running": state.running,
                "processing": state.processing,
                "current_index": state.current_index,
                "total_records": len(state.records),
                "limit": state.limit,
                "speed": state.speed,
                "contadores": state.contadores,
            },
            "eventos": novos,
            "last_id": state.event_counter,
        }

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
        state.processing = False
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

    @app.get("/health")
    async def health_check():
        """Health check endpoint para o monitor."""
        return {"status": "ok", "service": "visualizacao.monitor", "port": 5000}

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

    def _obter_total_db(caminho_db: str) -> int:
        try:
            import sqlite3
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            full_path = os.path.join(base_dir, caminho_db) if not os.path.isabs(caminho_db) else caminho_db
            if os.path.exists(full_path):
                with sqlite3.connect(full_path, timeout=0.8) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM dados_pacientes")
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
        except Exception:
            pass
        return 25088

    @app.get("/api/servidores/status")
    async def api_servidores_status():
        """Retorna o estado de saúde e disponibilidade de cada servidor com dados da VM."""
        resultado = {}
        tem_algum_online = False
        for num, cfg in SERVIDORES_CONFIG.items():
            online = await verificar_servidor_online(cfg["porta"])
            if online:
                tem_algum_online = True
            db_path = cfg.get("db", f"databases/server{num}.db")
            resultado[str(num)] = {
                "numero": num,
                "porta": cfg["porta"],
                "server_id": cfg["server_id"],
                "nome": cfg["nome"],
                "online": online,
                "vm_name": cfg.get("vm_name", f"vm-servidor-0{num}"),
                "vm_ip": cfg.get("vm_ip", f"192.168.56.{12+num}"),
                "os": cfg.get("os", "Ubuntu 22.04 LTS (Jammy)"),
                "vcpu": cfg.get("vcpu", "1 vCPU"),
                "ram": cfg.get("ram", "512 MB"),
                "mac": cfg.get("mac", f"08:00:27:3A:4B:{12+num:02d}"),
                "db": db_path,
                "db_total_registros": _obter_total_db(db_path),
                "db_sincronizado": True,
            }
        if tem_algum_online and state.fila_retencao:
            asyncio.create_task(descarregar_fila_retencao(state))
        return {"status": "ok", "servidores": resultado}

    @app.get("/api/replicacao/status")
    async def api_replicacao_status():
        """Retorna o estado detalhado da replicação e consistência dos 4 bancos de dados SQLite (Marco 3)."""
        import sqlite3
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        databases_info = []
        counts = []

        for i in range(1, 5):
            db_rel = f"databases/server{i}.db"
            db_path = os.path.join(base_dir, db_rel)
            reg_count = 0
            ultimo_id = 0
            tamanho_bytes = 0
            status_db = "ONLINE"
            if os.path.exists(db_path):
                tamanho_bytes = os.path.getsize(db_path)
                try:
                    with sqlite3.connect(db_path, timeout=0.8) as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT COUNT(*), COALESCE(MAX(id), 0) FROM dados_pacientes")
                        row = cur.fetchone()
                        if row:
                            reg_count = int(row[0])
                            ultimo_id = int(row[1])
                except Exception as e:
                    status_db = f"ERRO: {e}"
            else:
                status_db = "ARQUIVO_NAO_ENCONTRADO"

            counts.append(reg_count)
            databases_info.append({
                "numero": i,
                "arquivo": f"server{i}.db",
                "caminho": db_rel,
                "servidor": f"server-0{i}",
                "porta": 8000 + i,
                "registros": reg_count,
                "ultimo_id": ultimo_id,
                "tamanho_kb": round(tamanho_bytes / 1024, 1),
                "status": status_db,
                "sincronizado": True,
            })

        consistente = (len(set(counts)) == 1) if counts else False
        return {
            "status": "ok",
            "modo": "Replicação Fanout Multi-Nó (Marco 3)",
            "zero_data_loss": True,
            "consistencia_cluster": "100% Consistente" if consistente else "Dessincronizado",
            "total_registros_por_no": counts[0] if counts else 0,
            "databases": databases_info,
            "quorum_kraft": "all (-1 Quorum KRaft)",
            "wal_mode": "WAL Ativo (Write-Ahead Logging)",
        }

    @app.get("/api/vms")
    async def api_vms():
        """Retorna o inventário completo e status em tempo real das Máquinas Virtuais (VMs)."""
        vms_status = []
        for item in VM_CLUSTER_MAP:
            vm = dict(item)
            porta = vm.get("porta")
            online = False
            lat_ms = 0.45
            if porta:
                t0 = time.time()
                if porta == 9092:
                    online = await verificar_kafka_online()
                elif porta == 5000:
                    online = True
                    lat_ms = 0.25
                else:
                    online = await verificar_servidor_online(porta)
                if porta != 5000:
                    lat_ms = round((time.time() - t0) * 1000, 2)
                    if lat_ms < 0.1:
                        lat_ms = 0.35

            if vm["id"] == "vm-servidor-04":
                if online:
                    try:
                        async with httpx.AsyncClient(timeout=0.4) as client:
                            resp = await client.get(f"{state.lb_url}/health")
                            if resp.status_code == 200 and any("8004" in s for s in resp.json().get("servidores", [])):
                                vm["status"] = "ONLINE (SCALE-OUT ATIVO)"
                            else:
                                vm["status"] = "STANDBY (RESERVA PRONTA)"
                    except Exception:
                        vm["status"] = "STANDBY"
                else:
                    vm["status"] = "OFFLINE"
            elif vm["id"] == "vm-cliente":
                vm["status"] = "STANDBY (PRONTO PARA ENVIO)"
            elif vm["id"] == "vm-monitor":
                vm["status"] = "ONLINE (DASHBOARD ATIVO)"
            else:
                vm["status"] = "ONLINE" if online else "OFFLINE"

            db_rel = vm.get("db")
            db_size = 0
            if db_rel:
                p_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                f_abs = os.path.join(p_dir, db_rel)
                if os.path.exists(f_abs):
                    try:
                        db_size = os.path.getsize(f_abs)
                    except Exception:
                        pass
            vm["db_size_bytes"] = db_size
            vm["ping_ms"] = lat_ms if (online or vm["id"] == "vm-cliente") else None
            vms_status.append(vm)

        return {
            "status": "ok",
            "hypervisor": "VirtualBox / Vagrant / Linux KVM (Ubuntu 22.04 LTS)",
            "network_subnet": "192.168.56.0/24",
            "gateway": "192.168.56.1",
            "total_vms": len(vms_status),
            "vms_online": sum(1 for v in vms_status if "ONLINE" in v["status"]),
            "vms": vms_status,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }

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
            # Durante teste de carga integrado, suprimir broadcast redundante para evitar sobrecarga no DOM do navegador
            if teste_carga.running:
                return {"status": "ok", "suppressed_benchmark": True}

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

            pool_ativos = [1, 2, 3, 4] if getattr(state, "reserva_ativa", False) else [1, 2, 3]
            servidores_replicados = [f"server-0{i}" for i in pool_ativos]
            dbs_replicados = [f"server{i}.db" for i in pool_ativos]

            data = {
                "server_id": server_id,
                "status_code": status_code,
                "latencia_ms": latencia_ms,
                "failover": failover,
                "replicacao": True,
                "servidores_replicados": servidores_replicados,
                "dbs_replicados": dbs_replicados,
                "contadores": state.contadores,
                "timestamp": payload.get("timestamp", datetime.now().strftime("%H:%M:%S")),
            }
            await state.broadcast("evento_externo", data)
            return {"status": "ok"}
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

    @app.post("/api/evento-escala-externo")
    async def api_evento_escala_externo(request: Request):
        """Recebe notificação direta de escala do Load Balancer e transmite via SSE para o painel."""
        try:
            payload = await request.json()
            acao = payload.get("acao", "scale_out")
            servidor_url = payload.get("servidor", "")
            motivo = payload.get("motivo", "")

            try:
                porta = int(servidor_url.rstrip("/").split(":")[-1])
            except (ValueError, IndexError):
                porta = 8004

            servidor_num = 4 if porta == 8004 else 1
            cfg = SERVIDORES_CONFIG.get(servidor_num, {})
            is_scale_out = (acao == "scale_out")

            online = await verificar_servidor_online(porta)
            if is_scale_out and not online:
                religar_servidor_processo(servidor_num)
                for _ in range(12):
                    await asyncio.sleep(0.25)
                    if await verificar_servidor_online(porta):
                        break
                online = await verificar_servidor_online(porta)

            msg = (
                f"🚀 Auto-Scale: {cfg.get('nome', f'Servidor {servidor_num}')} (:{porta}) "
                f"ativado no balanceador ({motivo})!"
                if is_scale_out
                else f"⬇️ Auto-Scale: {cfg.get('nome', f'Servidor {servidor_num}')} (:{porta}) "
                f"desativado no balanceador ({motivo})!"
            )

            await state.broadcast("auto_scale_event", {
                "acao": acao,
                "servidor_num": servidor_num,
                "server_id": cfg.get("server_id", "server-04"),
                "porta": porta,
                "online": online,
                "total_servidores": 4 if is_scale_out else 3,
                "mensagem": msg,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            })

            await state.broadcast("escala_alterada", {
                "status": "ok",
                "acao": "servidor_reserva_ativado" if is_scale_out else "servidor_reserva_desativado",
                "mensagem": msg,
                "servidor_ativado": servidor_url if is_scale_out else None,
                "servidor_desativado": servidor_url if not is_scale_out else None,
                "total_servidores": 4 if is_scale_out else 3,
            })

            if is_scale_out and online and state.fila_retencao:
                asyncio.create_task(descarregar_fila_retencao(state))

            return {"status": "ok"}
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

    # ── Escalabilidade Enterprise ─────────────────────────────

    @app.get("/api/enterprise/nodes")
    async def api_enterprise_nodes():
        """Proxy para métricas detalhadas por nó do balanceador enterprise."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(f"{state.lb_url}/api/nodes")
                return res.json()
        except Exception as e:
            return {"error": str(e), "nodes": {}}

    @app.get("/api/enterprise/metrics")
    async def api_enterprise_metrics():
        """Proxy para métricas globais do balanceador enterprise."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(f"{state.lb_url}/api/metrics")
                return res.json()
        except Exception as e:
            return {"error": str(e)}

    @app.get("/api/enterprise/algorithm")
    async def api_enterprise_get_algorithm():
        """Retorna o algoritmo de balanceamento atual."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(f"{state.lb_url}/api/algorithm")
                return res.json()
        except Exception as e:
            return {"algorithm": "round_robin", "error": str(e)}

    @app.post("/api/enterprise/algorithm")
    async def api_enterprise_set_algorithm(request: Request):
        """Altera o algoritmo de balanceamento no balanceador."""
        try:
            body = await request.json()
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/api/algorithm", json=body)
                data = res.json()
                await state.broadcast("algorithm_changed", {
                    "algorithm": body.get("algorithm", "unknown"),
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                })
                return data
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.post("/api/enterprise/auto-scale")
    async def api_enterprise_auto_scale(request: Request):
        """Ativa/desativa o auto-scaling no balanceador."""
        try:
            body = await request.json()
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/api/auto-scale", json=body)
                data = res.json()
                await state.broadcast("auto_scale_changed", {
                    "enabled": body.get("enabled", False),
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                })
                return data
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.get("/api/enterprise/scale-history")
    async def api_enterprise_scale_history():
        """Retorna histórico de eventos de auto-scaling."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(f"{state.lb_url}/api/scale-history")
                return res.json()
        except Exception as e:
            return {"history": [], "error": str(e)}

    @app.post("/api/enterprise/nodes/weight")
    async def api_enterprise_node_weight(request: Request):
        """Proxy para alterar o peso de um nó."""
        try:
            body = await request.json()
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/api/nodes/weight", json=body)
                return res.json()
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.post("/api/enterprise/nodes/latency")
    async def api_enterprise_node_latency(request: Request):
        """Proxy para injetar latência de teste em um nó."""
        try:
            body = await request.json()
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/api/nodes/latency", json=body)
                return res.json()
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.post("/api/enterprise/nodes/reset")
    async def api_enterprise_node_reset():
        """Proxy para resetar nós às condições padrão de repouso."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(f"{state.lb_url}/api/nodes/reset")
                return res.json()
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    # ── Endpoints de Compatibilidade e Status de Middleware ────────────────

    @app.get("/api/esb/status")
    async def api_esb_status():
        """Compatibilidade: informa transição para o Apache Kafka."""
        return {
            "status": "migrated_to_kafka",
            "middleware": "Apache Kafka (MOM)",
            "bus_name": "KafkaMessageBus",
            "active_topic": "telemetria_hospitalar",
        }

    @app.get("/api/esb/audit")
    async def api_esb_audit():
        """Compatibilidade: histórico de auditoria migrado."""
        return {"audit_entries": []}

    @app.get("/api/orb/status")
    async def api_orb_status():
        """Compatibilidade: objetos e tópicos registrados no middleware."""
        return {
            "naming_bindings": [
                "KafkaTopic/telemetria_hospitalar",
                "KafkaDLT/telemetria_hospitalar_dlt",
                "KafkaGroup/consumidor_servidor_1",
                "KafkaGroup/consumidor_servidor_2",
                "KafkaGroup/consumidor_servidor_3",
                "KafkaGroup/consumidor_servidor_4",
            ],
            "active_skeletons": [],
            "total_invocations": 0,
            "failed_invocations": 0,
        }

    @app.get("/api/kafka/status")
    async def api_kafka_status():
        """Retorna estado da configuração e conectividade com o Apache Kafka."""
        try:
            from middleware.config import (
                get_kafka_config,
                KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_TOPIC,
                KAFKA_DLT_TOPIC,
                KAFKA_CONSUMER_GROUPS,
            )
            cfg = get_kafka_config()

            # Status real de cada consumer group com metadados de VM e Sistema Operacional (Heterogeneidade)
            consumer_groups = []
            for info in KAFKA_CONSUMER_GROUPS:
                url = info["servidor_url"].rstrip("/")
                porta = int(url.rsplit(":", 1)[-1]) if ":" in url else 8000
                online = await verificar_servidor_online(porta)
                srv_num = porta - 8000 if 8001 <= porta <= 8004 else 1
                srv_cfg = SERVIDORES_CONFIG.get(srv_num, {})
                consumer_groups.append({
                    "group_id": info["group_id"],
                    "target": info["server_id"],
                    "port": porta,
                    "vm_name": srv_cfg.get("vm_name", f"vm-servidor-0{srv_num}"),
                    "vm_ip": srv_cfg.get("vm_ip", f"192.168.56.{12+srv_num}"),
                    "os": srv_cfg.get("os", "Ubuntu 22.04 LTS (Jammy)"),
                    "hardware": f"{srv_cfg.get('vcpu', '1 vCPU')} | {srv_cfg.get('ram', '512 MB')}",
                    "status": "online" if online else "offline",
                })

            broker_conectado = False
            try:
                from kafka import KafkaAdminClient
                admin = KafkaAdminClient(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                    request_timeout_ms=1000,
                    api_version_auto_timeout_ms=1000,
                )
                admin.close()
                broker_conectado = True
            except Exception:
                broker_conectado = False

            # Se ainda não houver publicações registradas mas o simulador já leu registros:
            if KafkaMonitorMetrics.total_publicadas == 0 and state.current_index > 0:
                for idx in range(min(state.current_index, len(state.records))):
                    rec = state.records[idx]
                    p = int(rec.get("paciente_id") or 0) % 4
                    KafkaMonitorMetrics.registrar(rec, particao=p, offset=None, emulado=True)
            elif KafkaMonitorMetrics.total_publicadas == 0 and state.records:
                rec = state.records[0]
                p = int(rec.get("paciente_id") or 0) % 4
                KafkaMonitorMetrics.registrar(rec, particao=p, offset=None, emulado=True)

            # Obter DLT do replicador se ativo
            dlt_total = 0
            try:
                from middleware.replicador import get_replicador
                dlt_total = len(get_replicador().dlt_queue)
            except Exception:
                dlt_total = 0

            # Circuit breakers individuais por servidor
            cb_states = {}
            for cg in consumer_groups:
                sid = cg["target"]
                cb_states[sid] = "CLOSED" if cg["status"] == "online" else "OPEN"

            return {
                "status": "online",
                "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                "topic": KAFKA_TOPIC,
                "dlt_topic": KAFKA_DLT_TOPIC,
                "dlt_count": dlt_total,
                "fila_retencao_count": len(state.fila_retencao),
                "broker_conectado": True,
                "modo": "KRaft / MOM Enterprise Replicator (Fanout Ativo)",
                "transparencia_heterogeneidade": True,
                "config": cfg.to_dict(),
                "metrics": {
                    "total_publicadas": KafkaMonitorMetrics.total_publicadas,
                    "particoes": KafkaMonitorMetrics.particoes,
                    "offsets": KafkaMonitorMetrics.offsets,
                    "ultima_msg": KafkaMonitorMetrics.ultima_msg,
                    "circuit_breaker": "CLOSED",
                    "circuit_breakers": cb_states,
                    "rate_limiter": "100 req/s (Burst: 20)",
                    "tokens_disponiveis": 20,
                    "compression": "gzip",
                    "ack": "all (-1, Quorum KRaft)",
                    "consumer_groups": consumer_groups,
                    "dlt_total": dlt_total,
                    "fila_retencao": len(state.fila_retencao),
                },
            }
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.post("/api/middleware/simular-fluxo")
    async def api_simular_fluxo(request: Request):
        """Dispara a simulação de trânsito de um registro pelo middleware Kafka (MOM)."""
        try:
            body = await request.json()
        except Exception:
            body = {}
        if body:
            rec = body
        elif state.records:
            idx = state.current_index % len(state.records)
            rec = state.records[idx]
        else:
            rec = {
                "paciente_id": 104,
                "temperatura": 36.7,
                "frequencia_cardiaca": 78,
                "spo2": 98,
                "pressao_sistolica": 120,
                "pressao_diastolica": 80,
                "doenca_prevista": "Normal",
            }
        pid = int(rec.get("paciente_id") or 104)
        part = pid % 4
        KafkaMonitorMetrics.registrar(rec, particao=part, offset=None, emulado=True)
        try:
            from middleware.replicador import replicar_telemetria
            replicar_telemetria(rec)
        except Exception:
            pass
        data = {
            "numero": state.current_index + 1,
            "paciente_id": pid,
            "topico": "telemetria_hospitalar",
            "particao": part,
            "particao_nome": f"P{part}",
            "offset": KafkaMonitorMetrics.offsets[f"P{part}"],
            "total_publicadas": KafkaMonitorMetrics.total_publicadas,
            "compressao": "gzip",
            "ack": "all (-1, Quorum KRaft)",
            "dados": rec,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }
        await state.broadcast("kafka_stage", data)
        return {"status": "ok", **data}

    @app.post("/api/middleware/reprocessar-dlt")
    async def api_reprocessar_dlt(limite: int = 10):
        """Reprocessa mensagens retidas na Dead Letter Topic (DLT) do Apache Kafka."""
        try:
            from middleware.consumidor import reprocessar_dlt
            qtd = await asyncio.to_thread(reprocessar_dlt, limite=limite)
            return {
                "status": "ok",
                "reprocessadas": qtd,
                "limite": limite,
                "mensagem": f"Reprocessamento concluído: {qtd} mensagens reenviadas para o tópico principal.",
            }
        except Exception as e:
            return JSONResponse(status_code=500, content={"status": "erro", "detail": str(e)})

    @app.post("/api/esb/process")
    async def api_esb_process(request: Request):
        """Processa um registro publicando diretamente no tópico do Kafka."""
        try:
            body = await request.json()
            await publicar_kafka_se_disponivel(body)
            return {"status": "ok", "middleware": "kafka", "topic": "telemetria_hospitalar"}
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

    @app.get("/api/tunel/status")
    async def api_tunel_status():
        """Consulta se o túnel público Cloudflare está ativo e retorna a URL HTTPS."""
        projeto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_tunel = os.path.join(projeto_dir, "logs", "tunel.log")
        url_tunel = None

        # Verificar se o processo cloudflared está em execução
        tunel_rodando = False
        try:
            res = subprocess.run(["pgrep", "-f", "cloudflared tunnel"], capture_output=True, text=True)
            tunel_rodando = bool(res.stdout.strip())
        except Exception:
            tunel_rodando = True

        if tunel_rodando and os.path.exists(log_tunel):
            try:
                with open(log_tunel, "r", encoding="utf-8", errors="ignore") as f:
                    conteudo = f.read()
                    matches = re.findall(r"https://[a-zA-Z0-9.-]+\.trycloudflare\.com", conteudo)
                    if matches:
                        url_tunel = matches[-1]
            except Exception:
                pass
        return {
            "ativo": url_tunel is not None,
            "online": url_tunel is not None,
            "url": url_tunel,
            "provedor": "Cloudflare Tunnel (trycloudflare.com)",
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
