# load_balancer/balancer.py
"""
Load Balancer Enterprise — Balanceamento Inteligente e Escalável.

Versão Enterprise com:
- 5 Algoritmos: Round Robin, Weighted RR, Least Connections, Least Response Time, Hash
- Health-aware routing (só envia para nós saudáveis)
- Circuit Breaker por nó (isolamento automático)
- Métricas por nó (latência, throughput, erros, conexões)
- Sticky sessions por paciente_id
- Retry com backoff
- API de gestão: /api/nodes, /api/metrics, /api/algorithm, /api/scaling
- Auto-scaling integrado

Uso:
    python load_balancer/balancer.py
    python load_balancer/balancer.py --port 8000 --algorithm least_response_time
"""

import argparse
import asyncio
import sys
import os
import time
import threading
import statistics
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
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


# ============================================================
# Modelos
# ============================================================

class AlgorithmType(str, Enum):
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LEAST_RESPONSE_TIME = "least_response_time"
    PATIENT_HASH = "patient_hash"
    ADAPTIVE = "adaptive"


class NodeStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ServidorPayload(BaseModel):
    url: str


class AlgorithmPayload(BaseModel):
    algorithm: str


# ============================================================
# Métricas por Nó
# ============================================================

class NodeMetrics:
    """Métricas de performance de um nó backend."""

    def __init__(self, url: str):
        self.url = url
        self._lock = threading.RLock()

        # Contadores
        self.total_requests = 0
        self.total_success = 0
        self.total_errors = 0
        self.active_connections = 0

        # Latências (janela deslizante)
        self._latencies: list[float] = []
        self._max_latencies = 500
        self._timestamps: list[float] = []

        # Health
        self.status = NodeStatus.HEALTHY
        self.consecutive_failures = 0
        self.last_success_time = time.time()
        self.last_failure_time = 0
        self.last_check_time = 0
        self.last_health_response_ms = 0

        # Circuit breaker
        self.circuit_state = CircuitState.CLOSED
        self.circuit_failure_count = 0
        self.circuit_last_trip = 0
        self.circuit_half_open_attempts = 0

        # Weight (para Weighted Round Robin)
        self.weight = 100
        self.effective_weight = 100
        self.current_weight = 0

    def record_request(self):
        with self._lock:
            self.total_requests += 1
            self.active_connections += 1
            self._timestamps.append(time.time())

    def record_success(self, latency_ms: float):
        with self._lock:
            self.total_success += 1
            self.active_connections = max(0, self.active_connections - 1)
            self.consecutive_failures = 0
            self.last_success_time = time.time()

            self._latencies.append(latency_ms)
            if len(self._latencies) > self._max_latencies:
                self._latencies.pop(0)

            # Circuit breaker recovery
            if self.circuit_state == CircuitState.HALF_OPEN:
                self.circuit_half_open_attempts += 1
                if self.circuit_half_open_attempts >= 3:
                    self.circuit_state = CircuitState.CLOSED
                    self.circuit_failure_count = 0
                    self.status = NodeStatus.HEALTHY

            # Adjust effective weight up
            if self.effective_weight < self.weight:
                self.effective_weight = min(self.weight, self.effective_weight + 5)

    def record_failure(self):
        with self._lock:
            self.total_errors += 1
            self.active_connections = max(0, self.active_connections - 1)
            self.consecutive_failures += 1
            self.last_failure_time = time.time()

            # Adjust effective weight down
            self.effective_weight = max(1, self.effective_weight - 10)

            # Circuit breaker
            self.circuit_failure_count += 1
            if self.circuit_failure_count >= 5:
                self.circuit_state = CircuitState.OPEN
                self.circuit_last_trip = time.time()
                self.status = NodeStatus.UNHEALTHY

            elif self.consecutive_failures >= 3:
                self.status = NodeStatus.DEGRADED

            if self.circuit_state == CircuitState.HALF_OPEN:
                self.circuit_state = CircuitState.OPEN
                self.circuit_last_trip = time.time()
                self.circuit_half_open_attempts = 0

    def check_circuit(self) -> bool:
        """Verifica se requisições são permitidas pelo circuit breaker."""
        with self._lock:
            if self.circuit_state == CircuitState.CLOSED:
                return True
            elif self.circuit_state == CircuitState.OPEN:
                if time.time() - self.circuit_last_trip > 10:
                    self.circuit_state = CircuitState.HALF_OPEN
                    self.circuit_half_open_attempts = 0
                    return True
                return False
            elif self.circuit_state == CircuitState.HALF_OPEN:
                return self.circuit_half_open_attempts < 3
        return False

    def record_health_check(self, success: bool, response_ms: float = 0):
        with self._lock:
            self.last_check_time = time.time()
            self.last_health_response_ms = response_ms
            if success:
                self.consecutive_failures = 0
                # Auto-recuperação do Circuit Breaker quando o nó volta a responder ao health check
                if self.circuit_state == CircuitState.OPEN:
                    if time.time() - self.circuit_last_trip > 10:
                        self.circuit_state = CircuitState.HALF_OPEN
                        self.circuit_half_open_attempts = 1
                        self.status = NodeStatus.DEGRADED
                elif self.circuit_state == CircuitState.HALF_OPEN:
                    self.circuit_half_open_attempts += 1
                    if self.circuit_half_open_attempts >= 2:
                        self.circuit_state = CircuitState.CLOSED
                        self.circuit_failure_count = 0
                        self.status = NodeStatus.HEALTHY
                elif self.circuit_state == CircuitState.CLOSED:
                    self.status = NodeStatus.HEALTHY
                    self.circuit_failure_count = 0
            else:
                self.consecutive_failures += 1
                if self.consecutive_failures >= 3:
                    self.status = NodeStatus.UNHEALTHY

    @property
    def avg_latency_ms(self) -> float:
        with self._lock:
            return statistics.mean(self._latencies) if self._latencies else 0

    @property
    def p95_latency_ms(self) -> float:
        with self._lock:
            if not self._latencies:
                return 0
            s = sorted(self._latencies)
            return s[int(len(s) * 0.95)]

    @property
    def p99_latency_ms(self) -> float:
        with self._lock:
            if not self._latencies:
                return 0
            s = sorted(self._latencies)
            return s[int(len(s) * 0.99)]

    @property
    def tps(self) -> float:
        """Throughput (requests/second) nos últimos 60s."""
        with self._lock:
            now = time.time()
            recent = [t for t in self._timestamps if t > now - 60]
            self._timestamps = recent
            if len(recent) < 2:
                return 0
            duration = max(recent[-1] - recent[0], 1.0)
            return len(recent) / duration

    @property
    def error_rate(self) -> float:
        total = self.total_success + self.total_errors
        return self.total_errors / max(total, 1)

    @property
    def is_available(self) -> bool:
        with self._lock:
            if self.circuit_state == CircuitState.OPEN:
                if not self.check_circuit():
                    return False
            return self.status in (NodeStatus.HEALTHY, NodeStatus.DEGRADED) or self.circuit_state == CircuitState.HALF_OPEN

    def to_dict(self) -> dict:
        with self._lock:
            total = self.total_success + self.total_errors
            return {
                "url": self.url,
                "status": self.status.value,
                "circuit_breaker": self.circuit_state.value,
                "active_connections": self.active_connections,
                "total_requests": self.total_requests,
                "total_success": self.total_success,
                "total_errors": self.total_errors,
                "error_rate_pct": round(self.error_rate * 100, 2),
                "avg_latency_ms": round(self.avg_latency_ms, 1),
                "p95_latency_ms": round(self.p95_latency_ms, 1),
                "p99_latency_ms": round(self.p99_latency_ms, 1),
                "tps": round(self.tps, 2),
                "weight": self.weight,
                "effective_weight": self.effective_weight,
                "consecutive_failures": self.consecutive_failures,
                "last_success": datetime.fromtimestamp(self.last_success_time).isoformat() if self.last_success_time else None,
                "health_response_ms": round(self.last_health_response_ms, 1),
            }


# ============================================================
# Load Balancer Enterprise
# ============================================================

class LoadBalancerEnterprise:
    """
    Balanceador de carga enterprise com múltiplos algoritmos,
    health-aware routing e auto-scaling.
    """

    def __init__(
        self,
        servidores: list[str],
        servidores_reserva: list[str] = None,
        algorithm: str = "round_robin",
    ):
        self.servidores_reserva = [s.rstrip("/") for s in (servidores_reserva or [])]
        self.servidores_reserva_conhecidos = list(self.servidores_reserva)
        self.algorithm = AlgorithmType(algorithm)

        # Métricas por nó
        self.nodes: dict[str, NodeMetrics] = {}
        for url in servidores:
            url = url.rstrip("/")
            self.nodes[url] = NodeMetrics(url)

        # Round robin index
        self._rr_index = 0
        self._lock = threading.RLock()

        # Global metrics
        self.total_requisicoes = 0
        self.total_failovers = 0
        self._start_time = time.time()

        # Health check thread
        self._health_running = False
        self._health_thread = None

        # Auto-scaler
        self.auto_scale_enabled = False
        self.min_nodes = 2
        self.max_nodes = 8
        self.scale_out_threshold_latency = 500  # ms
        self.scale_out_threshold_tps = 50       # per node
        self.scale_in_threshold_utilization = 0.2
        self.cooldown_seconds = 60
        self._last_scale_action = 0
        self._scale_history: list[dict] = []
        self._adaptive_last_decision = "round_robin"
        self.on_scale_change: list = []
        self.replicacao_ativa = True

    def _notify_scale(self, action: str, server: str, reason: str):
        for cb in list(self.on_scale_change):
            try:
                cb(action, server, reason)
            except Exception:
                pass

    @property
    def servidores(self) -> list[str]:
        """Lista de servidores ativos (compatibilidade)."""
        return list(self.nodes.keys())

    @property
    def active_nodes(self) -> list[str]:
        """Nós disponíveis para receber tráfego."""
        return [url for url, m in self.nodes.items() if m.is_available]

    def _select_round_robin(self, available: list[str]) -> str:
        with self._lock:
            self._rr_index = self._rr_index % len(available)
            selected = available[self._rr_index]
            self._rr_index = (self._rr_index + 1) % len(available)
            return selected

    def _select_weighted_round_robin(self, available: list[str]) -> str:
        """Weighted Round Robin (Nginx-style smooth)."""
        with self._lock:
            total_weight = sum(self.nodes[u].effective_weight for u in available)
            best = None
            best_url = available[0]

            for url in available:
                node = self.nodes[url]
                node.current_weight += node.effective_weight
                if best is None or node.current_weight > best:
                    best = node.current_weight
                    best_url = url

            self.nodes[best_url].current_weight -= total_weight
            return best_url

    def _select_least_connections(self, available: list[str]) -> str:
        """Seleciona o nó com menos conexões ativas com desempate Round Robin."""
        with self._lock:
            min_conns = min(self.nodes[u].active_connections for u in available)
            candidates = [u for u in available if self.nodes[u].active_connections == min_conns]
            if len(candidates) == 1:
                return candidates[0]
            return self._select_round_robin(candidates)

    def _select_least_response_time(self, available: list[str]) -> str:
        """Seleciona o nó com menor latência média com desempate Round Robin."""
        with self._lock:
            min_lat = min(self.nodes[u].avg_latency_ms for u in available)
            candidates = [u for u in available if abs(self.nodes[u].avg_latency_ms - min_lat) <= 2.0]
            if len(candidates) == 1:
                return candidates[0]
            return self._select_round_robin(candidates)

    def _select_patient_hash(self, available: list[str], patient_id: int = 0) -> str:
        """Hash por paciente_id para sticky sessions."""
        idx = patient_id % len(available)
        return available[idx]

    def _select_adaptive(self, available: list[str], patient_id: int = 0) -> str:
        """
        Balanceamento Adaptativo (Adaptive Load Balancing):
        Inspeciona em tempo real as condições de carga e latência do cluster
        e escolhe dinamicamente o melhor algoritmo de distribuição:
        1. Se houver discrepância de latência entre nós (spread >= 5ms e nó mais lento >= 8ms):
           -> usa least_response_time para proteger o nó degradado
        2. Se houver conexões ativas desbalanceadas OU throughput desigual:
           -> usa least_connections para redistribuir a carga
        3. Se os nós tiverem pesos heterogêneos explícitos:
           -> usa weighted_round_robin
        4. Em condições normais:
           -> usa round_robin (alta eficiência e custo O(1))

        Os thresholds são calibrados para ambientes locais onde latências são
        baixas (< 50ms) e conexões drenam rapidamente.
        """
        # --- Heurística 1: Latência desigual entre nós ---
        # Exige que todos os nós ativos tenham ao menos 1 medição para evitar falso positivo na primeira requisição
        has_samples = all(len(self.nodes[u]._latencies) > 0 for u in available)
        if has_samples:
            latencies = [self.nodes[u].avg_latency_ms for u in available]
            max_lat = max(latencies) if latencies else 0.0
            min_lat = min(latencies) if latencies else 0.0
            lat_spread = max_lat - min_lat

            # Threshold sensível: se um nó está 8ms mais lento e já acima de 12ms
            if lat_spread >= 8.0 and max_lat >= 12.0:
                self._adaptive_last_decision = "least_response_time"
                return self._select_least_response_time(available)

        # --- Heurística 2: Conexões ativas desbalanceadas ou carga concorrente ---
        conns = [self.nodes[u].active_connections for u in available]
        max_conn = max(conns) if conns else 0
        min_conn = min(conns) if conns else 0
        conn_spread = max_conn - min_conn

        if (max_conn >= 1 and conn_spread >= 1) or sum(conns) >= 2:
            self._adaptive_last_decision = "least_connections"
            return self._select_least_connections(available)

        # --- Heurística 2b: Throughput desigual (requests acumulados) ---
        # Mesmo que as conexões drenem rápido, se um nó recebeu muito mais
        # requests que outro, indica desbalanceamento de carga
        requests = [self.nodes[u].total_requests for u in available]
        if requests and max(requests) > 0:
            max_req = max(requests)
            min_req = min(requests)
            req_spread = max_req - min_req
            # Se a diferença de requests entre nós for > 20% do total do nó
            # mais carregado e pelo menos 10 requests de diferença
            if req_spread >= 10 and max_req > 0 and (req_spread / max_req) > 0.20:
                self._adaptive_last_decision = "least_connections"
                return self._select_least_connections(available)

        # --- Heurística 3: Pesos heterogêneos ---
        weights = [self.nodes[u].weight for u in available]
        if len(set(weights)) > 1:
            self._adaptive_last_decision = "weighted_round_robin"
            return self._select_weighted_round_robin(available)

        # --- Fallback: Round Robin ---
        self._adaptive_last_decision = "round_robin"
        return self._select_round_robin(available)

    def proximo_servidor(self, patient_id: int = 0) -> str:
        """
        Seleciona o próximo servidor usando o algoritmo ativo.

        Args:
            patient_id: ID do paciente (para hash-based routing).

        Returns:
            URL do servidor selecionado.

        Raises:
            RuntimeError: Se nenhum servidor disponível.
        """
        available = self.active_nodes
        if not available:
            # Fallback: tentar todos os nós (mesmo degraded)
            available = list(self.nodes.keys())
            if not available:
                raise RuntimeError("Nenhum servidor ativo disponível no balanceador")

        self.total_requisicoes += 1

        if self.algorithm == AlgorithmType.ROUND_ROBIN:
            return self._select_round_robin(available)
        elif self.algorithm == AlgorithmType.WEIGHTED_ROUND_ROBIN:
            return self._select_weighted_round_robin(available)
        elif self.algorithm == AlgorithmType.LEAST_CONNECTIONS:
            return self._select_least_connections(available)
        elif self.algorithm == AlgorithmType.LEAST_RESPONSE_TIME:
            return self._select_least_response_time(available)
        elif self.algorithm == AlgorithmType.PATIENT_HASH:
            return self._select_patient_hash(available, patient_id)
        elif self.algorithm == AlgorithmType.ADAPTIVE:
            return self._select_adaptive(available, patient_id)
        else:
            return self._select_round_robin(available)

    def adicionar_servidor(self, url: str, weight: int = 100) -> bool:
        url = url.rstrip("/")
        if url not in self.nodes:
            self.nodes[url] = NodeMetrics(url)
            self.nodes[url].weight = weight
            self.nodes[url].effective_weight = weight
            if url in self.servidores_reserva:
                self.servidores_reserva.remove(url)
            return True
        return False

    def remover_servidor(self, url: str) -> bool:
        url = url.rstrip("/")
        if url in self.nodes:
            del self.nodes[url]
            if self._rr_index >= len(self.nodes):
                self._rr_index = 0
            return True
        return False

    def ativar_reserva(self, url: str = None) -> str | None:
        alvo = None
        if url:
            alvo = url.rstrip("/")
            if alvo in self.servidores_reserva:
                self.servidores_reserva.remove(alvo)
            if alvo not in self.nodes:
                self.nodes[alvo] = NodeMetrics(alvo)
            if alvo not in self.servidores_reserva_conhecidos:
                self.servidores_reserva_conhecidos.append(alvo)
        elif self.servidores_reserva:
            alvo = self.servidores_reserva.pop(0)
            if alvo not in self.nodes:
                self.nodes[alvo] = NodeMetrics(alvo)
            if alvo not in self.servidores_reserva_conhecidos:
                self.servidores_reserva_conhecidos.append(alvo)
        if alvo:
            self._notify_scale("scale_out", alvo, "ativacao_servidor_reserva")
        return alvo

    def desativar_reserva(self, url: str = None) -> str | None:
        if url:
            alvo = url.rstrip("/")
            if alvo in self.nodes:
                del self.nodes[alvo]
                if alvo not in self.servidores_reserva:
                    self.servidores_reserva.append(alvo)
                if self._rr_index >= len(self.nodes) and self.nodes:
                    self._rr_index = 0
                self._notify_scale("scale_in", alvo, "desativacao_servidor_reserva")
                return alvo
            return None

        alvo = None
        for s in reversed(list(self.nodes.keys())):
            if s in self.servidores_reserva_conhecidos or ":8004" in s:
                alvo = s
                break
        if not alvo and len(self.nodes) > 3:
            alvo = list(self.nodes.keys())[-1]

        if alvo and alvo in self.nodes:
            del self.nodes[alvo]
            if alvo not in self.servidores_reserva:
                self.servidores_reserva.append(alvo)
            if self._rr_index >= len(self.nodes) and self.nodes:
                self._rr_index = 0
            self._notify_scale("scale_in", alvo, "desativacao_servidor_reserva")
            return alvo
        return None

    # ── Auto-Scaling ──────────────────────────────────────

    def _reserva_ativa(self) -> str | None:
        """Retorna o nó reserva atualmente em operação, se houver.

        Usado no Scale-In para nunca desativar um nó principal.
        """
        for url in reversed(list(self.nodes.keys())):
            if url in self.servidores_reserva_conhecidos:
                return url
        return None

    def check_auto_scale(self):
        """Verifica se precisa escalar (chamado periodicamente)."""
        if not self.auto_scale_enabled:
            return

        now = time.time()
        active = self.active_nodes

        # ── Failover de emergência: nenhum nó saudável ──
        # Se todos os nós principais caírem, o reserva é a única forma de
        # manter o serviço de pé. Este caso precisa ser tratado antes do
        # critério de carga (latência/TPS), porque com o cluster inteiro
        # fora não há latência nem throughput medidos para disparar o
        # Scale-Out, e antes da checagem de cooldown, porque aqui o
        # objetivo é disponibilidade, não economia de recursos.
        if not active:
            alvo = self.ativar_reserva()
            if alvo:
                self._last_scale_action = now
                action = {
                    "action": "scale_out",
                    "trigger": "failover_sem_nos_saudaveis",
                    "server": alvo,
                    "reason": "Nenhum nó saudável no cluster",
                    "timestamp": datetime.now().isoformat(),
                    "nodes_before": 0,
                    "nodes_after": 1,
                }
                self._scale_history.append(action)
                print(
                    f"[AUTO-SCALE] 🚨 FAILOVER: nenhum nó saudável — "
                    f"servidor reserva {alvo} ativado!"
                )
            return

        if now - self._last_scale_action < self.cooldown_seconds:
            return

        # Métricas agregadas
        avg_latency = statistics.mean(self.nodes[u].avg_latency_ms for u in active) if active else 0
        avg_tps = statistics.mean(self.nodes[u].tps for u in active) if active else 0
        avg_connections = statistics.mean(self.nodes[u].active_connections for u in active) if active else 0
        total_tps = sum(self.nodes[u].tps for u in active)

        # Scale-Out: sobrecarga (latência/TPS) OU cluster abaixo do mínimo
        # de nós saudáveis (redundância perdida).
        abaixo_do_minimo = len(active) < self.min_nodes
        if (
            len(active) < self.max_nodes
            and self.servidores_reserva
            and (
                abaixo_do_minimo
                or avg_latency > self.scale_out_threshold_latency
                or avg_tps > self.scale_out_threshold_tps
            )
        ):
            alvo = self.ativar_reserva()
            if alvo:
                self._last_scale_action = now
                motivo = (
                    f"nodes_healthy={len(active)} < min_nodes={self.min_nodes}"
                    if abaixo_do_minimo
                    else f"latency={avg_latency:.0f}ms, tps_per_node={avg_tps:.0f}"
                )
                action = {
                    "action": "scale_out",
                    "trigger": "abaixo_do_minimo" if abaixo_do_minimo else "sobrecarga",
                    "server": alvo,
                    "reason": motivo,
                    "timestamp": datetime.now().isoformat(),
                    "nodes_before": len(active),
                    "nodes_after": len(active) + 1,
                }
                self._scale_history.append(action)
                print(f"[AUTO-SCALE] 🚀 SCALE-OUT: {alvo} ativado! ({action['reason']})")

        # Scale-In: só desativa um nó que seja reserva conhecida
        elif (
            len(active) > self.min_nodes
            and avg_latency < self.scale_out_threshold_latency * 0.3
            and avg_tps < self.scale_out_threshold_tps * 0.3
        ):
            alvo = self._reserva_ativa()
            if alvo:
                self.desativar_reserva(alvo)
                self._last_scale_action = now
                action = {
                    "action": "scale_in",
                    "trigger": "baixa_carga",
                    "server": alvo,
                    "reason": f"low_load: latency={avg_latency:.0f}ms, tps={avg_tps:.0f}",
                    "timestamp": datetime.now().isoformat(),
                    "nodes_before": len(active),
                    "nodes_after": len(active) - 1,
                }
                self._scale_history.append(action)
                print(f"[AUTO-SCALE] ⬇️ SCALE-IN: {alvo} desativado! ({action['reason']})")

    def get_global_metrics(self) -> dict:
        """Retorna métricas agregadas de todos os nós."""
        nodes = list(self.nodes.values())
        active = [n for n in nodes if n.is_available]

        total_requests = sum(n.total_requests for n in nodes)
        total_success = sum(n.total_success for n in nodes)
        total_errors = sum(n.total_errors for n in nodes)
        total_tps = sum(n.tps for n in nodes)

        latencies = [n.avg_latency_ms for n in active if n.avg_latency_ms > 0]
        avg_lat = statistics.mean(latencies) if latencies else 0
        p95_lats = [n.p95_latency_ms for n in active if n.p95_latency_ms > 0]
        avg_p95 = statistics.mean(p95_lats) if p95_lats else 0

        uptime = time.time() - self._start_time

        return {
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": round(uptime),
            "algorithm": self.algorithm.value,
            "adaptive_strategy": getattr(self, "_adaptive_last_decision", self.algorithm.value),
            "total_nodes": len(nodes),
            "active_nodes": len(active),
            "unhealthy_nodes": len([n for n in nodes if n.status == NodeStatus.UNHEALTHY]),
            "reserve_nodes": len(self.servidores_reserva),
            "total_requests": total_requests,
            "total_success": total_success,
            "total_errors": total_errors,
            "total_failovers": self.total_failovers,
            "error_rate_pct": round(total_errors / max(total_requests, 1) * 100, 2),
            "throughput_tps": round(total_tps, 2),
            "avg_latency_ms": round(avg_lat, 1),
            "avg_p95_latency_ms": round(avg_p95, 1),
            "auto_scale": {
                "enabled": self.auto_scale_enabled,
                "min_nodes": self.min_nodes,
                "max_nodes": self.max_nodes,
                "history": self._scale_history[-10:],
            },
        }


# Alias para retrocompatibilidade com marcos anteriores
RoundRobinBalancer = LoadBalancerEnterprise


# ============================================================
# Health Checker Background
# ============================================================

async def _health_check_loop(balancer: LoadBalancerEnterprise, client: httpx.AsyncClient):
    """Loop de health check em background."""
    while balancer._health_running:
        any_unhealthy = False
        for url, node in list(balancer.nodes.items()):
            try:
                start = time.time()
                resp = await client.get(f"{url}/health", timeout=3.0)
                elapsed = (time.time() - start) * 1000
                node.record_health_check(resp.status_code == 200, elapsed)
            except httpx.ConnectError:
                # Conexão recusada — o processo não está escutando na porta.
                # Marca imediatamente o nó como UNHEALTHY para failover instantâneo.
                node.consecutive_failures = 3
                node.status = NodeStatus.UNHEALTHY
                any_unhealthy = True
            except Exception:
                node.record_health_check(False)
                any_unhealthy = True

        # Auto-scaling check (roda toda iteração, é especialmente
        # importante quando nós acabaram de ser marcados unhealthy)
        balancer.check_auto_scale()

        # Quando há nós fora, checar com mais frequência (1.5s vs 3s)
        await asyncio.sleep(1.5 if any_unhealthy else 3)


# ============================================================
# Criar App FastAPI
# ============================================================

def criar_balanceador(
    servidores: list[str] = None,
    servidores_reserva: list[str] = None,
    port: int = 8000,
    algorithm: str = "round_robin",
    auto_scale: bool = True,
    replicacao: bool = True,
) -> FastAPI:
    if isinstance(servidores_reserva, int):
        port = servidores_reserva
        servidores_reserva = None
    elif isinstance(port, list):
        servidores_reserva = port
        port = 8000

    _servidores = servidores or SERVIDORES_PADRAO
    _reservas = servidores_reserva if servidores_reserva is not None else SERVIDORES_RESERVA_PADRAO
    balancer = LoadBalancerEnterprise(_servidores, _reservas, algorithm)
    balancer.auto_scale_enabled = auto_scale
    balancer.replicacao_ativa = replicacao

    client = httpx.AsyncClient(timeout=httpx.Timeout(connect=2.0, read=5.0, write=5.0, pool=5.0))

    async def _notificar_monitor_escala(acao: str, servidor_url: str, motivo: str):
        try:
            porta_monitor = os.environ.get("MONITOR_PORT", os.environ.get("PORT", "5000"))
            async with httpx.AsyncClient(timeout=0.5) as m_client:
                await m_client.post(
                    f"http://127.0.0.1:{porta_monitor}/api/evento-escala-externo",
                    json={
                        "acao": acao,
                        "servidor": servidor_url,
                        "motivo": motivo,
                    }
                )
        except Exception:
            pass

    def _on_scale(acao: str, servidor: str, motivo: str):
        try:
            asyncio.create_task(_notificar_monitor_escala(acao, servidor, motivo))
        except Exception:
            pass

    balancer.on_scale_change.append(_on_scale)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        balancer._health_running = True
        health_task = asyncio.create_task(_health_check_loop(balancer, client))

        print(f"[BALANCEADOR] Iniciado na porta {port} (algoritmo: {algorithm})")
        print(
            f"[BALANCEADOR] Auto-scaling: "
            f"{'ATIVO' if balancer.auto_scale_enabled else 'inativo'}"
        )
        print(f"[BALANCEADOR] Servidores ativos:")
        for i, s in enumerate(balancer.servidores):
            print(f"  [{i + 1}] {s}")
        if balancer.servidores_reserva:
            print(f"[BALANCEADOR] Servidores reserva:")
            for i, s in enumerate(balancer.servidores_reserva):
                print(f"  [R{i + 1}] {s}")
        print()
        yield
        balancer._health_running = False
        health_task.cancel()
        await client.aclose()

    app = FastAPI(
        title="Load Balancer Enterprise - Sistema Hospitalar",
        description="Balanceamento inteligente com 5 algoritmos, auto-scaling e health-aware routing",
        version="3.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    async def _notificar_monitor_background(servidor_url: str, status_code: int, latencia_ms: int, failover: bool, sid: str = None):
        try:
            porta_servidor = servidor_url.rstrip("/").split(":")[-1]
            mapa = {"8001": "server-01", "8002": "server-02", "8003": "server-03", "8004": "server-04"}
            server_id = sid or mapa.get(porta_servidor, "server-01")
            porta_monitor = os.environ.get("MONITOR_PORT", os.environ.get("PORT", "5000"))
            async with httpx.AsyncClient(timeout=0.25) as m_client:
                await m_client.post(
                    f"http://127.0.0.1:{porta_monitor}/api/evento-externo",
                    json={
                        "server_id": server_id,
                        "status_code": status_code,
                        "latencia_ms": latencia_ms,
                        "failover": failover,
                        "replicacao": getattr(balancer, "replicacao_ativa", True),
                    }
                )
        except Exception:
            pass

    # ── Encaminhamento ─────────────────────────────────────

    @app.post("/dados")
    async def encaminhar_post_dados(request: Request):
        if not balancer.nodes:
            return JSONResponse(status_code=503, content={"detail": "Nenhum servidor backend configurado"})

        t_inicio = time.time()
        body = await request.json()
        patient_id = body.get("paciente_id", 0) if isinstance(body, dict) else 0

        tentativas = len(balancer.nodes)
        falharam = []
        servidores_tentados = set()

        for tentativa in range(tentativas):
            try:
                servidor = balancer.proximo_servidor(patient_id)
            except RuntimeError:
                break

            if servidor in servidores_tentados:
                continue
            servidores_tentados.add(servidor)

            node = balancer.nodes.get(servidor)
            if node:
                node.record_request()

            print(f"[BALANCEADOR] POST /dados [{tentativa + 1}/{tentativas}] → {servidor} ({balancer.algorithm.value})")
            try:
                response = await client.post(f"{servidor}/dados", json=body)
                latencia_ms = round((time.time() - t_inicio) * 1000)
                resp_data = response.json()
                sid = resp_data.get("server_id") if isinstance(resp_data, dict) else None

                if node:
                    node.record_success(latencia_ms)

                asyncio.create_task(_notificar_monitor_background(servidor, response.status_code, latencia_ms, bool(falharam), sid))

                # Replicação Fanout (Marco 3 — Zero Data Loss):
                # Propaga a telemetria assincronamente para todos os outros nós ativos
                if getattr(balancer, "replicacao_ativa", True) and not request.headers.get("X-Replicated"):
                    async def _replicar_fanout_background(alvo_principal, dados):
                        outros = [u for u in balancer.nodes.keys() if u != alvo_principal]
                        if outros:
                            async with httpx.AsyncClient(timeout=3.0) as r_cli:
                                for u in outros:
                                    try:
                                        await r_cli.post(
                                            f"{u}/dados",
                                            json=dados,
                                            headers={"X-Replicated": "true"}
                                        )
                                    except Exception:
                                        pass
                    asyncio.create_task(_replicar_fanout_background(servidor, body))

                if falharam and isinstance(resp_data, dict):
                    resp_data["failover"] = True
                    resp_data["servidores_falharam"] = falharam
                    balancer.total_failovers += 1

                headers = {
                    "X-Failover": "true" if falharam else "false",
                    "X-Algorithm": balancer.algorithm.value,
                    "X-Adaptive-Strategy": getattr(balancer, "_adaptive_last_decision", balancer.algorithm.value),
                    "X-Node": servidor,
                    "X-Latency-Ms": str(latencia_ms),
                }
                return JSONResponse(status_code=response.status_code, content=resp_data, headers=headers)

            except (httpx.ConnectError, httpx.TimeoutException):
                print(f"[BALANCEADOR] AVISO: Falha em {servidor}. Failover...")
                if node:
                    node.record_failure()
                falharam.append(servidor)
                continue
            except Exception as e:
                if node:
                    node.record_failure()
                print(f"[BALANCEADOR] ERRO em {servidor}: {e}")
                return JSONResponse(status_code=500, content={"detail": str(e)})

        # Se todos os servidores configurados falharam, ativa o reserva on-demand
        if balancer.servidores_reserva:
            reserva = balancer.ativar_reserva()
            if reserva:
                balancer.total_failovers += 1
                node_reserva = balancer.nodes.get(reserva)
                if node_reserva:
                    node_reserva.record_request()
                try:
                    response = await client.post(f"{reserva}/dados", json=body)
                    latencia_ms = round((time.time() - t_inicio) * 1000)
                    resp_data = response.json()
                    sid = resp_data.get("server_id") if isinstance(resp_data, dict) else None
                    if node_reserva:
                        node_reserva.record_success(latencia_ms)
                    asyncio.create_task(_notificar_monitor_background(reserva, response.status_code, latencia_ms, True, sid))
                    if isinstance(resp_data, dict):
                        resp_data["failover"] = True
                        resp_data["servidores_falharam"] = falharam
                    headers = {
                        "X-Failover": "true",
                        "X-Algorithm": balancer.algorithm.value,
                        "X-Node": reserva,
                        "X-Latency-Ms": str(latencia_ms),
                    }
                    return JSONResponse(status_code=response.status_code, content=resp_data, headers=headers)
                except Exception:
                    pass

        return JSONResponse(status_code=502, content={"detail": "Todos os servidores backend falharam"})

    @app.get("/dados")
    async def encaminhar_get_dados(request: Request):
        if not balancer.nodes:
            return JSONResponse(status_code=503, content={"detail": "Nenhum servidor backend configurado"})

        t_inicio = time.time()
        query_string = str(request.url.query)
        tentativas = len(balancer.nodes)
        falharam = []

        for tentativa in range(tentativas):
            try:
                servidor = balancer.proximo_servidor()
            except RuntimeError:
                break

            url = f"{servidor}/dados"
            if query_string:
                url += f"?{query_string}"

            print(f"[BALANCEADOR] GET /dados [{tentativa + 1}/{tentativas}] → {servidor}")
            node = balancer.nodes.get(servidor)
            if node:
                node.record_request()
            try:
                response = await client.get(url)
                latencia_ms = round((time.time() - t_inicio) * 1000)
                if node:
                    node.record_success(latencia_ms)

                porta = servidor.rstrip("/").split(":")[-1]
                s_id = f"server-0{int(porta)-8000}" if porta.isdigit() and 8001 <= int(porta) <= 8004 else "server-01"

                resp_data = response.json()
                headers = {
                    "X-Failover": "true" if falharam else "false",
                    "X-Algorithm": balancer.algorithm.value,
                    "X-Adaptive-Strategy": getattr(balancer, "_adaptive_last_decision", balancer.algorithm.value),
                    "X-Node": servidor,
                    "X-Server-Id": s_id,
                    "X-Latency-Ms": str(latencia_ms),
                }
                return JSONResponse(status_code=response.status_code, content=resp_data, headers=headers)
            except (httpx.ConnectError, httpx.TimeoutException):
                if node:
                    node.record_failure()
                falharam.append(servidor)
                continue
            except Exception as e:
                if node:
                    node.record_failure()
                return JSONResponse(status_code=500, content={"detail": str(e)})

        if balancer.servidores_reserva:
            reserva = balancer.ativar_reserva()
            if reserva:
                url = f"{reserva}/dados"
                if query_string:
                    url += f"?{query_string}"
                try:
                    response = await client.get(url)
                    resp_data = response.json()
                    porta = reserva.rstrip("/").split(":")[-1]
                    s_id = f"server-0{int(porta)-8000}" if porta.isdigit() and 8001 <= int(porta) <= 8004 else "server-04"
                    headers = {
                        "X-Failover": "true",
                        "X-Algorithm": balancer.algorithm.value,
                        "X-Adaptive-Strategy": getattr(balancer, "_adaptive_last_decision", balancer.algorithm.value),
                        "X-Node": reserva,
                        "X-Server-Id": s_id,
                    }
                    return JSONResponse(status_code=response.status_code, content=resp_data, headers=headers)
                except Exception:
                    pass

        return JSONResponse(status_code=502, content={"detail": "Todos os servidores backend falharam"})

    @app.get("/dados/{paciente_id}")
    async def encaminhar_get_paciente(paciente_id: int):
        if not balancer.nodes:
            return JSONResponse(status_code=503, content={"detail": "Nenhum servidor backend configurado"})

        t_inicio = time.time()
        tentativas = len(balancer.nodes)
        falharam = []

        for tentativa in range(tentativas):
            try:
                servidor = balancer.proximo_servidor(paciente_id)
            except RuntimeError:
                break

            print(f"[BALANCEADOR] GET /dados/{paciente_id} [{tentativa + 1}/{tentativas}] → {servidor}")
            node = balancer.nodes.get(servidor)
            if node:
                node.record_request()
            try:
                response = await client.get(f"{servidor}/dados/{paciente_id}")
                latencia_ms = round((time.time() - t_inicio) * 1000)
                if node:
                    node.record_success(latencia_ms)

                porta = servidor.rstrip("/").split(":")[-1]
                s_id = f"server-0{int(porta)-8000}" if porta.isdigit() and 8001 <= int(porta) <= 8004 else "server-01"

                resp_data = response.json()
                headers = {
                    "X-Failover": "true" if falharam else "false",
                    "X-Algorithm": balancer.algorithm.value,
                    "X-Adaptive-Strategy": getattr(balancer, "_adaptive_last_decision", balancer.algorithm.value),
                    "X-Node": servidor,
                    "X-Server-Id": s_id,
                    "X-Latency-Ms": str(latencia_ms),
                }
                return JSONResponse(status_code=response.status_code, content=resp_data, headers=headers)
            except (httpx.ConnectError, httpx.TimeoutException):
                if node:
                    node.record_failure()
                falharam.append(servidor)
                continue
            except Exception as e:
                if node:
                    node.record_failure()
                return JSONResponse(status_code=500, content={"detail": str(e)})

        if balancer.servidores_reserva:
            reserva = balancer.ativar_reserva()
            if reserva:
                try:
                    response = await client.get(f"{reserva}/dados/{paciente_id}")
                    resp_data = response.json()
                    porta = reserva.rstrip("/").split(":")[-1]
                    s_id = f"server-0{int(porta)-8000}" if porta.isdigit() and 8001 <= int(porta) <= 8004 else "server-04"
                    headers = {
                        "X-Failover": "true",
                        "X-Algorithm": balancer.algorithm.value,
                        "X-Adaptive-Strategy": getattr(balancer, "_adaptive_last_decision", balancer.algorithm.value),
                        "X-Node": reserva,
                        "X-Server-Id": s_id,
                    }
                    return JSONResponse(status_code=response.status_code, content=resp_data, headers=headers)
                except Exception:
                    pass

        return JSONResponse(status_code=502, content={"detail": "Todos os servidores backend falharam"})

    # ── Gestão de Servidores ───────────────────────────────

    @app.post("/servidores/adicionar")
    async def adicionar_servidor(payload: ServidorPayload):
        adicionado = balancer.adicionar_servidor(payload.url)
        return {
            "status": "sucesso" if adicionado else "ja_existente",
            "servidor": payload.url,
            "servidores_ativos": balancer.servidores,
            "total_ativos": len(balancer.nodes),
        }

    @app.post("/servidores/remover")
    async def remover_servidor(payload: ServidorPayload):
        removido = balancer.remover_servidor(payload.url)
        return {
            "status": "sucesso" if removido else "nao_encontrado",
            "servidor": payload.url,
            "servidores_ativos": balancer.servidores,
            "total_ativos": len(balancer.nodes),
        }

    @app.post("/servidores/ativar-reserva")
    async def ativar_reserva():
        alvo = balancer.ativar_reserva()
        if not alvo:
            return JSONResponse(status_code=400, content={"detail": "Nenhum servidor reserva disponível."})
        print(f"[BALANCEADOR] 🚀 SCALE-OUT: Servidor reserva {alvo} ATIVADO!")
        n = len(balancer.nodes)
        return {
            "status": "ok",
            "acao": "servidor_reserva_ativado",
            "mensagem": f"Servidor reserva {alvo} ativado! Pool com {n} nós.",
            "servidor_ativado": alvo,
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_servidores": n,
            "distribuicao_esperada": f"{100 / n:.1f}%",
        }

    @app.post("/servidores/desativar-reserva")
    async def desativar_reserva(request: Request):
        url_alvo = None
        try:
            body = await request.json()
            if isinstance(body, dict):
                url_alvo = body.get("url")
        except Exception:
            pass
        alvo = balancer.desativar_reserva(url_alvo)
        if not alvo:
            return JSONResponse(status_code=400, content={"detail": "Nenhum servidor reserva ativo para desativar."})
        print(f"[BALANCEADOR] ⬇️ SCALE-IN: Servidor {alvo} DESATIVADO!")
        n = len(balancer.nodes)
        return {
            "status": "ok",
            "acao": "servidor_reserva_desativado",
            "servidor_desativado": alvo,
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_servidores": n,
            "distribuicao_esperada": f"{100 / n:.1f}%" if n else "0%",
        }

    # ── API Enterprise ─────────────────────────────────────

    @app.get("/api/nodes")
    async def api_nodes():
        """Retorna métricas detalhadas de cada nó."""
        return {
            "nodes": {url: m.to_dict() for url, m in balancer.nodes.items()},
            "reserve": balancer.servidores_reserva,
            "total_active": len(balancer.nodes),
            "total_healthy": len(balancer.active_nodes),
        }

    @app.get("/api/metrics")
    async def api_metrics():
        """Retorna métricas globais do balanceador."""
        return balancer.get_global_metrics()

    @app.get("/api/algorithm")
    async def api_get_algorithm():
        """Retorna o algoritmo ativo."""
        return {
            "algorithm": balancer.algorithm.value,
            "available": [a.value for a in AlgorithmType],
        }

    @app.post("/api/algorithm")
    async def api_set_algorithm(payload: AlgorithmPayload):
        """Altera o algoritmo de balanceamento em runtime."""
        try:
            balancer.algorithm = AlgorithmType(payload.algorithm)
            print(f"[BALANCEADOR] Algoritmo alterado para: {balancer.algorithm.value}")
            return {"algorithm": balancer.algorithm.value, "status": "ok"}
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"detail": f"Algoritmo '{payload.algorithm}' inválido. Use: {[a.value for a in AlgorithmType]}"},
            )

    @app.post("/api/auto-scale")
    async def api_toggle_auto_scale(request: Request):
        """Habilita/desabilita auto-scaling."""
        body = await request.json()
        balancer.auto_scale_enabled = body.get("enabled", False)
        if "min_nodes" in body:
            balancer.min_nodes = body["min_nodes"]
        if "max_nodes" in body:
            balancer.max_nodes = body["max_nodes"]
        return {
            "auto_scale_enabled": balancer.auto_scale_enabled,
            "min_nodes": balancer.min_nodes,
            "max_nodes": balancer.max_nodes,
        }

    @app.post("/api/nodes/weight")
    async def api_set_node_weight(request: Request):
        """Altera o peso de um nó para testar Weighted RR."""
        body = await request.json()
        target = str(body.get("node", ""))
        weight = int(body.get("weight", 100))
        for url, node in balancer.nodes.items():
            if target in url:
                node.weight = weight
                node.effective_weight = weight
                return {"status": "ok", "node": url, "weight": weight}
        return JSONResponse(status_code=404, content={"detail": f"Nó '{target}' não encontrado"})

    @app.post("/api/nodes/latency")
    async def api_inject_node_latency(request: Request):
        """Injeta histórico de latência artificial em um nó para testar Least Response Time."""
        body = await request.json()
        target = str(body.get("node", ""))
        latency = float(body.get("latency_ms", 150.0))
        for url, node in balancer.nodes.items():
            if target in url:
                node._latencies = [latency] * 10
                return {"status": "ok", "node": url, "avg_latency_ms": node.avg_latency_ms}
        return JSONResponse(status_code=404, content={"detail": f"Nó '{target}' não encontrado"})

    @app.post("/api/nodes/reset")
    async def api_reset_node_conditions():
        """Reseta pesos e latências dos nós para o estado normal (Round Robin)."""
        for node in balancer.nodes.values():
            node.weight = 100
            node.effective_weight = 100
            node._latencies.clear()
            node.active_connections = 0
        balancer._adaptive_last_decision = "round_robin"
        return {"status": "ok", "message": "Condições resetadas"}

    @app.get("/api/scale-history")
    async def api_scale_history():
        """Retorna histórico de ações de auto-scaling."""
        return {"history": balancer._scale_history}

    @app.get("/health")
    async def health():
        healthy = len(balancer.active_nodes)
        total = len(balancer.nodes)
        return {
            "status": "healthy" if healthy > 0 else "unhealthy",
            "total_nodes": total,
            "healthy_nodes": healthy,
            "total_ativos": len(balancer.servidores),
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_reservas": len(balancer.servidores_reserva),
            "algorithm": balancer.algorithm.value,
        }

    @app.get("/status")
    async def status():
        n_ativos = len(balancer.nodes)
        pct = f"{(100.0 / n_ativos):.1f}%" if n_ativos > 0 else "0%"
        return {
            "servidores": balancer.servidores,
            "servidores_ativos": balancer.servidores,
            "servidores_reserva": balancer.servidores_reserva,
            "total_requisicoes": balancer.total_requisicoes,
            "total_failovers": balancer.total_failovers,
            "proximo_servidor_indice": balancer._rr_index,
            "total_servidores": n_ativos,
            "distribuicao_esperada_por_no": pct,
            "algorithm": balancer.algorithm.value,
            "auto_scale_enabled": balancer.auto_scale_enabled,
            "nodes": {url: m.to_dict() for url, m in balancer.nodes.items()},
        }

    return app


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(
        description="Load Balancer Enterprise com múltiplos algoritmos e auto-scaling"
    )
    parser.add_argument("--port", type=int, default=8000, help="Porta do balanceador (padrão: 8000)")
    parser.add_argument("--servidores", type=str, default="", help="Lista de servidores ativos separados por vírgula")
    parser.add_argument("--servidores-reserva", type=str, default="", help="Lista de servidores reservas separados por vírgula")
    parser.add_argument(
        "--algorithm", type=str, default="round_robin",
        choices=[a.value for a in AlgorithmType],
        help="Algoritmo de balanceamento (padrão: round_robin)",
    )
    parser.add_argument("--auto-scale", action="store_true", default=True, help="Habilitar auto-scaling (padrão: True)")
    parser.add_argument("--no-auto-scale", action="store_false", dest="auto_scale", help="Desabilitar auto-scaling")
    parser.add_argument("--replicacao", action="store_true", default=True, help="Habilitar replicação Fanout entre nós (padrão: True)")
    parser.add_argument("--no-replicacao", action="store_false", dest="replicacao", help="Desabilitar replicação Fanout")

    args = parser.parse_args()

    servidores = (
        [s.strip() for s in args.servidores.split(",") if s.strip()]
        if args.servidores else SERVIDORES_PADRAO
    )
    servidores_reserva = (
        [s.strip() for s in args.servidores_reserva.split(",") if s.strip()]
        if args.servidores_reserva else SERVIDORES_RESERVA_PADRAO
    )

    app = criar_balanceador(
        servidores,
        servidores_reserva,
        args.port,
        args.algorithm,
        auto_scale=args.auto_scale,
        replicacao=args.replicacao,
    )

    uvicorn.run(app, host="0.0.0.0", port=args.port)
