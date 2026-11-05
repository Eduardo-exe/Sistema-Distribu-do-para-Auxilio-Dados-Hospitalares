# tests/test_escalabilidade.py
"""
Testes de Escalabilidade — Load Balancer Enterprise.

Testa todos os módulos de escalabilidade sem precisar de servidores reais:
1. Algoritmos de balanceamento (RR, WRR, LC, LRT, Hash)
2. Node metrics (latência, throughput, erros, conexões)
3. Circuit breaker por nó
4. Health-aware routing
5. Auto-scaling (scale-out, scale-in)
6. API de gestão

Para rodar:
    python -m pytest tests/test_escalabilidade.py -v
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_balancer.balancer import (
    LoadBalancerEnterprise,
    NodeMetrics,
    NodeStatus,
    CircuitState,
    AlgorithmType,
)


SERVIDORES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]

RESERVAS = [
    "http://localhost:8004",
]


# ============================================================
# 1. Testes de NodeMetrics
# ============================================================

class TestNodeMetrics:

    def test_criacao(self):
        m = NodeMetrics("http://localhost:8001")
        assert m.url == "http://localhost:8001"
        assert m.status == NodeStatus.HEALTHY
        assert m.active_connections == 0

    def test_record_request_success(self):
        m = NodeMetrics("http://localhost:8001")
        m.record_request()
        assert m.active_connections == 1
        m.record_success(10.0)
        assert m.active_connections == 0
        assert m.total_success == 1
        assert m.avg_latency_ms == 10.0

    def test_record_failure_degrades(self):
        m = NodeMetrics("http://localhost:8001")
        m.record_request()
        m.record_failure()
        m.record_request()
        m.record_failure()
        m.record_request()
        m.record_failure()
        assert m.status == NodeStatus.DEGRADED
        assert m.consecutive_failures == 3

    def test_circuit_breaker_trips(self):
        m = NodeMetrics("http://localhost:8001")
        for _ in range(5):
            m.record_request()
            m.record_failure()
        assert m.circuit_state == CircuitState.OPEN
        assert m.status == NodeStatus.UNHEALTHY
        assert m.check_circuit() is False

    def test_circuit_breaker_recovery(self):
        m = NodeMetrics("http://localhost:8001")
        for _ in range(5):
            m.record_request()
            m.record_failure()
        assert m.circuit_state == CircuitState.OPEN

        # Simular timeout
        m.circuit_last_trip = time.time() - 31
        assert m.check_circuit() is True
        assert m.circuit_state == CircuitState.HALF_OPEN

    def test_error_rate(self):
        m = NodeMetrics("http://localhost:8001")
        m.record_request()
        m.record_success(5.0)
        m.record_request()
        m.record_failure()
        assert m.error_rate == 0.5

    def test_weight_adjustment(self):
        m = NodeMetrics("http://localhost:8001")
        m.weight = 100
        m.effective_weight = 100
        m.record_request()
        m.record_failure()
        assert m.effective_weight < 100  # Diminuiu

    def test_to_dict(self):
        m = NodeMetrics("http://localhost:8001")
        m.record_request()
        m.record_success(15.0)
        d = m.to_dict()
        assert d["url"] == "http://localhost:8001"
        assert d["total_requests"] == 1
        assert d["avg_latency_ms"] == 15.0
        assert d["status"] == "healthy"


# ============================================================
# 2. Testes de Algoritmos
# ============================================================

class TestAlgoritmos:

    def test_round_robin(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "round_robin")
        results = [lb.proximo_servidor() for _ in range(6)]
        # Deve alternar entre os 3 servidores
        assert results[0] == results[3]
        assert results[1] == results[4]
        assert results[2] == results[5]

    def test_round_robin_distribuicao(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "round_robin")
        counts = {}
        for _ in range(90):
            s = lb.proximo_servidor()
            counts[s] = counts.get(s, 0) + 1
        # Cada servidor deve receber 30 requisições
        for s in SERVIDORES:
            assert counts[s] == 30

    def test_least_connections(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "least_connections")
        # Simular: server1 com 5 conexões ativas
        lb.nodes[SERVIDORES[0]].active_connections = 5
        lb.nodes[SERVIDORES[1]].active_connections = 1
        lb.nodes[SERVIDORES[2]].active_connections = 3
        selected = lb.proximo_servidor()
        assert selected == SERVIDORES[1]  # Menos conexões

    def test_least_response_time(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "least_response_time")
        # Simular latências
        lb.nodes[SERVIDORES[0]]._latencies = [100.0, 120.0]
        lb.nodes[SERVIDORES[1]]._latencies = [10.0, 15.0]
        lb.nodes[SERVIDORES[2]]._latencies = [50.0, 60.0]
        selected = lb.proximo_servidor()
        assert selected == SERVIDORES[1]  # Menor latência

    def test_patient_hash_sticky(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "patient_hash")
        # Mesmo paciente deve ir para o mesmo servidor
        s1 = lb.proximo_servidor(patient_id=42)
        s2 = lb.proximo_servidor(patient_id=42)
        s3 = lb.proximo_servidor(patient_id=42)
        assert s1 == s2 == s3

    def test_patient_hash_distribui(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "patient_hash")
        servidores_usados = set()
        for pid in range(100):
            s = lb.proximo_servidor(patient_id=pid)
            servidores_usados.add(s)
        assert len(servidores_usados) == 3  # Distribui entre todos

    def test_weighted_round_robin(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "weighted_round_robin")
        lb.nodes[SERVIDORES[0]].weight = 50
        lb.nodes[SERVIDORES[0]].effective_weight = 50
        lb.nodes[SERVIDORES[1]].weight = 30
        lb.nodes[SERVIDORES[1]].effective_weight = 30
        lb.nodes[SERVIDORES[2]].weight = 20
        lb.nodes[SERVIDORES[2]].effective_weight = 20

        counts = {}
        for _ in range(100):
            s = lb.proximo_servidor()
            counts[s] = counts.get(s, 0) + 1

        # Server1 (w=50) deve receber mais que server3 (w=20)
        assert counts[SERVIDORES[0]] > counts[SERVIDORES[2]]

    def test_least_connections_tie_break_round_robin(self):
        """Quando todas as conexões forem zero, deve alternar circularmente e não viciar no nó 1."""
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "least_connections")
        results = [lb.proximo_servidor() for _ in range(6)]
        assert results[0] != results[1]
        assert results[1] != results[2]
        assert len(set(results[:3])) == 3

    def test_adaptive_calm_uses_round_robin(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "adaptive")
        results = [lb.proximo_servidor() for _ in range(6)]
        assert lb._adaptive_last_decision == "round_robin"
        assert len(set(results[:3])) == 3

    def test_adaptive_degraded_latency_switches_to_least_response_time(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "adaptive")
        # Simula server1 e server3 saudáveis (10ms) e server2 degradado (190ms)
        lb.nodes[SERVIDORES[0]]._latencies = [10.0] * 5
        lb.nodes[SERVIDORES[1]]._latencies = [190.0] * 5
        lb.nodes[SERVIDORES[2]]._latencies = [12.0] * 5

        # Próxima chamada deve detectar degradação e desviar para os rápidos
        selected = lb.proximo_servidor()
        assert lb._adaptive_last_decision == "least_response_time"
        assert selected in [SERVIDORES[0], SERVIDORES[2]]
        assert selected != SERVIDORES[1]

    def test_adaptive_concurrency_switches_to_least_connections(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "adaptive")
        lb.nodes[SERVIDORES[0]].active_connections = 6
        lb.nodes[SERVIDORES[1]].active_connections = 1
        lb.nodes[SERVIDORES[2]].active_connections = 5

        selected = lb.proximo_servidor()
        assert lb._adaptive_last_decision == "least_connections"
        assert selected == SERVIDORES[1]
    def test_adaptive_heterogeneous_weights_switches_to_weighted_round_robin(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "adaptive")
        lb.nodes[SERVIDORES[0]].weight = 300
        lb.nodes[SERVIDORES[1]].weight = 100
        lb.nodes[SERVIDORES[2]].weight = 100

        selected = lb.proximo_servidor()
        assert lb._adaptive_last_decision == "weighted_round_robin"

    def test_adaptive_full_dynamic_cycle(self):
        """Valida que o algoritmo adaptativo comuta dinamicamente entre todas as 4 estratégias."""
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "adaptive")

        # 1. Estado inicial calmo -> Round Robin
        lb.proximo_servidor()
        assert lb._adaptive_last_decision == "round_robin"

        # 2. Concorrência e fila ativa -> Least Connections
        lb.nodes[SERVIDORES[0]].active_connections = 3
        lb.nodes[SERVIDORES[1]].active_connections = 0
        lb.proximo_servidor()
        assert lb._adaptive_last_decision == "least_connections"
        lb.nodes[SERVIDORES[0]].active_connections = 0

        # 3. Discrepância de latência -> Least Response Time
        lb.nodes[SERVIDORES[0]]._latencies = [15.0] * 5
        lb.nodes[SERVIDORES[1]]._latencies = [90.0] * 5
        lb.nodes[SERVIDORES[2]]._latencies = [15.0] * 5
        lb.proximo_servidor()
        assert lb._adaptive_last_decision == "least_response_time"
        lb.nodes[SERVIDORES[0]]._latencies.clear()
        lb.nodes[SERVIDORES[1]]._latencies.clear()
        lb.nodes[SERVIDORES[2]]._latencies.clear()

        # 4. Pesos heterogêneos -> Weighted Round Robin
        lb.nodes[SERVIDORES[0]].weight = 200
        lb.nodes[SERVIDORES[1]].weight = 100
        lb.proximo_servidor()
        assert lb._adaptive_last_decision == "weighted_round_robin"
        lb.nodes[SERVIDORES[0]].weight = 100

        # 5. Retorno ao estado calmo -> Round Robin
        lb.proximo_servidor()
        assert lb._adaptive_last_decision == "round_robin"

# ============================================================
# 3. Testes de Health-Aware Routing
# ============================================================

class TestHealthAwareRouting:

    def test_skip_unhealthy(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "round_robin")
        # Tornar server1 unhealthy
        for _ in range(5):
            lb.nodes[SERVIDORES[0]].record_request()
            lb.nodes[SERVIDORES[0]].record_failure()

        # Só deve selecionar server2 e server3
        results = set()
        for _ in range(20):
            s = lb.proximo_servidor()
            results.add(s)

        assert SERVIDORES[0] not in results
        assert SERVIDORES[1] in results
        assert SERVIDORES[2] in results

    def test_active_nodes(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "round_robin")
        assert len(lb.active_nodes) == 3

        # Derrubar um
        for _ in range(5):
            lb.nodes[SERVIDORES[0]].record_request()
            lb.nodes[SERVIDORES[0]].record_failure()

        assert len(lb.active_nodes) == 2

    def test_all_down_fallback(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS, "round_robin")
        # Derrubar todos
        for s in SERVIDORES:
            for _ in range(5):
                lb.nodes[s].record_request()
                lb.nodes[s].record_failure()

        assert len(lb.active_nodes) == 0
        # Deve usar fallback (todos os nós)
        s = lb.proximo_servidor()
        assert s in SERVIDORES


# ============================================================
# 4. Testes de Gestão de Servidores
# ============================================================

class TestGestaoServidores:

    def test_adicionar_servidor(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        assert lb.adicionar_servidor("http://localhost:9001") is True
        assert "http://localhost:9001" in lb.servidores
        assert lb.adicionar_servidor("http://localhost:9001") is False  # Já existe

    def test_remover_servidor(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        assert lb.remover_servidor(SERVIDORES[0]) is True
        assert SERVIDORES[0] not in lb.servidores
        assert lb.remover_servidor(SERVIDORES[0]) is False

    def test_ativar_reserva(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        assert len(lb.servidores) == 3
        alvo = lb.ativar_reserva()
        assert alvo == RESERVAS[0]
        assert len(lb.servidores) == 4
        assert RESERVAS[0] in lb.servidores

    def test_desativar_reserva(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.ativar_reserva()
        assert len(lb.servidores) == 4
        alvo = lb.desativar_reserva()
        assert alvo is not None
        assert len(lb.servidores) == 3

    def test_ativar_sem_reserva(self):
        lb = LoadBalancerEnterprise(SERVIDORES, [])
        assert lb.ativar_reserva() is None


# ============================================================
# 5. Testes de Auto-Scaling
# ============================================================

class TestAutoScaling:

    def test_auto_scale_disabled(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.auto_scale_enabled = False
        lb.check_auto_scale()
        # Não deve ter feito nada
        assert len(lb._scale_history) == 0

    def test_scale_out_on_high_latency(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.auto_scale_enabled = True
        lb.cooldown_seconds = 0
        lb.scale_out_threshold_latency = 100

        # Simular alta latência
        for s in SERVIDORES:
            lb.nodes[s]._latencies = [200.0] * 10

        lb.check_auto_scale()
        assert len(lb.servidores) == 4  # Scale-out happened
        assert len(lb._scale_history) == 1
        assert lb._scale_history[0]["action"] == "scale_out"

    def test_scale_in_on_low_load(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.ativar_reserva()  # Agora tem 4 nós
        lb.auto_scale_enabled = True
        lb.cooldown_seconds = 0
        lb.scale_out_threshold_latency = 500
        lb.scale_out_threshold_tps = 50

        # Simular baixa carga
        for s in lb.servidores:
            lb.nodes[s]._latencies = [5.0] * 10
            lb.nodes[s]._timestamps = []

        lb.check_auto_scale()
        assert len(lb.servidores) == 3  # Scale-in happened

    def test_cooldown_prevents_flapping(self):
        lb = LoadBalancerEnterprise(SERVIDORES, ["http://localhost:8004", "http://localhost:8005"])
        lb.auto_scale_enabled = True
        lb.cooldown_seconds = 60
        lb.scale_out_threshold_latency = 100

        for s in SERVIDORES:
            lb.nodes[s]._latencies = [200.0] * 10

        lb.check_auto_scale()
        count_before = len(lb._scale_history)

        # Segunda chamada dentro do cooldown
        for s in lb.servidores:
            if s in lb.nodes:
                lb.nodes[s]._latencies = [200.0] * 10

        lb.check_auto_scale()
        assert len(lb._scale_history) == count_before  # Não escalou novamente

    def test_respects_max_nodes(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.auto_scale_enabled = True
        lb.max_nodes = 3  # Já tem 3
        lb.cooldown_seconds = 0
        lb.scale_out_threshold_latency = 100

        for s in SERVIDORES:
            lb.nodes[s]._latencies = [200.0] * 10

        lb.check_auto_scale()
        assert len(lb.servidores) == 3  # Não escalou (max atingido)

    def test_failover_ativa_reserva_quando_todos_caem(self):
        """Sem nenhum nó saudável, o reserva precisa assumir automaticamente."""
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.auto_scale_enabled = True

        for s in SERVIDORES:
            for _ in range(5):
                lb.nodes[s].record_request()
                lb.nodes[s].record_failure()

        assert lb.active_nodes == []
        lb.check_auto_scale()

        assert RESERVAS[0] in lb.servidores
        assert lb.active_nodes == [RESERVAS[0]]
        assert lb._scale_history[-1]["trigger"] == "failover_sem_nos_saudaveis"

    def test_scale_out_quando_abaixo_do_minimo(self):
        """Perder redundância (nós saudáveis < min_nodes) também escala."""
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.auto_scale_enabled = True
        lb.cooldown_seconds = 0
        lb.min_nodes = 3

        for s in SERVIDORES[:2]:
            for _ in range(5):
                lb.nodes[s].record_request()
                lb.nodes[s].record_failure()

        assert len(lb.active_nodes) == 1
        lb.check_auto_scale()

        assert RESERVAS[0] in lb.servidores
        assert lb._scale_history[-1]["trigger"] == "abaixo_do_minimo"

    def test_scale_in_nao_desativa_no_principal(self):
        """Scale-In só pode remover servidores reserva, nunca os principais."""
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.auto_scale_enabled = True
        lb.cooldown_seconds = 0
        lb.min_nodes = 1

        lb.check_auto_scale()

        assert set(lb.servidores) == set(SERVIDORES)
        assert len(lb._scale_history) == 0


# ============================================================
# 6. Testes de Métricas Globais
# ============================================================

class TestMetricasGlobais:

    def test_global_metrics(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.nodes[SERVIDORES[0]].record_request()
        lb.nodes[SERVIDORES[0]].record_success(10.0)
        lb.nodes[SERVIDORES[1]].record_request()
        lb.nodes[SERVIDORES[1]].record_failure()

        metrics = lb.get_global_metrics()
        assert metrics["total_nodes"] == 3
        assert metrics["total_requests"] == 2
        assert metrics["total_success"] == 1
        assert metrics["total_errors"] == 1
        assert metrics["algorithm"] == "round_robin"

    def test_global_metrics_uptime(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        time.sleep(0.1)
        metrics = lb.get_global_metrics()
        assert metrics["uptime_seconds"] >= 0

    def test_servidores_property(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        assert lb.servidores == SERVIDORES

    def test_total_failovers(self):
        lb = LoadBalancerEnterprise(SERVIDORES, RESERVAS)
        lb.total_failovers = 5
        metrics = lb.get_global_metrics()
        assert metrics["total_failovers"] == 5
