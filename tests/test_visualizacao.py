# tests/test_visualizacao.py
"""
Testes unitários e de integração para a camada de visualização (monitor.py).
"""

import os
import sys

# Adicionar diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient
from visualizacao.monitor import create_app, MonitorState


PROJETO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(PROJETO_DIR, "data", "dataset.csv")
LB_URL = "http://localhost:8000"


@pytest.fixture
def monitor_client():
    app = create_app(CSV_PATH, LB_URL)
    with TestClient(app) as client:
        yield client


class TestMonitor:
    def test_csv_carregado_com_sucesso(self):
        state = MonitorState(CSV_PATH, LB_URL)
        assert len(state.records) > 0
        assert "paciente_id" in state.records[0]

    def test_serve_html_index(self, monitor_client):
        resp = monitor_client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Sistema Distribuído Hospitalar" in resp.text
        assert "arch-canvas" in resp.text
        assert "Round Robin" in resp.text

    def test_get_status_inicial(self, monitor_client):
        resp = monitor_client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["running"] is False
        assert data["processing"] is False
        assert data["current_index"] == 0
        assert data["contadores"]["total"] == 0
        assert data["total_records"] > 0

    def test_alterar_velocidade(self, monitor_client):
        resp = monitor_client.post("/api/velocidade?valor=2.5")
        assert resp.status_code == 200
        assert resp.json()["velocidade"] == 2.5

        status = monitor_client.get("/api/status").json()
        assert status["speed"] == 2.5

    def test_reiniciar_simulacao(self, monitor_client):
        resp = monitor_client.post("/api/reiniciar")
        assert resp.status_code == 200
        assert resp.json()["status"] == "reiniciado"

        status = monitor_client.get("/api/status").json()
        assert status["current_index"] == 0
        assert status["contadores"]["total"] == 0

    def test_elementos_marco3_e_escala_no_html(self, monitor_client):
        resp = monitor_client.get("/")
        assert resp.status_code == 200
        html = resp.text
        assert "btn-toggle-reserva" in html
        assert "node-s4" in html
        assert "server4.db" in html
        assert "Fanout" in html
        assert "STANDBY" in html

    def test_get_escala_status(self, monitor_client):
        resp = monitor_client.get("/api/escala/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "servidores_ativos" in data
        assert "servidores_reserva" in data
        assert "distribuicao_esperada_por_no" in data

    def test_elementos_enterprise_escalabilidade_no_html(self, monitor_client):
        resp = monitor_client.get("/")
        assert resp.status_code == 200
        html = resp.text
        assert "ent-panel" in html
        assert "ent-sel-algorithm" in html
        assert "ent-btn-autoscale" in html
        assert "ent-node-grid" in html
        assert "ent-scale-history" in html
        assert "Round Robin" in html
        assert "Least Connections" in html
        assert "Weighted Round Robin" in html

    def test_enterprise_proxy_endpoints(self, monitor_client):
        # Nodes proxy fallback
        resp = monitor_client.get("/api/enterprise/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data or "error" in data

        # Metrics proxy fallback
        resp = monitor_client.get("/api/enterprise/metrics")
        assert resp.status_code == 200

        # Algorithm proxy fallback
        resp = monitor_client.get("/api/enterprise/algorithm")
        assert resp.status_code == 200

        # Scale history proxy fallback
        resp = monitor_client.get("/api/enterprise/scale-history")
        assert resp.status_code == 200

    def test_elementos_kafka_middleware_no_html(self, monitor_client):
        resp = monitor_client.get("/")
        assert resp.status_code == 200
        html = resp.text
        assert "kafka-panel" in html
        assert "telemetria_hospitalar" in html
        assert "P0" in html
        assert "P1" in html
        assert "P2" in html
        assert "P3" in html
        assert "consumidor_servidor_1" in html
        assert "consumidor_servidor_2" in html
        assert "consumidor_servidor_3" in html
        assert "consumidor_servidor_4" in html
        assert "KRaft" in html
        assert "Token Bucket" in html

    def test_esb_orb_monitor_endpoints(self, monitor_client):
        resp_esb = monitor_client.get("/api/esb/status")
        assert resp_esb.status_code == 200
        assert "bus_name" in resp_esb.json()

        resp_audit = monitor_client.get("/api/esb/audit")
        assert resp_audit.status_code == 200
        assert "audit_entries" in resp_audit.json()

    def test_kafka_monitor_endpoint(self, monitor_client):
        resp_kafka = monitor_client.get("/api/kafka/status")
        assert resp_kafka.status_code == 200
        data = resp_kafka.json()
        assert "bootstrap_servers" in data
        assert "topic" in data
        assert data["topic"] == "telemetria_hospitalar"
        assert "metrics" in data
        assert "particoes" in data["metrics"]

    def test_evento_escala_externo(self, monitor_client):
        resp = monitor_client.post(
            "/api/evento-escala-externo",
            json={
                "acao": "scale_out",
                "servidor": "http://localhost:8004",
                "motivo": "failover_sem_nos_saudaveis",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        resp_in = monitor_client.post(
            "/api/evento-escala-externo",
            json={
                "acao": "scale_in",
                "servidor": "http://localhost:8004",
                "motivo": "low_load",
            },
        )
        assert resp_in.status_code == 200
        assert resp_in.json()["status"] == "ok"

    def test_vms_endpoint(self, monitor_client):
        resp = monitor_client.get("/api/vms")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "vms" in data
        assert len(data["vms"]) >= 7
        assert any(v["vm_name"] == "vm-servidor-01" for v in data["vms"])
        assert any(v["ip"] == "192.168.56.13" for v in data["vms"])
        assert any(v["vm_name"] == "vm-servidor-04" for v in data["vms"])
        assert any(v["ip"] == "192.168.56.16" for v in data["vms"])
        assert data["network_subnet"] == "192.168.56.0/24"
