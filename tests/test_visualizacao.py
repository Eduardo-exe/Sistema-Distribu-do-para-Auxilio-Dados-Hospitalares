# tests/test_visualizacao.py
"""
Testes unitários e de integração para a camada de visualização (monitor.py).
"""

import os
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
