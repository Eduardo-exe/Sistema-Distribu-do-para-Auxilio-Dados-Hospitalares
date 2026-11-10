# tests/test_multi_medicos_consulta.py
"""
Testes unitários e de integração para Múltiplos Médicos (Produtores)
e Consulta Distribuída de Prontuários (Read Path).
"""

import csv
import os
import sys

# Adicionar diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient
from visualizacao.monitor import create_app, MonitorState
from client.multi_medicos import MEDICOS_CONFIG

PROJETO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJETO_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "dataset.csv")
LB_URL = "http://localhost:8000"


class TestDatasetsMultiMedicos:
    """Validação da integridade dos 3 datasets especializados."""

    def test_existencia_dos_tres_datasets(self):
        for k, cfg in MEDICOS_CONFIG.items():
            caminho = cfg["csv"]
            assert os.path.exists(caminho), f"Dataset do médico {cfg['nome']} não encontrado: {caminho}"

    def test_quantidade_linhas_dos_datasets(self):
        for k, cfg in MEDICOS_CONFIG.items():
            caminho = cfg["csv"]
            with open(caminho, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                linhas = list(reader)
                # Header + 20.000 registros
                assert len(linhas) == 20001, f"{cfg['nome']} esperava 20001 linhas, obteve {len(linhas)}"

    def test_compatibilidade_cabecalho_com_dataset_original(self):
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            header_orig = next(csv.reader(f))

        for k, cfg in MEDICOS_CONFIG.items():
            with open(cfg["csv"], "r", encoding="utf-8") as f:
                header_med = next(csv.reader(f))
                assert header_med == header_orig, f"Cabeçalho divergente em {cfg['nome']}"


class TestMonitorMultiMedicosEConsulta:
    """Testes dos endpoints e estado de múltiplos médicos e consulta no monitor."""

    @pytest.fixture
    def app_client(self):
        app = create_app(CSV_PATH, LB_URL)
        with TestClient(app) as client:
            yield client

    def test_alternar_medico_no_monitor_state(self):
        state = MonitorState(CSV_PATH, LB_URL)
        assert len(state.records) > 0

        # Selecionar Dr. Carlos (UTI)
        state.set_medico("1")
        assert state.medico_ativo == "1"
        assert len(state.records) == 20000
        assert state.records[0]["medico"] == "Dr. Carlos (UTI)"

        # Selecionar Dra. Beatriz (Emergência)
        state.set_medico("2")
        assert state.medico_ativo == "2"
        assert len(state.records) == 20000
        assert state.records[0]["medico"] == "Dra. Beatriz (Emergência)"

        # Selecionar Dr. Lucas (Enfermaria)
        state.set_medico("3")
        assert state.medico_ativo == "3"
        assert len(state.records) == 20000
        assert state.records[0]["medico"] == "Dr. Lucas (Enfermaria)"

    def test_endpoint_selecionar_medico(self, app_client):
        resp = app_client.post("/api/medico/selecionar", json={"medico": "2"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["medico"] == "2"
        assert "Dra. Beatriz" in data["nome"]
        assert data["total"] == 20000

    def test_html_contem_controles_de_medico_e_consulta(self, app_client):
        resp = app_client.get("/")
        assert resp.status_code == 200
        html = resp.text
        assert "btn-consulta-prontuario" in html
        assert "sel-medico" in html
        assert "modal-consulta" in html
        assert "Dr. Carlos" in html
        assert "Dra. Beatriz" in html
        assert "Dr. Lucas" in html

