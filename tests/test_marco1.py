# tests/test_marco1.py
"""
Testes do Marco 1 — Simulação e Servidor Base.

Testa:
1. CSV é lido corretamente.
2. Registro é convertido corretamente.
3. POST /dados funciona.
4. Dados são armazenados no SQLite.
5. GET /dados funciona.
6. GET /dados/{paciente_id} funciona.
"""

import csv
import os
import sys
import tempfile

import pytest

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.models import CSV_COLUMN_MAP, converter_registro_csv, DadosPaciente
from server.database import init_db, inserir_dado, buscar_todos, buscar_por_paciente
from server.app import criar_app

from fastapi.testclient import TestClient


# ============================================================
# Caminhos
# ============================================================

PROJETO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(PROJETO_DIR, "data", "dataset.csv")


# ============================================================
# Teste 1: CSV é lido corretamente
# ============================================================

class TestLeituraCSV:
    """Testes de leitura e validação do CSV."""

    def test_csv_existe(self):
        """O arquivo CSV deve existir no caminho esperado."""
        assert os.path.exists(CSV_PATH), f"CSV não encontrado em: {CSV_PATH}"

    def test_csv_tem_colunas_esperadas(self):
        """O CSV deve conter todas as colunas esperadas."""
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            colunas = reader.fieldnames

        for coluna_csv in CSV_COLUMN_MAP.keys():
            assert coluna_csv in colunas, (
                f"Coluna '{coluna_csv}' não encontrada no CSV. "
                f"Colunas disponíveis: {colunas}"
            )

    def test_csv_tem_registros(self):
        """O CSV deve conter pelo menos um registro."""
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            primeiro = next(reader, None)
        assert primeiro is not None, "CSV está vazio"

    def test_csv_primeiro_registro_valores_validos(self):
        """O primeiro registro deve ter valores válidos para campos numéricos."""
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            primeiro = next(reader)

        # Patient Number deve ser conversível para int
        assert primeiro["Patient Number"].strip().isdigit()
        # Heart Rate deve ser conversível para int
        assert primeiro["Heart Rate (bpm)"].strip().isdigit()
        # Body Temperature deve ser conversível para float
        float(primeiro["Body Temperature (°C)"].strip())


# ============================================================
# Teste 2: Registro é convertido corretamente
# ============================================================

class TestConversaoRegistro:
    """Testes de conversão de registros do CSV para formato da API."""

    def _registro_exemplo(self) -> dict:
        """Retorna um registro de exemplo do CSV."""
        return {
            "Patient Number": "1",
            "Heart Rate (bpm)": "98",
            "SpO2 Level (%)": "96",
            "Systolic Blood Pressure (mmHg)": "120",
            "Diastolic Blood Pressure (mmHg)": "86",
            "Body Temperature (°C)": "38.1",
            "Fall Detection": "No",
            "Predicted Disease": "Diabetes Mellitus",
            "Data Accuracy (%)": "95",
            "Heart Rate Alert": "NORMAL",
            "SpO2 Level Alert": "NORMAL",
            "Blood Pressure Alert": "NORMAL",
            "Temperature Alert": "ABNORMAL",
        }

    def test_conversao_campos(self):
        """Todos os campos devem ser convertidos corretamente."""
        registro_csv = self._registro_exemplo()
        dados = converter_registro_csv(registro_csv)

        assert dados["paciente_id"] == 1
        assert dados["frequencia_cardiaca"] == 98
        assert dados["spo2"] == 96
        assert dados["pressao_sistolica"] == 120
        assert dados["pressao_diastolica"] == 86
        assert dados["temperatura"] == 38.1
        assert dados["deteccao_queda"] == "No"
        assert dados["doenca_prevista"] == "Diabetes Mellitus"
        assert dados["precisao_dados"] == 95
        assert dados["alerta_freq_cardiaca"] == "NORMAL"
        assert dados["alerta_spo2"] == "NORMAL"
        assert dados["alerta_pressao"] == "NORMAL"
        assert dados["alerta_temperatura"] == "ABNORMAL"

    def test_conversao_tipos(self):
        """Os tipos devem ser corretos após conversão."""
        registro_csv = self._registro_exemplo()
        dados = converter_registro_csv(registro_csv)

        assert isinstance(dados["paciente_id"], int)
        assert isinstance(dados["frequencia_cardiaca"], int)
        assert isinstance(dados["temperatura"], float)
        assert isinstance(dados["deteccao_queda"], str)

    def test_conversao_valida_pydantic(self):
        """O registro convertido deve ser validável pelo modelo Pydantic."""
        registro_csv = self._registro_exemplo()
        dados = converter_registro_csv(registro_csv)
        paciente = DadosPaciente(**dados)
        assert paciente.paciente_id == 1

    def test_conversao_registro_real_csv(self):
        """A conversão deve funcionar com um registro real do CSV."""
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            registro_csv = next(reader)

        dados = converter_registro_csv(registro_csv)
        paciente = DadosPaciente(**dados)
        assert paciente.paciente_id > 0


# ============================================================
# Teste 3, 4, 5, 6: API e banco de dados
# ============================================================

class TestAPI:
    """Testes da API FastAPI e armazenamento no SQLite."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Cria um banco temporário e o TestClient para cada teste."""
        self.db_path = str(tmp_path / "test.db")
        self.app = criar_app(server_id="test-server", db_path=self.db_path)
        self.client = TestClient(self.app)

    def _dados_exemplo(self) -> dict:
        """Retorna dados de exemplo no formato da API."""
        return {
            "paciente_id": 42,
            "frequencia_cardiaca": 80,
            "spo2": 97,
            "pressao_sistolica": 120,
            "pressao_diastolica": 80,
            "temperatura": 36.5,
            "deteccao_queda": "No",
            "doenca_prevista": "Normal",
            "precisao_dados": 95,
            "alerta_freq_cardiaca": "NORMAL",
            "alerta_spo2": "NORMAL",
            "alerta_pressao": "NORMAL",
            "alerta_temperatura": "NORMAL",
        }

    def test_post_dados(self):
        """POST /dados deve retornar 200 com dados válidos."""
        response = self.client.post("/dados", json=self._dados_exemplo())
        assert response.status_code == 200
        data = response.json()
        assert data["paciente_id"] == 42
        assert data["server_id"] == "test-server"
        assert "id" in data
        assert "timestamp" in data

    def test_post_dados_invalidos(self):
        """POST /dados deve retornar 422 com dados inválidos."""
        response = self.client.post("/dados", json={"paciente_id": "abc"})
        assert response.status_code == 422

    def test_dados_armazenados_sqlite(self):
        """Os dados devem ser armazenados corretamente no SQLite."""
        self.client.post("/dados", json=self._dados_exemplo())

        registros = buscar_todos(self.db_path)
        assert len(registros) == 1
        assert registros[0]["paciente_id"] == 42
        assert registros[0]["server_id"] == "test-server"

    def test_get_dados(self):
        """GET /dados deve retornar os registros armazenados."""
        # Inserir 3 registros
        for i in range(3):
            dados = self._dados_exemplo()
            dados["paciente_id"] = i + 1
            self.client.post("/dados", json=dados)

        response = self.client.get("/dados")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3

    def test_get_dados_paginacao(self):
        """GET /dados deve suportar paginação com limit e offset."""
        for i in range(5):
            dados = self._dados_exemplo()
            dados["paciente_id"] = i + 1
            self.client.post("/dados", json=dados)

        response = self.client.get("/dados?limit=2&offset=0")
        assert response.status_code == 200
        assert len(response.json()) == 2

        response = self.client.get("/dados?limit=2&offset=3")
        assert response.status_code == 200
        assert len(response.json()) == 2

    def test_get_dados_paciente(self):
        """GET /dados/{paciente_id} deve retornar registros de um paciente."""
        # Inserir registros de pacientes diferentes
        for pid in [10, 10, 20]:
            dados = self._dados_exemplo()
            dados["paciente_id"] = pid
            self.client.post("/dados", json=dados)

        response = self.client.get("/dados/10")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert all(d["paciente_id"] == 10 for d in data)

    def test_get_dados_paciente_inexistente(self):
        """GET /dados/{paciente_id} deve retornar 404 se paciente não existe."""
        response = self.client.get("/dados/99999")
        assert response.status_code == 404

    def test_post_resposta_identifica_servidor(self):
        """A resposta do POST deve identificar o servidor."""
        response = self.client.post("/dados", json=self._dados_exemplo())
        data = response.json()
        assert data["server_id"] == "test-server"

    def test_multiplos_registros_mesmo_paciente(self):
        """Múltiplos registros do mesmo paciente devem ser armazenados."""
        for _ in range(3):
            self.client.post("/dados", json=self._dados_exemplo())

        registros = buscar_por_paciente(self.db_path, 42)
        assert len(registros) == 3
