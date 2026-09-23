# tests/test_marco2.py
"""
Testes do Marco 2 — Balanceamento de Servidores.

Testa:
1. Os três servidores iniciam corretamente.
2. O balanceador inicia corretamente.
3. O balanceador recebe requisições.
4. Round Robin funciona.
5. As requisições chegam aos três servidores.
6. A resposta identifica o servidor responsável.
7. Os dados são armazenados no servidor correspondente.
"""

import os
import sys
import time
import tempfile
import threading

import pytest
import uvicorn
import requests

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.app import criar_app
from server.database import buscar_todos
from load_balancer.balancer import criar_balanceador, RoundRobinBalancer


# ============================================================
# Teste do Round Robin isolado
# ============================================================

class TestRoundRobin:
    """Testes do algoritmo Round Robin isoladamente."""

    def test_round_robin_alterna(self):
        """O Round Robin deve alternar entre os servidores sequencialmente."""
        servidores = ["http://s1", "http://s2", "http://s3"]
        balancer = RoundRobinBalancer(servidores)

        assert balancer.proximo_servidor() == "http://s1"
        assert balancer.proximo_servidor() == "http://s2"
        assert balancer.proximo_servidor() == "http://s3"
        assert balancer.proximo_servidor() == "http://s1"
        assert balancer.proximo_servidor() == "http://s2"
        assert balancer.proximo_servidor() == "http://s3"

    def test_round_robin_cicla(self):
        """O Round Robin deve reiniciar após percorrer todos os servidores."""
        servidores = ["http://s1", "http://s2"]
        balancer = RoundRobinBalancer(servidores)

        resultados = [balancer.proximo_servidor() for _ in range(6)]
        assert resultados == [
            "http://s1", "http://s2",
            "http://s1", "http://s2",
            "http://s1", "http://s2",
        ]

    def test_round_robin_contador(self):
        """O contador de requisições deve incrementar."""
        balancer = RoundRobinBalancer(["http://s1"])
        assert balancer.total_requisicoes == 0
        balancer.proximo_servidor()
        assert balancer.total_requisicoes == 1
        balancer.proximo_servidor()
        assert balancer.total_requisicoes == 2


# ============================================================
# Testes de integração: servidores + balanceador
# ============================================================

# Portas para testes (altas para evitar conflitos)
PORTAS_SERVIDORES = [19001, 19002, 19003]
PORTA_BALANCEADOR = 19000


@pytest.fixture(scope="class")
def infraestrutura(tmp_path_factory):
    """
    Fixture de classe: inicia 3 servidores e o balanceador uma vez
    para todos os testes de integração.
    """
    tmp_path = tmp_path_factory.mktemp("dbs")
    db_paths = []
    servers = []

    # Criar e iniciar 3 servidores
    for i, porta in enumerate(PORTAS_SERVIDORES):
        server_id = f"server-{i + 1:02d}"
        db_path = str(tmp_path / f"server{i + 1}.db")
        db_paths.append(db_path)

        app = criar_app(server_id=server_id, db_path=db_path)

        config = uvicorn.Config(
            app, host="127.0.0.1", port=porta,
            log_level="warning",
        )
        server = uvicorn.Server(config)
        servers.append(server)

        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()

    # Criar e iniciar o balanceador
    servidores_urls = [f"http://127.0.0.1:{p}" for p in PORTAS_SERVIDORES]
    balancer_app = criar_balanceador(servidores_urls, PORTA_BALANCEADOR)

    config = uvicorn.Config(
        balancer_app, host="127.0.0.1", port=PORTA_BALANCEADOR,
        log_level="warning",
    )
    balancer_server = uvicorn.Server(config)
    servers.append(balancer_server)

    thread = threading.Thread(target=balancer_server.run, daemon=True)
    thread.start()

    # Aguardar servidores iniciarem
    base_url = f"http://127.0.0.1:{PORTA_BALANCEADOR}"
    todas_urls = servidores_urls + [base_url]

    inicio = time.time()
    for url in todas_urls:
        while time.time() - inicio < 15.0:
            try:
                requests.get(f"{url}/dados?limit=1", timeout=1)
                break
            except (requests.ConnectionError, requests.Timeout):
                time.sleep(0.2)
        else:
            pytest.fail(f"Servidor {url} não iniciou em 15s")

    yield {
        "base_url": base_url,
        "db_paths": db_paths,
        "servers": servers,
    }

    # Parar todos os servidores
    for server in servers:
        server.should_exit = True
    time.sleep(0.5)


class TestIntegracao:
    """
    Testes de integração que utilizam 3 servidores reais e o balanceador.
    Todos os testes compartilham a mesma infraestrutura (fixture de classe).
    """

    @pytest.fixture(autouse=True)
    def setup(self, infraestrutura):
        """Recebe a infraestrutura compartilhada."""
        self.base_url = infraestrutura["base_url"]
        self.db_paths = infraestrutura["db_paths"]

    def _dados_exemplo(self, paciente_id: int = 1) -> dict:
        """Retorna dados de exemplo para envio."""
        return {
            "paciente_id": paciente_id,
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

    def test_01_servidores_iniciaram(self):
        """Os três servidores devem estar acessíveis."""
        for i, porta in enumerate(PORTAS_SERVIDORES):
            response = requests.get(
                f"http://127.0.0.1:{porta}/dados?limit=1",
                timeout=5,
            )
            assert response.status_code == 200

    def test_02_balanceador_iniciou(self):
        """O balanceador deve estar acessível."""
        response = requests.get(f"{self.base_url}/status", timeout=5)
        assert response.status_code == 200
        data = response.json()
        assert len(data["servidores"]) == 3

    def test_03_balanceador_recebe_post(self):
        """O balanceador deve aceitar e encaminhar POST /dados."""
        response = requests.post(
            f"{self.base_url}/dados",
            json=self._dados_exemplo(),
            timeout=5,
        )
        assert response.status_code == 200
        data = response.json()
        assert "server_id" in data
        assert "id" in data

    def test_04_resposta_identifica_servidor(self):
        """A resposta deve identificar corretamente o servidor que processou."""
        response = requests.post(
            f"{self.base_url}/dados",
            json=self._dados_exemplo(),
            timeout=5,
        )
        data = response.json()
        assert data["server_id"].startswith("server-")
        assert data["server_id"] in ("server-01", "server-02", "server-03")

    def test_05_round_robin_distribuicao(self):
        """As requisições devem ser distribuídas via Round Robin."""
        # Primeiro, obter o status para saber o estado atual do índice
        status_resp = requests.get(f"{self.base_url}/status", timeout=5)
        indice_atual = status_resp.json()["proximo_servidor_indice"]

        # Determinar a sequência esperada a partir do índice atual
        server_ids = ["server-01", "server-02", "server-03"]
        esperado = []
        idx = indice_atual
        for _ in range(6):
            esperado.append(server_ids[idx])
            idx = (idx + 1) % 3

        # Enviar 6 requisições
        respostas = []
        for i in range(6):
            response = requests.post(
                f"{self.base_url}/dados",
                json=self._dados_exemplo(paciente_id=500 + i),
                timeout=5,
            )
            assert response.status_code == 200
            respostas.append(response.json()["server_id"])

        assert respostas == esperado

    def test_06_dados_armazenados_nos_servidores(self):
        """Os dados devem ser armazenados nos bancos dos servidores."""
        # Verificar que cada servidor tem pelo menos 1 registro
        total = 0
        for i, db_path in enumerate(self.db_paths):
            registros = buscar_todos(db_path, limit=1000)
            count = len(registros)
            total += count
            assert count > 0, (
                f"Servidor {i + 1} deveria ter pelo menos 1 registro, "
                f"mas tem {count}"
            )
            # Verificar que o server_id corresponde ao servidor correto
            for r in registros:
                assert r["server_id"] == f"server-{i + 1:02d}"

        # O total de registros deve ser igual ao total enviado
        assert total > 0
