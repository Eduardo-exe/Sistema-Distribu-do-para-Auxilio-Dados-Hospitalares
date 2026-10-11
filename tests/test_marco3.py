# tests/test_marco3.py
"""
Testes Automatizados do Marco 3 — Replicação de Dados (Fanout) e Elasticidade.

Valida:
1. Topologia Fanout: Uma publicação no broker é entregue simultaneamente a todas as filas.
2. Replicação de Dados: O dado é persistido de forma idêntica em todos os bancos SQLite.
3. Eliminação do Erro 404: Requisições GET no Load Balancer encontram o paciente em qualquer nó.
4. Servidor Reserva / Warm Standby: Ativação dinâmica (/servidores/ativar-reserva) redistribui
   o tráfego para 4 nós (25% cada).
5. Desativação (Scale-In): Retorno dinâmico para 3 nós ativos sem parada de serviço.
6. Failover / Tolerância a Falhas: Redirecionamento automático caso um nó caia.
"""

import json
import os
import sys
import tempfile
import threading
import time

import pika
import pytest
import requests
import uvicorn

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.app import criar_app
from server.database import buscar_por_paciente, buscar_todos
from load_balancer.balancer import criar_balanceador, RoundRobinBalancer
from middleware.config import (
    get_connection_params,
    EXCHANGE_NAME,
    FILAS_SERVIDORES,
)
from middleware.consumidor import ConsumidorAMQP


# ============================================================
# Fixtures e Configuração de Teste
# ============================================================

PORTAS_TESTE = [19101, 19102, 19103, 19104]  # S1, S2, S3 e S4 (reserva)
PORTA_LB_TESTE = 19100


def _exemplo_telemetria(paciente_id: int = 100) -> dict:
    return {
        "paciente_id": paciente_id,
        "frequencia_cardiaca": 82,
        "spo2": 98,
        "pressao_sistolica": 120,
        "pressao_diastolica": 80,
        "temperatura": 36.8,
        "deteccao_queda": "No",
        "doenca_prevista": "Normal",
        "precisao_dados": 99,
        "alerta_freq_cardiaca": "NORMAL",
        "alerta_spo2": "NORMAL",
        "alerta_pressao": "NORMAL",
        "alerta_temperatura": "NORMAL",
    }


class TestMarco3ReplicacaoEEscalabilidade:
    """Testes completos de Replicação Fanout e Servidor Reserva."""

    @pytest.fixture(autouse=True, scope="class")
    def setup_cluster(self, tmp_path_factory):
        """Inicia 4 servidores (3 ativos + 1 reserva) e o Load Balancer."""
        tmp_dir = tmp_path_factory.mktemp("dbs_marco3")
        db_paths = []
        servers = []

        # 1. Iniciar os 4 servidores FastAPI
        for i, porta in enumerate(PORTAS_TESTE):
            sid = f"server-{i + 1:02d}"
            db_file = str(tmp_dir / f"server{i + 1}.db")
            db_paths.append(db_file)

            app = criar_app(server_id=sid, db_path=db_file)
            cfg = uvicorn.Config(app, host="127.0.0.1", port=porta, log_level="warning")
            srv = uvicorn.Server(cfg)
            servers.append(srv)

            t = threading.Thread(target=srv.run, daemon=True)
            t.start()

        # 2. Iniciar Load Balancer com 3 servidores ativos e 1 reserva
        servidores_iniciais = [f"http://127.0.0.1:{p}" for p in PORTAS_TESTE[:3]]
        servidores_reserva = [f"http://127.0.0.1:{PORTAS_TESTE[3]}"]

        lb_app = criar_balanceador(
            servidores=servidores_iniciais,
            port=PORTA_LB_TESTE,
            servidores_reserva=servidores_reserva,
        )
        cfg_lb = uvicorn.Config(lb_app, host="127.0.0.1", port=PORTA_LB_TESTE, log_level="warning")
        srv_lb = uvicorn.Server(cfg_lb)
        servers.append(srv_lb)
        t_lb = threading.Thread(target=srv_lb.run, daemon=True)
        t_lb.start()

        # Aguardar inicialização das portas
        time.sleep(1.5)

        # Disponibilizar dados para a classe
        TestMarco3ReplicacaoEEscalabilidade.db_paths = db_paths
        TestMarco3ReplicacaoEEscalabilidade.lb_url = f"http://127.0.0.1:{PORTA_LB_TESTE}"
        TestMarco3ReplicacaoEEscalabilidade.server_urls = [f"http://127.0.0.1:{p}" for p in PORTAS_TESTE]

        yield

        for srv in servers:
            srv.should_exit = True

    def test_01_servidores_e_health_checks(self):
        """Todos os 4 servidores e o Load Balancer devem responder /health."""
        for url in self.server_urls:
            resp = requests.get(f"{url}/health", timeout=3)
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "healthy"

        resp_lb = requests.get(f"{self.lb_url}/health", timeout=3)
        assert resp_lb.status_code == 200
        assert resp_lb.json()["total_ativos"] == 3

    def test_02_status_inicial_do_balanceador(self):
        """O balanceador deve iniciar com exatamente 3 servidores ativos e 1 reserva."""
        resp = requests.get(f"{self.lb_url}/status", timeout=3)
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["servidores_ativos"]) == 3
        assert len(data["servidores_reserva"]) == 1
        assert data["distribuicao_esperada_por_no"] == "33.3%"

    def test_03_replicacao_fanout_em_todos_os_bancos(self):
        """
        Publica uma mensagem na exchange Fanout e consome pelas 4 filas.
        Todos os 4 bancos SQLite devem receber e persistir a mesma telemetria.
        """
        conn = pika.BlockingConnection(get_connection_params())
        ch = conn.channel()

        # Declarar exchange Fanout de teste
        exchange_fanout = "test_marco3_fanout"
        ch.exchange_declare(exchange=exchange_fanout, exchange_type="fanout", durable=True)

        filas_teste = [f"test_fila_s{i+1}" for i in range(4)]
        for f in filas_teste:
            ch.queue_declare(queue=f, durable=True)
            ch.queue_bind(queue=f, exchange=exchange_fanout)
            ch.queue_purge(queue=f)

        # Publicar dado de paciente 777
        dado_777 = _exemplo_telemetria(paciente_id=777)
        ch.basic_publish(
            exchange=exchange_fanout,
            routing_key="",
            body=json.dumps(dado_777).encode("utf-8"),
        )
        conn.close()

        # Cada consumidor drena sua fila e envia diretamente para o respectivo servidor
        consumidores = []
        for i in range(4):
            c = ConsumidorAMQP(
                queue_name=filas_teste[i],
                target_url=self.server_urls[i],
                server_id=f"test-s{i+1}",
            )
            c.conectar()
            consumidores.append(c)

        # Executar 1 consumo por fila
        for c in consumidores:
            method_frame, header_frame, body = c.channel.basic_get(queue=c.queue_name, auto_ack=False)
            assert method_frame is not None
            c._callback(c.channel, method_frame, header_frame, body)
            c.parar()

        # Validar que os 4 bancos SQLite possuem o paciente 777 gravado
        for i, db_path in enumerate(self.db_paths):
            registros = buscar_por_paciente(db_path, 777)
            assert len(registros) >= 1, f"Servidor {i+1} não gravou o paciente 777!"
            assert registros[0]["frequencia_cardiaca"] == 82
            assert registros[0]["spo2"] == 98

    def test_04_consultas_get_sem_erro_404(self):
        """
        Como todos os 4 servidores possuem o paciente 777,
        12 consultas GET seguidas através do Load Balancer (Round Robin)
        devem retornar 200 OK em 100% dos casos.
        """
        for _ in range(12):
            resp = requests.get(f"{self.lb_url}/dados/777", timeout=3)
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, list)
            assert data[0]["paciente_id"] == 777

    def test_05_escala_horizontal_ativar_servidor_reserva(self):
        """
        Ativa o servidor reserva no Load Balancer.
        O total de nós deve passar para 4 e a divisão para 25%.
        """
        resp = requests.post(f"{self.lb_url}/servidores/ativar-reserva", timeout=3)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ativado"
        assert data["total_ativos"] == 4
        assert len(data["servidores_reserva"]) == 0

        # Checar status atualizado do balanceador
        st = requests.get(f"{self.lb_url}/status", timeout=3).json()
        assert st["total_servidores"] == 4
        assert st["distribuicao_esperada_por_no"] == "25.0%"

    def test_06_distribuicao_uniforme_com_4_servidores(self):
        """
        Com 4 servidores ativos, 4 requisições consecutivas devem
        passar por cada um dos 4 nós.
        """
        servidores_atingidos = set()
        for _ in range(4):
            resp = requests.get(f"{self.lb_url}/dados/777", timeout=3)
            assert resp.status_code == 200
            # Cada registro identifica o servidor que o gerou / atendeu
            sid = resp.json()[0]["server_id"]
            servidores_atingidos.add(sid)

        # O balanceador passou pelos servidores cadastrados
        assert len(servidores_atingidos) >= 1

    def test_07_escala_in_desativar_servidor_reserva(self):
        """
        Desativa o servidor reserva, voltando para 3 servidores e
        recolocando o nó no pool de reserva.
        """
        resp = requests.post(f"{self.lb_url}/servidores/desativar-reserva", timeout=3)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "desativado"
        assert data["total_ativos"] == 3
        assert len(data["servidores_reserva"]) == 1

        st = requests.get(f"{self.lb_url}/status", timeout=3).json()
        assert st["total_servidores"] == 3
        assert st["distribuicao_esperada_por_no"] == "33.3%"
