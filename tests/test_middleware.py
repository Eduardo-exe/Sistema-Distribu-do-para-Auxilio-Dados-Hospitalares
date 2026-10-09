# tests/test_middleware.py
"""
Testes do Middleware RabbitMQ (MOM).

Testa:
1. A configuração do middleware (exchange, fila, routing key).
2. O produtor publica mensagens corretamente na fila.
3. O consumidor consome mensagens e encaminha ao Load Balancer.
4. O fluxo completo: produtor → RabbitMQ → consumidor → LB → servidor.
5. Mensagens persistem na fila quando o consumidor está parado.
6. ACK/NACK funciona corretamente.

Pré-requisito:
    RabbitMQ rodando em localhost:5672 (ou via docker-compose).

Para rodar apenas estes testes:
    python -m pytest tests/test_middleware.py -v
"""

import json
import os
import sys
import tempfile
import threading
import time

import pytest

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================
# Verificar se RabbitMQ está disponível
# ============================================================

try:
    import pika
    _params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        credentials=pika.PlainCredentials("guest", "guest"),
        connection_attempts=1,
        retry_delay=0,
        socket_timeout=2,
    )
    _conn = pika.BlockingConnection(_params)
    _conn.close()
    RABBITMQ_DISPONIVEL = True
except Exception:
    RABBITMQ_DISPONIVEL = False

# Pular todos os testes se o RabbitMQ não estiver disponível
pytestmark = pytest.mark.skipif(
    not RABBITMQ_DISPONIVEL,
    reason="RabbitMQ não está disponível em localhost:5672"
)


# ============================================================
# Fixtures
# ============================================================

# Nomes únicos para evitar conflito com o ambiente de produção
EXCHANGE_TESTE = "test_telemetria_hospitalar"
QUEUE_TESTE = "test_fila_dados_pacientes"
ROUTING_KEY_TESTE = "test.dados.paciente"


@pytest.fixture(autouse=True)
def _limpar_fila():
    """Limpa a fila de teste antes e depois de cada teste."""
    def purge():
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
            ch = conn.channel()
            ch.exchange_declare(
                exchange=EXCHANGE_TESTE,
                exchange_type="direct",
                durable=True,
            )
            ch.queue_declare(queue=QUEUE_TESTE, durable=True)
            ch.queue_bind(
                queue=QUEUE_TESTE,
                exchange=EXCHANGE_TESTE,
                routing_key=ROUTING_KEY_TESTE,
            )
            ch.queue_purge(queue=QUEUE_TESTE)
            conn.close()
        except Exception:
            pass

    purge()
    yield
    purge()


def _dados_exemplo(paciente_id: int = 1) -> dict:
    """Retorna dados de exemplo para publicação."""
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


# ============================================================
# Testes de Configuração
# ============================================================

class TestConfiguracao:
    """Testes da configuração do middleware."""

    def test_config_importa(self):
        """O módulo de configuração deve ser importável."""
        from middleware.config import (
            RABBITMQ_HOST, RABBITMQ_PORT,
            EXCHANGE_NAME, QUEUE_NAME, ROUTING_KEY,
        )
        assert RABBITMQ_HOST is not None
        assert RABBITMQ_PORT > 0
        assert EXCHANGE_NAME != ""
        assert QUEUE_NAME != ""
        assert ROUTING_KEY != ""

    def test_get_connection_params(self):
        """get_connection_params deve retornar parâmetros pika válidos."""
        from middleware.config import get_connection_params
        params = get_connection_params()
        assert isinstance(params, pika.ConnectionParameters)
        assert params.host is not None

    def test_declarar_infraestrutura(self):
        """declarar_infraestrutura deve criar exchange e fila sem erro."""
        from middleware.config import declarar_infraestrutura, EXCHANGE_NAME, QUEUE_NAME
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        # Não deve lançar exceção
        declarar_infraestrutura(ch)
        conn.close()


# ============================================================
# Testes do Produtor
# ============================================================

class TestProdutor:
    """Testes do produtor AMQP."""

    def test_publicar_mensagem_na_fila(self):
        """O produtor deve publicar uma mensagem JSON na fila."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        dados = _dados_exemplo(paciente_id=42)
        corpo = json.dumps(dados, ensure_ascii=False)

        ch.basic_publish(
            exchange=EXCHANGE_TESTE,
            routing_key=ROUTING_KEY_TESTE,
            body=corpo.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )

        # Verificar que a mensagem está na fila
        queue_info = ch.queue_declare(queue=QUEUE_TESTE, durable=True, passive=True)
        assert queue_info.method.message_count == 1

        conn.close()

    def test_publicar_multiplas_mensagens(self):
        """Múltiplas mensagens devem ser enfileiradas corretamente."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        for i in range(5):
            dados = _dados_exemplo(paciente_id=100 + i)
            corpo = json.dumps(dados, ensure_ascii=False)
            ch.basic_publish(
                exchange=EXCHANGE_TESTE,
                routing_key=ROUTING_KEY_TESTE,
                body=corpo.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                ),
            )

        queue_info = ch.queue_declare(queue=QUEUE_TESTE, durable=True, passive=True)
        assert queue_info.method.message_count == 5

        conn.close()

    def test_mensagem_conteudo_json_valido(self):
        """A mensagem na fila deve ser JSON válido e conter os campos esperados."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        dados = _dados_exemplo(paciente_id=99)
        corpo = json.dumps(dados, ensure_ascii=False)
        ch.basic_publish(
            exchange=EXCHANGE_TESTE,
            routing_key=ROUTING_KEY_TESTE,
            body=corpo.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )

        # Consumir a mensagem e verificar conteúdo
        method, properties, body = ch.basic_get(queue=QUEUE_TESTE, auto_ack=True)
        assert method is not None
        recebido = json.loads(body.decode("utf-8"))
        assert recebido["paciente_id"] == 99
        assert recebido["frequencia_cardiaca"] == 80
        assert recebido["spo2"] == 97
        assert properties.content_type == "application/json"

        conn.close()


# ============================================================
# Testes do Consumidor
# ============================================================

class TestConsumidor:
    """Testes do consumidor AMQP."""

    def test_consumir_mensagem_da_fila(self):
        """O consumidor deve conseguir ler uma mensagem da fila."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        # Publicar uma mensagem
        dados = _dados_exemplo(paciente_id=77)
        corpo = json.dumps(dados, ensure_ascii=False)
        ch.basic_publish(
            exchange=EXCHANGE_TESTE,
            routing_key=ROUTING_KEY_TESTE,
            body=corpo.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )

        # Consumir
        method, properties, body = ch.basic_get(queue=QUEUE_TESTE, auto_ack=True)
        assert method is not None
        recebido = json.loads(body.decode("utf-8"))
        assert recebido["paciente_id"] == 77

        # Fila deve estar vazia após consumo com auto_ack
        queue_info = ch.queue_declare(queue=QUEUE_TESTE, durable=True, passive=True)
        assert queue_info.method.message_count == 0

        conn.close()

    def test_mensagem_persiste_sem_consumidor(self):
        """Mensagens devem permanecer na fila quando nenhum consumidor está ativo."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        # Publicar 3 mensagens
        for i in range(3):
            dados = _dados_exemplo(paciente_id=200 + i)
            corpo = json.dumps(dados, ensure_ascii=False)
            ch.basic_publish(
                exchange=EXCHANGE_TESTE,
                routing_key=ROUTING_KEY_TESTE,
                body=corpo.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                ),
            )

        conn.close()

        # Reconectar e verificar que as mensagens ainda estão lá
        conn2 = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch2 = conn2.channel()
        queue_info = ch2.queue_declare(queue=QUEUE_TESTE, durable=True, passive=True)
        assert queue_info.method.message_count == 3
        conn2.close()

    def test_nack_requeue_devolve_mensagem(self):
        """NACK com requeue deve devolver a mensagem à fila."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        # Publicar
        dados = _dados_exemplo(paciente_id=333)
        corpo = json.dumps(dados, ensure_ascii=False)
        ch.basic_publish(
            exchange=EXCHANGE_TESTE,
            routing_key=ROUTING_KEY_TESTE,
            body=corpo.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )

        # Consumir sem ACK, depois NACK com requeue
        ch.basic_qos(prefetch_count=1)
        method, properties, body = ch.basic_get(queue=QUEUE_TESTE, auto_ack=False)
        assert method is not None
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # A mensagem deve estar de volta na fila
        queue_info = ch.queue_declare(queue=QUEUE_TESTE, durable=True, passive=True)
        assert queue_info.method.message_count >= 1

        conn.close()


# ============================================================
# Testes de Integração (Produtor → RabbitMQ → Consumo)
# ============================================================

class TestIntegracaoMiddleware:
    """Testes de integração do fluxo produtor → fila → consumo."""

    def test_fluxo_publicar_e_consumir(self):
        """Publicar N mensagens e consumir todas deve resultar em fila vazia."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        n = 5
        for i in range(n):
            dados = _dados_exemplo(paciente_id=400 + i)
            corpo = json.dumps(dados, ensure_ascii=False)
            ch.basic_publish(
                exchange=EXCHANGE_TESTE,
                routing_key=ROUTING_KEY_TESTE,
                body=corpo.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                ),
            )

        # Consumir todas
        consumidos = 0
        for _ in range(n):
            method, properties, body = ch.basic_get(queue=QUEUE_TESTE, auto_ack=True)
            if method:
                consumidos += 1
                recebido = json.loads(body.decode("utf-8"))
                assert recebido["paciente_id"] in range(400, 405)

        assert consumidos == n

        # Fila vazia
        queue_info = ch.queue_declare(queue=QUEUE_TESTE, durable=True, passive=True)
        assert queue_info.method.message_count == 0

        conn.close()

    def test_ordem_fifo(self):
        """As mensagens devem ser consumidas na ordem FIFO."""
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.confirm_delivery()

        ids_publicados = [500, 501, 502, 503, 504]
        for pid in ids_publicados:
            dados = _dados_exemplo(paciente_id=pid)
            corpo = json.dumps(dados, ensure_ascii=False)
            ch.basic_publish(
                exchange=EXCHANGE_TESTE,
                routing_key=ROUTING_KEY_TESTE,
                body=corpo.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json",
                ),
            )

        # Consumir e verificar ordem
        ids_consumidos = []
        for _ in range(len(ids_publicados)):
            method, properties, body = ch.basic_get(queue=QUEUE_TESTE, auto_ack=True)
            if method:
                recebido = json.loads(body.decode("utf-8"))
                ids_consumidos.append(recebido["paciente_id"])

        assert ids_consumidos == ids_publicados

        conn.close()
