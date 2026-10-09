# middleware/config.py
"""
Configuração centralizada do middleware RabbitMQ.

Define os parâmetros de conexão, exchange, fila e credenciais
utilizados pelo produtor e consumidor AMQP.

As configurações podem ser sobrescritas via variáveis de ambiente
para facilitar o deploy em VMs separadas.
"""

import os


# ============================================================
# Conexão com o RabbitMQ
# ============================================================

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

# ============================================================
# Exchange e Fila
# ============================================================

EXCHANGE_NAME = "telemetria_hospitalar"
EXCHANGE_TYPE = "direct"
QUEUE_NAME = "fila_dados_pacientes"
ROUTING_KEY = "dados.paciente"

# ============================================================
# URL do Load Balancer (usado pelo consumidor)
# ============================================================

LOAD_BALANCER_URL = os.getenv("LOAD_BALANCER_URL", "http://localhost:8000")

# ============================================================
# Parâmetros de qualidade de serviço (QoS)
# ============================================================

# Prefetch count: quantas mensagens o consumidor recebe de uma vez
# antes de enviar ACK. Valor 1 = processar uma de cada vez (mais seguro).
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "1"))

# ============================================================
# Helpers
# ============================================================

def get_connection_params():
    """
    Retorna os parâmetros de conexão do pika para o RabbitMQ.

    Returns:
        pika.ConnectionParameters configurado.
    """
    import pika

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        # Heartbeat para manter a conexão viva
        heartbeat=600,
        # Timeout de conexão bloqueante
        blocked_connection_timeout=300,
    )


def declarar_infraestrutura(channel):
    """
    Declara a exchange e a fila no RabbitMQ, criando-as caso
    não existam (idempotente).

    Args:
        channel: Canal pika já aberto.
    """
    # Declarar exchange do tipo direct
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,  # Sobrevive a reinícios do RabbitMQ
    )

    # Declarar fila durável
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,  # Mensagens sobrevivem a reinícios
    )

    # Vincular fila à exchange com a routing key
    channel.queue_bind(
        queue=QUEUE_NAME,
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY,
    )
