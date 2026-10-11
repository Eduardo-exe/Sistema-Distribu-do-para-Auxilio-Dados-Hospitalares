# middleware/config.py
"""
Configuração centralizada do middleware RabbitMQ (MOM).

Marco 3 — Replicação de Dados com Padrão Fanout / Publish-Subscribe:
- Exchange do tipo 'fanout' para broadcast simultâneo de sinais vitais
- Filas dedicadas para cada servidor backend (fila_servidor_1 a 4)
- Sincronização e espelhamento integral de telemetria entre SQLite locais

As configurações podem ser sobrescritas via variáveis de ambiente
para facilitar o deploy em máquinas virtuais heterogêneas ou Docker.
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
# Exchange e Filas (Marco 3: Replicação Fanout)
# ============================================================

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "telemetria_hospitalar")
# Marco 3: Padrão Fanout para replicação entre múltiplos bancos
EXCHANGE_TYPE = os.getenv("EXCHANGE_TYPE", "fanout")
QUEUE_NAME = os.getenv("QUEUE_NAME", "fila_dados_pacientes")
ROUTING_KEY = os.getenv("ROUTING_KEY", "dados.paciente")

# Filas dedicadas para replicação em cada servidor backend
FILAS_SERVIDORES = [
    {
        "fila": "fila_servidor_1",
        "servidor_url": os.getenv("SERVER_1_URL", "http://localhost:8001"),
        "server_id": "server-01",
    },
    {
        "fila": "fila_servidor_2",
        "servidor_url": os.getenv("SERVER_2_URL", "http://localhost:8002"),
        "server_id": "server-02",
    },
    {
        "fila": "fila_servidor_3",
        "servidor_url": os.getenv("SERVER_3_URL", "http://localhost:8003"),
        "server_id": "server-03",
    },
    {
        "fila": "fila_servidor_4",
        "servidor_url": os.getenv("SERVER_4_URL", "http://localhost:8004"),
        "server_id": "server-04",
    },
]

# ============================================================
# URL do Load Balancer (usado em consultas ou modo direto)
# ============================================================

LOAD_BALANCER_URL = os.getenv("LOAD_BALANCER_URL", "http://localhost:8000")

# ============================================================
# Parâmetros de qualidade de serviço (QoS)
# ============================================================

# Prefetch count: 1 mensagem por vez antes de ACK para controle de fluxo
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "1"))


# ============================================================
# Helpers de Conexão e Topologia
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
        heartbeat=600,
        blocked_connection_timeout=300,
    )


def declarar_infraestrutura(channel, exchange_type: str = None):
    """
    Declara a exchange e as filas de replicação no RabbitMQ (idempotente).
    No Marco 3, conecta todas as filas à exchange fanout para replicação total.

    Args:
        channel: Canal pika já aberto.
        exchange_type: Tipo da exchange (opcional, padrão EXCHANGE_TYPE).
    """
    import pika

    _tipo = exchange_type or EXCHANGE_TYPE

    try:
        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type=_tipo,
            durable=True,
        )
    except pika.exceptions.ChannelClosedByBroker:
        # Em caso de divergência de tipo pré-existente (ex: direct para fanout), recria
        try:
            channel.exchange_delete(exchange=EXCHANGE_NAME)
            channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=_tipo,
                durable=True,
            )
        except Exception:
            pass

    # 1. Fila única legada (mantida para compatibilidade)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(
        queue=QUEUE_NAME,
        exchange=EXCHANGE_NAME,
        routing_key=ROUTING_KEY if _tipo == "direct" else "",
    )

    # 2. Filas dedicadas de cada servidor (Marco 3 — Replicação Fanout)
    for item in FILAS_SERVIDORES:
        nome_fila = item["fila"]
        channel.queue_declare(queue=nome_fila, durable=True)
        channel.queue_bind(
            queue=nome_fila,
            exchange=EXCHANGE_NAME,
            routing_key="",  # Ignorado em fanout
        )
