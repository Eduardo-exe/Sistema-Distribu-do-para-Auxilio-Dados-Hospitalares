# middleware/config.py
"""
Configuração centralizada do middleware Kafka — Sistema Distribuído Hospitalar.

Suporta:
- Configuração via variáveis de ambiente com fallback
- Múltiplos brokers Kafka para alta disponibilidade
- Parâmetros de tópicos, partições e replicação
- Consumer groups para processamento paralelo
- Configuração de serialização e compressão
- Parâmetros de retry e dead letter topic (DLT)
- Circuit Breaker, Rate Limiting e observabilidade

As configurações podem ser sobrescritas via variáveis de ambiente
para facilitar o deploy em Docker, VMs ou Kubernetes.
"""

import os
import logging
from dataclasses import dataclass


logger = logging.getLogger("middleware.config")


# ============================================================
# Conexão com o Kafka
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# Lista de brokers separada por vírgula
KAFKA_BROKERS = [
    s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()
]


# ============================================================
# Tópico principal e Dead Letter Topic (DLT)
# ============================================================

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetria_hospitalar")
KAFKA_DLT_TOPIC = os.getenv("KAFKA_DLT_TOPIC", "telemetria_hospitalar_dlt")
KAFKA_PRIORITY_TOPIC = os.getenv("KAFKA_PRIORITY_TOPIC", "telemetria_hospitalar_priority")

# Número de partições (usado na criação automática do tópico)
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


# ============================================================
# Consumer Groups
# ============================================================

KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "consumidor_hospitalar")

# Grupos dedicados para replicação em cada servidor backend
KAFKA_CONSUMER_GROUPS = [
    {
        "group_id": "consumidor_servidor_1",
        "servidor_url": os.getenv("SERVER_1_URL", "http://localhost:8001"),
        "server_id": "server-01",
    },
    {
        "group_id": "consumidor_servidor_2",
        "servidor_url": os.getenv("SERVER_2_URL", "http://localhost:8002"),
        "server_id": "server-02",
    },
    {
        "group_id": "consumidor_servidor_3",
        "servidor_url": os.getenv("SERVER_3_URL", "http://localhost:8003"),
        "server_id": "server-03",
    },
    {
        "group_id": "consumidor_servidor_4",
        "servidor_url": os.getenv("SERVER_4_URL", "http://localhost:8004"),
        "server_id": "server-04",
    },
]


# ============================================================
# Producer Config
# ============================================================

KAFKA_ACKS = os.getenv("KAFKA_ACKS", "all")  # 0, 1, all
KAFKA_RETRIES = int(os.getenv("KAFKA_RETRIES", "3"))
KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "16384"))  # bytes
KAFKA_LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))  # ms
KAFKA_BUFFER_MEMORY = int(os.getenv("KAFKA_BUFFER_MEMORY", "33554432"))  # 32MB
KAFKA_COMPRESSION_TYPE = os.getenv("KAFKA_COMPRESSION_TYPE", "gzip")  # none, gzip, snappy, lz4, zstd
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", "1048576"))  # 1MB

# Idempotência (evita duplicatas no producer)
KAFKA_ENABLE_IDEMPOTENCE = os.getenv("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() == "true"


# ============================================================
# Consumer Config
# ============================================================

KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")  # earliest, latest
KAFKA_ENABLE_AUTO_COMMIT = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
KAFKA_AUTO_COMMIT_INTERVAL = int(os.getenv("KAFKA_AUTO_COMMIT_INTERVAL", "5000"))  # ms
KAFKA_MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "10"))
KAFKA_MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))  # 5min
KAFKA_SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))  # 30s
KAFKA_HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))  # 10s
KAFKA_FETCH_MAX_BYTES = int(os.getenv("KAFKA_FETCH_MAX_BYTES", "52428800"))  # 50MB


# ============================================================
# Dead Letter Topic (DLT) — Retry
# ============================================================

DLT_MAX_RETRIES = int(os.getenv("DLT_MAX_RETRIES", "5"))
DLT_RETRY_BASE_DELAY = float(os.getenv("DLT_RETRY_BASE_DELAY", "1.0"))
DLT_RETRY_MAX_DELAY = float(os.getenv("DLT_RETRY_MAX_DELAY", "60.0"))
DLT_RETRY_MULTIPLIER = float(os.getenv("DLT_RETRY_MULTIPLIER", "2.0"))


# ============================================================
# Circuit Breaker
# ============================================================

CIRCUIT_BREAKER_ENABLED = os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true"
CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
CIRCUIT_BREAKER_RECOVERY_TIMEOUT = float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "30.0"))
CIRCUIT_BREAKER_SUCCESS_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_SUCCESS_THRESHOLD", "3"))


# ============================================================
# Rate Limiting
# ============================================================

RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_TOKENS_PER_SECOND = float(os.getenv("RATE_LIMIT_TOKENS_PER_SECOND", "100.0"))
RATE_LIMIT_BURST_SIZE = int(os.getenv("RATE_LIMIT_BURST_SIZE", "200"))


# ============================================================
# URL do Load Balancer (consumidor modo legado)
# ============================================================

LOAD_BALANCER_URL = os.getenv("LOAD_BALANCER_URL", "http://localhost:8000")


# ============================================================
# Logging
# ============================================================

LOG_LEVEL = os.getenv("MIDDLEWARE_LOG_LEVEL", "INFO")


# ============================================================
# Dataclass de Configuração Consolidada
# ============================================================

@dataclass
class KafkaConfig:
    """Configuração consolidada do middleware Kafka."""

    # Brokers
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    brokers: list = None

    # Tópicos
    topic: str = KAFKA_TOPIC
    dlt_topic: str = KAFKA_DLT_TOPIC
    priority_topic: str = KAFKA_PRIORITY_TOPIC
    num_partitions: int = KAFKA_NUM_PARTITIONS
    replication_factor: int = KAFKA_REPLICATION_FACTOR

    # Consumer
    group_id: str = KAFKA_GROUP_ID
    auto_offset_reset: str = KAFKA_AUTO_OFFSET_RESET
    enable_auto_commit: bool = KAFKA_ENABLE_AUTO_COMMIT
    max_poll_records: int = KAFKA_MAX_POLL_RECORDS

    # Producer
    acks: str = KAFKA_ACKS
    retries: int = KAFKA_RETRIES
    compression_type: str = KAFKA_COMPRESSION_TYPE
    enable_idempotence: bool = KAFKA_ENABLE_IDEMPOTENCE
    batch_size: int = KAFKA_BATCH_SIZE
    linger_ms: int = KAFKA_LINGER_MS

    # DLT
    dlt_max_retries: int = DLT_MAX_RETRIES
    dlt_retry_base_delay: float = DLT_RETRY_BASE_DELAY

    # Circuit Breaker
    circuit_breaker_enabled: bool = CIRCUIT_BREAKER_ENABLED
    circuit_breaker_threshold: int = CIRCUIT_BREAKER_FAILURE_THRESHOLD
    circuit_breaker_timeout: float = CIRCUIT_BREAKER_RECOVERY_TIMEOUT

    # Rate Limiting
    rate_limit_enabled: bool = RATE_LIMIT_ENABLED
    rate_limit_tps: float = RATE_LIMIT_TOKENS_PER_SECOND

    def __post_init__(self):
        if self.brokers is None:
            self.brokers = KAFKA_BROKERS

    def validate(self) -> list[str]:
        """Valida a configuração e retorna lista de avisos."""
        warnings = []

        if not self.brokers:
            warnings.append("Nenhum broker Kafka configurado!")

        if self.num_partitions <= 0:
            warnings.append(f"Número de partições inválido: {self.num_partitions}")

        if self.acks not in ("0", "1", "all"):
            warnings.append(f"Valor de acks inválido: {self.acks}")

        if self.max_poll_records <= 0:
            warnings.append(f"max_poll_records deve ser positivo: {self.max_poll_records}")

        return warnings

    def summary(self) -> str:
        """Retorna um resumo da configuração."""
        return (
            f"Kafka Config\n"
            f"  Brokers:     {', '.join(self.brokers)}\n"
            f"  Tópico:      {self.topic}\n"
            f"  Partições:   {self.num_partitions}\n"
            f"  Replicação:  {self.replication_factor}\n"
            f"  Group ID:    {self.group_id}\n"
            f"  Acks:        {self.acks}\n"
            f"  Compressão:  {self.compression_type}\n"
            f"  Idempotente: {'sim' if self.enable_idempotence else 'não'}\n"
            f"  Auto Commit: {'sim' if self.enable_auto_commit else 'não'}\n"
            f"  DLT:         {self.dlt_topic}\n"
            f"  Circuit Breaker: {'ativo' if self.circuit_breaker_enabled else 'inativo'}\n"
            f"  Rate Limit:  {'ativo' if self.rate_limit_enabled else 'inativo'}"
        )

    def to_dict(self) -> dict:
        """Retorna a configuração serializada em dicionário."""
        from dataclasses import asdict
        return asdict(self)


# Instância global
_kafka_config = KafkaConfig()


def get_kafka_config() -> KafkaConfig:
    """Retorna a configuração global do Kafka."""
    return _kafka_config


def configurar_logging():
    """Configura o sistema de logging do middleware Kafka."""
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    logger.info("Logging do middleware Kafka configurado: nível=%s", LOG_LEVEL)


# Compatibilidade com componentes internos
FILAS_SERVIDORES = KAFKA_CONSUMER_GROUPS
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "10"))
HEALTH_CHECK_TIMEOUT = int(os.getenv("HEALTH_CHECK_TIMEOUT", "5"))
HEALTH_CHECK_UNHEALTHY_THRESHOLD = int(os.getenv("HEALTH_CHECK_UNHEALTHY_THRESHOLD", "3"))

def get_config() -> KafkaConfig:
    """Alias para get_kafka_config."""
    return get_kafka_config()


def assegurar_topicos_kafka(
    brokers: list[str] = None,
    topicos: list[str] = None,
    partitions: int = None,
    replication_factor: int = None,
) -> bool:
    """
    Verifica se os tópicos configurados existem no Apache Kafka.
    Caso não existam, provisiona-os com as partições e replicação definidas.

    Retorna True se os tópicos estão prontos/criados, False em caso de falha.
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable, KafkaError

    cfg = get_kafka_config()
    brokers = brokers or cfg.brokers
    num_parts = partitions if partitions is not None else cfg.num_partitions
    rep_fact = replication_factor if replication_factor is not None else cfg.replication_factor

    alvo_topicos = topicos or [
        cfg.topic,
        cfg.dlt_topic,
        cfg.priority_topic,
    ]

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=brokers,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        existentes = set(admin.list_topics())
        novos = []
        for t in alvo_topicos:
            if t not in existentes:
                novos.append(
                    NewTopic(name=t, num_partitions=num_parts, replication_factor=rep_fact)
                )

        if novos:
            try:
                admin.create_topics(new_topics=novos, validate_only=False)
                logger.info("✓ Tópicos Kafka provisionados: %s", [n.name for n in novos])
            except TopicAlreadyExistsError:
                pass
        else:
            logger.debug("Todos os tópicos Kafka já existem: %s", alvo_topicos)

        admin.close()
        return True
    except (NoBrokersAvailable, KafkaError) as e:
        logger.warning("Kafka indisponível para assegurar tópicos (%s): %s", brokers, e)
        return False
    except Exception as e:
        logger.warning("Falha inesperada ao assegurar tópicos Kafka: %s", e)
        return False


def obter_status_middleware(brokers: list[str] = None) -> dict:
    """
    Consulta o estado operacional do middleware Apache Kafka.

    Retorna um dicionário com status, brokers e tópicos disponíveis.
    """
    from kafka.admin import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable, KafkaError

    cfg = get_kafka_config()
    brokers = brokers or cfg.brokers

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=brokers,
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000,
        )
        topicos = admin.list_topics()
        admin.close()
        return {
            "status": "online",
            "brokers": brokers,
            "topico_principal": cfg.topic,
            "topico_dlt": cfg.dlt_topic,
            "topico_prioridade": cfg.priority_topic,
            "topicos_disponiveis": sorted(topicos),
            "consumer_groups_configurados": len(KAFKA_CONSUMER_GROUPS),
        }
    except (NoBrokersAvailable, KafkaError, Exception) as e:
        return {
            "status": "offline",
            "brokers": brokers,
            "erro": str(e),
        }

