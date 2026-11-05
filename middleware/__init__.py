# middleware/__init__.py
"""
==============================================================================
Middleware Apache Kafka (MOM) — Sistema Distribuído Hospitalar
==============================================================================

Camada de middleware de mensageria baseada exclusivamente em Apache Kafka
para comunicação assíncrona de telemetria hospitalar de alto desempenho,
tolerância a falhas, particionamento e replicação em múltiplos servidores.

Componentes:
    - config: Configurações centrais do Kafka (brokers, tópicos, consumer groups)
    - produtor: Publicador Kafka com particionamento por paciente, compressão e idempotência
    - consumidor: Consumidor Kafka com Consumer Groups, Circuit Breaker e Dead Letter Topic (DLT)

Os símbolos públicos são resolvidos sob demanda (PEP 562). Importar os
submódulos aqui de forma antecipada faz o pacote inteiro carregar antes de
`python -m middleware.consumidor` executar o módulo, o que gera
RuntimeWarning e dificulta diagnosticar falhas de import.
"""

import importlib

_EXPORTS = {
    "config": [
        "KafkaConfig",
        "get_kafka_config",
        "assegurar_topicos_kafka",
        "obter_status_middleware",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_BROKERS",
        "KAFKA_TOPIC",
        "KAFKA_DLT_TOPIC",
        "KAFKA_PRIORITY_TOPIC",
        "KAFKA_CONSUMER_GROUPS",
    ],
    "produtor": [
        "publicar_dados_kafka",
        "criar_produtor_kafka",
        "ProdutorKafka",
    ],
    "consumidor": [
        "ConsumidorKafka",
        "CircuitBreaker",
        "CircuitState",
        "SimpleRateLimiter",
        "iniciar_multi_consumidores",
        "reprocessar_dlt",
    ],
}

__all__ = [name for names in _EXPORTS.values() for name in names]


def __getattr__(name: str):
    for modulo, nomes in _EXPORTS.items():
        if name in nomes:
            valor = getattr(importlib.import_module(f"middleware.{modulo}"), name)
            globals()[name] = valor
            return valor
    raise AttributeError(f"module 'middleware' has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + __all__)
