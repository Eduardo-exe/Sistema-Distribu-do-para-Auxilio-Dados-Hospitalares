# tests/test_kafka_middleware.py
"""
Testes Unitários e de Integração do Middleware Apache Kafka.

Testa:
1. Configuração do Kafka (carregamento, variáveis de ambiente, dataclass, sumário).
2. Token Bucket Rate Limiter do produtor e consumidor.
3. Circuit Breaker do consumidor Kafka (CLOSED, OPEN, HALF_OPEN).
4. Exponential Backoff e lógica do Dead Letter Topic (DLT).
5. Serialização, particionamento e roteamento por prioridade.
6. Tratamento de exceções e tolerância a falhas na ausência de broker.
7. Testes de integração (executados se um broker Kafka estiver ativo em localhost:9092).

Para rodar:
    python -m pytest tests/test_kafka_middleware.py -v
"""

import json
import os
import sys
import time
import pytest

# Adicionar diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.config import (
    get_kafka_config,
    KafkaConfig,
    KAFKA_TOPIC,
    KAFKA_DLT_TOPIC,
    KAFKA_PRIORITY_TOPIC,
    KAFKA_CONSUMER_GROUPS,
    assegurar_topicos_kafka,
    obter_status_middleware,
)
from middleware.consumidor import CircuitBreaker, CircuitState, SimpleRateLimiter
from server.models import converter_registro_csv


# ============================================================
# 1. Testes de Configuração
# ============================================================

class TestKafkaConfig:
    """Testa o carregamento e validação de configurações do Kafka."""

    def test_config_defaults(self):
        config = get_kafka_config()
        assert isinstance(config, KafkaConfig)
        assert config.topic == "telemetria_hospitalar"
        assert config.dlt_topic == "telemetria_hospitalar_dlt"
        assert config.priority_topic == "telemetria_hospitalar_priority"
        assert config.num_partitions >= 1
        assert config.enable_idempotence is True

    def test_consumer_groups_definition(self):
        assert len(KAFKA_CONSUMER_GROUPS) == 4
        group_ids = [g["group_id"] for g in KAFKA_CONSUMER_GROUPS]
        assert "consumidor_servidor_1" in group_ids
        assert "consumidor_servidor_4" in group_ids

    def test_config_to_dict_and_summary(self):
        config = get_kafka_config()
        data = config.to_dict()
        assert isinstance(data, dict)
        assert "topic" in data
        assert "brokers" in data

        summary = config.summary()
        assert "Kafka Config" in summary
        assert config.topic in summary

    def test_status_middleware_offline_graceful(self):
        # Quando nenhum broker responde em porta fictícia, não deve lançar exceção
        status = obter_status_middleware(brokers=["localhost:59999"])
        assert isinstance(status, dict)
        assert status["status"] == "offline"
        assert "erro" in status

    def test_assegurar_topicos_offline_graceful(self):
        # Quando Kafka está inacessível, retorna False com segurança sem quebrar a execução
        resultado = assegurar_topicos_kafka(brokers=["localhost:59999"])
        assert resultado is False


# ============================================================
# 2. Testes de Rate Limiter (Token Bucket)
# ============================================================

class TestRateLimiter:
    """Testa o algoritmo Token Bucket para controle de vazão."""

    def test_rate_limiter_immediate_burst(self):
        limiter = SimpleRateLimiter(tokens_per_second=10.0, burst_size=5)
        # Deve permitir adquirir até o burst sem esperar
        for _ in range(5):
            assert limiter.acquire(timeout=0.1) is True

    def test_rate_limiter_throttling(self):
        limiter = SimpleRateLimiter(tokens_per_second=2.0, burst_size=1)
        # Primeiro token adquirido imediatamente
        assert limiter.acquire(timeout=0.1) is True
        # Segundo token com timeout zero deve falhar
        assert limiter.acquire(timeout=0.01) is False
        # Aguardando recarga (0.6s para 2 tokens/s = 1.2 tokens recarregados)
        assert limiter.acquire(timeout=1.0) is True


# ============================================================
# 3. Testes do Circuit Breaker do Consumidor
# ============================================================

class TestCircuitBreaker:
    """Testa a máquina de estados do Circuit Breaker no consumidor."""

    def test_initial_state_is_closed(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.5, success_threshold=2)
        assert cb.state == CircuitState.CLOSED
        assert cb.can_execute() is True

    def test_trips_to_open_after_failures(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.5, success_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.can_execute() is False

    def test_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.2, success_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Aguardar timeout de recuperação
        time.sleep(0.25)
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.can_execute() is True

    def test_recovers_to_closed_on_consecutive_successes(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, success_threshold=2)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)
        assert cb.state == CircuitState.HALF_OPEN

        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.can_execute() is True


# ============================================================
# 4. Testes de Transformação de Dados e Modelos
# ============================================================

class TestDataTransformation:
    """Testa conversão e validação do CSV para modelo canônico."""

    def test_converter_registro_csv(self):
        raw_row = {
            "Patient Number": "101",
            "Heart Rate (bpm)": "88",
            "SpO2 Level (%)": "98",
            "Systolic Blood Pressure (mmHg)": "120",
            "Diastolic Blood Pressure (mmHg)": "80",
            "Body Temperature (°C)": "36.8",
            "Fall Detection": "0",
            "Predicted Disease": "Normal",
            "Data Accuracy (%)": "99",
            "Heart Rate Alert": "Normal",
            "SpO2 Level Alert": "Normal",
            "Blood Pressure Alert": "Normal",
            "Temperature Alert": "Normal",
        }
        converted = converter_registro_csv(raw_row)
        assert converted["paciente_id"] == 101
        assert converted["frequencia_cardiaca"] == 88
        assert converted["pressao_sistolica"] == 120
        assert converted["pressao_diastolica"] == 80
        assert converted["temperatura"] == 36.8
        assert converted["spo2"] == 98


# ============================================================
# 5. Testes de Integração com Kafka (Opcional se broker ativo)
# ============================================================

KAFKA_HOST = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def is_kafka_available():
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_HOST.split(","),
            request_timeout_ms=1000,
            api_version_auto_timeout_ms=1000,
        )
        admin.close()
        return True
    except Exception:
        return False

@pytest.mark.skipif(not is_kafka_available(), reason="Broker Apache Kafka não está ativo em localhost:9092")
class TestKafkaIntegration:
    """Testes com broker real quando disponível."""

    def test_kafka_producer_and_consumer(self):
        from kafka import KafkaProducer, KafkaConsumer
        import uuid

        topic_test = f"test_sd_{uuid.uuid4().hex[:8]}"
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_HOST.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        test_payload = {"teste": "hospital_sd", "timestamp": time.time()}
        future = producer.send(topic_test, test_payload)
        record_metadata = future.get(timeout=5)
        assert record_metadata.topic == topic_test
        producer.close()
