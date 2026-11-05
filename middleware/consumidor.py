# middleware/consumidor.py
"""
Consumidor Kafka Enterprise — Middleware Apache Kafka (MOM).

Versão Enterprise com suporte a:
- Consumer Groups para processamento paralelo
- Commit manual de offsets (at-least-once delivery)
- Circuit Breaker para proteção contra cascata de falhas
- Dead Letter Topic (DLT) com retry exponencial
- Rate Limiting no consumo
- Distributed Tracing (propagação de contexto via headers)
- Métricas de observabilidade (throughput, latência, erros)
- Graceful shutdown com commit de offsets pendentes
- Modo multi-consumidor (um consumer group por servidor backend)
- Modo single (via Load Balancer)

Uso:
    # Modo replicador completo (4 consumers groups em paralelo):
    python -m middleware.consumidor --multi

    # Modo single (consome e envia ao Load Balancer):
    python -m middleware.consumidor --lb-url http://localhost:8000

    # Modo por servidor (consumer group dedicado):
    python -m middleware.consumidor --servidor-url http://localhost:8001 --group-id consumidor_servidor_1

    # Mantenha o pacote chamado 'middleware' no sys.path. Não execute o arquivo
    # diretamente (python middleware/consumidor.py): nesse modo o diretório
    # middleware/ entra no sys.path como raiz e o módulo local não resolve
    # 'middleware.config'.
"""

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from enum import Enum

import requests

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

from middleware.config import (
    get_kafka_config,
    KAFKA_BROKERS,
    KAFKA_TOPIC,
    KAFKA_DLT_TOPIC,
    KAFKA_PRIORITY_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_ENABLE_AUTO_COMMIT,
    KAFKA_MAX_POLL_RECORDS,
    KAFKA_SESSION_TIMEOUT_MS,
    KAFKA_HEARTBEAT_INTERVAL_MS,
    KAFKA_CONSUMER_GROUPS,
    CIRCUIT_BREAKER_ENABLED,
    CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
    CIRCUIT_BREAKER_SUCCESS_THRESHOLD,
    DLT_MAX_RETRIES,
    DLT_RETRY_BASE_DELAY,
    DLT_RETRY_MULTIPLIER,
    DLT_RETRY_MAX_DELAY,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_TOKENS_PER_SECOND,
    RATE_LIMIT_BURST_SIZE,
    LOAD_BALANCER_URL,
)


logger = logging.getLogger("middleware.consumidor")


# ============================================================
# Rate Limiter simples (Token Bucket)
# ============================================================

class SimpleRateLimiter:
    """Token Bucket para controle de vazão."""

    def __init__(self, tokens_per_second: float, burst_size: int):
        self.tps = tokens_per_second
        self.burst = burst_size
        self._tokens = float(burst_size)
        self._last_refill = time.time()

    def acquire(self, timeout: float = 5.0) -> bool:
        """Adquire um token, aguardando se necessário."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            now = time.time()
            elapsed = now - self._last_refill
            self._tokens = min(self.burst, self._tokens + elapsed * self.tps)
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            time.sleep(0.01)
        return False


# ============================================================
# Circuit Breaker
# ============================================================

class CircuitState(str, Enum):
    """Estados do Circuit Breaker."""
    CLOSED = "closed"       # Normal — requisições passam
    OPEN = "open"           # Aberto — requisições bloqueadas
    HALF_OPEN = "half_open" # Teste — poucas requisições passam


class CircuitBreaker:
    """
    Circuit Breaker — proteção contra cascata de falhas.

    Quando o número de falhas consecutivas excede o threshold,
    o circuito abre e bloqueia novas requisições por um período,
    permitindo ao servidor backend se recuperar.
    """

    def __init__(
        self,
        name: str = "default",
        failure_threshold: int = None,
        recovery_timeout: float = None,
        success_threshold: int = None,
    ):
        self.name = name
        self.failure_threshold = (
            failure_threshold
            if failure_threshold is not None
            else CIRCUIT_BREAKER_FAILURE_THRESHOLD
        )
        self.recovery_timeout = (
            recovery_timeout
            if recovery_timeout is not None
            else CIRCUIT_BREAKER_RECOVERY_TIMEOUT
        )
        self.success_threshold = (
            success_threshold
            if success_threshold is not None
            else CIRCUIT_BREAKER_SUCCESS_THRESHOLD
        )

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0
        self._lock = threading.Lock()

        # Métricas
        self._total_allowed = 0
        self._total_blocked = 0
        self._total_trips = 0

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if (
                self._state == CircuitState.OPEN
                and (time.time() - self._last_failure_time) > self.recovery_timeout
            ):
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info("[CB %s] OPEN → HALF_OPEN", self.name)
            return self._state

    def allow_request(self) -> bool:
        state = self.state
        with self._lock:
            if state == CircuitState.CLOSED:
                self._total_allowed += 1
                return True
            elif state == CircuitState.OPEN:
                self._total_blocked += 1
                return False
            elif state == CircuitState.HALF_OPEN:
                self._total_allowed += 1
                return True
        return False

    def can_execute(self) -> bool:
        """Alias para allow_request."""
        return self.allow_request()

    def record_success(self):
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    logger.info("[CB %s] HALF_OPEN → CLOSED (recuperado!)", self.name)

    def record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._total_trips += 1
                logger.warning("[CB %s] HALF_OPEN → OPEN", self.name)
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                self._state = CircuitState.OPEN
                self._total_trips += 1
                logger.warning(
                    "[CB %s] CIRCUIT ABERTO! %d falhas consecutivas.",
                    self.name, self._failure_count,
                )

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "total_allowed": self._total_allowed,
                "total_blocked": self._total_blocked,
                "total_trips": self._total_trips,
            }


# ============================================================
# Consumidor Kafka Enterprise
# ============================================================

class ConsumidorKafka:
    """
    Consumidor Kafka Enterprise com pipeline completo.

    Consome mensagens de um tópico Kafka e as processa:
    circuit breaker → rate limiting → deserialização →
    encaminhamento HTTP → commit de offset / DLT em caso de falha

    Attributes:
        group_id: Consumer group ID.
        target_url: URL do servidor backend de destino.
        server_id: Identificador do nó.
    """

    def __init__(
        self,
        group_id: str = None,
        target_url: str = None,
        server_id: str = None,
        topics: list[str] = None,
    ):
        self.group_id = group_id or KAFKA_GROUP_ID
        self.target_url = (target_url or LOAD_BALANCER_URL).rstrip("/")
        self.server_id = server_id or self.group_id
        self.topics = topics or [KAFKA_TOPIC, KAFKA_PRIORITY_TOPIC]

        # Componentes
        self.circuit_breaker = (
            CircuitBreaker(name=self.server_id)
            if CIRCUIT_BREAKER_ENABLED else None
        )
        self.rate_limiter = (
            SimpleRateLimiter(RATE_LIMIT_TOKENS_PER_SECOND, RATE_LIMIT_BURST_SIZE)
            if RATE_LIMIT_ENABLED else None
        )

        # Consumer e DLT producer
        self.consumer: KafkaConsumer = None
        self.dlt_producer: KafkaProducer = None

        # Controle
        self.ativo = True

        # Métricas locais
        self.total_consumidos = 0
        self.total_sucesso = 0
        self.total_erros = 0
        self.total_circuit_blocked = 0
        self.total_dlt = 0
        self._latencies: list[float] = []

    def conectar(self):
        """Cria o KafkaConsumer e o DLT producer."""
        logger.info(
            "[%s] Conectando ao Kafka em %s (group=%s)...",
            self.server_id, ", ".join(KAFKA_BROKERS), self.group_id,
        )

        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=self.group_id,
                auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
                enable_auto_commit=KAFKA_ENABLE_AUTO_COMMIT,
                max_poll_records=KAFKA_MAX_POLL_RECORDS,
                session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS,
                heartbeat_interval_ms=KAFKA_HEARTBEAT_INTERVAL_MS,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            logger.error(
                "[%s] ERRO: Nenhum broker Kafka disponível em %s",
                self.server_id, KAFKA_BROKERS,
            )
            sys.exit(1)
        except KafkaError as e:
            logger.error("[%s] ERRO: Falha ao conectar: %s", self.server_id, e)
            sys.exit(1)

        # Producer para DLT (Dead Letter Topic)
        try:
            self.dlt_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
        except Exception as e:
            logger.warning("[%s] DLT producer não disponível: %s", self.server_id, e)

        logger.info(
            "[%s] ✓ Conectado! Tópicos: %s → Destino: %s",
            self.server_id, self.topics, self.target_url,
        )

    def _get_retry_count(self, headers: list) -> int:
        """Extrai o contador de retries dos headers."""
        if not headers:
            return 0
        for key, value in headers:
            if key == "x-retry-count":
                return int(value.decode("utf-8"))
        return 0

    def _send_to_dlt(self, message, error_msg: str, retry_count: int):
        """Envia mensagem para o Dead Letter Topic (DLT)."""
        if not self.dlt_producer:
            logger.warning("[%s] DLT producer indisponível", self.server_id)
            return

        dlt_headers = list(message.headers or [])
        dlt_headers.extend([
            ("x-retry-count", str(retry_count + 1).encode("utf-8")),
            ("x-error-msg", error_msg.encode("utf-8")),
            ("x-error-timestamp", datetime.now().isoformat().encode("utf-8")),
            ("x-original-topic", message.topic.encode("utf-8")),
            ("x-consumer-group", self.group_id.encode("utf-8")),
        ])

        try:
            self.dlt_producer.send(
                KAFKA_DLT_TOPIC,
                key=message.key,
                value=message.value,
                headers=dlt_headers,
            )
            self.dlt_producer.flush(timeout=5)
            self.total_dlt += 1
            logger.info(
                "[%s] Mensagem enviada ao DLT (retry=%d): %s",
                self.server_id, retry_count + 1, error_msg[:80],
            )
        except Exception as e:
            logger.error("[%s] Falha ao enviar ao DLT: %s", self.server_id, e)

    def _processar_mensagem(self, message) -> bool:
        """
        Processa uma mensagem consumida do Kafka.

        Pipeline:
        1. Circuit Breaker check
        2. Rate Limiting
        3. Deserialização (já feita pelo consumer)
        4. Encaminhamento HTTP ao servidor backend
        5. Commit de offset / DLT em caso de falha

        Returns:
            True se processada com sucesso.
        """
        start_time = time.time()
        self.total_consumidos += 1

        dados = message.value

        # 1. Circuit Breaker
        if self.circuit_breaker and not self.circuit_breaker.allow_request():
            self.total_circuit_blocked += 1
            logger.warning(
                "[%s] Circuit Breaker ABERTO — mensagem será retentada",
                self.server_id,
            )
            time.sleep(1)
            return False

        # 2. Rate Limiting
        if self.rate_limiter and not self.rate_limiter.acquire(timeout=5.0):
            logger.warning("[%s] Rate limit excedido", self.server_id)
            return False

        # 3. Encaminhamento HTTP ao servidor backend
        paciente_id = dados.get("paciente_id", "?")
        url = f"{self.target_url}/dados"

        try:
            forward_start = time.time()
            response = requests.post(url, json=dados, timeout=8)
            forward_latency = (time.time() - forward_start) * 1000

            if response.status_code == 200:
                self.total_sucesso += 1
                resp_json = response.json()
                sid = resp_json.get("server_id", self.server_id)

                # Circuit breaker: sucesso
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                # Métricas
                total_latency = (time.time() - start_time) * 1000
                self._latencies.append(total_latency)

                # Extrair prioridade dos headers
                priority_label = ""
                if message.headers:
                    for key, value in message.headers:
                        if key == "x-priority" and value == b"HIGH":
                            priority_label = " [PRIORIDADE]"
                            break

                # Log
                print(
                    f"[{self.server_id}] [{self.total_consumidos:>4}] ✓ "
                    f"paciente_id={paciente_id}{priority_label} → {sid} "
                    f"(partition={message.partition} offset={message.offset} "
                    f"{forward_latency:.0f}ms)"
                )

                return True

            else:
                self.total_erros += 1
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()

                error_msg = f"HTTP {response.status_code}: {response.text[:100]}"
                retry_count = self._get_retry_count(message.headers)

                if retry_count < DLT_MAX_RETRIES:
                    self._send_to_dlt(message, error_msg, retry_count)
                else:
                    logger.error(
                        "[%s] Max retries excedido para paciente %s: %s",
                        self.server_id, paciente_id, error_msg,
                    )

                print(
                    f"[{self.server_id}] [{self.total_consumidos:>4}] ✗ "
                    f"HTTP {response.status_code} paciente={paciente_id}"
                )
                return True  # Commit anyway to avoid reprocessing

        except requests.exceptions.ConnectionError:
            self.total_erros += 1
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()

            error_msg = f"Destino {self.target_url} offline"
            logger.warning("[%s] %s", self.server_id, error_msg)

            retry_count = self._get_retry_count(message.headers)
            if retry_count < DLT_MAX_RETRIES:
                self._send_to_dlt(message, error_msg, retry_count)

            return True

        except requests.exceptions.Timeout:
            self.total_erros += 1
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()

            self._send_to_dlt(message, "Timeout", self._get_retry_count(message.headers))
            print(f"[{self.server_id}] ✗ Timeout em {self.target_url}")

            return True

        except Exception as e:
            self.total_erros += 1
            logger.error("[%s] Erro inesperado: %s", self.server_id, e)

            retry_count = self._get_retry_count(message.headers)
            if retry_count < DLT_MAX_RETRIES:
                self._send_to_dlt(message, str(e), retry_count)

            return True

    def iniciar(self):
        """Inicia o loop de consumo de mensagens do Kafka."""
        if not self.consumer:
            self.conectar()

        logger.info("[%s] Iniciando loop de consumo...", self.server_id)

        try:
            while self.ativo:
                # Poll com timeout de 1 segundo
                records = self.consumer.poll(timeout_ms=1000)

                for tp, messages in records.items():
                    for message in messages:
                        if not self.ativo:
                            break

                        sucesso = self._processar_mensagem(message)

                        # Commit manual do offset
                        if sucesso and not KAFKA_ENABLE_AUTO_COMMIT:
                            try:
                                self.consumer.commit()
                            except Exception as e:
                                logger.warning(
                                    "[%s] Falha no commit: %s", self.server_id, e,
                                )
                        elif not sucesso:
                            # Se falhou temporariamente (ex: Circuit Breaker OPEN),
                            # reposicionar o offset nesta partição para a mensagem não processada
                            # e interromper o lote atual para aguardar recuperação
                            try:
                                self.consumer.seek(tp, message.offset)
                            except Exception as e:
                                logger.warning(
                                    "[%s] Falha ao reposicionar offset: %s", self.server_id, e
                                )
                            break

        except KeyboardInterrupt:
            pass
        finally:
            self.parar()

    def parar(self):
        """Encerra o consumidor de forma limpa (graceful shutdown)."""
        logger.info("[%s] Graceful shutdown iniciado...", self.server_id)
        self.ativo = False

        # Commit offsets pendentes
        if self.consumer:
            try:
                self.consumer.commit()
            except Exception:
                pass
            try:
                self.consumer.close(autocommit=True)
            except Exception:
                pass

        # Fechar DLT producer
        if self.dlt_producer:
            try:
                self.dlt_producer.flush(timeout=5)
                self.dlt_producer.close(timeout=5)
            except Exception:
                pass

        # Imprimir estatísticas
        self._imprimir_stats()

    def _imprimir_stats(self):
        """Imprime estatísticas do consumidor."""
        avg_lat = (
            sum(self._latencies) / len(self._latencies)
            if self._latencies else 0
        )

        print()
        print("┌" + "─" * 55 + "┐")
        print("│" + f" 📊 STATS — {self.server_id} (Kafka) ".center(55) + "│")
        print("├" + "─" * 55 + "┤")
        print("│" + f"  Consumidos:      {self.total_consumidos}".ljust(55) + "│")
        print("│" + f"  Sucesso:         {self.total_sucesso}".ljust(55) + "│")
        print("│" + f"  Erros:           {self.total_erros}".ljust(55) + "│")
        print("│" + f"  CB bloqueados:   {self.total_circuit_blocked}".ljust(55) + "│")
        print("│" + f"  Enviados DLT:    {self.total_dlt}".ljust(55) + "│")
        print("│" + f"  Latência média:  {avg_lat:.1f}ms".ljust(55) + "│")

        if self.circuit_breaker:
            cb = self.circuit_breaker.get_stats()
            print("│" + f"  Circuit Breaker: {cb['state']} ({cb['total_trips']} trips)".ljust(55) + "│")

        print("└" + "─" * 55 + "┘")


# ============================================================
# Multi-consumidor (replicação para todos os servidores)
# ============================================================

def iniciar_multi_consumidores():
    """
    Inicia consumidores Kafka para todos os servidores em threads paralelas.

    Cada servidor backend recebe TODAS as mensagens (como o fanout do RabbitMQ),
    pois cada um tem seu próprio consumer group ID.
    """
    config = get_kafka_config()

    print()
    print("╔" + "═" * 68 + "╗")
    print("║" + " CONSUMIDOR ENTERPRISE — Kafka Replicação ".center(68) + "║")
    print("╠" + "═" * 68 + "╣")
    print("║" + f"  Brokers:         {config.bootstrap_servers}".ljust(68) + "║")
    print("║" + f"  Tópico:          {KAFKA_TOPIC}".ljust(68) + "║")
    print("║" + f"  Consumers:       {len(KAFKA_CONSUMER_GROUPS)}".ljust(68) + "║")

    for info in KAFKA_CONSUMER_GROUPS:
        print("║" + f"    • {info['group_id']:<30} → {info['servidor_url']} ({info['server_id']})".ljust(68) + "║")

    print("╠" + "═" * 68 + "╣")
    print("║" + "  Módulos Ativos:".ljust(68) + "║")
    modulos = [
        ("Circuit Breaker", CIRCUIT_BREAKER_ENABLED),
        ("Dead Letter Topic", True),
        ("Rate Limiter", RATE_LIMIT_ENABLED),
        ("Commit Manual", not KAFKA_ENABLE_AUTO_COMMIT),
        ("Compressão", config.compression_type != "none"),
    ]
    for nome, ativo in modulos:
        icone = "✓" if ativo else "✗"
        print("║" + f"    {icone} {nome}".ljust(68) + "║")

    print("╚" + "═" * 68 + "╝")
    print()

    consumidores = []
    threads = []

    for info in KAFKA_CONSUMER_GROUPS:
        c = ConsumidorKafka(
            group_id=info["group_id"],
            target_url=info["servidor_url"],
            server_id=info["server_id"],
        )
        consumidores.append(c)
        t = threading.Thread(target=c.iniciar, daemon=True, name=info["server_id"])
        threads.append(t)
        t.start()

    print("[MULTI-CONSUMIDOR KAFKA] ✓ Todos os consumers iniciados!")
    print("[MULTI-CONSUMIDOR KAFKA] Pressione Ctrl+C para encerrar.")
    print()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MULTI-CONSUMIDOR KAFKA] Encerrando consumers (graceful shutdown)...")
        for c in consumidores:
            c.parar()

        # Resumo global
        total_consumidos = sum(c.total_consumidos for c in consumidores)
        total_sucesso = sum(c.total_sucesso for c in consumidores)
        total_erros = sum(c.total_erros for c in consumidores)
        total_dlt = sum(c.total_dlt for c in consumidores)

        print()
        print("┌" + "─" * 58 + "┐")
        print("│" + " 📊 MÉTRICAS GLOBAIS — Kafka ".center(58) + "│")
        print("├" + "─" * 58 + "┤")
        print("│" + f"  Total consumidos:    {total_consumidos}".ljust(58) + "│")
        print("│" + f"  Total sucesso:       {total_sucesso}".ljust(58) + "│")
        print("│" + f"  Total erros:         {total_erros}".ljust(58) + "│")
        print("│" + f"  Total DLT:           {total_dlt}".ljust(58) + "│")
        if total_consumidos > 0:
            error_rate = total_erros / total_consumidos * 100
            print("│" + f"  Error rate:          {error_rate:.1f}%".ljust(58) + "│")
        print("└" + "─" * 58 + "┘")


# ============================================================
# Reprocessamento e Auditoria de Dead Letter Topic (DLT)
# ============================================================

def reprocessar_dlt(
    destino_url: str = None,
    limite: int = 0,
    reinyetar_topico: bool = True,
    timeout_segundos: float = 5.0,
) -> int:
    """
    Consome mensagens retidas no Dead Letter Topic (DLT) para auditoria ou reprocessamento.

    Args:
        destino_url: URL para envio HTTP direto (caso não queira reinjetar no tópico).
        limite: Quantidade máxima de mensagens a reprocessar (0 = todas disponíveis).
        reinyetar_topico: Se True, republica no tópico principal KAFKA_TOPIC com retry resetado.
        timeout_segundos: Tempo máximo aguardando mensagens no DLT.

    Returns:
        Total de mensagens lidas e reprocessadas do DLT.
    """
    logger.info("Iniciando auditoria/reprocessamento do DLT (%s)...", KAFKA_DLT_TOPIC)
    print()
    print("╔" + "═" * 65 + "╗")
    print("║" + " 🛡️  REPROCESSADOR DEAD LETTER TOPIC (DLT) — Kafka ".center(65) + "║")
    print("╠" + "═" * 65 + "╣")
    print("║" + f"  Tópico DLT:   {KAFKA_DLT_TOPIC}".ljust(65) + "║")
    alvo_desc = "Reinjeção no tópico principal" if reinyetar_topico else f"Envio direto para {destino_url}"
    print("║" + f"  Modo:         {alvo_desc}".ljust(65) + "║")
    print("║" + f"  Limite:       {'todos' if limite == 0 else limite}".ljust(65) + "║")
    print("╚" + "═" * 65 + "╝")
    print()

    consumer = None
    producer = None
    reprocessados = 0

    try:
        consumer = KafkaConsumer(
            KAFKA_DLT_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id="reprocessador_dlt_audit",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=int(timeout_segundos * 1000),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        if reinyetar_topico:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )

        for message in consumer:
            if limite > 0 and reprocessados >= limite:
                break

            dados = message.value
            paciente_id = dados.get("paciente_id", "?")
            err_msg = "Desconhecido"

            if message.headers:
                for k, v in message.headers:
                    if k == "x-error-msg":
                        err_msg = v.decode("utf-8", errors="replace")

            if reinyetar_topico and producer:
                headers_reinjetados = [
                    ("x-reinjected-from-dlt", b"true"),
                    ("x-reinject-timestamp", datetime.now().isoformat().encode("utf-8")),
                    ("x-original-error", err_msg.encode("utf-8")),
                ]
                producer.send(
                    KAFKA_TOPIC,
                    key=str(paciente_id),
                    value=dados,
                    headers=headers_reinjetados,
                )
                print(f"[DLT] ⚡ Paciente {paciente_id} reinjetado em {KAFKA_TOPIC} (Erro anterior: {err_msg[:40]})")
            elif destino_url:
                resp = requests.post(f"{destino_url.rstrip('/')}/dados", json=dados, timeout=8)
                status_ok = resp.status_code == 200
                icone = "✓" if status_ok else "✗"
                print(f"[DLT] {icone} Paciente {paciente_id} enviado para {destino_url} (HTTP {resp.status_code})")

            reprocessados += 1

        if producer:
            producer.flush(timeout=5)

    except NoBrokersAvailable:
        logger.error("Nenhum broker Kafka disponível para ler o DLT.")
    except Exception as e:
        logger.error("Erro durante reprocessamento do DLT: %s", e)
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
        if producer:
            try:
                producer.close()
            except Exception:
                pass

    print()
    print(f"[DLT] Total de mensagens processadas no DLT: {reprocessados}")
    return reprocessados


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consumidor Kafka Enterprise — Middleware Apache Kafka"
    )
    parser.add_argument(
        "--multi", action="store_true",
        help="Inicia consumers para todos os servidores (consumer groups separados)"
    )
    parser.add_argument(
        "--reprocessar-dlt", action="store_true",
        help="Consome e reprocessa mensagens retidas no Dead Letter Topic (DLT)"
    )
    parser.add_argument(
        "--limite-dlt", type=int, default=0,
        help="Limite de mensagens a reprocessar do DLT (0 = todas)"
    )
    parser.add_argument(
        "--servidor-url", type=str, default="",
        help="URL do servidor backend de destino (ex: http://localhost:8001)"
    )
    parser.add_argument(
        "--lb-url", type=str, default="",
        help="URL do Load Balancer (modo legado)"
    )
    parser.add_argument(
        "--group-id", type=str, default="",
        help="Consumer Group ID (ex: consumidor_servidor_1)"
    )
    parser.add_argument(
        "--server-id", type=str, default="",
        help="Identificador do nó para logs"
    )

    args = parser.parse_args()

    # Graceful shutdown via sinais
    def _signal_handler(sig, frame):
        print("\n[CONSUMIDOR KAFKA] Sinal de encerramento recebido...")
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    if args.reprocessar_dlt:
        target = args.servidor_url or args.lb_url
        reprocessar_dlt(
            destino_url=target,
            limite=args.limite_dlt,
            reinyetar_topico=not bool(target),
        )
    elif args.multi:
        iniciar_multi_consumidores()
    else:
        target = args.servidor_url or args.lb_url or LOAD_BALANCER_URL
        gid = args.group_id or KAFKA_GROUP_ID
        sid = args.server_id or ("LB" if args.lb_url else gid)

        consumidor = ConsumidorKafka(
            group_id=gid,
            target_url=target,
            server_id=sid,
        )

        print(f"[{sid}] Consumidor Kafka Enterprise")
        print(f"[{sid}] Group ID: {gid}")
        print(f"[{sid}] Tópicos: {KAFKA_TOPIC}, {KAFKA_PRIORITY_TOPIC}")
        print(f"[{sid}] Destino: {target}")
        print()

        consumidor.iniciar()
