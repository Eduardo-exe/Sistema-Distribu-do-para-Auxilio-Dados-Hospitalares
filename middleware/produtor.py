# middleware/produtor.py
"""
Produtor Kafka Enterprise — Publicador de telemetria hospitalar no Apache Kafka.

Versão Enterprise com suporte a:
- Serialização JSON com chave por paciente_id (particionamento)
- Compressão nativa do Kafka (gzip, snappy, lz4, zstd)
- Idempotência no producer (exactly-once semantics)
- Batching nativo do Kafka (batch.size + linger.ms)
- Callbacks de confirmação (acks=all)
- Rate Limiting (Token Bucket) para controle de vazão
- Roteamento por prioridade (tópico de prioridade)
- Distributed Tracing com headers customizados
- Métricas de throughput e latência
- Graceful Shutdown com flush

Lê registros do CSV e publica cada um como mensagem JSON no tópico
Kafka, substituindo o envio HTTP direto ou via RabbitMQ.

Uso:
    python -m middleware.produtor
    python -m middleware.produtor --csv data/dataset.csv --intervalo 0.5 --limite 20
    python -m middleware.produtor --compressao gzip --prioridade
    KAFKA_BOOTSTRAP_SERVERS=192.168.56.11:9092 python -m middleware.produtor
"""

import argparse
import csv
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from server.models import converter_registro_csv
from middleware.config import (
    get_kafka_config,
    assegurar_topicos_kafka,
    KAFKA_BROKERS,
    KAFKA_TOPIC,
    KAFKA_PRIORITY_TOPIC,
    KAFKA_ACKS,
    KAFKA_RETRIES,
    KAFKA_BATCH_SIZE,
    KAFKA_LINGER_MS,
    KAFKA_BUFFER_MEMORY,
    KAFKA_COMPRESSION_TYPE,
    KAFKA_MAX_REQUEST_SIZE,
    KAFKA_ENABLE_IDEMPOTENCE,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_TOKENS_PER_SECOND,
    RATE_LIMIT_BURST_SIZE,
)


logger = logging.getLogger("middleware.produtor")


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
# Produtor Kafka Enterprise
# ============================================================

class ProdutorKafka:
    """
    Produtor Kafka Enterprise com pipeline de publicação.

    Publica mensagens no Apache Kafka com:
    - Chave por paciente_id para particionamento consistente
    - Compressão e batching nativos do Kafka
    - Callbacks de sucesso/erro por mensagem
    - Rate limiting e roteamento por prioridade

    Attributes:
        csv_path: Caminho para o arquivo CSV fonte.
        intervalo: Intervalo em segundos entre publicações.
        limite: Número máximo de registros (0 = todos).
    """

    def __init__(
        self,
        csv_path: str,
        intervalo: float = 1.0,
        limite: int = 0,
        enable_priority: bool = False,
    ):
        self.csv_path = csv_path
        self.intervalo = intervalo
        self.limite = limite
        self.enable_priority = enable_priority

        # Kafka config
        self.config = get_kafka_config()

        # Rate limiter
        self.rate_limiter = (
            SimpleRateLimiter(RATE_LIMIT_TOKENS_PER_SECOND, RATE_LIMIT_BURST_SIZE)
            if RATE_LIMIT_ENABLED else None
        )

        # Producer (será criado em conectar())
        self.producer: KafkaProducer = None

        # A idempotência só é aplicada se o cliente Kafka instalado a suportar
        # (kafka-python-ng 2.2.x não implementa enable_idempotence).
        self._idempotencia_ativa = (
            KAFKA_ENABLE_IDEMPOTENCE
            and "enable_idempotence" in KafkaProducer.DEFAULT_CONFIG
        )

        # Controle
        self._ativo = True

        # Métricas locais
        self.publicados = 0
        self.erros = 0
        self.confirmados = 0
        self._latencies: list[float] = []

    def conectar(self):
        """Cria o KafkaProducer e testa a conexão com os brokers."""
        logger.info(
            "Conectando ao Kafka em %s...", ", ".join(KAFKA_BROKERS),
        )

        producer_config = dict(
            bootstrap_servers=KAFKA_BROKERS,
            acks=KAFKA_ACKS,
            retries=KAFKA_RETRIES,
            batch_size=KAFKA_BATCH_SIZE,
            linger_ms=KAFKA_LINGER_MS,
            buffer_memory=KAFKA_BUFFER_MEMORY,
            compression_type=KAFKA_COMPRESSION_TYPE,
            max_request_size=KAFKA_MAX_REQUEST_SIZE,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        if self._idempotencia_ativa:
            producer_config["enable_idempotence"] = True
        elif KAFKA_ENABLE_IDEMPOTENCE:
            logger.warning(
                "Idempotência solicitada, mas o cliente Kafka instalado não suporta "
                "'enable_idempotence'. Publicando sem ela."
            )

        try:
            self.producer = KafkaProducer(**producer_config)
            # Garantir tópicos criados com o particionamento correto
            assegurar_topicos_kafka(self.config.brokers)
        except NoBrokersAvailable:
            logger.error("ERRO: Nenhum broker Kafka disponível em %s", KAFKA_BROKERS)
            sys.exit(1)
        except KafkaError as e:
            logger.error("ERRO: Falha ao conectar ao Kafka: %s", e)
            sys.exit(1)

        logger.info("✓ Conectado ao Kafka!")

    def _on_success(self, record_metadata):
        """Callback de sucesso na publicação."""
        self.confirmados += 1
        logger.debug(
            "Mensagem confirmada: topic=%s partition=%d offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_error(self, exc):
        """Callback de erro na publicação."""
        self.erros += 1
        logger.error("Erro ao publicar mensagem: %s", exc)

    def _determinar_prioridade(self, dados: dict) -> bool:
        """
        Verifica se o dado é de alta prioridade baseado nos alertas.

        Returns:
            True se é alta prioridade (muitos alertas ABNORMAL).
        """
        alertas = [
            dados.get("alerta_freq_cardiaca", ""),
            dados.get("alerta_spo2", ""),
            dados.get("alerta_pressao", ""),
            dados.get("alerta_temperatura", ""),
        ]
        num_abnormal = sum(1 for a in alertas if a == "ABNORMAL")
        return num_abnormal >= 3  # 3+ alertas = prioridade alta

    def _processar_mensagem(self, dados_api: dict) -> bool:
        """
        Processa e publica uma mensagem no Kafka.

        Args:
            dados_api: Dados do paciente convertidos para formato API.

        Returns:
            True se enviada com sucesso.
        """
        # 1. Rate Limiting
        if self.rate_limiter and not self.rate_limiter.acquire(timeout=5.0):
            logger.warning("Rate limit excedido, aguardando...")
            return False

        # 2. Determinar tópico (normal ou prioridade)
        if self.enable_priority and self._determinar_prioridade(dados_api):
            topic = KAFKA_PRIORITY_TOPIC
        else:
            topic = KAFKA_TOPIC

        # 3. Chave de particionamento (por paciente_id)
        key = str(dados_api.get("paciente_id", ""))

        # 4. Headers para tracing e metadados
        trace_id = str(uuid.uuid4())
        headers = [
            ("x-trace-id", trace_id.encode("utf-8")),
            ("x-producer-timestamp", datetime.now().isoformat().encode("utf-8")),
            ("x-source", b"produtor-kafka-enterprise"),
        ]

        if self.enable_priority:
            is_priority = self._determinar_prioridade(dados_api)
            headers.append(("x-priority", b"HIGH" if is_priority else b"NORMAL"))

        # 5. Publicar (assíncrono com callbacks)
        start_time = time.time()
        try:
            future = self.producer.send(
                topic,
                key=key,
                value=dados_api,
                headers=headers,
            )
            future.add_callback(self._on_success)
            future.add_errback(self._on_error)

            self.publicados += 1
            latency_ms = (time.time() - start_time) * 1000
            self._latencies.append(latency_ms)

            return True

        except KafkaError as e:
            self.erros += 1
            logger.error("Erro ao enviar para Kafka: %s", e)
            return False

    def publicar(self):
        """
        Executa o loop principal de publicação.

        Lê o CSV progressivamente e publica cada registro no Kafka.
        """
        if not os.path.exists(self.csv_path):
            logger.error("Arquivo CSV não encontrado: %s", self.csv_path)
            sys.exit(1)

        if not self.producer:
            self.conectar()

        # Banner de inicialização
        config = self.config
        print()
        print("╔" + "═" * 65 + "╗")
        print("║" + " PRODUTOR ENTERPRISE — Middleware Kafka ".center(65) + "║")
        print("╠" + "═" * 65 + "╣")
        print("║" + f"  Brokers:     {config.bootstrap_servers}".ljust(65) + "║")
        print("║" + f"  Tópico:      {KAFKA_TOPIC}".ljust(65) + "║")
        print("║" + f"  Partições:   {config.num_partitions}".ljust(65) + "║")
        print("║" + f"  Arquivo:     {os.path.basename(self.csv_path)}".ljust(65) + "║")
        print("║" + f"  Intervalo:   {self.intervalo}s".ljust(65) + "║")
        print("║" + f"  Limite:      {'todos' if self.limite == 0 else self.limite}".ljust(65) + "║")
        print("╠" + "═" * 65 + "╣")
        print("║" + "  Módulos:".ljust(65) + "║")

        modulos = [
            ("Acks", config.acks),
            ("Compressão", config.compression_type),
            ("Idempotência", "ativa" if self._idempotencia_ativa else "inativa"),
            ("Batch Size", f"{config.batch_size} bytes"),
            ("Linger", f"{config.linger_ms}ms"),
            ("Rate Limiter", "ativo" if self.rate_limiter else "inativo"),
            ("Prioridade", "ativa" if self.enable_priority else "inativa"),
        ]
        for nome, valor in modulos:
            print("║" + f"    • {nome}: {valor}".ljust(65) + "║")

        print("╚" + "═" * 65 + "╝")
        print()

        # Loop de publicação
        registros_lidos = 0
        try:
            with open(self.csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for registro_csv in reader:
                    if not self._ativo:
                        break
                    if self.limite > 0 and registros_lidos >= self.limite:
                        break

                    # Converter registro CSV para formato API
                    dados_api = converter_registro_csv(registro_csv)
                    registros_lidos += 1

                    # Processar através do pipeline
                    sucesso = self._processar_mensagem(dados_api)

                    if sucesso:
                        paciente_id = dados_api.get("paciente_id", "?")
                        is_priority = (
                            self.enable_priority and self._determinar_prioridade(dados_api)
                        )
                        status_char = "⚡" if is_priority else "✓"
                        priority_label = " [PRIORIDADE]" if is_priority else ""

                        print(
                            f"[{registros_lidos:>5}] {status_char} "
                            f"paciente_id={paciente_id:>5}{priority_label} | "
                            f"FC={dados_api['frequencia_cardiaca']} "
                            f"SpO2={dados_api['spo2']} "
                            f"PA={dados_api['pressao_sistolica']}/{dados_api['pressao_diastolica']} "
                            f"Temp={dados_api['temperatura']}"
                        )

                    # Intervalo entre publicações
                    if self.intervalo > 0:
                        time.sleep(self.intervalo)

        except KeyboardInterrupt:
            print("\n[PRODUTOR KAFKA] Interrompido pelo usuário.")
        finally:
            # Flush — garantir que todas as mensagens pendentes sejam enviadas
            if self.producer:
                logger.info("Flush de mensagens pendentes...")
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)

        # Relatório final
        self._imprimir_relatorio(registros_lidos)

    def _imprimir_relatorio(self, registros_lidos: int):
        """Imprime relatório final de publicação."""
        avg_lat = (
            sum(self._latencies) / len(self._latencies) if self._latencies else 0
        )
        sorted_lats = sorted(self._latencies) if self._latencies else [0]
        p95 = sorted_lats[int(len(sorted_lats) * 0.95)] if len(sorted_lats) > 1 else 0
        p99 = sorted_lats[int(len(sorted_lats) * 0.99)] if len(sorted_lats) > 1 else 0

        print()
        print("╔" + "═" * 55 + "╗")
        print("║" + " 📊 RELATÓRIO DE PRODUÇÃO — Kafka ".center(55) + "║")
        print("╠" + "═" * 55 + "╣")
        print("║" + f"  Registros lidos:    {registros_lidos}".ljust(55) + "║")
        print("║" + f"  Publicados:         {self.publicados}".ljust(55) + "║")
        print("║" + f"  Confirmados:        {self.confirmados}".ljust(55) + "║")
        print("║" + f"  Erros:              {self.erros}".ljust(55) + "║")
        taxa = (self.publicados - self.erros) / max(registros_lidos, 1) * 100
        print("║" + f"  Taxa de sucesso:    {taxa:.1f}%".ljust(55) + "║")
        print("╠" + "═" * 55 + "╣")
        print("║" + f"  Latência média:     {avg_lat:.2f}ms".ljust(55) + "║")
        print("║" + f"  Latência p95:       {p95:.2f}ms".ljust(55) + "║")
        print("║" + f"  Latência p99:       {p99:.2f}ms".ljust(55) + "║")
        print("╚" + "═" * 55 + "╝")

    def parar(self):
        """Encerra o produtor de forma limpa (graceful shutdown)."""
        logger.info("Graceful shutdown iniciado...")
        self._ativo = False

        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=5)
            except Exception:
                pass


# ============================================================
# Função de compatibilidade
# ============================================================

def publicar_dados_kafka(csv_path: str, intervalo: float, limite: int = 0):
    """
    Função de compatibilidade — publica dados via Kafka.

    Args:
        csv_path: Caminho para o arquivo CSV.
        intervalo: Intervalo em segundos entre cada publicação.
        limite: Número máximo de registros (0 = todos).
    """
    produtor = ProdutorKafka(
        csv_path=csv_path,
        intervalo=intervalo,
        limite=limite,
    )
    produtor.publicar()


def criar_produtor_kafka(**kwargs) -> ProdutorKafka:
    """Cria uma instância do ProdutorKafka."""
    return ProdutorKafka(**kwargs)


publicar_dados = publicar_dados_kafka


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    projeto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_padrao = os.path.join(projeto_dir, "data", "dataset.csv")

    parser = argparse.ArgumentParser(
        description="Produtor Kafka Enterprise — Publica dados do CSV no Apache Kafka"
    )
    parser.add_argument(
        "--csv", type=str, default=csv_padrao,
        help="Caminho do arquivo CSV (padrão: data/dataset.csv)"
    )
    parser.add_argument(
        "--intervalo", type=float, default=1.0,
        help="Intervalo em segundos entre cada publicação (padrão: 1.0)"
    )
    parser.add_argument(
        "--limite", type=int, default=0,
        help="Número máximo de registros a publicar, 0 = todos (padrão: 0)"
    )
    parser.add_argument(
        "--compressao", type=str, default=None,
        choices=["gzip", "snappy", "lz4", "zstd", "none"],
        help="Algoritmo de compressão (padrão: gzip)"
    )
    parser.add_argument(
        "--prioridade", action="store_true", default=False,
        help="Habilitar roteamento por prioridade para tópico dedicado"
    )

    args = parser.parse_args()

    # Sobrescrever compressão se especificado
    if args.compressao and args.compressao != "none":
        os.environ["KAFKA_COMPRESSION_TYPE"] = args.compressao

    produtor = ProdutorKafka(
        csv_path=args.csv,
        intervalo=args.intervalo,
        limite=args.limite,
        enable_priority=args.prioridade,
    )

    # Graceful shutdown
    def _signal_handler(sig, frame):
        produtor.parar()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    produtor.publicar()
