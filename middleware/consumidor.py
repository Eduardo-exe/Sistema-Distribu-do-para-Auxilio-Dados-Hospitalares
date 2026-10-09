# middleware/consumidor.py
"""
Consumidor AMQP — Consome mensagens do RabbitMQ e encaminha ao Load Balancer.

O consumidor:
1. Conecta ao RabbitMQ via AMQP
2. Declara a exchange e a fila (idempotente)
3. Consome mensagens da fila uma a uma (prefetch=1)
4. Para cada mensagem, faz HTTP POST ao Load Balancer
5. Envia ACK somente após confirmação do Load Balancer
6. Em caso de erro no POST, faz NACK (requeue) para retentar

Uso:
    python middleware/consumidor.py
    python middleware/consumidor.py --lb-url http://192.168.56.12:8000
    RABBITMQ_HOST=192.168.56.11 python middleware/consumidor.py
"""

import argparse
import json
import os
import sys

import pika
import requests

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.config import (
    get_connection_params,
    declarar_infraestrutura,
    EXCHANGE_NAME,
    QUEUE_NAME,
    ROUTING_KEY,
    LOAD_BALANCER_URL,
    PREFETCH_COUNT,
)


class ConsumidorAMQP:
    """
    Consumidor de mensagens AMQP do RabbitMQ.

    Consome mensagens da fila de telemetria hospitalar e encaminha
    cada uma via HTTP POST ao Load Balancer, garantindo entrega
    com ACK/NACK manual.
    """

    def __init__(self, lb_url: str = None):
        """
        Args:
            lb_url: URL base do Load Balancer.
        """
        self.lb_url = lb_url or LOAD_BALANCER_URL
        self.total_consumidos = 0
        self.total_sucesso = 0
        self.total_erros = 0
        self.connection = None
        self.channel = None

    def conectar(self):
        """Estabelece conexão com o RabbitMQ e configura o canal."""
        params = get_connection_params()
        print(f"[CONSUMIDOR] Conectando ao RabbitMQ em {params.host}:{params.port}...")

        try:
            self.connection = pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[CONSUMIDOR] ERRO: Não foi possível conectar ao RabbitMQ: {e}")
            print(f"[CONSUMIDOR] Verifique se o RabbitMQ está rodando em {params.host}:{params.port}")
            sys.exit(1)

        self.channel = self.connection.channel()

        # Declarar infraestrutura (idempotente)
        declarar_infraestrutura(self.channel)

        # Configurar QoS: processar uma mensagem de cada vez
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

        print(f"[CONSUMIDOR] Conectado com sucesso!")
        print(f"[CONSUMIDOR] Fila: {QUEUE_NAME}")
        print(f"[CONSUMIDOR] Load Balancer: {self.lb_url}")
        print(f"[CONSUMIDOR] Prefetch: {PREFETCH_COUNT}")

    def _callback(self, ch, method, properties, body):
        """
        Callback executado para cada mensagem recebida da fila.

        Args:
            ch: Canal pika.
            method: Metadados da entrega (delivery_tag, routing_key, etc.).
            properties: Propriedades da mensagem (content_type, etc.).
            body: Corpo da mensagem em bytes.
        """
        self.total_consumidos += 1

        try:
            # Decodificar mensagem JSON
            dados = json.loads(body.decode("utf-8"))
            paciente_id = dados.get("paciente_id", "?")

            print(
                f"[CONSUMIDOR] [{self.total_consumidos:>5}] Consumido: "
                f"paciente_id={paciente_id} ← fila '{QUEUE_NAME}'"
            )

            # Encaminhar ao Load Balancer via HTTP POST
            url = f"{self.lb_url}/dados"
            response = requests.post(url, json=dados, timeout=10)

            if response.status_code == 200:
                resp_json = response.json()
                server_id = resp_json.get("server_id", "desconhecido")
                self.total_sucesso += 1

                print(
                    f"[CONSUMIDOR] [{self.total_consumidos:>5}] ✓ Encaminhado: "
                    f"paciente_id={paciente_id} → {server_id} "
                    f"(via {self.lb_url})"
                )

                # ACK: mensagem processada com sucesso, remover da fila
                ch.basic_ack(delivery_tag=method.delivery_tag)

            else:
                self.total_erros += 1
                print(
                    f"[CONSUMIDOR] [{self.total_consumidos:>5}] ✗ HTTP {response.status_code}: "
                    f"paciente_id={paciente_id} → {response.text[:100]}"
                )
                # NACK com requeue: devolver à fila para retentar
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except requests.exceptions.ConnectionError:
            self.total_erros += 1
            print(
                f"[CONSUMIDOR] [{self.total_consumidos:>5}] ✗ Não foi possível "
                f"conectar ao Load Balancer ({self.lb_url})"
            )
            # NACK com requeue: o Load Balancer pode estar temporariamente fora
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except requests.exceptions.Timeout:
            self.total_erros += 1
            print(
                f"[CONSUMIDOR] [{self.total_consumidos:>5}] ✗ Timeout ao "
                f"conectar ao Load Balancer"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except json.JSONDecodeError as e:
            self.total_erros += 1
            print(
                f"[CONSUMIDOR] [{self.total_consumidos:>5}] ✗ Mensagem inválida "
                f"(não é JSON): {e}"
            )
            # ACK mesmo com erro: mensagem corrompida não deve voltar à fila
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.total_erros += 1
            print(
                f"[CONSUMIDOR] [{self.total_consumidos:>5}] ✗ Erro inesperado: {e}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def iniciar(self):
        """
        Inicia o consumo de mensagens (loop bloqueante).

        Fica ouvindo a fila indefinidamente até receber Ctrl+C.
        """
        print()
        print("=" * 60)
        print("  CONSUMIDOR AMQP — Middleware RabbitMQ")
        print("=" * 60)
        print(f"  Fila:           {QUEUE_NAME}")
        print(f"  Exchange:       {EXCHANGE_NAME}")
        print(f"  Routing Key:    {ROUTING_KEY}")
        print(f"  Load Balancer:  {self.lb_url}")
        print(f"  Prefetch Count: {PREFETCH_COUNT}")
        print("=" * 60)
        print()
        print("[CONSUMIDOR] Aguardando mensagens... (Ctrl+C para parar)")
        print()

        # Registrar callback e iniciar consumo
        self.channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=self._callback,
            auto_ack=False,  # ACK manual para garantir processamento
        )

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("\n[CONSUMIDOR] Interrompido pelo usuário.")
            self.channel.stop_consuming()
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()

            print()
            print("=" * 60)
            print(f"  Consumo finalizado!")
            print(f"  Total consumidos: {self.total_consumidos}")
            print(f"  Sucesso:          {self.total_sucesso}")
            print(f"  Erros:            {self.total_erros}")
            print("=" * 60)


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consumidor AMQP — Consome da fila e encaminha ao Load Balancer"
    )
    parser.add_argument(
        "--lb-url", type=str, default=LOAD_BALANCER_URL,
        help=f"URL do Load Balancer (padrão: {LOAD_BALANCER_URL})"
    )

    args = parser.parse_args()

    consumidor = ConsumidorAMQP(lb_url=args.lb_url)
    consumidor.conectar()
    consumidor.iniciar()
