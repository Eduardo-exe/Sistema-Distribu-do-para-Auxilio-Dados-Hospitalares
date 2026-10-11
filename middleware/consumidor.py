# middleware/consumidor.py
"""
Consumidor AMQP — Middleware RabbitMQ (MOM).

Marco 3 — Replicação de Dados (Fanout):
- Consome mensagens das filas individuais de cada servidor
- Encaminha diretamente ao endpoint do servidor backend (/dados)
- Garante persistência com ACK manual e reenfileiramento (NACK) em caso de falha
- Suporta modo unitário (1 fila/servidor) ou modo multi-servidor (--multi)

Uso:
    # Modo replicador completo (consome todas as 4 filas em paralelo):
    python middleware/consumidor.py --multi

    # Modo por nó (ideal para deploy em VMs independentes):
    python middleware/consumidor.py --fila fila_servidor_1 --servidor-url http://localhost:8001
    python middleware/consumidor.py --fila fila_servidor_2 --servidor-url http://localhost:8002
    python middleware/consumidor.py --fila fila_servidor_3 --servidor-url http://localhost:8003
    python middleware/consumidor.py --fila fila_servidor_4 --servidor-url http://localhost:8004

    # Modo legado (via Load Balancer):
    python middleware/consumidor.py --lb-url http://localhost:8000
"""

import argparse
import json
import os
import sys
import threading
import time

import pika
import requests

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.config import (
    get_connection_params,
    declarar_infraestrutura,
    EXCHANGE_NAME,
    EXCHANGE_TYPE,
    QUEUE_NAME,
    ROUTING_KEY,
    FILAS_SERVIDORES,
    LOAD_BALANCER_URL,
    PREFETCH_COUNT,
)


class ConsumidorAMQP:
    """
    Consumidor de mensagens AMQP do RabbitMQ para um servidor de destino.

    Consome dados de uma fila específica e encaminha via HTTP POST ao servidor
    backend associado, garantindo confiabilidade com ACK/NACK manual.
    """

    def __init__(
        self,
        queue_name: str = None,
        target_url: str = None,
        server_id: str = None,
    ):
        """
        Args:
            queue_name: Nome da fila RabbitMQ a escutar.
            target_url: URL base do servidor ou balanceador de destino.
            server_id: Identificador descritivo do nó.
        """
        self.queue_name = queue_name or QUEUE_NAME
        self.target_url = (target_url or LOAD_BALANCER_URL).rstrip("/")
        self.server_id = server_id or self.queue_name
        self.total_consumidos = 0
        self.total_sucesso = 0
        self.total_erros = 0
        self.connection = None
        self.channel = None
        self.ativo = True

    def conectar(self):
        """Estabelece conexão com o RabbitMQ e configura canal e prefetch."""
        params = get_connection_params()
        print(f"[{self.server_id}] Conectando ao RabbitMQ em {params.host}:{params.port}...")

        try:
            self.connection = pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[{self.server_id}] ERRO: Não foi possível conectar ao RabbitMQ: {e}")
            sys.exit(1)

        self.channel = self.connection.channel()

        # Declarar infraestrutura completa (idempotente)
        declarar_infraestrutura(self.channel)

        # Configurar QoS
        self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

        print(f"[{self.server_id}] ✓ Conectado! Fila: '{self.queue_name}' → Destino: {self.target_url}")

    def _callback(self, ch, method, properties, body):
        """Processa mensagem recebida da fila."""
        if not self.ativo:
            return

        self.total_consumidos += 1

        try:
            dados = json.loads(body.decode("utf-8"))
            paciente_id = dados.get("paciente_id", "?")

            # Encaminhar ao servidor de destino via POST /dados
            url = f"{self.target_url}/dados"
            response = requests.post(url, json=dados, timeout=8)

            if response.status_code == 200:
                self.total_sucesso += 1
                resp_json = response.json()
                sid = resp_json.get("server_id", self.server_id)
                print(
                    f"[{self.server_id}] [{self.total_consumidos:>4}] ✓ Replicado: "
                    f"paciente_id={paciente_id} em {sid} (200 OK)"
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                self.total_erros += 1
                print(
                    f"[{self.server_id}] [{self.total_consumidos:>4}] ✗ HTTP {response.status_code} "
                    f"ao persistir paciente {paciente_id}: {response.text[:80]}"
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except requests.exceptions.ConnectionError:
            self.total_erros += 1
            print(
                f"[{self.server_id}] [{self.total_consumidos:>4}] ✗ Destino {self.target_url} offline. "
                f"Mensagem mantida na fila RabbitMQ (requeue=True)."
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(1)

        except requests.exceptions.Timeout:
            self.total_erros += 1
            print(f"[{self.server_id}] ✗ Timeout em {self.target_url}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        except json.JSONDecodeError as e:
            self.total_erros += 1
            print(f"[{self.server_id}] ✗ JSON inválido descartado: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            self.total_erros += 1
            print(f"[{self.server_id}] ✗ Erro inesperado: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def iniciar(self):
        """Inicia escuta bloqueante da fila."""
        if not self.channel:
            self.conectar()

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._callback,
            auto_ack=False,
        )

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.parar()

    def parar(self):
        """Interrompe o consumo com segurança."""
        self.ativo = False
        if self.channel and self.channel.is_open:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception:
                pass


def iniciar_multi_consumidores():
    """
    Inicia consumidores dedicados para todas as filas de replicação
    em threads paralelas (Marco 3).
    """
    print()
    print("=" * 65)
    print("  MIDDLEWARE FANOUT — Replicação e Sincronização de Bancos")
    print("=" * 65)
    print(f"  Exchange:       {EXCHANGE_NAME} (tipo: {EXCHANGE_TYPE})")
    print(f"  Filas Ativas:   {len(FILAS_SERVIDORES)}")
    for f in FILAS_SERVIDORES:
        print(f"    • {f['fila']:<18} → {f['servidor_url']} ({f['server_id']})")
    print("=" * 65)
    print()

    consumidores = []
    threads = []

    for info in FILAS_SERVIDORES:
        c = ConsumidorAMQP(
            queue_name=info["fila"],
            target_url=info["servidor_url"],
            server_id=info["server_id"],
        )
        consumidores.append(c)
        t = threading.Thread(target=c.iniciar, daemon=True)
        threads.append(t)
        t.start()

    print("[MULTI-CONSUMIDOR] Todos os ouvintes de replicação iniciados!")
    print("[MULTI-CONSUMIDOR] Pressione Ctrl+C para encerrar.")
    print()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MULTI-CONSUMIDOR] Encerrando ouvintes...")
        for c in consumidores:
            c.parar()


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consumidor AMQP — Middleware RabbitMQ (Marco 3: Replicação Fanout)"
    )
    parser.add_argument(
        "--multi", action="store_true",
        help="Inicia consumidores para todas as filas de replicação em paralelo"
    )
    parser.add_argument(
        "--fila", type=str, default="",
        help="Nome da fila específica a escutar"
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
        "--server-id", type=str, default="",
        help="Identificador do nó para logs"
    )

    args = parser.parse_args()

    if args.multi:
        iniciar_multi_consumidores()
    else:
        # Se passado --lb-url ou nenhum argumento, usa a fila legada e aponta para o LB
        target = args.servidor_url or args.lb_url or LOAD_BALANCER_URL
        queue = args.fila or QUEUE_NAME
        sid = args.server_id or ("LB" if args.lb_url else queue)

        consumidor = ConsumidorAMQP(
            queue_name=queue,
            target_url=target,
            server_id=sid,
        )
        consumidor.conectar()
        consumidor.iniciar()
