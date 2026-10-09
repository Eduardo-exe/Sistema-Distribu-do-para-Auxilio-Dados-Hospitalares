# middleware/produtor.py
"""
Produtor AMQP — Publicador de telemetria hospitalar no RabbitMQ.

Lê registros do CSV e publica cada um como mensagem JSON na fila
do RabbitMQ, substituindo o envio HTTP direto do csv_sender.

O produtor:
1. Conecta ao RabbitMQ via AMQP
2. Declara a exchange e a fila (idempotente)
3. Lê o CSV progressivamente
4. Publica cada registro como mensagem persistente na fila
5. Confirma a entrega com publisher confirms

Uso:
    python middleware/produtor.py
    python middleware/produtor.py --csv data/dataset.csv --intervalo 0.5 --limite 20
    RABBITMQ_HOST=192.168.56.11 python middleware/produtor.py
"""

import argparse
import csv
import json
import os
import sys
import time

import pika

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.models import converter_registro_csv
from middleware.config import (
    get_connection_params,
    declarar_infraestrutura,
    EXCHANGE_NAME,
    ROUTING_KEY,
)


def publicar_dados(csv_path: str, intervalo: float, limite: int = 0):
    """
    Lê o CSV e publica cada registro na fila do RabbitMQ.

    Args:
        csv_path: Caminho para o arquivo CSV.
        intervalo: Intervalo em segundos entre cada publicação.
        limite: Número máximo de registros a publicar (0 = todos).
    """
    if not os.path.exists(csv_path):
        print(f"ERRO: Arquivo CSV não encontrado: {csv_path}")
        sys.exit(1)

    # ── Conectar ao RabbitMQ ────────────────────────────────
    params = get_connection_params()
    print(f"[PRODUTOR] Conectando ao RabbitMQ em {params.host}:{params.port}...")

    try:
        connection = pika.BlockingConnection(params)
    except pika.exceptions.AMQPConnectionError as e:
        print(f"[PRODUTOR] ERRO: Não foi possível conectar ao RabbitMQ: {e}")
        print(f"[PRODUTOR] Verifique se o RabbitMQ está rodando em {params.host}:{params.port}")
        sys.exit(1)

    channel = connection.channel()

    # Ativar publisher confirms para garantir entrega
    channel.confirm_delivery()

    # Declarar exchange e fila
    declarar_infraestrutura(channel)

    print()
    print("=" * 60)
    print("  PRODUTOR AMQP — Middleware RabbitMQ")
    print("=" * 60)
    print(f"  RabbitMQ:  {params.host}:{params.port}")
    print(f"  Exchange:  {EXCHANGE_NAME}")
    print(f"  Routing:   {ROUTING_KEY}")
    print(f"  Arquivo:   {csv_path}")
    print(f"  Intervalo: {intervalo}s")
    print(f"  Limite:    {'todos' if limite == 0 else limite}")
    print("=" * 60)
    print()

    publicados = 0
    erros = 0

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for registro_csv in reader:
                if limite > 0 and publicados >= limite:
                    break

                # Converter registro do CSV para formato da API
                dados_api = converter_registro_csv(registro_csv)
                publicados += 1

                # Serializar para JSON
                corpo_mensagem = json.dumps(dados_api, ensure_ascii=False)

                try:
                    # Publicar com persistência (delivery_mode=2)
                    channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key=ROUTING_KEY,
                        body=corpo_mensagem.encode("utf-8"),
                        properties=pika.BasicProperties(
                            delivery_mode=pika.DeliveryMode.Persistent,
                            content_type="application/json",
                            content_encoding="utf-8",
                        ),
                    )

                    print(
                        f"[{publicados:>5}] ✓ Publicado: paciente_id={dados_api['paciente_id']:>5} | "
                        f"FC={dados_api['frequencia_cardiaca']} "
                        f"SpO2={dados_api['spo2']} "
                        f"PA={dados_api['pressao_sistolica']}/{dados_api['pressao_diastolica']} "
                        f"Temp={dados_api['temperatura']} "
                        f"→ fila '{ROUTING_KEY}'"
                    )

                except pika.exceptions.UnroutableError:
                    erros += 1
                    print(
                        f"[{publicados:>5}] ✗ Mensagem não roteável: "
                        f"paciente_id={dados_api['paciente_id']}"
                    )
                except pika.exceptions.NackError:
                    erros += 1
                    print(
                        f"[{publicados:>5}] ✗ Mensagem rejeitada pelo broker: "
                        f"paciente_id={dados_api['paciente_id']}"
                    )
                except Exception as e:
                    erros += 1
                    print(
                        f"[{publicados:>5}] ✗ Erro ao publicar: "
                        f"paciente_id={dados_api['paciente_id']} → {e}"
                    )

                # Aguardar intervalo entre publicações
                if intervalo > 0:
                    time.sleep(intervalo)

    except KeyboardInterrupt:
        print("\n[PRODUTOR] Interrompido pelo usuário.")
    finally:
        # Fechar conexão de forma limpa
        if connection and connection.is_open:
            connection.close()

    print()
    print("=" * 60)
    print(f"  Produção finalizada!")
    print(f"  Publicados: {publicados}")
    print(f"  Erros:      {erros}")
    print(f"  Sucesso:    {publicados - erros}")
    print("=" * 60)


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    projeto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_padrao = os.path.join(projeto_dir, "data", "dataset.csv")

    parser = argparse.ArgumentParser(
        description="Produtor AMQP — Publica dados do CSV no RabbitMQ"
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

    args = parser.parse_args()

    publicar_dados(
        csv_path=args.csv,
        intervalo=args.intervalo,
        limite=args.limite,
    )
