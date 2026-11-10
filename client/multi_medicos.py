# client/multi_medicos.py
"""
Emissor Multi-Médicos de Telemetria Hospitalar (Marco 1, 2 e 3).

Simula múltiplos médicos / setores hospitalares enviando dados de pacientes
simultaneamente para o cluster:
  - Médico 1: Dr. Carlos (UTI / Terapia Intensiva)
  - Médico 2: Dra. Beatriz (Pronto-Socorro / Emergência)
  - Médico 3: Dr. Lucas (Enfermaria Geral / Clínica)

Suporta dois canais de envio:
  - HTTP direto (padrão): envia requisições POST para o Load Balancer (:8000)
  - Middleware Apache Kafka (--via-kafka): publica no tópico do Kafka MOM (:9092)

Uso:
    # Simular os 3 médicos em paralelo (10 registros cada, intervalo 0.8s):
    python client/multi_medicos.py --limite 10 --intervalo 0.8

    # Simular apenas a Dra. Beatriz (Emergência):
    python client/multi_medicos.py --medico 2 --limite 15

    # Simular os 3 médicos publicando direto no Middleware Kafka:
    python client/multi_medicos.py --via-kafka --limite 20 --intervalo 0.5
"""

import argparse
import csv
import os
import sys
import threading
import time
from typing import Dict, List, Optional
import requests

# Adicionar raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.models import converter_registro_csv

# Diretório base de datasets
PROJETO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJETO_DIR, "data")

MEDICOS_CONFIG = {
    "1": {
        "id": "medico_1",
        "nome": "Dr. Carlos",
        "setor": "UTI",
        "icone": "🩺🔴",
        "csv": os.path.join(DATA_DIR, "dataset_medico_1_uti.csv"),
        "cor": "\033[91m",  # Vermelho
    },
    "2": {
        "id": "medico_2",
        "nome": "Dra. Beatriz",
        "setor": "Emergência",
        "icone": "🩺🟠",
        "csv": os.path.join(DATA_DIR, "dataset_medico_2_emergencia.csv"),
        "cor": "\033[93m",  # Amarelo/Laranja
    },
    "3": {
        "id": "medico_3",
        "nome": "Dr. Lucas",
        "setor": "Enfermaria",
        "icone": "🩺🟢",
        "csv": os.path.join(DATA_DIR, "dataset_medico_3_enfermaria.csv"),
        "cor": "\033[92m",  # Verde
    },
}
RESET_COR = "\033[0m"


def enviar_fluxo_medico(
    medico_key: str,
    url: str,
    intervalo: float,
    limite: int = 0,
    via_kafka: bool = False,
    lock_print: Optional[threading.Lock] = None,
):
    """Executa o loop de envio de um médico específico."""
    cfg = MEDICOS_CONFIG[medico_key]
    csv_path = cfg["csv"]
    nome = cfg["nome"]
    setor = cfg["setor"]
    icone = cfg["icone"]
    cor = cfg["cor"]

    if not os.path.exists(csv_path):
        if lock_print:
            with lock_print:
                print(f"[ERRO] Dataset não encontrado para {nome}: {csv_path}")
        else:
            print(f"[ERRO] Dataset não encontrado para {nome}: {csv_path}")
        return

    enviados = 0
    erros = 0

    # Inicializar produtor Kafka se necessário
    kafka_producer = None
    if via_kafka:
        try:
            from middleware.produtor import obter_produtor
            from middleware.config import KAFKA_TOPIC
            kafka_producer = obter_produtor()
        except Exception as e:
            if lock_print:
                with lock_print:
                    print(f"[{nome}] Falha ao conectar no Kafka: {e}. Usando fallback HTTP.")
            via_kafka = False

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if limite > 0 and enviados >= limite:
                break

            dados = converter_registro_csv(row)
            # Metadados do médico
            dados["medico_responsavel"] = f"{nome} ({setor})"
            dados["setor_hospitalar"] = setor
            enviados += 1

            if via_kafka and kafka_producer:
                try:
                    import json
                    from middleware.config import KAFKA_TOPIC
                    pid = dados["paciente_id"]
                    key_bytes = str(pid).encode("utf-8")
                    val_bytes = json.dumps(dados).encode("utf-8")
                    headers = [
                        ("x-medico", nome.encode("utf-8")),
                        ("x-setor", setor.encode("utf-8")),
                    ]
                    kafka_producer.send(KAFKA_TOPIC, key=key_bytes, value=val_bytes, headers=headers)
                    if lock_print:
                        with lock_print:
                            print(
                                f"{cor}{icone} [{nome} - {setor}] #{enviados:>3} ➔ KAFKA MOM: "
                                f"Paciente {pid} ({dados.get('doenca_prevista', 'Normal')}) "
                                f"FC={dados['frequencia_cardiaca']} SpO2={dados['spo2']}%{RESET_COR}"
                            )
                except Exception as ex:
                    erros += 1
                    if lock_print:
                        with lock_print:
                            print(f"{cor}[{nome}] ERRO Kafka: {ex}{RESET_COR}")
            else:
                try:
                    resp = requests.post(url, json=dados, timeout=5.0)
                    if resp.status_code == 200:
                        rj = resp.json()
                        sid = rj.get("server_id", "server-01")
                        if lock_print:
                            with lock_print:
                                print(
                                    f"{cor}{icone} [{nome} - {setor}] #{enviados:>3} ➔ {sid}: "
                                    f"Paciente {dados['paciente_id']} ({dados.get('doenca_prevista', 'Normal')}) "
                                    f"FC={dados['frequencia_cardiaca']} SpO2={dados['spo2']}% "
                                    f"PA={dados['pressao_sistolica']}/{dados['pressao_diastolica']}{RESET_COR}"
                                )
                    else:
                        erros += 1
                        if lock_print:
                            with lock_print:
                                print(f"{cor}[{nome}] ✗ HTTP {resp.status_code}: {resp.text[:60]}{RESET_COR}")
                except Exception as ex:
                    erros += 1
                    if lock_print:
                        with lock_print:
                            print(f"{cor}[{nome}] ✗ Falha conexão: {ex}{RESET_COR}")

            if intervalo > 0:
                time.sleep(intervalo)

    if lock_print:
        with lock_print:
            print(f"{cor}  ✓ {nome} ({setor}) finalizou: {enviados} enviados ({erros} erros){RESET_COR}")


def main():
    parser = argparse.ArgumentParser(description="Emissor Multi-Médicos de Telemetria Hospitalar")
    parser.add_argument(
        "--medico",
        type=str,
        default="todos",
        choices=["1", "2", "3", "todos"],
        help="Médico a executar: 1 (Dr. Carlos - UTI), 2 (Dra. Beatriz - Emergência), 3 (Dr. Lucas - Enfermaria) ou todos",
    )
    parser.add_argument(
        "--url",
        type=str,
        default="http://localhost:8000/dados",
        help="URL do Load Balancer (padrão: http://localhost:8000/dados)",
    )
    parser.add_argument(
        "--intervalo",
        type=float,
        default=0.8,
        help="Intervalo entre envios por médico em segundos (padrão: 0.8)",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=10,
        help="Limite de registros por médico (0 = todos os 20.000)",
    )
    parser.add_argument(
        "--via-kafka",
        action="store_true",
        default=False,
        help="Publicar diretamente no tópico do Apache Kafka MOM",
    )

    args = parser.parse_args()

    print("=" * 68)
    print("  EQUIPE MÉDICA HOSPITALAR — EMISSOR DE TELEMETRIA MULTI-CLIENTE")
    print("=" * 68)
    print(f"  Canal de Ingestão: {'APACHE KAFKA MOM (:9092)' if args.via_kafka else 'LOAD BALANCER (' + args.url + ')'}")
    print(f"  Intervalo:        {args.intervalo}s por médico")
    print(f"  Limite por Médico: {'Todos (20.000)' if args.limite == 0 else args.limite}")
    print("  Médicos Ativos:")
    if args.medico == "todos":
        for k, v in MEDICOS_CONFIG.items():
            print(f"    • {v['icone']} {v['nome']} — {v['setor']} ({os.path.basename(v['csv'])})")
    else:
        v = MEDICOS_CONFIG[args.medico]
        print(f"    • {v['icone']} {v['nome']} — {v['setor']} ({os.path.basename(v['csv'])})")
    print("=" * 68)
    print()

    lock_print = threading.Lock()

    if args.medico == "todos":
        threads = []
        for k in ["1", "2", "3"]:
            t = threading.Thread(
                target=enviar_fluxo_medico,
                args=(k, args.url, args.intervalo, args.limite, args.via_kafka, lock_print),
                daemon=True,
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
    else:
        enviar_fluxo_medico(
            args.medico, args.url, args.intervalo, args.limite, args.via_kafka, lock_print
        )

    print()
    print("=" * 68)
    print("  ✓ Todos os envios da equipe médica foram concluídos!")
    print("=" * 68)


if __name__ == "__main__":
    main()
