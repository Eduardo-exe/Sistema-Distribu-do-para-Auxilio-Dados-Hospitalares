# client/csv_sender.py
"""
Leitor e reprodutor do CSV de dados hospitalares.

Lê o CSV progressivamente e envia cada registro via HTTP POST,
simulando a chegada de dados de telemetria de pacientes.

Uso:
    python client/csv_sender.py --url http://localhost:8000/dados --intervalo 1.0 --limite 10
"""

import argparse
import csv
import sys
import os
import time

import requests

# Adicionar o diretório raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.models import CSV_COLUMN_MAP, converter_registro_csv


def enviar_dados(url: str, csv_path: str, intervalo: float, limite: int = 0):
    """
    Lê o CSV e envia registros progressivamente via HTTP POST.

    Args:
        url: URL do endpoint POST /dados.
        csv_path: Caminho para o arquivo CSV.
        intervalo: Intervalo em segundos entre cada envio.
        limite: Número máximo de registros a enviar (0 = todos).
    """
    if not os.path.exists(csv_path):
        print(f"ERRO: Arquivo CSV não encontrado: {csv_path}")
        sys.exit(1)

    print("=" * 60)
    print("  LEITOR DE DADOS — CSV Sender")
    print("=" * 60)
    print(f"  Arquivo:   {csv_path}")
    print(f"  Destino:   {url}")
    print(f"  Intervalo: {intervalo}s")
    print(f"  Limite:    {'todos' if limite == 0 else limite}")
    print("=" * 60)
    print()

    enviados = 0
    erros = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for registro_csv in reader:
            if limite > 0 and enviados >= limite:
                break

            # Converter registro do CSV para formato da API
            dados_api = converter_registro_csv(registro_csv)
            enviados += 1

            try:
                response = requests.post(url, json=dados_api, timeout=10)

                if response.status_code == 200:
                    resp_json = response.json()
                    server_id = resp_json.get("server_id", "desconhecido")
                    print(
                        f"[{enviados:>5}] ✓ Paciente {dados_api['paciente_id']:>5} → "
                        f"{server_id} | "
                        f"FC={dados_api['frequencia_cardiaca']} "
                        f"SpO2={dados_api['spo2']} "
                        f"PA={dados_api['pressao_sistolica']}/{dados_api['pressao_diastolica']} "
                        f"Temp={dados_api['temperatura']}"
                    )
                else:
                    erros += 1
                    print(
                        f"[{enviados:>5}] ✗ Paciente {dados_api['paciente_id']} → "
                        f"HTTP {response.status_code}: {response.text}"
                    )

            except requests.exceptions.ConnectionError:
                erros += 1
                print(
                    f"[{enviados:>5}] ✗ Paciente {dados_api['paciente_id']} → "
                    f"ERRO: Não foi possível conectar a {url}"
                )
            except requests.exceptions.Timeout:
                erros += 1
                print(
                    f"[{enviados:>5}] ✗ Paciente {dados_api['paciente_id']} → "
                    f"ERRO: Timeout"
                )
            except Exception as e:
                erros += 1
                print(
                    f"[{enviados:>5}] ✗ Paciente {dados_api['paciente_id']} → "
                    f"ERRO: {e}"
                )

            # Aguardar intervalo antes de enviar o próximo registro
            if intervalo > 0:
                time.sleep(intervalo)

    print()
    print("=" * 60)
    print(f"  Finalizado!")
    print(f"  Enviados: {enviados}")
    print(f"  Erros:    {erros}")
    print(f"  Sucesso:  {enviados - erros}")
    print("=" * 60)


# ============================================================
# Ponto de entrada
# ============================================================

if __name__ == "__main__":
    # Caminho padrão do CSV (relativo ao diretório raiz do projeto)
    projeto_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_padrao = os.path.join(projeto_dir, "data", "dataset.csv")

    parser = argparse.ArgumentParser(
        description="Leitor e reprodutor de dados do CSV"
    )
    parser.add_argument(
        "--url", type=str, default="http://localhost:8000/dados",
        help="URL do endpoint POST /dados (padrão: http://localhost:8000/dados)"
    )
    parser.add_argument(
        "--csv", type=str, default=csv_padrao,
        help="Caminho do arquivo CSV (padrão: data/dataset.csv)"
    )
    parser.add_argument(
        "--intervalo", type=float, default=1.0,
        help="Intervalo em segundos entre cada envio (padrão: 1.0)"
    )
    parser.add_argument(
        "--limite", type=int, default=0,
        help="Número máximo de registros a enviar, 0 = todos (padrão: 0)"
    )

    args = parser.parse_args()

    enviar_dados(
        url=args.url,
        csv_path=args.csv,
        intervalo=args.intervalo,
        limite=args.limite,
    )
