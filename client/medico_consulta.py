# client/medico_consulta.py
"""
Terminal de Consulta Médica e Leitura de Prontuários (Read Path).

Permite aos médicos consultarem os dados e histórico de pacientes no cluster
através do Load Balancer (:8000).

Demonstra na prática:
  - Balanceamento de carga em requisições de leitura (GET /dados/{id})
  - Consistência e replicação (qualquer servidor responde com os mesmos dados)
  - Tolerância a falhas na leitura (se um servidor cair, o LB faz failover)

Uso:
    # Consultar prontuário de um paciente específico:
    python client/medico_consulta.py --paciente 1

    # Consultar 5 pacientes aleatórios em sequência:
    python client/medico_consulta.py --aleatorio --quantidade 5

    # Simular médicos consultando prontuários continuamente a cada 1.5s:
    python client/medico_consulta.py --loop --intervalo 1.5
"""

import argparse
import os
import random
import sys
import time
from datetime import datetime
import requests

# Cores ANSI para o terminal
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"

MEDICOS_CONSULTANTES = [
    ("Dr. Carlos", "UTI"),
    ("Dra. Beatriz", "Emergência"),
    ("Dr. Lucas", "Enfermaria"),
    ("Dra. Mariana", "Cardiologia"),
    ("Dr. Roberto", "Clínica Geral"),
]


def consultar_paciente(paciente_id: int, url_base: str = "http://localhost:8000") -> bool:
    """Realiza uma consulta de prontuário e imprime os dados detalhados."""
    medico, setor = random.choice(MEDICOS_CONSULTANTES)
    url = f"{url_base}/dados/{paciente_id}"

    t0 = time.time()
    try:
        resp = requests.get(url, timeout=4.0)
        latencia_ms = (time.time() - t0) * 1000
    except requests.exceptions.ConnectionError:
        print(f"{RED}✗ [{medico} - {setor}] Não foi possível conectar ao Load Balancer ({url_base}){RESET}")
        return False
    except Exception as e:
        print(f"{RED}✗ [{medico} - {setor}] Erro na requisição: {e}{RESET}")
        return False

    is_failover = resp.headers.get("X-Failover") == "true"
    failover_badge = f"{YELLOW} ⚠️ [FAILOVER AUTOMÁTICO]{RESET}" if is_failover else ""

    if resp.status_code == 200:
        registros = resp.json()
        total_reg = len(registros) if isinstance(registros, list) else 1
        ultimo = registros[-1] if isinstance(registros, list) and registros else registros

        # Servidor que atendeu a consulta
        sid = ultimo.get("server_id", "cluster")

        print(
            f"{CYAN}🔍 [{medico} - {setor}]{RESET} consultou {BOLD}Paciente #{paciente_id}{RESET} "
            f"➔ Atendido por {GREEN}{sid}{RESET} em {latencia_ms:.1f}ms{failover_badge}"
        )
        print(
            f"   Histórico: {total_reg} medições salvas | "
            f"FC: {ultimo.get('frequencia_cardiaca')} bpm | "
            f"SpO2: {ultimo.get('spo2')}% | "
            f"PA: {ultimo.get('pressao_sistolica')}/{ultimo.get('pressao_diastolica')} mmHg | "
            f"Temp: {ultimo.get('temperatura')}°C | "
            f"Diagnóstico: {BOLD}{ultimo.get('doenca_prevista', 'Normal')}{RESET}"
        )
        print("   " + "-" * 62)
        return True
    elif resp.status_code == 404:
        print(
            f"{YELLOW}ℹ️  [{medico} - {setor}] Paciente #{paciente_id} não possui registros no banco ainda (404). ({latencia_ms:.1f}ms){RESET}"
        )
        return False
    else:
        print(
            f"{RED}✗ [{medico} - {setor}] Erro HTTP {resp.status_code} na consulta do Paciente #{paciente_id}{RESET}"
        )
        return False


def main():
    parser = argparse.ArgumentParser(description="Terminal de Consulta Médica (Leitura de Prontuários)")
    parser.add_argument(
        "--paciente",
        type=int,
        default=None,
        help="ID específico do paciente a consultar (ex: 1)",
    )
    parser.add_argument(
        "--aleatorio",
        action="store_true",
        default=False,
        help="Consultar pacientes aleatórios",
    )
    parser.add_argument(
        "--quantidade",
        type=int,
        default=5,
        help="Quantidade de consultas aleatórias (padrão: 5)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Executar em loop contínuo de consultas médicas",
    )
    parser.add_argument(
        "--intervalo",
        type=float,
        default=1.5,
        help="Intervalo em segundos entre consultas no modo loop (padrão: 1.5)",
    )
    parser.add_argument(
        "--url",
        type=str,
        default="http://localhost:8000",
        help="URL do Load Balancer (padrão: http://localhost:8000)",
    )

    args = parser.parse_args()

    print("=" * 68)
    print("  TERMINAL DE CONSULTA MÉDICA — LEITURA DISTRIBUÍDA DE PRONTUÁRIOS")
    print("=" * 68)
    print(f"  Load Balancer de Leitura: {args.url}")
    print("  Garantia de Consistência: Dados replicados via Kafka em todos os nós")
    print("=" * 68)
    print()

    if args.loop:
        print("Iniciando simulação contínua de consultas médicas (Ctrl+C para encerrar)...")
        print()
        try:
            while True:
                pid = random.randint(1, 30)
                consultar_paciente(pid, args.url)
                time.sleep(args.intervalo)
        except KeyboardInterrupt:
            print("\nConsultas encerradas pelo usuário.")
            return

    if args.paciente is not None:
        consultar_paciente(args.paciente, args.url)
    elif args.aleatorio or args.paciente is None:
        pids = [1, 2, 3, 4, 5] if not args.aleatorio else [random.randint(1, 40) for _ in range(args.quantidade)]
        for pid in pids[:args.quantidade]:
            consultar_paciente(pid, args.url)
            time.sleep(0.4)

    print()
    print("✓ Sessão de consultas médicas concluída.")


if __name__ == "__main__":
    main()
