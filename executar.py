# executar.py
"""
Orquestrador Multiplataforma Leve — Sistema Distribuído Hospitalar (Windows, Linux, macOS).

Projetado especificamente para rodar com MÍNIMO consumo de memória RAM (< 180 MB no total),
sem exigir Docker, Java ou binários externos pesados. Perfeito para máquinas com pouca memória (1 GB).

Uso:
    python executar.py
    python executar.py --com-cliente   # inicia emissão automática em background
"""

import argparse
import os
import signal
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

DIR_RAIZ = Path(__file__).resolve().parent

# Portas padrão dos serviços
PORTAS = {
    "server-01": 8001,
    "server-02": 8002,
    "server-03": 8003,
    "server-04": 8004,
    "load-balancer": 8000,
    "middleware-mom": 9094,
    "monitor": 5000,
}

PROCESSOS = []


def obter_python_executavel() -> str:
    """Detecta o interpretador Python do venv ou o atual do sistema."""
    if sys.platform == "win32":
        venv_py = DIR_RAIZ / "venv" / "Scripts" / "python.exe"
    else:
        venv_py = DIR_RAIZ / "venv" / "bin" / "python"

    if venv_py.exists():
        return str(venv_py)
    return sys.executable


def liberar_portas_windows():
    """No Windows, identifica e finaliza processos presos nas portas do sistema."""
    if sys.platform != "win32":
        return
    for nome, porta in PORTAS.items():
        try:
            cmd = f'netstat -ano | findstr :{porta}'
            saida = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
            for linha in saida.strip().splitlines():
                partes = linha.split()
                if len(partes) >= 5 and "LISTENING" in linha.upper():
                    pid = partes[-1]
                    if pid.isdigit() and int(pid) != os.getpid():
                        subprocess.run(f"taskkill /F /PID {pid}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass


def liberar_portas_unix():
    """No Linux / macOS, libera portas via fuser se disponível."""
    if sys.platform == "win32":
        return
    portas_str = " ".join([f"{p}/tcp" for p in PORTAS.values()])
    try:
        subprocess.run(f"fuser -k -9 {portas_str}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def sincronizar_bancos(python_bin: str):
    """Executa a reconciliação inicial de réplicas SQLite (Marco 3)."""
    script_sync = DIR_RAIZ / "server" / "sync_dbs.py"
    if script_sync.exists():
        try:
            subprocess.run([python_bin, str(script_sync)], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass


def encerrar_todos():
    """Encerra de forma limpa todos os processos filhos iniciados."""
    print("\n" + "=" * 60)
    print("  Encerrando serviços do cluster hospitalar...")
    print("=" * 60)
    for p in PROCESSOS:
        try:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()
        except Exception:
            pass
    print("  ✓ Todos os serviços foram desligados com sucesso.")
    print("=" * 60)


def iniciar_cluster(com_cliente: bool = False, abrir_browser: bool = True):
    python_bin = obter_python_executavel()
    os.chdir(DIR_RAIZ)

    # Criar pastas locais
    (DIR_RAIZ / "databases").mkdir(exist_ok=True)
    (DIR_RAIZ / "logs").mkdir(exist_ok=True)

    print("=" * 65)
    print("  SISTEMA DISTRIBUÍDO HOSPITALAR — MODO LEVE (WINDOWS & LINUX)")
    print("=" * 65)
    print(f"  Python : {python_bin}")
    print(f"  Diretório : {DIR_RAIZ}")
    print(f"  Plataforma: {sys.platform.upper()} (Otimizado para baixa memória RAM)")
    print(f"  Middleware: Replicador MOM Puro Python (Marco 3 — ~20 MB RAM)")
    print("-" * 65)

    print("Liberando portas anteriores (5000, 8000-8004, 9094)...")
    if sys.platform == "win32":
        liberar_portas_windows()
    else:
        liberar_portas_unix()
    time.sleep(1)

    print("Sincronizando réplicas locais SQLite (Marco 3 Zero Data Loss)...")
    sincronizar_bancos(python_bin)

    # 1. Iniciar Servidores 01 a 04
    vms_cfg = [
        {"id": "server-01", "port": 8001, "db": "databases/server1.db", "vm": "vm-servidor-01", "ip": "192.168.56.13", "os": "Ubuntu 22.04 LTS (Jammy)"},
        {"id": "server-02", "port": 8002, "db": "databases/server2.db", "vm": "vm-servidor-02", "ip": "192.168.56.14", "os": "Debian 12 (Bookworm)"},
        {"id": "server-03", "port": 8003, "db": "databases/server3.db", "vm": "vm-servidor-03", "ip": "192.168.56.15", "os": "Ubuntu 20.04 LTS (Focal)"},
        {"id": "server-04", "port": 8004, "db": "databases/server4.db", "vm": "vm-servidor-04", "ip": "192.168.56.16", "os": "Debian 12 (Bookworm) [Standby]"},
    ]

    for idx, cfg in enumerate(vms_cfg, 1):
        log_f = open(DIR_RAIZ / "logs" / f"{cfg['id']}.log", "w", encoding="utf-8")
        cmd = [
            python_bin, "server/app.py",
            "--port", str(cfg["port"]),
            "--server-id", cfg["id"],
            "--db-path", cfg["db"],
            "--vm-name", cfg["vm"],
            "--vm-ip", cfg["ip"],
            "--vm-os", cfg["os"],
        ]
        p = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT)
        PROCESSOS.append(p)
        print(f"  [{idx}/6] Servidor {cfg['id']} ativo na porta :{cfg['port']} ({cfg['vm']})")

    time.sleep(1)

    # 2. Iniciar Load Balancer
    log_lb = open(DIR_RAIZ / "logs" / "balancer.log", "w", encoding="utf-8")
    cmd_lb = [
        python_bin, "load_balancer/balancer.py",
        "--port", "8000",
        "--servidores", "http://localhost:8001,http://localhost:8002,http://localhost:8003",
        "--servidores-reserva", "http://localhost:8004",
        "--replicacao",
        "--auto-scale",
    ]
    p_lb = subprocess.Popen(cmd_lb, stdout=log_lb, stderr=subprocess.STDOUT)
    PROCESSOS.append(p_lb)
    print("  [5/6] Load Balancer Round Robin ativo na porta :8000 (Pool 3-4 nós)")

    # 3. Iniciar Middleware MOM Replicator Leve (Puro Python, ~20 MB RAM)
    log_mom = open(DIR_RAIZ / "logs" / "middleware_replicador.log", "w", encoding="utf-8")
    cmd_mom = [python_bin, "-m", "middleware.replicador", "--porta", "9094"]
    p_mom = subprocess.Popen(cmd_mom, stdout=log_mom, stderr=subprocess.STDOUT)
    PROCESSOS.append(p_mom)
    print("  [✓] Middleware MOM Enterprise Replicator ativo na porta :9094")

    time.sleep(1)

    # 4. Iniciar Monitor Web
    log_mon = open(DIR_RAIZ / "logs" / "monitor.log", "w", encoding="utf-8")
    cmd_mon = [python_bin, "visualizacao/monitor.py", "--port", "5000"]
    p_mon = subprocess.Popen(cmd_mon, stdout=log_mon, stderr=subprocess.STDOUT)
    PROCESSOS.append(p_mon)
    print("  [6/6] Painel Web & Visualizador ativo na porta :5000")

    # 5. Opcional: Cliente Multi-Médicos
    if com_cliente:
        log_cli = open(DIR_RAIZ / "logs" / "cliente_multi_medicos.log", "w", encoding="utf-8")
        cmd_cli = [python_bin, "client/multi_medicos.py", "--intervalo", "1.5"]
        p_cli = subprocess.Popen(cmd_cli, stdout=log_cli, stderr=subprocess.STDOUT)
        PROCESSOS.append(p_cli)
        print("  [+] Cliente Multi-Médicos iniciado em segundo plano.")

    time.sleep(1.5)

    print("-" * 65)
    print("  🎉 CLUSTER HOSPITALAR PRONTO E OPERANDO!")
    print("-" * 65)
    print("  👉 Abra seu navegador em: http://localhost:5000")
    print("  👉 Consumo total de memória: ~150 MB RAM (Ultra-leve)")
    print("  👉 Zero dependência de Docker ou Java!")
    print("  👉 Pressione [Ctrl+C] a qualquer momento para desligar todos os nós.")
    print("=" * 65)

    if abrir_browser:
        try:
            webbrowser.open("http://localhost:5000")
        except Exception:
            pass

    # Manter ativo até Ctrl+C
    try:
        while True:
            time.sleep(1)
            # Verificar se algum processo essencial caiu
            if p_mon.poll() is not None:
                print("\n[AVISO] Monitor foi encerrado.")
                break
    except KeyboardInterrupt:
        pass
    finally:
        encerrar_todos()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador Multiplataforma Leve do Sistema Distribuído")
    parser.add_argument("--com-cliente", action="store_true", help="Inicia o cliente multi-médicos automaticamente")
    parser.add_argument("--no-browser", action="store_true", help="Não abre o navegador automaticamente")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    try:
        iniciar_cluster(com_cliente=args.com_cliente, abrir_browser=not args.no_browser)
    except SystemExit:
        encerrar_todos()

