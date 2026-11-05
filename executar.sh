#!/usr/bin/env bash
set +m
# ==============================================================================
# Script de Inicialização — Sistema Distribuído Hospitalar
# ==============================================================================
#
# Uso:
#   ./executar.sh              → Inicia todo o cluster com Apache Kafka (modo padrão)
#   ./executar.sh --tunel      → Inicia cluster + cria link público HTTPS (Cloudflare)
#   ./executar.sh --no-middleware → Inicia sem Kafka (modo HTTP direto)
#
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Logs imediatos: sem isso o stdout dos processos redirecionados para logs/
# fica em buffer e os arquivos de log parecem vazios durante a execução.
export PYTHONUNBUFFERED=1

# 1. Identificar interpretador Python do venv
if [ -d "$DIR/venv" ]; then
    PYTHON="$DIR/venv/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON="python3"
else
    PYTHON="python"
fi

# 2. Verificar modo de execução e argumentos
MODO_MIDDLEWARE="kafka"
MODO_TUNEL=true

for arg in "$@"; do
    if [[ "$arg" == "--no-middleware" ]] || [[ "$arg" == "--sem-middleware" ]]; then
        MODO_MIDDLEWARE="nenhum"
    fi
    if [[ "$arg" == "--no-tunel" ]] || [[ "$arg" == "--sem-tunel" ]]; then
        MODO_TUNEL=false
    fi
done

# 3. Criar pastas necessárias
mkdir -p databases logs

echo "============================================================"
echo "  SISTEMA DISTRIBUÍDO PARA AUXÍLIO DE DADOS HOSPITALARES"
echo "============================================================"
echo "Python: $PYTHON"
echo "Diretório: $DIR"
if [ "$MODO_MIDDLEWARE" = "kafka" ]; then
    echo "Modo: MIDDLEWARE APACHE KAFKA (MOM Enterprise)"
    echo "  ☕ Apache Kafka Broker : KRaft Mode (:9092)"
    echo "  📦 Tópico de Telemetria : telemetria_hospitalar (4 partições)"
    echo "  🔄 Replicação Fanout   : Consumer Groups dedicados (server1..server4)"
    echo "  🛡️  Tolerância a Falhas : Circuit Breaker + Dead Letter Topic (DLT)"
else
    echo "Modo: HTTP direto (sem middleware)"
fi
echo "------------------------------------------------------------"

# Liberar portas caso instâncias antigas ainda estejam ativas
echo "Verificando e liberando portas do sistema..."
fuser -k -9 5000/tcp 8000/tcp 8001/tcp 8002/tcp 8003/tcp 8004/tcp 2>/dev/null || true
pkill -f "cloudflared" 2>/dev/null || true
rm -f "$DIR/logs/tunel.log"
sleep 1

PIDS=()

cleanup() {
    echo ""
    echo "============================================================"
    echo "  Encerrando serviços..."
    echo "============================================================"
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    fuser -k -9 5000/tcp 8000/tcp 8001/tcp 8002/tcp 8003/tcp 8004/tcp 2>/dev/null || true
    docker stop kafka-sd 2>/dev/null || true
    pkill -f "cloudflared" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "  ✓ Todos os serviços foram finalizados com sucesso."
    echo "============================================================"
    exit 0
}

trap cleanup INT TERM

# ── Iniciar Servidores Backend (VMs Reais Heterogêneas) ─────────────
echo "[1/6] Iniciando Servidor 01 (VM: vm-servidor-01 @ 192.168.56.13:8001 | Ubuntu 22.04 LTS)..."
"$PYTHON" server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db --vm-name vm-servidor-01 --vm-ip 192.168.56.13 --vm-os "Ubuntu 22.04 LTS (Jammy)" > logs/server1.log 2>&1 &
PIDS+=($!)

echo "[2/6] Iniciando Servidor 02 (VM: vm-servidor-02 @ 192.168.56.14:8002 | Debian 12 Bookworm)..."
"$PYTHON" server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db --vm-name vm-servidor-02 --vm-ip 192.168.56.14 --vm-os "Debian 12 (Bookworm)" > logs/server2.log 2>&1 &
PIDS+=($!)

echo "[3/6] Iniciando Servidor 03 (VM: vm-servidor-03 @ 192.168.56.15:8003 | Ubuntu 20.04 LTS)..."
"$PYTHON" server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db --vm-name vm-servidor-03 --vm-ip 192.168.56.15 --vm-os "Ubuntu 20.04 LTS (Focal)" > logs/server3.log 2>&1 &
PIDS+=($!)

echo "[4/6] Iniciando Servidor 04 Reserva (VM: vm-servidor-04 @ 192.168.56.16:8004 | Debian 12 Bookworm) [Warm Standby]..."
"$PYTHON" server/app.py --port 8004 --server-id server-04 --db-path databases/server4.db --vm-name vm-servidor-04 --vm-ip 192.168.56.16 --vm-os "Debian 12 (Bookworm)" > logs/server4.log 2>&1 &
PIDS+=($!)

sleep 1

# ── Iniciar Balanceador de Carga ───────────────────────────────
echo "[5/6] Iniciando Load Balancer Round Robin (:8000)..."
"$PYTHON" load_balancer/balancer.py --port 8000 \
    --servidores http://localhost:8001,http://localhost:8002,http://localhost:8003 \
    --servidores-reserva http://localhost:8004 \
    --auto-scale > logs/balancer.log 2>&1 &
PIDS+=($!)

sleep 1

# ── Iniciar Monitor / Interface Visual ─────────────────────────
PORTA_MONITOR="${PORT:-5000}"
echo "[6/6] Iniciando Monitor e Interface Visual (:$PORTA_MONITOR)..."
"$PYTHON" visualizacao/monitor.py --port "$PORTA_MONITOR" > logs/monitor.log 2>&1 &
PIDS+=($!)

sleep 1.5

# ── Middleware Apache Kafka (Modo Padrão) ─────────────────────
if [ "$MODO_MIDDLEWARE" = "kafka" ]; then
    echo ""
    echo "── Broker Apache Kafka (MOM Enterprise) ─────────────────────"

    KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
    KAFKA_HOST=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d: -f1)
    KAFKA_PORT=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d: -f2)
    KAFKA_PORT="${KAFKA_PORT:-9092}"

    if ! timeout 2 bash -c "echo > /dev/tcp/$KAFKA_HOST/$KAFKA_PORT" 2>/dev/null; then
        if command -v docker &>/dev/null; then
            echo "[MW] Subindo broker Apache Kafka via Docker (KRaft mode)..."
            docker run -d --rm --name kafka-sd \
                -p 9092:9092 \
                -e KAFKA_NODE_ID=1 \
                -e KAFKA_PROCESS_ROLES=broker,controller \
                -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
                -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
                -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
                -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
                -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
                -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
                -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
                -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
                -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
                -e KAFKA_NUM_PARTITIONS=4 \
                apache/kafka:latest 2>/dev/null || docker start kafka-sd 2>/dev/null || true
            sleep 3
        fi
    fi

    echo "[MW] Verificando Kafka em $KAFKA_BOOTSTRAP_SERVERS..."
    MAX_TENTATIVAS=15
    TENTATIVA=0
    while [ $TENTATIVA -lt $MAX_TENTATIVAS ]; do
        if timeout 2 bash -c "echo > /dev/tcp/$KAFKA_HOST/$KAFKA_PORT" 2>/dev/null; then
            echo "[MW] ✓ Apache Kafka acessível!"
            break
        fi
        TENTATIVA=$((TENTATIVA + 1))
        echo "[MW] Tentativa $TENTATIVA/$MAX_TENTATIVAS — aguardando Kafka..."
        sleep 2
    done

    if [ $TENTATIVA -ge $MAX_TENTATIVAS ]; then
        echo "[MW] ✗ AVISO: Kafka não está acessível. Verifique se o broker está rodando."
    else
        echo "[MW] Iniciando Consumidores Kafka Multi-Group..."
        KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" "$PYTHON" -m middleware.consumidor --multi > logs/kafka_consumidor.log 2>&1 &
        PIDS+=($!)
        sleep 1
        echo "[MW] ✓ Consumidores Kafka ativos (replicação ativa fanout em server1..server4)!"
    fi
fi

# Detectar IP local para facilitar acesso via celular e outros dispositivos da rede
IP_LOCAL=$(ip -4 addr show scope global 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n 1)
[ -z "$IP_LOCAL" ] && IP_LOCAL="127.0.0.1"

# ── Túnel Público Cloudflare (Iniciado Automaticamente com o Sistema) ──
URL_TUNEL=""
if [ "$MODO_TUNEL" = true ]; then
    echo ""
    echo "── Túnel Público Cloudflare (Acesso Externo Seguro) ─────────"
    echo "Iniciando túnel para a porta $PORTA_MONITOR..."
    BIN_CLOUDFLARED="$DIR/bin/cloudflared"
    if [ ! -f "$BIN_CLOUDFLARED" ] || [ ! -s "$BIN_CLOUDFLARED" ]; then
        echo "Baixando cliente Cloudflare Tunnel (cloudflared)..."
        mkdir -p "$DIR/bin"
        curl -sL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o "$BIN_CLOUDFLARED"
    fi
    chmod +x "$BIN_CLOUDFLARED" 2>/dev/null || true

    LOG_TUNEL="$DIR/logs/tunel.log"
    rm -f "$LOG_TUNEL"
    "$BIN_CLOUDFLARED" tunnel --url "http://127.0.0.1:$PORTA_MONITOR" > "$LOG_TUNEL" 2>&1 &
    PID_TUNEL=$!
    PIDS+=("$PID_TUNEL")

    echo "Aguardando geração do link público seguro (HTTPS)..."
    for i in {1..20}; do
        if [ -f "$LOG_TUNEL" ]; then
            URL_TUNEL=$(grep -oE 'https://[a-zA-Z0-9.-]+\.trycloudflare\.com' "$LOG_TUNEL" | head -n 1)
            [ -n "$URL_TUNEL" ] && break
        fi
        sleep 1
    done
fi

echo "------------------------------------------------------------"
echo "  ✓ Todos os componentes estão prontos e ativos!"
echo "------------------------------------------------------------"
echo "  🌐 Acesso Local (Este PC) : http://localhost:$PORTA_MONITOR"
echo "  📱 Acesso na Rede (Wi-Fi)  : http://$IP_LOCAL:$PORTA_MONITOR"

if [ -n "$URL_TUNEL" ]; then
    echo ""
    echo "  ============================================================"
    echo "  🎉 LINK PÚBLICO GERADO COM SUCESSO (HTTPS MUNDIAL):"
    echo ""
    echo "     👉 $URL_TUNEL"
    echo ""
    echo "  (Funciona de qualquer celular, 4G/5G, fora de casa e sem senhas)"
    echo "  ============================================================"
else
    echo "  🌍 Link Público (Internet) : gere a qualquer momento com ./tunel.sh"
fi

echo "  ⚖️  Load Balancer (API)    : http://$IP_LOCAL:8000"
echo "  🏥 Servidor 01 (:8001)     : http://$IP_LOCAL:8001 (VM: vm-servidor-01 | 192.168.56.13)"
echo "  🏥 Servidor 02 (:8002)     : http://$IP_LOCAL:8002 (VM: vm-servidor-02 | 192.168.56.14)"
echo "  🏥 Servidor 03 (:8003)     : http://$IP_LOCAL:8003 (VM: vm-servidor-03 | 192.168.56.15)"
echo "  🛡️  Servidor 04 (:8004)     : http://$IP_LOCAL:8004 (VM: vm-servidor-04 | 192.168.56.16 - Warm Standby)"
echo "  📁 Logs dos serviços       : $DIR/logs/"
echo ""
echo "  🖥️  Topologia de Máquinas Virtuais (Sub-rede 192.168.56.0/24):"
echo "    • vm-cliente      192.168.56.10  (Produtor de Telemetria)"
echo "    • vm-middleware   192.168.56.11  (Apache Kafka KRaft :9092)"
echo "    • vm-balanceador  192.168.56.12  (Load Balancer Enterprise :8000)"
echo "    • vm-servidor-01  192.168.56.13  (Servidor 01 :8001 + server1.db)"
echo "    • vm-servidor-02  192.168.56.14  (Servidor 02 :8002 + server2.db)"
echo "    • vm-servidor-03  192.168.56.15  (Servidor 03 :8003 + server3.db)"
echo "    • vm-servidor-04  192.168.56.16  (Servidor 04 :8004 + server4.db - Reserva)"
echo "    • vm-monitor      192.168.56.17  (Monitor & Dashboard :5000)"
echo ""
echo "  ⚡ Comandos de Escalabilidade Dinâmica:"
echo "    Ativar Reserva (Scale-Out)   : curl -X POST http://localhost:8000/servidores/ativar-reserva"
echo "    Desativar Reserva (Scale-In) : curl -X POST http://localhost:8000/servidores/desativar-reserva"

if [ "$MODO_MIDDLEWARE" = "kafka" ]; then
    echo ""
    echo "  ☕ Apache Kafka Middleware : $KAFKA_BOOTSTRAP_SERVERS"
    echo "  📦 Tópico de Telemetria    : telemetria_hospitalar (4 partições)"
    echo "  🔄 Replicação Fanout       : Consumer Groups (server1..server4)"
    echo "  🛡️  Tolerância a Falhas     : Circuit Breaker + Dead Letter Topic (DLT)"
    echo ""
    echo "  ⚡ Comandos de Envio via Middleware:"
    echo "    Via Kafka MOM (Recomendado) : python client/csv_sender.py --via-kafka --intervalo 0.3 --limite 10"
    echo "    Produtor direto Kafka       : python -m middleware.produtor --intervalo 0.3 --limite 10"
fi

echo "------------------------------------------------------------"
if [ -n "$URL_TUNEL" ]; then
    echo "  💡 Link pronto! Copie e cole no navegador:"
    echo "     👉 $URL_TUNEL"
else
    echo "  💡 No celular ou outro PC no mesmo Wi-Fi, abra:"
    echo "     👉 http://$IP_LOCAL:$PORTA_MONITOR"
fi
echo ""
echo "  Pressione [Ctrl+C] a qualquer momento para desligar todos os nós."
echo "============================================================"

# Manter o script ativo até Ctrl+C ser pressionado
while true; do
    sleep 2 &
    wait 2>/dev/null || true
done
