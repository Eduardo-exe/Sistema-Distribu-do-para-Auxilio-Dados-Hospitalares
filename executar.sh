#!/usr/bin/env bash
set +m
# ==============================================================================
# Script de Inicialização — Sistema Distribuído Hospitalar
# ==============================================================================
#
# Uso:
#   ./executar.sh              → Modo original (sem middleware)
#   ./executar.sh --middleware → Modo com RabbitMQ (requer RabbitMQ rodando)
#
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# 1. Identificar interpretador Python do venv
if [ -d "$DIR/venv" ]; then
    PYTHON="$DIR/venv/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON="python3"
else
    PYTHON="python"
fi

# 2. Verificar modo de execução (Middleware RabbitMQ ativo por padrão)
MODO_MIDDLEWARE=true
if [[ "$1" == "--no-middleware" ]] || [[ "$1" == "--sem-middleware" ]]; then
    MODO_MIDDLEWARE=false
fi

# 3. Criar pastas necessárias
mkdir -p databases logs

echo "============================================================"
echo "  SISTEMA DISTRIBUÍDO PARA AUXÍLIO DE DADOS HOSPITALARES"
echo "============================================================"
echo "Python: $PYTHON"
echo "Diretório: $DIR"
if [ "$MODO_MIDDLEWARE" = true ]; then
    echo "Modo: MIDDLEWARE RabbitMQ (MOM)"
else
    echo "Modo: HTTP direto (sem middleware)"
fi
echo "------------------------------------------------------------"

# Liberar portas caso instâncias antigas ainda estejam ativas
echo "Verificando e liberando portas do sistema..."
fuser -k -9 5000/tcp 8000/tcp 8001/tcp 8002/tcp 8003/tcp 8004/tcp 2>/dev/null || true
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
    docker stop rabbitmq-sd 2>/dev/null || true
    wait 2>/dev/null || true
    echo "  ✓ Todos os serviços foram finalizados com sucesso."
    echo "============================================================"
    exit 0
}

trap cleanup INT TERM

# ── Iniciar Servidores Backend ─────────────────────────────────
echo "[1/6] Iniciando Servidor 01 (:8001)..."
"$PYTHON" server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db > logs/server1.log 2>&1 &
PIDS+=($!)

echo "[2/6] Iniciando Servidor 02 (:8002)..."
"$PYTHON" server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db > logs/server2.log 2>&1 &
PIDS+=($!)

echo "[3/6] Iniciando Servidor 03 (:8003)..."
"$PYTHON" server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db > logs/server3.log 2>&1 &
PIDS+=($!)

echo "[4/6] Iniciando Servidor 04 Reserva (:8004) [Warm Standby]..."
"$PYTHON" server/app.py --port 8004 --server-id server-04 --db-path databases/server4.db > logs/server4.log 2>&1 &
PIDS+=($!)

sleep 1

# ── Iniciar Balanceador de Carga ───────────────────────────────
echo "[5/6] Iniciando Load Balancer Round Robin (:8000)..."
"$PYTHON" load_balancer/balancer.py --port 8000 \
    --servidores http://localhost:8001,http://localhost:8002,http://localhost:8003 \
    --servidores-reserva http://localhost:8004 > logs/balancer.log 2>&1 &
PIDS+=($!)

sleep 1

# ── Iniciar Monitor / Interface Visual ─────────────────────────
PORTA_MONITOR="${PORT:-5000}"
echo "[6/6] Iniciando Monitor e Interface Visual (:$PORTA_MONITOR)..."
"$PYTHON" visualizacao/monitor.py --port "$PORTA_MONITOR" > logs/monitor.log 2>&1 &
PIDS+=($!)

sleep 1.5

# ── Se modo middleware, iniciar consumidores Fanout ───────────
if [ "$MODO_MIDDLEWARE" = true ]; then
    echo ""
    echo "── Middleware RabbitMQ (Marco 3: Replicação Fanout) ──────────"

    # Verificar se RabbitMQ está acessível ou iniciar automaticamente no Docker
    RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
    RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"

    if ! timeout 2 bash -c "echo > /dev/tcp/$RABBITMQ_HOST/$RABBITMQ_PORT" 2>/dev/null; then
        if command -v docker &>/dev/null; then
            echo "[MW] Subindo broker RabbitMQ automaticamente via Docker..."
            docker run -d --rm --name rabbitmq-sd -p 5672:5672 -p 15672:15672 rabbitmq:3-management 2>/dev/null || docker start rabbitmq-sd 2>/dev/null || true
            sleep 2
        fi
    fi

    echo "[MW] Verificando RabbitMQ em $RABBITMQ_HOST:$RABBITMQ_PORT..."
    MAX_TENTATIVAS=10
    TENTATIVA=0
    while [ $TENTATIVA -lt $MAX_TENTATIVAS ]; do
        if timeout 2 bash -c "echo > /dev/tcp/$RABBITMQ_HOST/$RABBITMQ_PORT" 2>/dev/null; then
            echo "[MW] ✓ RabbitMQ acessível!"
            break
        fi
        TENTATIVA=$((TENTATIVA + 1))
        echo "[MW] Tentativa $TENTATIVA/$MAX_TENTATIVAS — aguardando RabbitMQ..."
        sleep 2
    done

    if [ $TENTATIVA -ge $MAX_TENTATIVAS ]; then
        echo "[MW] ✗ AVISO: RabbitMQ não está acessível. Sistema continuará no modo direto."
    else
        echo "[MW] Iniciando Consumidores AMQP Fanout (Marco 3)..."
        RABBITMQ_HOST="$RABBITMQ_HOST" "$PYTHON" middleware/consumidor.py --multi > logs/consumidor.log 2>&1 &
        PIDS+=($!)
        sleep 1
        echo "[MW] ✓ Consumidores Fanout ativos (sincronizando server1.db a server4.db)!"
    fi
fi

echo "------------------------------------------------------------"
echo "  ✓ Todos os componentes estão prontos e ativos!"
echo "------------------------------------------------------------"
echo "  🌐 Interface Visual Web : http://localhost:5000"
echo "  ⚖️  Load Balancer Status : http://localhost:8000/status"
echo "  🏥 Servidor 01 (Ativo)  : http://localhost:8001"
echo "  🏥 Servidor 02 (Ativo)  : http://localhost:8002"
echo "  🏥 Servidor 03 (Ativo)  : http://localhost:8003"
echo "  🛡️  Servidor 04 (Reserva): http://localhost:8004 (Warm Standby)"
echo "  📁 Logs dos serviços    : $DIR/logs/"
echo ""
echo "  ⚡ Comandos de Escalabilidade Dinâmica:"
echo "    Ativar Reserva (Scale-Out)   : curl -X POST http://localhost:8000/servidores/ativar-reserva"
echo "    Desativar Reserva (Scale-In) : curl -X POST http://localhost:8000/servidores/desativar-reserva"

if [ "$MODO_MIDDLEWARE" = true ]; then
    echo ""
    echo "  🐰 RabbitMQ Management  : http://${RABBITMQ_HOST:-localhost}:15672"
    echo "  📨 Replicação Fanout    : ativa em 4 filas (logs/consumidor.log)"
    echo ""
    echo "  Para enviar dados VIA MIDDLEWARE FANOUT:"
    echo "    python client/csv_sender.py --via-rabbitmq --intervalo 0.3 --limite 20"
    echo ""
    echo "  Ou usando o produtor diretamente:"
    echo "    python middleware/produtor.py --intervalo 0.3 --limite 20"
fi

echo "------------------------------------------------------------"
echo "Abra http://localhost:5000 no navegador para interagir com o sistema."
echo "Pressione [Ctrl+C] a qualquer momento para desligar todos os nós."
echo "============================================================"

# Manter o script ativo aguardando os processos ou Ctrl+C
# Manter o script ativo até Ctrl+C ser pressionado (resistente a testes de queda de nós)
while true; do
    sleep 2 &
    wait  2>/dev/null || true
done
