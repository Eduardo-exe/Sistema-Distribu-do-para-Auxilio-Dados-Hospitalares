#!/usr/bin/env bash
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

# 2. Verificar modo de execução
MODO_MIDDLEWARE=false
if [[ "$1" == "--middleware" ]]; then
    MODO_MIDDLEWARE=true
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
fuser -k -9 5000/tcp 8000/tcp 8001/tcp 8002/tcp 8003/tcp 2>/dev/null || true
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
    wait 2>/dev/null || true
    echo "  ✓ Todos os serviços foram finalizados com sucesso."
    echo "============================================================"
    exit 0
}

trap cleanup INT TERM

# ── Iniciar Servidores ─────────────────────────────────────────
echo "[1/5] Iniciando Servidor 01 (:8001)..."
"$PYTHON" server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db > logs/server1.log 2>&1 &
PIDS+=($!)

echo "[2/5] Iniciando Servidor 02 (:8002)..."
"$PYTHON" server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db > logs/server2.log 2>&1 &
PIDS+=($!)

echo "[3/5] Iniciando Servidor 03 (:8003)..."
"$PYTHON" server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db > logs/server3.log 2>&1 &
PIDS+=($!)

sleep 1

# ── Iniciar Balanceador de Carga ───────────────────────────────
echo "[4/5] Iniciando Load Balancer Round Robin (:8000)..."
"$PYTHON" load_balancer/balancer.py --port 8000 --servidores http://localhost:8001,http://localhost:8002,http://localhost:8003 > logs/balancer.log 2>&1 &
PIDS+=($!)

sleep 1

# ── Iniciar Monitor / Interface Visual ─────────────────────────
echo "[5/5] Iniciando Monitor e Interface Visual (:5000)..."
"$PYTHON" visualizacao/monitor.py --port 5000 > logs/monitor.log 2>&1 &
PIDS+=($!)

sleep 1.5

# ── Se modo middleware, iniciar consumidor AMQP ────────────────
if [ "$MODO_MIDDLEWARE" = true ]; then
    echo ""
    echo "── Middleware RabbitMQ ──────────────────────────────────────"

    # Verificar se RabbitMQ está acessível
    RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
    RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"

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
        echo "[MW] ✗ ERRO: RabbitMQ não está acessível em $RABBITMQ_HOST:$RABBITMQ_PORT"
        echo "[MW]   Inicie o RabbitMQ antes de usar o modo --middleware:"
        echo "[MW]     docker run -d --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management"
        echo "[MW]   Ou:"
        echo "[MW]     docker-compose up rabbitmq"
        echo ""
        echo "[MW] O sistema continua rodando no modo HTTP direto."
    else
        echo "[MW] Iniciando Consumidor AMQP..."
        RABBITMQ_HOST="$RABBITMQ_HOST" "$PYTHON" middleware/consumidor.py --lb-url http://localhost:8000 > logs/consumidor.log 2>&1 &
        PIDS+=($!)
        sleep 1
        echo "[MW] ✓ Consumidor AMQP iniciado!"
    fi
fi

echo "------------------------------------------------------------"
echo "  ✓ Todos os componentes estão prontos e ativos!"
echo "------------------------------------------------------------"
echo "  🌐 Interface Visual Web : http://localhost:5000"
echo "  ⚖️  Load Balancer Status : http://localhost:8000/status"
echo "  🖥️  Servidor 01 (API)    : http://localhost:8001"
echo "  🖥️  Servidor 02 (API)    : http://localhost:8002"
echo "  🖥️  Servidor 03 (API)    : http://localhost:8003"
echo "  📁 Logs dos serviços    : $DIR/logs/"

if [ "$MODO_MIDDLEWARE" = true ]; then
    echo "  🐰 RabbitMQ Management  : http://${RABBITMQ_HOST:-localhost}:15672"
    echo "  📨 Consumidor AMQP      : ativo (logs/consumidor.log)"
    echo ""
    echo "  Para enviar dados VIA MIDDLEWARE:"
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
wait "${PIDS[@]}"
