#!/usr/bin/env bash
# ==============================================================================
# Script de Túnel Público Cloudflare — Sistema Distribuído Hospitalar
# ==============================================================================
# Cria um link público HTTPS instantâneo para acessar o monitor de qualquer
# celular, computador ou rede externa sem necessidade de abrir portas no roteador.
#
# Uso:
#   ./tunel.sh        → Expõe a interface visual (porta 5000)
#   ./tunel.sh 8000   → Expõe o load balancer (porta 8000)
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORTA="${1:-5000}"
BIN_CLOUDFLARED="$DIR/bin/cloudflared"

# 1. Garantir que o binário do cloudflared existe
if [ ! -f "$BIN_CLOUDFLARED" ]; then
    echo "============================================================"
    echo "  Baixando cliente Cloudflare Tunnel (cloudflared)..."
    echo "============================================================"
    mkdir -p "$DIR/bin"
    curl -sL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o "$BIN_CLOUDFLARED"
    chmod +x "$BIN_CLOUDFLARED"
    echo "  ✓ Download concluído!"
fi

# 2. Verificar se o serviço local está respondendo
if ! timeout 1 bash -c "echo > /dev/tcp/localhost/$PORTA" 2>/dev/null; then
    echo "⚠️  AVISO: Nenhum serviço foi detectado rodando na porta $PORTA."
    echo "   Lembre-se de rodar ./executar.sh em outro terminal!"
    echo ""
fi

echo "============================================================"
echo "  Iniciando Túnel Público Cloudflare..."
echo "  Porta local: $PORTA"
echo "============================================================"

LOG_TUNEL="$DIR/logs/tunel.log"
mkdir -p "$DIR/logs"

# Iniciar cloudflared salvando log
"$BIN_CLOUDFLARED" tunnel --url "http://127.0.0.1:$PORTA" > "$LOG_TUNEL" 2>&1 &
PID_TUNEL=$!

cleanup() {
    echo ""
    echo "Encerrando túnel público..."
    kill "$PID_TUNEL" 2>/dev/null || true
    pkill -P "$PID_TUNEL" 2>/dev/null || true
    echo "✓ Túnel finalizado."
    exit 0
}

trap cleanup INT TERM

# Aguardar extração do link público do log
echo "Aguardando geração do link seguro..."
URL_TUNEL=""
TENTATIVAS=0
MAX_TENTATIVAS=20

while [ $TENTATIVAS -lt $MAX_TENTATIVAS ]; do
    if [ -f "$LOG_TUNEL" ]; then
        URL_TUNEL=$(grep -oP 'https://[a-zA-Z0-9-]+\.trycloudflare\.com' "$LOG_TUNEL" | head -n 1)
        if [ -n "$URL_TUNEL" ]; then
            break
        fi
    fi
    sleep 1
    TENTATIVAS=$((TENTATIVAS + 1))
done

if [ -n "$URL_TUNEL" ]; then
    echo ""
    echo "============================================================"
    echo "  🎉 TÚNEL PÚBLICO CRIADO COM SUCESSO!"
    echo "============================================================"
    echo ""
    echo "  🔗 LINK PÚBLICO (Copie e envie para qualquer pessoa):"
    echo ""
    echo "     👉 $URL_TUNEL"
    echo ""
    echo "  🔒 Protocolo : HTTPS (Certificado SSL válido automático)"
    echo "  🖥️  Destino   : http://localhost:$PORTA (Seu PC)"
    echo "  🌍 Acesso    : Funciona no celular, 4G/5G, fora de casa e sem senhas!"
    echo "============================================================"
    echo "  Pressione [Ctrl+C] a qualquer momento para fechar o túnel."
    echo "============================================================"
else
    echo "✗ Não foi possível extrair a URL do túnel automaticamente."
    echo "Verifique o log em: $LOG_TUNEL"
fi

# Manter ativo aguardando o processo do túnel
wait "$PID_TUNEL" 2>/dev/null || true
