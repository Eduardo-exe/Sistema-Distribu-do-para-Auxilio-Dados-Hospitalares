#!/usr/bin/env bash
# ==============================================================================
# Teste de Carga — Demonstração de Gargalo e Scale-Out
# ==============================================================================
#
# Uso:
#   ./teste_carga.sh           → Envia 30 requisições e mostra tempos
#   ./teste_carga.sh 50        → Envia 50 requisições
#
# O script:
#   1. Lê dados reais do CSV (data/dataset.csv)
#   2. Dispara requisições simultâneas ao Load Balancer (:8000)
#   3. Mostra qual servidor atendeu cada uma e quanto tempo levou
#   4. No final, exibe a distribuição de carga entre os servidores
#
# Fluxo sugerido para apresentação:
#   1. Rodar com 3 servidores → ver distribuição 33% cada
#   2. Ativar servidor reserva (botão no painel ou curl)
#   3. Rodar novamente → ver distribuição 25% cada
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

LB_URL="http://localhost:8000"
CSV_FILE="$DIR/data/dataset.csv"
TOTAL=${1:-30}

# Cores
AZUL='\033[0;34m'
ROXO='\033[0;35m'
ROSA='\033[0;95m'
VERDE='\033[0;32m'
AMARELO='\033[1;33m'
VERMELHO='\033[0;31m'
CINZA='\033[0;90m'
RESET='\033[0m'
BOLD='\033[1m'

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  🏥 TESTE DE CARGA — Sistema Distribuído Hospitalar${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════════${RESET}"

# Verificar status do balanceador
echo -e "\n${AMARELO}📊 Consultando status do Load Balancer...${RESET}"
STATUS=$(curl -s "$LB_URL/status" 2>/dev/null)
if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}❌ Load Balancer não está respondendo em $LB_URL${RESET}"
    echo -e "${CINZA}   Execute primeiro: ./executar.sh${RESET}"
    exit 1
fi

ATIVOS=$(echo "$STATUS" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d['total_servidores'])" 2>/dev/null)
DIST=$(echo "$STATUS" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d['distribuicao_esperada_por_no'])" 2>/dev/null)
RESERVAS=$(echo "$STATUS" | python3 -c "import sys, json; d=json.load(sys.stdin); print(len(d.get('servidores_reserva',[])))" 2>/dev/null)

echo -e "  Servidores ativos: ${BOLD}${ATIVOS}${RESET}"
echo -e "  Servidores reserva: ${BOLD}${RESERVAS}${RESET}"
echo -e "  Distribuição esperada: ${BOLD}${DIST} por nó${RESET}"
echo -e "  Requisições a enviar: ${BOLD}${TOTAL}${RESET}"

echo -e "\n${AMARELO}🚀 Disparando ${TOTAL} requisições ao Load Balancer...${RESET}"
echo -e "${CINZA}────────────────────────────────────────────────────────────${RESET}"

# Contadores
S1=0; S2=0; S3=0; S4=0
ERROS=0
TEMPO_TOTAL=0

# Ler linhas do CSV (pular header)
LINHAS=$(tail -n +2 "$CSV_FILE" | head -n "$TOTAL")

i=0
while IFS=',' read -r line; do
    i=$((i + 1))

    # Extrair campos do CSV (adaptar conforme colunas)
    # Usar Python para converter a linha CSV para JSON
    JSON=$(python3 -c "
import csv, json, sys, io
sys.path.insert(0, '.')
from server.models import CSV_COLUMN_MAP, converter_registro_csv

reader = csv.DictReader(io.StringIO('$(head -1 "$CSV_FILE")\n$line'))
for row in reader:
    print(json.dumps(converter_registro_csv(row)))
" 2>/dev/null)

    if [ -z "$JSON" ]; then
        continue
    fi

    # Medir tempo de resposta
    INICIO=$(date +%s%N)
    RESP=$(curl -s -w "\n%{http_code}" -X POST "$LB_URL/dados" \
        -H "Content-Type: application/json" \
        -d "$JSON" 2>/dev/null)
    FIM=$(date +%s%N)

    HTTP_CODE=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | sed '$d')
    TEMPO_MS=$(( (FIM - INICIO) / 1000000 ))
    TEMPO_TOTAL=$((TEMPO_TOTAL + TEMPO_MS))

    # Extrair server_id da resposta
    SERVER_ID=$(echo "$BODY" | python3 -c "import sys, json; print(json.load(sys.stdin).get('server_id','?'))" 2>/dev/null)

    # Colorir por servidor
    COR=$CINZA
    case "$SERVER_ID" in
        server-01) COR=$AZUL; S1=$((S1 + 1)) ;;
        server-02) COR=$ROXO; S2=$((S2 + 1)) ;;
        server-03) COR=$ROSA; S3=$((S3 + 1)) ;;
        server-04) COR=$VERDE; S4=$((S4 + 1)) ;;
        *) ERROS=$((ERROS + 1)) ;;
    esac

    # Indicador de latência
    LAT_ICON="⚡"
    if [ "$TEMPO_MS" -gt 100 ]; then LAT_ICON="🟡"; fi
    if [ "$TEMPO_MS" -gt 300 ]; then LAT_ICON="🔴"; fi

    printf "  ${COR}#%03d${RESET} → ${COR}%-12s${RESET} ${CINZA}│${RESET} ${LAT_ICON} %4dms ${CINZA}│${RESET} HTTP ${HTTP_CODE}\n" \
        "$i" "$SERVER_ID" "$TEMPO_MS"

done <<< "$LINHAS"

# Resultados
MEDIA=$((TEMPO_TOTAL / (i > 0 ? i : 1)))
echo -e "${CINZA}────────────────────────────────────────────────────────────${RESET}"
echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  📊 RESULTADO DO TESTE DE CARGA${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════════${RESET}"
echo ""
echo -e "  Total enviadas:   ${BOLD}$i${RESET} requisições"
echo -e "  Tempo médio:      ${BOLD}${MEDIA}ms${RESET} por requisição"
echo -e "  Erros:            ${BOLD}${ERROS}${RESET}"
echo ""
echo -e "  ${BOLD}Distribuição de Carga:${RESET}"

# Calcular porcentagens
SUCESSO=$((S1 + S2 + S3 + S4))
if [ "$SUCESSO" -gt 0 ]; then
    P1=$((S1 * 100 / SUCESSO))
    P2=$((S2 * 100 / SUCESSO))
    P3=$((S3 * 100 / SUCESSO))
    P4=$((S4 * 100 / SUCESSO))
else
    P1=0; P2=0; P3=0; P4=0
fi

# Barras visuais
bar() {
    local pct=$1
    local width=$((pct / 3))
    printf '%0.s█' $(seq 1 $width 2>/dev/null) 2>/dev/null
}

echo -e "  ${AZUL}Servidor 01:${RESET}  $S1 req  ($P1%)  ${AZUL}$(bar $P1)${RESET}"
echo -e "  ${ROXO}Servidor 02:${RESET}  $S2 req  ($P2%)  ${ROXO}$(bar $P2)${RESET}"
echo -e "  ${ROSA}Servidor 03:${RESET}  $S3 req  ($P3%)  ${ROSA}$(bar $P3)${RESET}"
if [ "$S4" -gt 0 ] || [ "$RESERVAS" = "0" ]; then
    echo -e "  ${VERDE}Servidor 04:${RESET}  $S4 req  ($P4%)  ${VERDE}$(bar $P4)${RESET}  (Reserva)"
else
    echo -e "  ${CINZA}Servidor 04:  — (Standby / não ativo no balanceamento)${RESET}"
fi

echo ""
echo -e "${CINZA}────────────────────────────────────────────────────────────${RESET}"

# Dica
if [ "$RESERVAS" -gt 0 ]; then
    echo -e "  ${AMARELO}💡 Dica: Ative o servidor reserva e rode novamente:${RESET}"
    echo -e "     ${VERDE}curl -X POST $LB_URL/servidores/ativar-reserva${RESET}"
    echo -e "     ${CINZA}Depois: ./teste_carga.sh $TOTAL${RESET}"
else
    echo -e "  ${VERDE}✅ Servidor reserva ATIVO! Compare com o resultado anterior.${RESET}"
    echo -e "  ${AMARELO}💡 Para desativar: curl -X POST $LB_URL/servidores/desativar-reserva${RESET}"
fi
echo ""
