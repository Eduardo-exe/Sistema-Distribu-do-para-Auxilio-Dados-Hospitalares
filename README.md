# Sistema Distribuído para Auxílio de Dados Hospitalares

Sistema distribuído acadêmico para simulação de recebimento, processamento e
armazenamento de dados de monitoramento de pacientes hospitalares.

Desenvolvido para a disciplina de **Sistemas Distribuídos**.

## Objetivo

Demonstrar conceitos de sistemas distribuídos através de uma arquitetura com:

- **Leitor de dados**: lê um CSV real de monitoramento de pacientes e envia
  progressivamente via HTTP, simulando telemetria em tempo real.
- **Balanceador de carga**: distribui as requisições entre múltiplos servidores
  utilizando o algoritmo Round Robin.
- **Servidores independentes**: cada instância recebe, valida e armazena os
  dados em seu próprio banco SQLite.

### Arquitetura

```
                     CSV
                      |
                      v
               LEITOR DE DADOS
                      |
                      v
                BALANCEADOR (:8000)
                      |
           +----------+----------+
           |          |          |
           v          v          v
       SERVER 1   SERVER 2   SERVER 3
        :8001      :8002      :8003
           |          |          |
           v          v          v
       server1.db  server2.db  server3.db
```

## Estrutura do Projeto

```
Projeto SD/
├── data/
│   └── dataset.csv                  # Symlink para o CSV original
├── server/
│   ├── __init__.py
│   ├── app.py                       # API FastAPI (servidor)
│   ├── database.py                  # Camada de banco de dados SQLite
│   └── models.py                    # Modelos Pydantic + mapeamento CSV
├── client/
│   └── csv_sender.py                # Leitor/reprodutor do CSV
├── load_balancer/
│   └── balancer.py                  # Balanceador Round Robin
├── visualizacao/                    # Camada de Visualização Didática
│   ├── monitor.py                   # Servidor monitor FastAPI + SSE
│   └── index.html                   # Interface web animada em tempo real
├── tests/
│   ├── test_marco1.py               # Testes do Marco 1
│   └── test_marco2.py               # Testes do Marco 2
├── databases/                       # Bancos SQLite (criados automaticamente)
├── requirements.txt                 # Dependências Python
├── README.md                        # Este arquivo
└── Synthetic_patient-HealthCare-Monitoring_dataset.csv  # CSV original (Kaggle)
```

## Dependências

- Python 3.10+
- FastAPI
- Uvicorn
- Requests
- HTTPX
- Pytest

## Instalação

### 1. Criar ambiente virtual

```bash
cd "Projeto SD"
python3 -m venv venv
source venv/bin/activate
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Verificar o CSV

O dataset já deve estar presente na raiz do projeto:

```
Synthetic_patient-HealthCare-Monitoring_dataset.csv
```

Um symlink `data/dataset.csv` aponta para ele. Se precisar recriar:

```bash
ln -sf "../Synthetic_patient-HealthCare-Monitoring_dataset.csv" data/dataset.csv
```

## Como Configurar o CSV

O sistema utiliza o arquivo `Synthetic_patient-HealthCare-Monitoring_dataset.csv`
do Kaggle, que contém 60.000 registros de monitoramento de pacientes com as
seguintes colunas:

| Coluna no CSV | Campo na API |
|---|---|
| Patient Number | paciente_id |
| Heart Rate (bpm) | frequencia_cardiaca |
| SpO2 Level (%) | spo2 |
| Systolic Blood Pressure (mmHg) | pressao_sistolica |
| Diastolic Blood Pressure (mmHg) | pressao_diastolica |
| Body Temperature (°C) | temperatura |
| Fall Detection | deteccao_queda |
| Predicted Disease | doenca_prevista |
| Data Accuracy (%) | precisao_dados |
| Heart Rate Alert | alerta_freq_cardiaca |
| SpO2 Level Alert | alerta_spo2 |
| Blood Pressure Alert | alerta_pressao |
| Temperature Alert | alerta_temperatura |

O mapeamento é feito automaticamente pelo sistema (ver `server/models.py`).
Não é necessário alterar o CSV original.

## Execução

### Ativar o ambiente virtual

Em **todos os terminais**, ative o venv primeiro:

```bash
cd "Projeto SD"
source venv/bin/activate
```

### Terminal 1 — Servidor 1 (porta 8001)

```bash
python server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db
```

### Terminal 2 — Servidor 2 (porta 8002)

```bash
python server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db
```

### Terminal 3 — Servidor 3 (porta 8003)

```bash
python server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db
```

### Terminal 4 — Balanceador (porta 8000)

```bash
python load_balancer/balancer.py
```

Opções disponíveis:

```bash
python load_balancer/balancer.py --port 8000 --servidores http://localhost:8001,http://localhost:8002,http://localhost:8003
```

### Terminal 5 — Leitor do CSV

Enviar 12 registros com intervalo de 0.5 segundos:

```bash
python client/csv_sender.py --url http://localhost:8000/dados --intervalo 0.5 --limite 12
```

Enviar todos os registros com intervalo de 1 segundo:

```bash
python client/csv_sender.py --url http://localhost:8000/dados --intervalo 1.0
```

#### Parâmetros do csv_sender.py

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--url` | `http://localhost:8000/dados` | URL do endpoint POST |
| `--csv` | `data/dataset.csv` | Caminho do arquivo CSV |
| `--intervalo` | `1.0` | Segundos entre cada envio |
| `--limite` | `0` (todos) | Número máximo de registros |

---

## Interface Visual Didática (Demonstração Interativa)

Para apresentações e defesa acadêmica, o projeto conta com uma camada de visualização em tempo real (FastAPI + Server-Sent Events + SVG interativo) que ilustra visualmente todo o tráfego de dados e os princípios de sistemas distribuídos sem alterar qualquer lógica dos nós.

### Como Executar

Com os 3 servidores (portas 8001, 8002, 8003) e o Load Balancer (porta 8000) já em execução, abra um terminal e execute:

```bash
python visualizacao/monitor.py
```

Em seguida, abra no navegador:
```
http://localhost:5000
```

### Recursos da Interface

- **Animação em Tempo Real do Pacote**: visualização gráfica do fluxo completo percorrendo a topologia:
  1. Leitura do registro real no CSV (`data/dataset.csv`).
  2. Preparação do payload HTTP no cliente.
  3. Envio via `POST /dados` ao Load Balancer (porta 8000).
  4. Decisão de roteamento pelo algoritmo **Round Robin**.
  5. Recepção e validação Pydantic no servidor selecionado (porta 8001, 8002 ou 8003).
  6. Persistência relacional no SQLite correspondente (`server1.db`, `server2.db` ou `server3.db`).
  7. Retorno do status `HTTP 200 OK` com confirmação.
- **Controles de Apresentação**:
  - ▶ **Iniciar**: execução automática contínua.
  - ⏸ **Pausar**: congela a simulação a qualquer instante.
  - ⏭ **Próximo (Passo a Passo)**: ideal para explicar em detalhes o caminho de um pacote individual para a banca/professor.
  - ↻ **Reiniciar**: zera contadores e reinicia a leitura do início do dataset.
- **Painel de Sinais Vitais do Paciente**: exibe os dados reais do paciente em trânsito (FC com batimento cardíaco animado, SpO2, Pressão Arterial Sistólica/Diastólica, Temperatura, Nível de Dor e Consciência).
- **Indicador do Algoritmo Round Robin**: destaque visual do ponteiro do balanceador alternando entre `Servidor 01 → Servidor 02 → Servidor 03`.
- **Métricas de Distribuição de Carga**: barras de progresso proporcionais demonstrando em tempo real a divisão igualitária de carga (~33,3% para cada nó).
- **Log de Eventos em Tempo Real**: feed cronológico com código de cores por servidor e timestamps.
- **Modo Foco**: expande o diagrama da arquitetura para projeção em tela cheia.

---

## Como Testar com Postman

### POST /dados — Enviar dados manualmente

- **URL**: `http://localhost:8000/dados` (via balanceador) ou `http://localhost:8001/dados` (direto)
- **Método**: POST
- **Body** (JSON):

```json
{
    "paciente_id": 1,
    "frequencia_cardiaca": 80,
    "spo2": 97,
    "pressao_sistolica": 120,
    "pressao_diastolica": 80,
    "temperatura": 36.5,
    "deteccao_queda": "No",
    "doenca_prevista": "Normal",
    "precisao_dados": 95,
    "alerta_freq_cardiaca": "NORMAL",
    "alerta_spo2": "NORMAL",
    "alerta_pressao": "NORMAL",
    "alerta_temperatura": "NORMAL"
}
```

### GET /dados — Listar dados

- **URL**: `http://localhost:8000/dados?limit=10&offset=0`
- **Método**: GET

### GET /dados/{paciente_id} — Buscar paciente

- **URL**: `http://localhost:8000/dados/1`
- **Método**: GET

### GET /status — Status do balanceador

- **URL**: `http://localhost:8000/status`
- **Método**: GET

## Como Verificar a Distribuição das Requisições

### 1. Pelo terminal do balanceador

O balanceador exibe no terminal cada requisição encaminhada:

```
[BALANCEADOR] Requisição 1 → http://localhost:8001
[BALANCEADOR] Requisição 2 → http://localhost:8002
[BALANCEADOR] Requisição 3 → http://localhost:8003
[BALANCEADOR] Requisição 4 → http://localhost:8001
...
```

### 2. Pelo terminal do leitor de dados

O csv_sender exibe o servidor que respondeu cada requisição:

```
[    1] ✓ Paciente     1 → server-01 | FC=98 SpO2=96 PA=120/86 Temp=38.1
[    2] ✓ Paciente     2 → server-02 | FC=105 SpO2=97 PA=177/104 Temp=37.6
[    3] ✓ Paciente     3 → server-03 | FC=90 SpO2=85 PA=139/57 Temp=37.0
[    4] ✓ Paciente     4 → server-01 | FC=102 SpO2=87 PA=101/77 Temp=36.4
...
```

### 3. Pela resposta do POST

Cada resposta inclui `server_id` identificando qual servidor processou.

### 4. Pelo endpoint /status

```bash
curl http://localhost:8000/status
```

## Como Verificar os Bancos SQLite

### Usando sqlite3 no terminal

```bash
# Verificar servidor 1
sqlite3 databases/server1.db "SELECT COUNT(*) FROM dados_pacientes;"
sqlite3 databases/server1.db "SELECT id, paciente_id, server_id FROM dados_pacientes LIMIT 5;"

# Verificar servidor 2
sqlite3 databases/server2.db "SELECT COUNT(*) FROM dados_pacientes;"

# Verificar servidor 3
sqlite3 databases/server3.db "SELECT COUNT(*) FROM dados_pacientes;"

# Comparar contagens (devem ser aproximadamente iguais)
echo "Server 1:"; sqlite3 databases/server1.db "SELECT COUNT(*) FROM dados_pacientes;"
echo "Server 2:"; sqlite3 databases/server2.db "SELECT COUNT(*) FROM dados_pacientes;"
echo "Server 3:"; sqlite3 databases/server3.db "SELECT COUNT(*) FROM dados_pacientes;"
```

## Testes Automatizados

### Executar todos os testes

```bash
source venv/bin/activate
python -m pytest tests/ -v
```

### Apenas Marco 1

```bash
python -m pytest tests/test_marco1.py -v
```

### Apenas Marco 2

```bash
python -m pytest tests/test_marco2.py -v
```

### Testes incluídos

**Marco 1 (17 testes)**:
- CSV existe e é legível
- Colunas esperadas estão presentes
- Conversão de registros funciona
- Tipos de dados estão corretos
- Validação Pydantic funciona
- POST /dados recebe e armazena
- GET /dados lista registros com paginação
- GET /dados/{paciente_id} filtra por paciente
- Resposta identifica o servidor

**Marco 2 (9 testes)**:
- Round Robin alterna corretamente
- Round Robin cicla após percorrer todos
- Contador de requisições incrementa
- 3 servidores iniciam e respondem
- Balanceador inicia e responde
- POST via balanceador funciona
- Distribuição Round Robin é verificável
- Dados são armazenados no servidor correto
