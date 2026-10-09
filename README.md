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

### Arquitetura (com Middleware RabbitMQ)

```
                     CSV
                      |
                      v
               LEITOR DE DADOS
                      |
                      v  [publica via AMQP]
              ┌────────────────────┐
              │   MIDDLEWARE MOM   │
              │     RabbitMQ       │
              │  (porta 5672)      │
              │  Exchange + Fila   │
              └────────────────────┘
                      |
                      v  [consome via AMQP]
               CONSUMIDOR AMQP
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

> O cliente publica mensagens na fila do RabbitMQ (Middleware Orientado a Mensagens).
> Um consumidor AMQP consome da fila e encaminha via HTTP ao balanceador.
> O sistema também suporta o modo HTTP direto (sem middleware) para compatibilidade.

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
│   └── csv_sender.py                # Leitor/reprodutor do CSV (HTTP ou RabbitMQ)
├── middleware/                      # ★ MIDDLEWARE MOM (RabbitMQ)
│   ├── __init__.py
│   ├── config.py                    # Configuração centralizada do RabbitMQ
│   ├── produtor.py                  # Publicador AMQP (CSV → fila)
│   └── consumidor.py                # Consumidor AMQP (fila → Load Balancer)
├── load_balancer/
│   └── balancer.py                  # Balanceador Round Robin
├── visualizacao/                    # Camada de Visualização Didática
│   ├── monitor.py                   # Servidor monitor FastAPI + SSE
│   └── index.html                   # Interface web animada em tempo real
├── tests/
│   ├── test_marco1.py               # Testes do Marco 1
│   ├── test_marco2.py               # Testes do Marco 2
│   └── test_middleware.py           # Testes do Middleware RabbitMQ
├── databases/                       # Bancos SQLite (criados automaticamente)
├── docker-compose.yml               # Docker Compose (RabbitMQ + sistema)
├── Dockerfile                       # Imagem Docker do sistema
├── Vagrantfile                      # Definição das 4 VMs (VirtualBox)
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
- **Pika** (cliente AMQP para RabbitMQ)
- Pytest
- **RabbitMQ 3.x** (para modo middleware)
- **Docker + Docker Compose** (opcional, para desenvolvimento local)
- **Vagrant + VirtualBox** (opcional, para deploy em VMs separadas)

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

### Terminal 5 — Leitor do CSV (modo HTTP direto)

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
| `--via-rabbitmq` | desativado | Enviar via middleware RabbitMQ |

---

## Middleware RabbitMQ (MOM)

O sistema possui uma camada de **Middleware Orientado a Mensagens (MOM)** usando
**RabbitMQ**, que desacopla temporalmente o produtor de dados dos servidores de
processamento.

### O que é o Middleware no contexto de SD?

Em Sistemas Distribuídos, **middleware** é a camada de software entre o sistema
operacional e a aplicação distribuída. Ele provê:

- **Transparência de acesso**: o produtor não precisa saber quais servidores estão ativos
- **Desacoplamento temporal**: mensagens são enfileiradas mesmo que os servidores estejam fora
- **Confiabilidade**: mensagens persistentes sobrevivem a reinícios do broker
- **Escalabilidade**: múltiplos consumidores podem processar a fila em paralelo

### Componentes do Middleware

| Componente | Arquivo | Descrição |
|---|---|---|
| Configuração | `middleware/config.py` | Exchange, fila, credenciais |
| Produtor AMQP | `middleware/produtor.py` | Publica dados do CSV na fila |
| Consumidor AMQP | `middleware/consumidor.py` | Consome e encaminha ao LB |

### Como Executar com Middleware

#### 1. Iniciar o RabbitMQ

Usando Docker (mais rápido):

```bash
docker run -d --rm --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

Acesse a interface de gerenciamento: http://localhost:15672 (guest/guest)

#### 2. Executar o sistema com middleware

```bash
./executar.sh --middleware
```

#### 3. Enviar dados via middleware

```bash
python client/csv_sender.py --via-rabbitmq --intervalo 0.3 --limite 20
```

Ou usando o produtor diretamente:

```bash
python middleware/produtor.py --intervalo 0.3 --limite 20
```

### Fluxo das Mensagens

```
CSV Sender → [publica AMQP] → RabbitMQ (fila) → [consome AMQP] → HTTP POST → Load Balancer → Server
```

1. O **Produtor** lê o CSV e publica cada registro como mensagem JSON persistente
2. O **RabbitMQ** armazena na fila `fila_dados_pacientes` (exchange `telemetria_hospitalar`)
3. O **Consumidor** lê uma mensagem de cada vez (prefetch=1)
4. Para cada mensagem, faz `HTTP POST /dados` ao Load Balancer
5. Só envia **ACK** após confirmação do Load Balancer (HTTP 200)
6. Se houver erro, envia **NACK** com requeue para retentar

---

## Deploy em Máquinas Virtuais (VirtualBox + Vagrant)

O projeto inclui um `Vagrantfile` com 6 VMs totalmente isoladas:

| VM | IP | Componentes |
|---|---|---|
| `vm-cliente` | 192.168.56.10 | Leitor CSV + Produtor AMQP |
| `vm-middleware` | 192.168.56.11 | RabbitMQ Server (portas 5672 e 15672) |
| `vm-balanceador` | 192.168.56.12 | Consumidor AMQP + Balanceador Round Robin (:8000) |
| `vm-servidor1` | 192.168.56.13 | Servidor 01 (FastAPI :8001) + `server1.db` |
| `vm-servidor2` | 192.168.56.14 | Servidor 02 (FastAPI :8002) + `server2.db` |
| `vm-servidor3` | 192.168.56.15 | Servidor 03 (FastAPI :8003) + `server3.db` |

### Pré-requisitos

- [VirtualBox](https://www.virtualbox.org/)
- [Vagrant](https://www.vagrantup.com/)

### Criar e Provisionar as VMs

```bash
vagrant up
```

### Executar em VMs Separadas

**VM-SERVIDOR-01** (192.168.56.13):
```bash
vagrant ssh vm-servidor1
cd /opt/projeto-sd
python3 server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db
```

**VM-SERVIDOR-02** (192.168.56.14):
```bash
vagrant ssh vm-servidor2
cd /opt/projeto-sd
python3 server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db
```

**VM-SERVIDOR-03** (192.168.56.15):
```bash
vagrant ssh vm-servidor3
cd /opt/projeto-sd
python3 server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db
```

**VM-BALANCEADOR** (192.168.56.12):
```bash
vagrant ssh vm-balanceador
cd /opt/projeto-sd
python3 load_balancer/balancer.py --port 8000 \
  --servidores http://192.168.56.13:8001,http://192.168.56.14:8002,http://192.168.56.15:8003 &
RABBITMQ_HOST=192.168.56.11 python3 middleware/consumidor.py --lb-url http://localhost:8000
```

**VM-CLIENTE** (192.168.56.10):
```bash
vagrant ssh vm-cliente
cd /opt/projeto-sd
RABBITMQ_HOST=192.168.56.11 python3 middleware/produtor.py --limite 20 --intervalo 0.3
```

### Acessar a Interface do RabbitMQ

No navegador do host: http://192.168.56.11:15672 (guest/guest)

---

## Docker Compose (Desenvolvimento Local)

Para rodar tudo localmente com containers:

```bash
# Apenas RabbitMQ (para desenvolvimento)
docker-compose up rabbitmq

# Sistema completo (servidores + balanceador + consumidor + RabbitMQ)
docker-compose up

# Enviar dados via produtor
docker-compose --profile envio up produtor
```

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

### Apenas Middleware

```bash
# Requer RabbitMQ rodando
python -m pytest tests/test_middleware.py -v
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

**Middleware RabbitMQ (12 testes)**:
- Configuração importa e valida
- Parâmetros de conexão são válidos
- Exchange e fila são declarados corretamente
- Produtor publica mensagens na fila
- Múltiplas mensagens são enfileiradas
- Conteúdo JSON é preservado
- Consumidor lê mensagens da fila
- Mensagens persistem sem consumidor ativo
- NACK com requeue devolve mensagem
- Fluxo publicar/consumir esvazia a fila
- Ordem FIFO é mantida
- ACK remove mensagem definitivamente
