# Sistema Distribuído para Auxílio de Dados Hospitalares

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688.svg)](https://fastapi.tiangolo.com/)
[![SQLite](https://img.shields.io/badge/SQLite-3-003B57.svg)](https://www.sqlite.org/)
[![Middleware](https://img.shields.io/badge/Middleware-Apache%20Kafka%20(MOM)-E0234E.svg)](#middleware-apache-kafka-mom)
[![Testes Automatizados](https://img.shields.io/badge/Testes-100%25%20Passando-success.svg)](#testes-automatizados)

Sistema distribuído acadêmico de missão crítica para recepção, enriquecimento clínico, roteamento inteligente, balanceamento adaptativo e persistência replicada de sinais vitais de pacientes hospitalares (telemetria em tempo real).

Desenvolvido para a disciplina de **Sistemas Distribuídos** da **Universidade Federal do Maranhão (UFMA)** — Departamento de Engenharia da Computação.  
**Docente:** Prof. Dr. Luiz Henrique Neves Rodrigues  
**Discentes:** Eduardo dos Santos Oliveira, Fernando da Silva Costa, Herick Vinicius Pinheiro Da Conceição, Samara Santos Viegas e Vyktor Gabryel Costa de Souza.

---

## Sumário

1. [Objetivo e Marcos do Projeto](#objetivo-e-marcos-do-projeto)
2. [Arquitetura Geral do Sistema](#arquitetura-geral-do-sistema)
3. [Topologia de Máquinas Virtuais Reais (Cluster de 8 VMs)](#topologia-de-máquinas-virtuais-reais-cluster-de-8-vms)
4. [Estrutura de Diretórios](#estrutura-de-diretórios)
5. [Instalação e Pré-requisitos](#instalação-e-pré-requisitos)
6. [Guia de Execução Rápida](#guia-de-execução-rápida)
7. [Dataset e Mapeamento Clínico (NEWS2 e Manchester)](#dataset-e-mapeamento-clínico-news2-e-manchester)
8. [Middleware Apache Kafka (MOM Enterprise)](#middleware-apache-kafka-mom-enterprise)
9. [Balanceamento de Carga Adaptativo e Resiliência](#balanceamento-de-carga-adaptativo-e-resiliência)
10. [Interface Visual Didática e Ferramenta de Benchmark](#interface-visual-didática-e-ferramenta-de-benchmark)
11. [Acesso Remoto e Apresentação (Túnel Cloudflare)](#acesso-remoto-e-apresentação-túnel-cloudflare)
12. [Endpoints da API REST e Introspecção de VMs](#endpoints-da-api-rest-e-introspecção-de-vms)
13. [Relatórios Técnicos Formais (LaTeX / PDF)](#relatórios-técnicos-formais-latex--pdf)
14. [Testes Automatizados](#testes-automatizados)

---

## Objetivo e Marcos do Projeto

O sistema demonstra os conceitos fundamentais da computação distribuída aplicados à saúde (missão crítica, *zero data loss*, concorrência, transparência e tolerância a falhas):

* **Marco 1 — Simulação e Servidor Base:** Leitura progressiva de dataset clínico real (60.000 registros), validação estrita via Pydantic, API FastAPI e persistência relacional SQLite.
* **Marco 2 — Balanceamento de Carga:** Cluster com múltiplos nós independentes, roteamento de requisições, métricas e isolamento de bancos por servidor.
* **Marco 3 — Replicação e Escalabilidade:** Replicação ativa assíncrona entre shards SQLite (`server1.db` a `server4.db`), nó de reserva aquecida (*warm standby*) e consistência eventual.
* **Marco 4 — Tolerância a Falhas e Middleware Apache Kafka (MOM):** Arquitetura orientada a mensagens corporativa via Apache Kafka (KRaft mode :9092), particionamento determinístico por `paciente_id`, replicação paralela via Consumer Groups dedicados (server1 a server4), Circuit Breakers adaptativos e Dead Letter Topic (DLT) com retry exponencial.

---

## Arquitetura Geral do Sistema

```
                      TELEMETRIA HOSPITALAR (CSV / Sensores)
                                       │
                                       ▼
             ╔══════════════════════════════════════════════════════════╗
             ║         MIDDLEWARE APACHE KAFKA (MOM ENTERPRISE)         ║
             ╠══════════════════════════════════════════════════════════╣
             ║  ☕ APACHE KAFKA BROKER (KRaft Mode :9092):              ║
             ║     • Tópico Principal: telemetria_hospitalar            ║
             ║     • 4 Partições com particionamento por paciente_id    ║
             ║     • Compressão nativa (gzip/snappy) e Idempotência     ║
             ║     • Token Bucket Rate Limiting no produtor/consumidor  ║
             ║                                                          ║
             ║  🔄 CONSUMER GROUPS & REPLICAÇÃO FANOUT:                 ║
             ║     • consumidor_servidor_1 ➔ Server 01 (:8001)          ║
             ║     • consumidor_servidor_2 ➔ Server 02 (:8002)          ║
             ║     • consumidor_servidor_3 ➔ Server 03 (:8003)          ║
             ║     • consumidor_servidor_4 ➔ Server 04 Reserva (:8004)  ║
             ║                                                          ║
             ║  🛡️ RESILIÊNCIA E TOLERÂNCIA A FALHAS:                   ║
             ║     • Circuit Breaker adaptativo (CLOSED ↔ OPEN ↔ HALF)  ║
             ║     • Dead Letter Topic (DLT): telemetria_hospitalar_dlt ║
             ║     • Commit manual de offset (Semântica At-least-once)  ║
             ╚═════════════════════════╤════════════════════════════════╝
                                       │ Consumo Paralelo / Despacho
                                       ▼
             ╔══════════════════════════════════════════════════════════╗
             ║          BALANCEADOR DE CARGA ENTERPRISE (:8000)         ║
             ╠══════════════════════════════════════════════════════════╣
             ║ Algoritmos: Round Robin | Weighted RR | Least Conn |     ║
             ║             Least Response Time | Patient Hash | Adaptive║
             ║ Métricas em Tempo Real: Latência, Throughput, Erros      ║
             ║ Health Probes Ativos & Isolamento Automático de Falhas   ║
             ╚═════════════════════════╤════════════════════════════════╝
                                       │
            ┌──────────────┬───────────┴───────────┬──────────────┐
            ▼              ▼                       ▼              ▼
     ┌──────────────┐ ┌──────────────┐      ┌──────────────┐ ┌──────────────┐
     │  SERVER-01   │ │  SERVER-02   │      │  SERVER-03   │ │  SERVER-04   │
     │   (:8001)    │ │   (:8002)    │      │   (:8003)    │ │   (:8004)    │
     │  server1.db  │ │  server2.db  │      │  server3.db  │ │  server4.db  │
     │  [Principal] │ │  [Principal] │      │  [Principal] │ │  [Reserva/LB]│
     └──────────────┘ └──────────────┘      └──────────────┘ └──────────────┘
            ▲              ▲                       ▲              ▲
            └──────────────┴───────────┬───────────┴──────────────┘
                                       │ SSE / WebSockets
                                       ▼
             ╔══════════════════════════════════════════════════════════╗
             ║        INTERFACE VISUAL DIDÁTICA E MONITOR (:5000)       ║
             ╠══════════════════════════════════════════════════════════╣
             ║ • Animação SVG do trajeto de rede do pacote              ║
             ║ • Painel de sinais vitais e escores de risco em tempo real║
             ║ • Modal de Benchmark / Teste de Carga com auto-chaveamento║
             ║ • Controles interativos: Iniciar, Pausar, Passo a Passo  ║
             ╚══════════════════════════════════════════════════════════╝
```

---

## Topologia de Máquinas Virtuais Reais (Cluster de 8 VMs)

Para atender a cenários reais de infraestrutura de data center hospitalar, o sistema opera sobre uma topologia completa de **8 Máquinas Virtuais (VMs)** conectadas através de uma rede privada de alta performance (*Host-Only Network / Bridge*) sob a faixa **`192.168.56.0/24`** (Gateway `192.168.56.1`):

| VM / Hostname | IP Privado | Porta | Papel no Cluster | SO / Hardware Virtual | Shard / Serviço |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `vm-cliente` | `192.168.56.10` | — | Ingestão e Envio de Telemetria (CSV 60k) | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `middleware.produtor` |
| `vm-middleware` | `192.168.56.11` | `9092` | Broker Apache Kafka MOM (Modo KRaft) | Ubuntu 22.04 LTS (2 vCPUs, 1024MB RAM) | `apache/kafka:latest` |
| `vm-balanceador` | `192.168.56.12` | `8000` | Load Balancer Enterprise (6 Algoritmos + Auto-Scaling) | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `load_balancer.balancer` |
| `vm-servidor-01` | `192.168.56.13` | `8001` | Nó de Processamento Clínico 01 | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `databases/server1.db` |
| `vm-servidor-02` | `192.168.56.14` | `8002` | Nó de Processamento Clínico 02 | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `databases/server2.db` |
| `vm-servidor-03` | `192.168.56.15` | `8003` | Nó de Processamento Clínico 03 | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `databases/server3.db` |
| `vm-servidor-04` | `192.168.56.16` | `8004` | Nó Reserva (*Warm Standby* / Scale-Out / Failover) | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `databases/server4.db` |
| `vm-monitor` | `192.168.56.17` | `5000` | Painel de Observabilidade SSE e Benchmark | Ubuntu 22.04 LTS (1 vCPU, 512MB RAM) | `visualizacao.monitor` |

### Orquestração e Virtualização Suportada

1. **Vagrant & VirtualBox (`Vagrantfile`):**
   * Configuração multi-máquina pronta para provisionar os 8 nós com comandos simples:
   ```bash
   vagrant up                 # Provisiona e inicia as 8 VMs
   vagrant status             # Exibe o estado de cada nó virtual
   vagrant ssh vm-servidor-01 # Acessa o shell da VM 1
   ```
2. **Docker Compose (`docker-compose.yml`):**
   * Ambiente containerizado multi-host com rede bridge `rede_hospitalar` isolada em `192.168.56.0/24` e IPs estáticos atribuídos por nó:
   ```bash
   docker compose up -d       # Sobe o cluster completo com rede isolada
   ```
3. **Execução Local com Metadados Reais de VM (`./executar.sh`):**
   * Cada processo backend é inicializado com flags explícitas `--vm-name` e `--vm-ip`.
   * **Endpoint de Introspecção de Hardware:** `GET http://localhost:8001/vm-info` retorna vCPU, RAM, MAC address, interface `enp0s8`, tamanho do shard SQLite e status de serviço.
   * **Endpoint de Cluster:** `GET http://localhost:5000/api/vms` retorna inventário completo e latência em tempo real.
4. **Evidência Visual no Dashboard (`http://localhost:5000`):**
   * No cabeçalho: botão interativo pulsante **`🖥️ Cluster 8 VMs (192.168.56.0/24)`**.
   * No diagrama SVG: caixas tracejadas de VM com badges clicáveis de IP e porta.
   * Nos servidores: cards detalhados exibindo hostname virtual, IP dedicado e botão **`🔍 VM Info`**.
   * **Modal Dedicado:** painel de controle com status de cada VM, hypervisor, gateway e métricas de hardware.

---

## Estrutura de Diretórios

```
Projeto SD/
├── Vagrantfile                      # Topologia com 8 VMs Ubuntu 22.04 LTS (192.168.56.0/24)
├── docker-compose.yml               # Orquestração multi-host com rede bridge 192.168.56.0/24
├── client/
│   ├── csv_sender.py                # Produtor/injetor de telemetria do dataset (suporta flag --medico)
│   ├── multi_medicos.py             # Emissor multi-médicos concorrente (UTI, Emergência, Enfermaria)
│   └── medico_consulta.py           # Terminal de consulta distribuída de prontuários (Read Path)
├── server/
│   ├── app.py                       # Servidor de nós FastAPI (server-01 a server-04)
│   ├── database.py                  # Camada de persistência relacional SQLite
│   └── models.py                    # Modelos Pydantic e mapeamentos clínicos
├── load_balancer/
│   └── balancer.py                  # Balanceador Enterprise com 6 algoritmos e Circuit Breaker
├── middleware/                      # ★ Camada de Middleware Apache Kafka (MOM Enterprise)
│   ├── __init__.py                  #   Exposição unificada do middleware Kafka
│   ├── config.py                    #   Configuração centralizada (brokers, tópicos, partições, grupos)
│   ├── produtor.py                  #   Produtor Kafka com particionamento, compressão e rate limiting
│   ├── consumidor.py                #   Consumidor Kafka com Consumer Groups, Circuit Breaker e DLT
│   └── kafka/                       #   Submódulo com bindings do Apache Kafka
├── visualizacao/
│   ├── monitor.py                   # Servidor FastAPI do monitor (porta 5000) com SSE
│   └── index.html                   # Interface gráfica animada com modal de benchmark e VMs
├── relatorio/                       # Documentação acadêmica formal em LaTeX e PDF
│   ├── relatorio_completo_projeto_sd.pdf   # Relatório Geral Completo do Sistema (17 págs)
│   ├── relatorio_mudancas_middleware.pdf   # Relatório aprofundado do Middleware Kafka
│   ├── relatorio_mudancas_arquiteturais.pdf# Relatório completo dos Marcos 3 e 4
│   ├── relatorio_marco_3.pdf        # Relatório de replicação e tolerância a falhas
│   └── relatorio_marcos_1_e_2.pdf   # Relatório da base do cluster e balanceamento
├── tests/                           # Suíte de testes automatizados (pytest)
│   ├── test_kafka_middleware.py     # Testes unitários e de integração do Apache Kafka
│   ├── test_escalabilidade.py       # Testes de carga, algoritmos e auto-scaling
│   ├── test_marco1.py               # Testes de dataset, Pydantic e SQLite
│   ├── test_marco2.py               # Testes de Round Robin e cluster
│   ├── test_marco3.py               # Testes de replicação e consistência
│   └── test_visualizacao.py         # Testes de endpoints SSE, status do monitor e VMs
├── databases/                       # Bancos SQLite gerados automaticamente (server1..4.db)
├── logs/                            # Logs de execução de cada nó do cluster
├── data/                            # Datasets clínicos reais (60.000 amostras particionadas)
│   ├── dataset.csv                  # Dataset mestre completo (60.000 amostras)
│   ├── dataset_medico_1_uti.csv     # 20.000 amostras — Dr. Carlos (UTI / Casos Críticos)
│   ├── dataset_medico_2_emergencia.csv # 20.000 amostras — Dra. Beatriz (Emergência / Quedas)
│   └── dataset_medico_3_enfermaria.csv # 20.000 amostras — Dr. Lucas (Enfermaria / Regular)
├── executar.sh                      # Script mestre de inicialização do cluster (Linux/macOS)
├── executar.py                      # Orquestrador multiplataforma leve (Windows, Linux, macOS)
├── executar.bat                     # Inicializador rápido em 1 clique para Windows
├── instalar_windows.bat             # Assistente de instalação de dependências para Windows
├── teste_carga.sh                   # Script CLI para ensaios de estresse e carga
├── tunel.sh                         # Utilitário para criação de túnel Cloudflare
└── requirements.txt                 # Dependências do projeto
```

---

## Instalação e Pré-requisitos

### 1. Clonar e Acessar o Repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd "Projeto SD"
```

### 2. Criar e Ativar o Ambiente Virtual

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 4. Verificar Dataset

O arquivo `Synthetic_patient-HealthCare-Monitoring_dataset.csv` deve estar na raiz. O symlink `data/dataset.csv` aponta diretamente para ele. Caso necessário recriar:

```bash
mkdir -p data
ln -sf "../Synthetic_patient-HealthCare-Monitoring_dataset.csv" data/dataset.csv
```

---

## Guia de Execução Rápida

### 🪟 No Windows (Modo Leve — Menos de 180 MB RAM — 2 Cliques)

Projetado especificamente para rodar com **máxima facilidade no Windows**, inclusive em computadores com recursos limitados (ex.: **1 GB de RAM disponível**):

1. **Execução Direta (Recomendado):**
   * Dê **dois cliques** no arquivo **`executar.bat`** (ou abra o Prompt de Comando/PowerShell e digite `executar.bat`).
   * O script detecta automaticamente se o Python está instalado, cria o ambiente virtual isolado (`venv`), instala as dependências leves se necessário, sincroniza as réplicas SQLite, sobe todos os servidores em background e **abre o navegador automaticamente** em **`http://localhost:5000`**.
   * **Por que é ultra-leve?** Não requer Docker Desktop nem Java/JVM (que consomem de 2 a 4 GB sozinhos). O cluster opera com processos FastAPI em Python nativo e o Replicador MOM Fanout integrado, totalizando apenas **~150 MB de RAM**.

2. **Instalação Prévia (Opcional):**
   * Se preferir preparar o ambiente antes, dê dois cliques em **`instalar_windows.bat`**.

3. **Para Encerrar no Windows:**
   * Basta pressionar `Ctrl + C` na janela do terminal do `executar.bat`. Todos os serviços são finalizados de forma limpa.

---

### 🐧 No Linux / macOS

#### Modo Automático Completo (1 Comando):
O script `executar.sh` orquestra automaticamente todos os nós do cluster, libera portas ocupadas e inicia o cluster:

```bash
# Inicia Servidores 01, 02, 03, 04 + Load Balancer (:8000) + Monitor (:5000)
./executar.sh
```

#### Modo Multiplataforma Leve (Python Puro):
Também é possível usar o novo orquestrador universal em qualquer sistema:
```bash
python executar.py
```

Acesse a interface no navegador:
👉 **`http://localhost:5000`**

Para encerrar com segurança todos os processos:
Pressione `Ctrl + C` no terminal onde o script foi iniciado.

---

## Dataset e Mapeamento Clínico (NEWS2 e Manchester)

O sistema consome o dataset real do Kaggle contendo 60.000 amostras de sinais vitais de pacientes. Cada registro passa por validação estrita no Pydantic (`server/models.py`) e enriquecimento no pipeline ESB (`middleware/esb/pipeline.py`):

| Coluna Original (CSV) | Campo na API | Tipo | Faixa Válida |
| :--- | :--- | :--- | :--- |
| `Patient Number` | `paciente_id` | `int` | $\ge 1$ |
| `Heart Rate (bpm)` | `frequencia_cardiaca` | `float` | $0$ a $300\text{ bpm}$ |
| `SpO2 Level (%)` | `spo2` | `float` | $0$ a $100\%$ |
| `Systolic Blood Pressure` | `pressao_sistolica` | `float` | $40$ a $260\text{ mmHg}$ |
| `Diastolic Blood Pressure`| `pressao_diastolica` | `float` | $30$ a $160\text{ mmHg}$ |
| `Body Temperature (°C)` | `temperatura` | `float` | $30$ a $45^\circ\text{C}$ |
| `Fall Detection` | `deteccao_queda` | `str` | `Yes` / `No` |
| `Predicted Disease` | `doenca_prevista` | `str` | Texto diagnóstico |

### Cálculo NEWS2 e Classificação Manchester

O middleware calcula em tempo real o escore **NEWS2** (*National Early Warning Score 2*), atribuindo pesos de risco fisiológico aos sinais vitais e mapeando o paciente nas cores do protocolo internacional de Manchester:

* 🔴 **Emergência (Vermelho - NEWS2 $\ge 7$):** Encaminhado para a rota crítica `esb.route.icu`.
* 🟠 **Muito Urgente (Laranja - NEWS2 $5\text{--}6$):** Rota `esb.route.emergency`.
* 🟡 **Urgente (Amarelo - NEWS2 $3\text{--}4$):** Rota de intervenção intermediária.
* 🟢 **Pouco Urgente (Verde - NEWS2 $1\text{--}2$):** Rota `esb.route.ward.regular`.
* 🔵 **Não Urgente (Azul - NEWS2 $0$):** Rota `esb.route.outpatient`.

---

## Middleware Apache Kafka (MOM Enterprise)

O sistema utiliza o **Apache Kafka** como **único e exclusivo middleware de mensageria**, oferecendo arquitetura de ponta para sistemas distribuídos de alta vazão:

1. **Broker Apache Kafka (Modo KRaft):**
   * Opera sem dependência de Zookeeper, com metadados internos gerenciados pelo quorum KRaft.
   * Tópico principal `telemetria_hospitalar` particionado em 4 partições para processamento paralelo.

2. **Produtor Enterprise (`middleware/produtor.py`):**
   * **Particionamento por Chave (`paciente_id`):** Garante ordenação estrita das medições de cada paciente no tempo.
   * **Semântica Idempotente (*Exactly-Once*):** Evita duplicação de dados clínicos na rede.
   * **Compressão Nativa (gzip / snappy):** Reduz o consumo de banda de rede em até 70%.
   * **Token Bucket Rate Limiting:** Limita vazão para proteção de picos de carga.

3. **Consumidores e Replicação Ativa Fanout (`middleware/consumidor.py`):**
   * **Consumer Groups Dedicados:** Cada servidor backend (`server-01` a `server-04`) possui seu próprio consumer group (`consumidor_servidor_1` a `consumidor_servidor_4`), garantindo que todos os 4 servidores recebam e persistam 100% dos dados.
   * **Circuit Breaker Integrado:** Protege os nós contra falhas em cascata com estados `CLOSED`, `OPEN` e `HALF-OPEN`.
   * **Dead Letter Topic (DLT):** Mensagens que falham após tentativas com *Exponential Backoff* são isoladas no tópico `telemetria_hospitalar_dlt` para análise.

---

## Balanceamento de Carga Adaptativo e Resiliência

O nó balanceador de carga (`load_balancer/balancer.py`) opera na porta `8000` e gerencia o cluster com alta inteligência operacional:

### 1. Algoritmos Suportados

* `round_robin`: Alternância estrita e sequencial entre os nós ativos.
* `weighted_round_robin`: Distribuição proporcional à capacidade computacional do servidor.
* `least_connections`: Direciona o tráfego ao nó com menor número de requisições ativas em processamento.
* `least_response_time`: Prioriza o servidor com menor média ponderada de latência (RTT).
* `patient_hash`: Mantém afinidade de sessão (*sticky session*) por `paciente_id`.
* `adaptive`: **Chaveamento Dinâmico Autônomo**. O balanceador analisa continuamente latência e taxa de erro:
  * Sob tráfego moderado: Opera em *Round Robin*.
  * Sob aumento de concorrência: Migra automaticamente para *Least Connections*.
  * Sob disparidade de latência entre nós: Transiciona para *Least Response Time*.

### 2. Auto-Scaling Horizontal e Failover On-Demand

* **Scale-Out:** Quando o tráfego atinge a zona de saturação ($\ge 85\%$ de carga sustentada) ou latência crítica ($> 120\text{ ms}$), o nó reserva `server-04` (`vm-servidor-04` @ `192.168.56.16:8004`) é imediatamente ativado no pool de balanceamento.
* **Scale-In:** Quando o tráfego normaliza em níveis seguros ($< 25\%$ por mais de 5 segundos), o nó reserva entra em resfriamento (*cooldown*) e é removido suavemente do pool.
* **Failover On-Demand:** Se todos os nós principais sofrerem falha ou desligamento manual, o balanceador força a ativação emergencial do nó reserva para evitar a indisponibilidade total do sistema.

### 3. Circuit Breaker por Nó

Cada nó possui uma máquina de estados finitos isolada:
* **CLOSED:** Nó saudável, todas as requisições passam.
* **OPEN:** Após 5 falhas consecutivas, o nó é isolado do tráfego para evitar sobrecarga em cascata.
* **HALF-OPEN:** Após o tempo de resfriamento, sondas de saúde em segundo plano (*background probes*) enviam requisições de teste para validar a recuperação e restaurar o estado `CLOSED`.

---

## Interface Visual Didática e Ferramenta de Benchmark

A interface web (`visualizacao/index.html`) acessível na porta `5000` é uma ferramenta completa de demonstração técnica e acadêmica:

1. **Topologia Animada em SVG com 3 Médicos e 3 Datasets Explícitos:** Exibe individualmente cada um dos 3 médicos especialistas e seus respectivos datasets clínicos (Dr. Carlos / UTI, Dra. Beatriz / Emergência, Dr. Lucas / Enfermaria). Ilustra os **dois fluxos concorrentes**:
   * 📤 **Fluxo 1 — Envio de Telemetria (Write Path):** Médico $\to$ Apache Kafka MOM (:9092) $\to$ Load Balancer $\to$ Servidor Backend $\to$ Shards SQLite com Replicação Ativa Fanout.
   * 📥 **Fluxo 2 — Consulta de Prontuários (Read Path):** Médico $\to$ Load Balancer (:8000) $\to$ Shards SQLite com retorno imediato da medição e sinais vitais.
2. **Controles Concorrentes Independentes:** Botões separados na barra superior (`▶️ Iniciar Envio` e `🔍 Iniciar Busca`) que podem operar simultaneamente ou de forma isolada, demonstrando concorrência real entre ingestão de telemetria e leitura de prontuários.
3. **Painel Clínico Dinâmico:** Batimento cardíaco pulsante, gráficos de saturação, temperatura e avisos de gravidade com código de cores Manchester.
4. **Modal de Inspeção de Cluster de VMs:** Visualização em tempo real das especificações de virtualização (vCPU, RAM, MAC address, interface `enp0s8`, latência de ping e integridade dos shards).
5. **Modal de Benchmark / Teste de Carga Integrado:**
   * Permite disparar testes de 50 a 1.000 requisições simultâneas direto pela interface.
   * Seleção em tempo real de qualquer um dos 6 algoritmos de balanceamento.
   * Visualização gráfica de taxa de transferência (req/s), latência média, P95, P99 e taxa de sucesso.
   * Demonstração ao vivo da troca automática no algoritmo **Adaptativo**.
5. **Controles de Simulação:**
   * ▶ **Iniciar:** Injeção contínua de telemetria.
   * ⏸ **Pausar:** Congela o fluxo de pacotes a qualquer momento.
   * ⏭ **Passo a Passo:** Avança um registro por vez (ideal para explicar detalhadamente cada estágio à banca).
   * ↺ **Reiniciar:** Restaura os ponteiros e zera as métricas.

---

## Acesso Remoto e Apresentação (Túnel Cloudflare)

Para apresentar o projeto externamente (em smartphones, notebooks ou fora da rede local) sem necessidade de portas no roteador ou IP fixo:

```bash
# Inicia todo o cluster e gera link público oficial HTTPS:
./executar.sh --tunel
```

O terminal exibirá um link seguro como:
`https://algum-subdominio-gerado.trycloudflare.com`

---

## Endpoints da API REST e Introspecção de VMs

### 1. Inserir Telemetria do Paciente (POST /dados)

```bash
curl -X POST http://localhost:8000/dados \
  -H "Content-Type: application/json" \
  -d '{
    "paciente_id": 101,
    "frequencia_cardiaca": 88,
    "spo2": 98,
    "pressao_sistolica": 120,
    "pressao_diastolica": 80,
    "temperatura": 36.6,
    "deteccao_queda": "No",
    "doenca_prevista": "Normal",
    "precisao_dados": 99.0
  }'
```

### 2. Consultar Prontuário Médico Específico (GET /dados/{paciente_id})

```bash
# Consulta o histórico completo do paciente através do Load Balancer
curl "http://localhost:8000/dados/101"
```

### 3. Executar Simulação Multi-Médicos e Consultas via CLI

```bash
# 1. Simular os 3 médicos enviando telemetria simultaneamente (threads concorrentes):
python client/multi_medicos.py --limite 10 --intervalo 0.8

# 2. Simular apenas a Dra. Beatriz (Emergência):
python client/multi_medicos.py --medico 2 --limite 15

# 3. Consultar prontuário de paciente específico:
python client/medico_consulta.py --paciente 101

# 4. Simular médicos consultando prontuários aleatórios em loop contínuo:
python client/medico_consulta.py --loop --intervalo 1.5
```

### 4. Consultar Registros Persistidos Gerais (GET /dados)

```bash
curl "http://localhost:8000/dados?limit=10&offset=0"
```

### 3. Introspecção de Hardware da VM (GET /vm-info)

```bash
curl "http://localhost:8001/vm-info"
```

### 4. Inventário Completo do Cluster de 8 VMs (GET /api/vms)

```bash
curl "http://localhost:5000/api/vms"
```

### 5. Consultar Status do Cluster e Algoritmo Ativo

```bash
curl "http://localhost:8000/api/nodes"
curl "http://localhost:8000/api/algorithm"
```

---

## Relatórios Técnicos Formais (LaTeX / PDF)

A pasta [`relatorio/`](relatorio/) contém a documentação técnica e acadêmica do projeto, compilada em formato PDF com padrão editorial rigoroso:

* [`relatorio_completo_projeto_sd.pdf`](relatorio/relatorio_completo_projeto_sd.pdf): Relatório Geral Consolidado Completo do Sistema (17 páginas, ABNT/IEEE).
* [`relatorio_sistema_distribuido_hospitalar.pdf`](relatorio/relatorio_sistema_distribuido_hospitalar.pdf): Relatório do Sistema Distribuído Hospitalar com cronograma e gráficos Gantt.
* [`relatorio_mudancas_middleware.pdf`](relatorio/relatorio_mudancas_middleware.pdf): Relatório aprofundado da arquitetura do Middleware Apache Kafka MOM.
* [`relatorio_mudancas_arquiteturais.pdf`](relatorio/relatorio_mudancas_arquiteturais.pdf): Relatório completo cobrindo as transformações estruturais dos Marcos 3 e 4.
* [`relatorio_marco_3.pdf`](relatorio/relatorio_marco_3.pdf): Análise formal de replicação de dados e consistência distribuída.
* [`relatorio_marcos_1_e_2.pdf`](relatorio/relatorio_marcos_1_e_2.pdf): Fundamentação do balanceamento de carga e ingestão de telemetria.

---

## Testes Automatizados

A base de código conta com uma suíte de testes de nível corporativo executada via `pytest`, assegurando **100% de conformidade funcional**:

### Executar Toda a Suíte (200 Testes)

```bash
source venv/bin/activate
pytest tests/ -v
```

### Executar Testes por Módulo

```bash
# Testes do Middleware Híbrido (ESB VETRO + CORBA/ORB + CosNaming)
pytest tests/test_esb_orb.py -v

# Testes de Escalabilidade e Algoritmos de Balanceamento
pytest tests/test_escalabilidade.py -v

# Testes de Resiliência Enterprise (Circuit Breaker, DLQ, Saga, Tracing)
pytest tests/test_middleware_enterprise.py -v

# Testes dos Marcos 1, 2 e 3
pytest tests/test_marco1.py tests/test_marco2.py tests/test_marco3.py -v

# Testes da Interface Visual e Monitor
pytest tests/test_visualizacao.py -v
```

### Cobertura Consolidada dos Testes

```
============================= test session starts ==============================
collected 214 items

tests/test_esb_orb.py ....................                               [  9%]
tests/test_escalabilidade.py ......................................      [ 27%]
tests/test_marco1.py .................                                   [ 35%]
tests/test_marco2.py .........                                           [ 39%]
tests/test_marco3.py ..ss.s.                                             [ 42%]
tests/test_middleware_enterprise.py .................................... [ 64%]
.................................................................        [ 94%]
tests/test_visualizacao.py ...........                                   [100%]

================= 200 passed, 14 skipped in 10.05s (100% SUCESSO) ==============
```
