# Walkthrough — Marcos 1, 2 e Interface Visual Didática

## O que foi implementado

### Marco 1 — Simulação e Servidor Base
- **Análise do CSV**: 60.000 registros, 13 colunas, sem valores ausentes, sem timestamp.
- **Mapeamento de colunas**: tradução automática de nomes do CSV (inglês) para nomes da API (português).
- **Servidor FastAPI** com 3 endpoints: `POST /dados`, `GET /dados`, `GET /dados/{paciente_id}`.
- **Banco SQLite** com tabela `dados_pacientes` (15 colunas: 13 do CSV + id + timestamp + server_id).
- **Leitor CSV** que envia registros progressivamente via HTTP com intervalo configurável.

### Marco 2 — Balanceamento de Servidores
- **3 instâncias independentes** do servidor (portas 8001, 8002, 8003).
- **Balanceador Round Robin** na porta 8000 que distribui requisições sequencialmente.
- **Identificação de servidor** em cada resposta (`server_id`).
- **Bancos SQLite separados** por servidor (`server1.db`, `server2.db`, `server3.db`).
- **Endpoint /status** no balanceador para monitoramento.

### Interface Visual Didática (Camada de Apresentação)
- **Servidor Monitor FastAPI (`visualizacao/monitor.py`)**:
  - Servidor na porta 5000 com Server-Sent Events (SSE).
  - Lê o CSV real (`data/dataset.csv`).
  - Executa requisições reais através do Load Balancer existente (`:8000`), sem alterar nenhuma linha do código dos Marcos 1 e 2.
  - Streaming em tempo real de eventos: leitura do CSV, envio do cliente, recepção no Load Balancer, roteamento Round Robin, processamento no servidor e persistência no banco SQLite.
  - Suporte a modo contínuo (Play/Pause) e modo passo a passo (Step-by-step).
- **Interface Visual Animada (`visualizacao/index.html`)**:
  - Design moderno escuro com glassmorphism, tipografia Inter e JetBrains Mono.
  - Topologia de rede SVG interativa com caminhos animados de fluxo de dados.
  - Partícula animada (flying packet) que percorre visualmente os links da rede de acordo com o destino de cada requisição.
  - Indicador do algoritmo Round Robin (nós 1 → 2 → 3 → 1).
  - Painel de telemetria médica em tempo real com animação de pulso cardíaco, SpO2, PA, temperatura e dor.
  - Métricas e barras de distribuição de carga em tempo real (~33,3% por nó).
  - Terminal de log cronológico com cores por componente.
  - Modo Foco / Apresentação para projeções e defesa da disciplina.

---

## Arquivos Criados

| Arquivo | Descrição |
|---|---|
| [`server/__init__.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/server/__init__.py) | Package init |
| [`server/models.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/server/models.py) | Modelos Pydantic + mapeamento CSV→API |
| [`server/database.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/server/database.py) | Camada de acesso SQLite |
| [`server/app.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/server/app.py) | API FastAPI (servidor) |
| [`client/csv_sender.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/client/csv_sender.py) | Leitor/reprodutor do CSV |
| [`load_balancer/balancer.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/load_balancer/balancer.py) | Balanceador Round Robin |
| [`visualizacao/monitor.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/visualizacao/monitor.py) | Servidor FastAPI + SSE para visualização didática |
| [`visualizacao/index.html`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/visualizacao/index.html) | Interface gráfica com animação de rede SVG |
| [`tests/test_marco1.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/tests/test_marco1.py) | 17 testes do Marco 1 |
| [`tests/test_marco2.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/tests/test_marco2.py) | 9 testes do Marco 2 |
| [`tests/test_visualizacao.py`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/tests/test_visualizacao.py) | 5 testes da camada de visualização |
| [`requirements.txt`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/requirements.txt) | Dependências |
| [`README.md`](file:///home/eduardo-exe/Documentos/Sistemas%20Distribuidos/Projeto%20SD/README.md) | Documentação completa com instruções da interface |
| `data/dataset.csv` | Symlink para o CSV original |

---

## Resultado dos Testes Automatizados

```
======================== 31 passed, 1 warning in 2.54s ========================

tests/test_marco1.py::TestLeituraCSV::test_csv_existe                    PASSED
tests/test_marco1.py::TestLeituraCSV::test_csv_tem_colunas_esperadas     PASSED
tests/test_marco1.py::TestLeituraCSV::test_csv_tem_registros             PASSED
tests/test_marco1.py::TestLeituraCSV::test_csv_primeiro_registro_valores PASSED
tests/test_marco1.py::TestConversaoRegistro::test_conversao_campos       PASSED
tests/test_marco1.py::TestConversaoRegistro::test_conversao_tipos        PASSED
tests/test_marco1.py::TestConversaoRegistro::test_conversao_valida       PASSED
tests/test_marco1.py::TestConversaoRegistro::test_conversao_real_csv     PASSED
tests/test_marco1.py::TestAPI::test_post_dados                           PASSED
tests/test_marco1.py::TestAPI::test_post_dados_invalidos                 PASSED
tests/test_marco1.py::TestAPI::test_dados_armazenados_sqlite             PASSED
tests/test_marco1.py::TestAPI::test_get_dados                            PASSED
tests/test_marco1.py::TestAPI::test_get_dados_paginacao                  PASSED
tests/test_marco1.py::TestAPI::test_get_dados_paciente                   PASSED
tests/test_marco1.py::TestAPI::test_get_dados_paciente_inexistente       PASSED
tests/test_marco1.py::TestAPI::test_post_resposta_identifica_servidor    PASSED
tests/test_marco1.py::TestAPI::test_multiplos_registros_mesmo_paciente   PASSED
tests/test_marco2.py::TestRoundRobin::test_round_robin_alterna           PASSED
tests/test_marco2.py::TestRoundRobin::test_round_robin_cicla             PASSED
tests/test_marco2.py::TestRoundRobin::test_round_robin_contador          PASSED
tests/test_marco2.py::TestIntegracao::test_01_servidores_iniciaram       PASSED
tests/test_marco2.py::TestIntegracao::test_02_balanceador_iniciou        PASSED
tests/test_marco2.py::TestIntegracao::test_03_balanceador_recebe_post    PASSED
tests/test_marco2.py::TestIntegracao::test_04_resposta_identifica        PASSED
tests/test_marco2.py::TestIntegracao::test_05_round_robin_distribuicao   PASSED
tests/test_marco2.py::TestIntegracao::test_06_dados_armazenados          PASSED
tests/test_visualizacao.py::TestMonitor::test_csv_carregado_com_sucesso  PASSED
tests/test_visualizacao.py::TestMonitor::test_serve_html_index           PASSED
tests/test_visualizacao.py::TestMonitor::test_get_status_inicial          PASSED
tests/test_visualizacao.py::TestMonitor::test_alterar_velocidade         PASSED
tests/test_visualizacao.py::TestMonitor::test_reiniciar_simulacao         PASSED
```

---

## Verificação End-to-End da Visualização

Com a infraestrutura ativa (Servidor 1 na 8001, Servidor 2 na 8002, Servidor 3 na 8003 e Balanceador na 8000), executamos 3 requisições passo a passo através do monitor (`/api/proximo`):

1. **Requisição 1**: Roteada para `server-01` (:8001) e gravada em `databases/server1.db`.
2. **Requisição 2**: Roteada para `server-02` (:8002) e gravada em `databases/server2.db`.
3. **Requisição 3**: Roteada para `server-03` (:8003) e gravada em `databases/server3.db`.

Consulta direta via `sqlite3`:
- `server1.db`: 1 registro
- `server2.db`: 1 registro
- `server3.db`: 1 registro

A alternância cíclica perfeita 1 → 2 → 3 foi confirmada tanto no balanceador quanto no banco de dados.

---

## Como Abrir e Usar na Apresentação

1. Iniciar os servidores e o balanceador:
```bash
python server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db
python server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db
python server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db
python load_balancer/balancer.py
```

2. Iniciar o monitor:
```bash
python visualizacao/monitor.py
```

3. Abrir o navegador em:
```
http://localhost:5000
```
