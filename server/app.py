# server/app.py
"""
Servidor FastAPI para receber e consultar dados de monitoramento de pacientes.

Uso:
    python server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db
"""

import argparse
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

# Adicionar o diretório raiz do projeto ao path para importações
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.models import DadosPaciente, DadosResponse
from server.database import init_db, inserir_dado, buscar_todos, buscar_por_paciente


# ============================================================
# Configuração padrão (pode ser sobrescrita via argparse)
# ============================================================

SERVER_ID = "server-01"
DB_PATH = "databases/server1.db"


def criar_app(server_id: str = None, db_path: str = None) -> FastAPI:
    """
    Cria e configura a aplicação FastAPI.

    Args:
        server_id: Identificador do servidor.
        db_path: Caminho para o banco de dados SQLite.

    Returns:
        Instância configurada do FastAPI.
    """
    _server_id = server_id or SERVER_ID
    _db_path = db_path or DB_PATH

    # Inicializar o banco de dados imediatamente
    init_db(_db_path)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        """Evento de ciclo de vida do servidor."""
        print(f"[{_server_id}] Servidor iniciado. Banco: {_db_path}")
        yield

    app = FastAPI(
        title=f"Sistema Hospitalar - {_server_id}",
        description="API para receber dados de monitoramento de pacientes",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.post("/dados", response_model=DadosResponse)
    def receber_dados(dados: DadosPaciente):
        """
        Recebe dados de monitoramento de um paciente.

        Valida os campos, armazena no SQLite e retorna confirmação
        com o identificador do servidor que processou a requisição.
        """
        registro = inserir_dado(_db_path, dados.model_dump(), _server_id)
        print(f"[{_server_id}] Recebido: paciente_id={dados.paciente_id} | "
              f"FC={dados.frequencia_cardiaca} | SpO2={dados.spo2} | "
              f"PA={dados.pressao_sistolica}/{dados.pressao_diastolica} | "
              f"Temp={dados.temperatura}")
        return registro

    @app.get("/dados", response_model=list[DadosResponse])
    def listar_dados(
        limit: int = Query(default=100, ge=1, le=1000, description="Máximo de registros"),
        offset: int = Query(default=0, ge=0, description="Registros a pular"),
    ):
        """
        Lista os dados armazenados com paginação.

        Parâmetros:
            limit: Número máximo de registros (1-1000, padrão 100).
            offset: Número de registros a pular (padrão 0).
        """
        registros = buscar_todos(_db_path, limit, offset)
        return registros

    @app.get("/dados/{paciente_id}", response_model=list[DadosResponse])
    def buscar_paciente(paciente_id: int):
        """
        Busca todos os registros de um paciente específico.

        Parâmetros:
            paciente_id: Número de identificação do paciente.
        """
        registros = buscar_por_paciente(_db_path, paciente_id)
        if not registros:
            return JSONResponse(
                status_code=404,
                content={"detail": f"Nenhum registro encontrado para paciente {paciente_id}"},
            )
        return registros

    return app


# ============================================================
# Ponto de entrada: execução via linha de comando
# ============================================================

if __name__ == "__main__":
    import uvicorn

    parser = argparse.ArgumentParser(
        description="Servidor de dados hospitalares"
    )
    parser.add_argument(
        "--port", type=int, default=8001,
        help="Porta do servidor (padrão: 8001)"
    )
    parser.add_argument(
        "--server-id", type=str, default="server-01",
        help="Identificador do servidor (padrão: server-01)"
    )
    parser.add_argument(
        "--db-path", type=str, default="databases/server1.db",
        help="Caminho do banco de dados SQLite (padrão: databases/server1.db)"
    )

    args = parser.parse_args()

    # Atualizar configuração global para uso pelo criar_app
    SERVER_ID = args.server_id
    DB_PATH = args.db_path

    app = criar_app(args.server_id, args.db_path)

    print(f"=== Servidor {args.server_id} ===")
    print(f"    Porta: {args.port}")
    print(f"    Banco: {args.db_path}")
    print(f"    URL:   http://localhost:{args.port}")
    print()

    uvicorn.run(app, host="0.0.0.0", port=args.port)
