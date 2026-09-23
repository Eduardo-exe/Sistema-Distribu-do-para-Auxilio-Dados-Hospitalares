# server/database.py
"""
Camada de acesso ao banco de dados SQLite.

Este módulo gerencia:
- Criação da tabela de dados de pacientes.
- Inserção de novos registros.
- Consulta de registros (todos ou por paciente).
"""

import sqlite3
import os
from datetime import datetime


def init_db(db_path: str) -> None:
    """
    Inicializa o banco de dados, criando a tabela se não existir.

    Args:
        db_path: Caminho para o arquivo SQLite.
    """
    # Garantir que o diretório existe
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dados_pacientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paciente_id INTEGER NOT NULL,
            frequencia_cardiaca INTEGER NOT NULL,
            spo2 INTEGER NOT NULL,
            pressao_sistolica INTEGER NOT NULL,
            pressao_diastolica INTEGER NOT NULL,
            temperatura REAL NOT NULL,
            deteccao_queda TEXT NOT NULL,
            doenca_prevista TEXT NOT NULL,
            precisao_dados INTEGER NOT NULL,
            alerta_freq_cardiaca TEXT NOT NULL,
            alerta_spo2 TEXT NOT NULL,
            alerta_pressao TEXT NOT NULL,
            alerta_temperatura TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            server_id TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def inserir_dado(db_path: str, dados: dict, server_id: str) -> dict:
    """
    Insere um registro de dados de paciente no banco.

    Args:
        db_path: Caminho para o arquivo SQLite.
        dados: Dicionário com os dados do paciente (campos da API).
        server_id: Identificador do servidor que processou a requisição.

    Returns:
        Dicionário com todos os campos do registro inserido, incluindo
        id, timestamp e server_id.
    """
    timestamp = datetime.now().isoformat()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dados_pacientes (
            paciente_id, frequencia_cardiaca, spo2,
            pressao_sistolica, pressao_diastolica, temperatura,
            deteccao_queda, doenca_prevista, precisao_dados,
            alerta_freq_cardiaca, alerta_spo2, alerta_pressao,
            alerta_temperatura, timestamp, server_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        dados["paciente_id"],
        dados["frequencia_cardiaca"],
        dados["spo2"],
        dados["pressao_sistolica"],
        dados["pressao_diastolica"],
        dados["temperatura"],
        dados["deteccao_queda"],
        dados["doenca_prevista"],
        dados["precisao_dados"],
        dados["alerta_freq_cardiaca"],
        dados["alerta_spo2"],
        dados["alerta_pressao"],
        dados["alerta_temperatura"],
        timestamp,
        server_id,
    ))
    conn.commit()
    registro_id = cursor.lastrowid
    conn.close()

    return {
        "id": registro_id,
        **dados,
        "timestamp": timestamp,
        "server_id": server_id,
    }


def buscar_todos(db_path: str, limit: int = 100, offset: int = 0) -> list[dict]:
    """
    Busca todos os registros com paginação.

    Args:
        db_path: Caminho para o arquivo SQLite.
        limit: Número máximo de registros a retornar.
        offset: Número de registros a pular.

    Returns:
        Lista de dicionários com os registros encontrados.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM dados_pacientes ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset),
    )
    registros = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return registros


def buscar_por_paciente(db_path: str, paciente_id: int) -> list[dict]:
    """
    Busca todos os registros de um paciente específico.

    Args:
        db_path: Caminho para o arquivo SQLite.
        paciente_id: Identificador do paciente.

    Returns:
        Lista de dicionários com os registros do paciente.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM dados_pacientes WHERE paciente_id = ? ORDER BY id",
        (paciente_id,),
    )
    registros = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return registros
