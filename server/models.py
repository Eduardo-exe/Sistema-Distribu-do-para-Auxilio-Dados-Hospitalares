# server/models.py
"""
Modelos de dados e mapeamento de colunas do CSV.

Este módulo define:
- O mapeamento entre os nomes das colunas do CSV original e os nomes
  utilizados na API.
- Os modelos Pydantic para validação dos dados recebidos e das respostas.
"""

from pydantic import BaseModel
from typing import Optional


# ============================================================
# Mapeamento: nome da coluna no CSV → nome do campo na API
# ============================================================

CSV_COLUMN_MAP = {
    "Patient Number": "paciente_id",
    "Heart Rate (bpm)": "frequencia_cardiaca",
    "SpO2 Level (%)": "spo2",
    "Systolic Blood Pressure (mmHg)": "pressao_sistolica",
    "Diastolic Blood Pressure (mmHg)": "pressao_diastolica",
    "Body Temperature (°C)": "temperatura",
    "Fall Detection": "deteccao_queda",
    "Predicted Disease": "doenca_prevista",
    "Data Accuracy (%)": "precisao_dados",
    "Heart Rate Alert": "alerta_freq_cardiaca",
    "SpO2 Level Alert": "alerta_spo2",
    "Blood Pressure Alert": "alerta_pressao",
    "Temperature Alert": "alerta_temperatura",
}


def converter_registro_csv(registro_csv: dict) -> dict:
    """
    Converte um registro lido do CSV (com nomes originais das colunas)
    para o formato esperado pela API (com nomes mapeados).

    Args:
        registro_csv: Dicionário com chaves sendo os nomes das colunas do CSV.

    Returns:
        Dicionário com chaves sendo os nomes dos campos da API.
    """
    dados = {}
    for coluna_csv, campo_api in CSV_COLUMN_MAP.items():
        valor = registro_csv.get(coluna_csv, "")
        # Converter tipos numéricos
        if campo_api == "paciente_id":
            dados[campo_api] = int(valor)
        elif campo_api in ("frequencia_cardiaca", "spo2", "pressao_sistolica",
                           "pressao_diastolica", "precisao_dados"):
            dados[campo_api] = int(valor)
        elif campo_api == "temperatura":
            dados[campo_api] = float(valor)
        else:
            dados[campo_api] = str(valor)
    return dados


# ============================================================
# Modelo de entrada: dados enviados pelo cliente
# ============================================================

class DadosPaciente(BaseModel):
    """Modelo para validação dos dados de monitoramento de um paciente."""
    paciente_id: int
    frequencia_cardiaca: int
    spo2: int
    pressao_sistolica: int
    pressao_diastolica: int
    temperatura: float
    deteccao_queda: str
    doenca_prevista: str
    precisao_dados: int
    alerta_freq_cardiaca: str
    alerta_spo2: str
    alerta_pressao: str
    alerta_temperatura: str


# ============================================================
# Modelo de resposta: dados retornados pela API
# ============================================================

class DadosResponse(BaseModel):
    """Modelo de resposta com dados armazenados e metadados do servidor."""
    id: int
    paciente_id: int
    frequencia_cardiaca: int
    spo2: int
    pressao_sistolica: int
    pressao_diastolica: int
    temperatura: float
    deteccao_queda: str
    doenca_prevista: str
    precisao_dados: int
    alerta_freq_cardiaca: str
    alerta_spo2: str
    alerta_pressao: str
    alerta_temperatura: str
    timestamp: str
    server_id: str
