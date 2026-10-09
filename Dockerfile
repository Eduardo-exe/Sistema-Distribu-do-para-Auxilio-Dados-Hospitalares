FROM python:3.11-slim

WORKDIR /app

# Instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do projeto
COPY . .

# Criar diretórios necessários
RUN mkdir -p databases logs data

# Criar symlink para o CSV se não existir
RUN if [ -f "Synthetic_patient-HealthCare-Monitoring_dataset.csv" ] && [ ! -f "data/dataset.csv" ]; then \
        ln -sf "../Synthetic_patient-HealthCare-Monitoring_dataset.csv" data/dataset.csv; \
    fi

EXPOSE 8000 8001 8002 8003 5000
