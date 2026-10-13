FROM python:3.11-slim

# Dependências de sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    psmisc \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Configurar usuário não-root (UID 1000) exigido pelo Hugging Face Spaces
RUN useradd -m -u 1000 user
WORKDIR /app

# Instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do projeto
COPY --chown=user:user . /app

# Criar diretórios de trabalho e garantir permissões
RUN mkdir -p /app/databases /app/logs /app/data && \
    chmod -R 777 /app/databases /app/logs /app/data && \
    chmod +x /app/executar.sh

# Criar symlink para o CSV se não existir
RUN if [ -f "Synthetic_patient-HealthCare-Monitoring_dataset.csv" ] && [ ! -f "data/dataset.csv" ]; then \
        ln -sf "../Synthetic_patient-HealthCare-Monitoring_dataset.csv" data/dataset.csv; \
    fi

USER user

# Porta padrão do Hugging Face Spaces
ENV PORT=7860
EXPOSE 7860 5000 8000

CMD ["./executar.sh", "--no-middleware"]
