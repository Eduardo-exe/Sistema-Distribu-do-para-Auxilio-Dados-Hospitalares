@echo off
chcp 65001 > nul
title Sistema Distribuído Hospitalar (Modo Leve - Windows)
cd /d "%~dp0"

echo ============================================================
echo   SISTEMA DISTRIBUÍDO HOSPITALAR — INICIALIZADOR WINDOWS
echo ============================================================
echo.

:: 1. Verificar se o Python está instalado
where python >nul 2>nul
if %errorlevel% neq 0 (
    where py >nul 2>nul
    if %errorlevel% neq 0 (
        echo [ERRO] Python não foi encontrado no seu computador!
        echo.
        echo Por favor, instale o Python 3.10 ou superior:
        echo 1. Baixe em: https://www.python.org/downloads/
        echo 2. ATENÇÃO: Marque a caixa "Add Python to PATH" durante a instalação.
        echo.
        pause
        exit /b 1
    )
)

:: 2. Criar ambiente virtual se ainda não existir
if not exist "venv\Scripts\python.exe" (
    echo [CONFIGURAÇÃO INICIAL] Criando ambiente virtual Python (venv)...
    python -m venv venv 2>nul || py -m venv venv
    if not exist "venv\Scripts\python.exe" (
        echo [ERRO] Falha ao criar ambiente virtual!
        pause
        exit /b 1
    )
    echo [CONFIGURAÇÃO INICIAL] Instalando bibliotecas leves (menos de 180MB RAM)...
    venv\Scripts\python.exe -m pip install --upgrade pip --quiet
    venv\Scripts\python.exe -m pip install -r requirements.txt --quiet
    echo [✓] Instalação concluída com sucesso!
    echo.
)

:: 3. Executar o orquestrador multiplataforma
echo [INICIANDO] Subindo servidores, balanceador, réplicas e painel web...
venv\Scripts\python.exe executar.py %*

if %errorlevel% neq 0 (
    echo.
    echo Ocorreu um erro ao executar o sistema.
    pause
)
