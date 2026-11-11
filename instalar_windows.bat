@echo off
chcp 65001 > nul
title Instalação - Sistema Distribuído Hospitalar (Windows)
cd /d "%~dp0"

echo ============================================================
echo   INSTALADOR DE DEPENDÊNCIAS — SISTEMA DISTRIBUÍDO
echo ============================================================
echo.
echo Este assistente prepara o ambiente no Windows para rodar
echo o sistema consumindo menos de 180 MB de memória RAM.
echo.

:: 1. Checar Python
where python >nul 2>nul
if %errorlevel% neq 0 (
    where py >nul 2>nul
    if %errorlevel% neq 0 (
        echo [ERRO] Python não foi encontrado no PATH do Windows!
        echo Acesse https://www.python.org/downloads/ e instale o Python.
        echo Lembre-se de marcar "Add Python to PATH".
        pause
        exit /b 1
    )
    set PY_CMD=py
) else (
    set PY_CMD=python
)

echo [1/4] Criando pastas databases/ e logs/...
if not exist "databases" mkdir databases
if not exist "logs" mkdir logs

echo [2/4] Criando ambiente virtual Python (venv)...
%PY_CMD% -m venv venv

echo [3/4] Instalando pacotes do requirements.txt...
venv\Scripts\python.exe -m pip install --upgrade pip
venv\Scripts\python.exe -m pip install -r requirements.txt

echo [4/4] Sincronizando réplicas iniciais SQLite...
venv\Scripts\python.exe server/sync_dbs.py

echo.
echo ============================================================
echo   🎉 INSTALAÇÃO CONCLUÍDA COM SUCESSO!
echo ============================================================
echo.
echo Para ligar o cluster, basta dar dois cliques no arquivo:
echo.
echo     👉  executar.bat
echo.
echo O navegador será aberto automaticamente em http://localhost:5000.
echo.
pause

