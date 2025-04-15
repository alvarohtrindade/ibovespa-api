#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Agendador para atualização diária dos dados do IBOVESPA.
Este script pode ser configurado como uma tarefa cron para execução diária.
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Configuração de logs
def ensure_directory_exists(directory: str) -> None:
    """Garante que o diretório exista, criando-o se necessário."""
    if not os.path.exists(directory):
        os.makedirs(directory)

# Configura o diretório de logs
log_dir = 'logs'
ensure_directory_exists(log_dir)

log_file = os.path.join(log_dir, f'scheduler_{datetime.now().strftime("%Y%m%d")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Caminho para o arquivo orquestrador.py
script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'orquestrador.py')

def run_update():
    """
    Executa a atualização diária dos dados do IBOVESPA.
    """
    logger.info("Iniciando atualização diária dos dados do IBOVESPA")
    
    try:
        # Executa o comando sync que verifica e executa a ação apropriada
        # (carga inicial ou atualização)
        command = [sys.executable, script_path, 'sync']
        
        logger.info(f"Executando comando: {' '.join(command)}")
        
        # Executa o comando e captura saída e erro
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        stdout, stderr = process.communicate()
        
        # Registra a saída
        if stdout:
            logger.info("Saída do comando:")
            for line in stdout.splitlines():
                logger.info(line)
        
        # Registra erros, se houver
        if stderr:
            logger.error("Erros encontrados:")
            for line in stderr.splitlines():
                logger.error(line)
        
        # Verifica o código de retorno
        if process.returncode == 0:
            logger.info("Atualização diária concluída com sucesso")
        else:
            logger.error(f"Atualização diária falhou com código {process.returncode}")
    
    except Exception as e:
        logger.error(f"Erro ao executar atualização diária: {str(e)}")

if __name__ == "__main__":
    run_update()