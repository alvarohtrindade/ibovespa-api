#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Orquestrador simplificado para gerenciar atualizações dos dados do IBOVESPA.
Este script é responsável por coordenar a obtenção e armazenamento dos dados.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Importa os módulos simplificados do projeto
from fetch_data import fetch_ibovespa_data, ensure_directory_exists
from db_manager import IbovespaDBManager

# Configuração de logs
def setup_logging(log_level: str = 'INFO') -> None:
    """
    Configura o sistema de logs.
    
    Args:
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Nível de log inválido: {log_level}')
    
    # Garante que o diretório de logs existe
    ensure_directory_exists('logs')
    
    # Configura o logger principal
    logger = logging.getLogger()
    logger.setLevel(numeric_level)
    
    # Remove handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Adiciona handler para arquivo
    log_file = os.path.join('logs', f'orquestrador_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    # Adiciona handler para console
    console_handler = logging.StreamHandler()
    
    # Formato do log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Adiciona os handlers ao logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def validate_env_file() -> None:
    """
    Valida se o arquivo .env existe e possui as credenciais necessárias.
    
    Raises:
        FileNotFoundError: Se o arquivo .env não for encontrado
        ValueError: Se alguma credencial necessária estiver ausente
    """
    if not os.path.exists('.env'):
        raise FileNotFoundError("Arquivo .env não encontrado. Crie o arquivo com as credenciais do MySQL.")
    
    # Adapta para as novas variáveis de ambiente
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Variáveis de ambiente ausentes no arquivo .env: {', '.join(missing_vars)}")


def parse_args() -> argparse.Namespace:
    """
    Analisa os argumentos da linha de comando.
    
    Returns:
        Namespace com os argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Ferramenta para obtenção e armazenamento de dados do IBOVESPA',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Argumentos globais
    parser.add_argument('--log-level', type=str, default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Nível de log')
    
    # Subcomandos
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponíveis')
    
    # Comando setup - Inicialização da tabela
    setup_parser = subparsers.add_parser('setup', 
                                        help='Configura a tabela Ft_Ibovespa no banco de dados')
    
    # Comando loaddata - Carga inicial de dados
    loaddata_parser = subparsers.add_parser('loaddata', 
                                          help='Carrega dados históricos do IBOVESPA desde 2021')
    loaddata_parser.add_argument('--start-date', type=str, default='2018-01-01', 
                               help='Data de início (YYYY-MM-DD)')
    
    # Comando update - Atualização diária
    update_parser = subparsers.add_parser('update', 
                                         help='Atualiza os dados do IBOVESPA com dados recentes')
    update_parser.add_argument('--days-lookback', type=int, default=5, 
                              help='Número de dias anteriores para buscar (para garantir sobreposição)')
    
    # Comando sync - Verifica e sincroniza dados
    sync_parser = subparsers.add_parser('sync', 
                                      help='Sincroniza o banco com os dados mais recentes do IBOVESPA')
    
    return parser.parse_args()


def cmd_setup(args: argparse.Namespace) -> None:
    """
    Configura a tabela Ft_Ibovespa no banco de dados.
    
    Args:
        args: Argumentos da linha de comando
    """
    logger = logging.getLogger(__name__)
    
    try:
        with IbovespaDBManager() as db:
            # Cria a tabela se não existir
            db.create_ft_ibovespa_table()
            
            # Verifica quantos registros existem
            row_count = db.get_table_row_count()
            
            logger.info(f"Tabela Ft_Ibovespa configurada com sucesso. Registros existentes: {row_count}")
            
            # Se não há registros, sugere executar o comando loaddata
            if row_count == 0:
                logger.info("A tabela está vazia. Execute 'python orquestrador.py loaddata' para carregar dados históricos.")
    
    except Exception as e:
        logger.error(f"Erro ao configurar tabela: {str(e)}")
        raise


def cmd_loaddata(args: argparse.Namespace) -> None:
    """
    Carrega dados históricos do IBOVESPA desde a data especificada.
    
    Args:
        args: Argumentos da linha de comando
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Verifica se a tabela já tem dados
        with IbovespaDBManager() as db:
            row_count = db.get_table_row_count()
            
            if row_count > 0:
                logger.warning(f"A tabela Ft_Ibovespa já contém {row_count} registros.")
                logger.warning("Para atualizar com dados recentes, use o comando 'update' em vez de 'loaddata'.")
                
                # Pergunta se deseja continuar
                response = input("Deseja continuar e carregar todos os dados novamente? (s/N): ")
                if response.lower() not in ['s', 'sim', 'y', 'yes']:
                    logger.info("Operação cancelada pelo usuário.")
                    return
        
        # Busca os dados desde a data especificada
        logger.info(f"Obtendo dados históricos do IBOVESPA desde {args.start_date}")
        df = fetch_ibovespa_data(start_date=args.start_date)
        
        if df.empty:
            logger.error("Não foi possível obter dados do IBOVESPA")
            return
        
        logger.info(f"Obtidos {len(df)} registros do IBOVESPA")
        
        # Insere os dados no banco
        with IbovespaDBManager() as db:
            # Cria a tabela se não existir
            db.create_ft_ibovespa_table()
            
            # Insere os dados
            count = db.insert_ibovespa_data(df)
            
            logger.info(f"Dados carregados com sucesso: {count} registros processados")
    
    except Exception as e:
        logger.error(f"Erro ao carregar dados históricos: {str(e)}")
        raise


def cmd_update(args: argparse.Namespace) -> None:
    """
    Atualiza os dados do IBOVESPA com dados recentes.
    
    Args:
        args: Argumentos da linha de comando
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Obtém a data do último registro no banco
        with IbovespaDBManager() as db:
            # Verifica se a tabela existe
            try:
                last_date = db.get_last_update_date()
            except Exception:
                logger.error("Tabela Ft_Ibovespa não encontrada. Execute 'python orquestrador.py setup' primeiro.")
                return
            
            # Se não houver dados, sugere executar o comando loaddata
            if not last_date:
                logger.warning("Nenhum registro encontrado na tabela Ft_Ibovespa.")
                logger.info("Execute 'python orquestrador.py loaddata' para carregar dados históricos.")
                return
        
        # Calcula a data de início para atualização (com período de sobreposição)
        start_date = (last_date - timedelta(days=args.days_lookback)).strftime('%Y-%m-%d')
        
        logger.info(f"Último registro do IBOVESPA: {last_date}")
        logger.info(f"Buscando dados a partir de {start_date}")
        
        # Busca os dados recentes
        df = fetch_ibovespa_data(start_date=start_date)
        
        if df.empty:
            logger.warning("Nenhum dado novo encontrado para o IBOVESPA")
            return
        
        logger.info(f"Obtidos {len(df)} registros do IBOVESPA")
        
        # Insere os dados no banco
        with IbovespaDBManager() as db:
            count = db.insert_ibovespa_data(df)
            
            logger.info(f"Dados atualizados com sucesso: {count} registros processados")
    
    except Exception as e:
        logger.error(f"Erro ao atualizar dados: {str(e)}")
        raise


def cmd_sync(args: argparse.Namespace) -> None:
    """
    Sincroniza o banco com os dados mais recentes do IBOVESPA.
    Verifica se existe a tabela e dados, e executa a ação adequada.
    
    Args:
        args: Argumentos da linha de comando
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Verifica o estado atual do banco
        with IbovespaDBManager() as db:
            try:
                # Tenta criar a tabela se não existir
                db.create_ft_ibovespa_table()
                
                # Verifica se há dados
                row_count = db.get_table_row_count()
                last_date = db.get_last_update_date()
                
                logger.info(f"Estado atual: {row_count} registros, última data: {last_date if last_date else 'N/A'}")
                
                # Determina a ação adequada
                if row_count == 0:
                    logger.info("Nenhum registro encontrado. Iniciando carga histórica...")
                    # Reutiliza a função de carga inicial mas com argumentos default
                    loaddata_args = argparse.Namespace()
                    loaddata_args.start_date = '2018-01-01'
                    cmd_loaddata(loaddata_args)
                else:
                    logger.info("Registros encontrados. Atualizando com dados recentes...")
                    # Reutiliza a função de atualização mas com argumentos default
                    update_args = argparse.Namespace()
                    update_args.days_lookback = 5
                    cmd_update(update_args)
            
            except Exception as e:
                logger.error(f"Erro ao verificar estado do banco: {str(e)}")
                raise
    
    except Exception as e:
        logger.error(f"Erro ao sincronizar dados: {str(e)}")
        raise


def main() -> None:
    """
    Função principal do orquestrador.
    """
    # Verifica se o arquivo .env existe e contém as credenciais necessárias
    try:
        validate_env_file()
    except (FileNotFoundError, ValueError) as e:
        print(f"ERRO: {str(e)}")
        print("Por favor, crie um arquivo .env com as seguintes variáveis:")
        print("DB_HOST=seu_host")
        print("DB_PORT=3306")
        print("DB_USER=seu_usuario")
        print("DB_PASSWORD=sua_senha")
        print("DB_NAME=nome_do_banco")
        sys.exit(1)
    
    # Analisa os argumentos da linha de comando
    args = parse_args()
    
    # Configura o sistema de logs
    setup_logging(args.log_level if hasattr(args, 'log_level') else 'INFO')
    
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando orquestrador com comando: {args.command}")
    
    # Executa o comando solicitado
    try:
        if args.command == 'setup':
            cmd_setup(args)
        elif args.command == 'loaddata':
            cmd_loaddata(args)
        elif args.command == 'update':
            cmd_update(args)
        elif args.command == 'sync':
            cmd_sync(args)
        else:
            print("Comando não especificado. Use --help para ver os comandos disponíveis.")
            sys.exit(1)
        
        logger.info(f"Comando {args.command} concluído com sucesso")
    
    except Exception as e:
        logger.error(f"Erro ao executar o comando {args.command}: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()