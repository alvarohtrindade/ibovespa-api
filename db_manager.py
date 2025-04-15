#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Módulo simplificado para gerenciamento do banco de dados MySQL.
Responsável pela criação e atualização da tabela Ft_Ibovespa.
Otimizado com inserções em lote para melhor desempenho.
"""

import pandas as pd
import mysql.connector
from mysql.connector import pooling
import logging
import os
from typing import Optional, List, Dict, Tuple, Union, Any
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import numpy as np

# Carregamento das variáveis de ambiente
load_dotenv()

# Configuração básica de logs
def ensure_directory_exists(directory: str) -> None:
    """Garante que o diretório exista, criando-o se necessário."""
    if not os.path.exists(directory):
        os.makedirs(directory)

# Configuração de logs
def setup_logging():
    """Configura o logger para este módulo"""
    ensure_directory_exists('logs')
    logger = logging.getLogger(__name__)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Handler para arquivo
        file_handler = logging.FileHandler(os.path.join('logs', 'db_manager.log'), encoding='utf-8')
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        
        # Adiciona os handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# Inicializa o logger
logger = setup_logging()

# Função para converter NaN para None
def nan_to_none(value: Any) -> Any:
    """
    Converte valores NaN/None para None (compatível com SQL NULL).
    
    Args:
        value: Valor a ser convertido
        
    Returns:
        None se o valor for NaN/None, caso contrário o valor original
    """
    if pd.isna(value) or value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return value

class IbovespaDBManager:
    """Classe simplificada para gerenciar operações com o banco de dados MySQL para o IBOVESPA."""
    
    def __init__(self, pool_size: int = 5):
        """
        Inicializa o gerenciador de banco de dados MySQL para o IBOVESPA.
        
        Args:
            pool_size: Tamanho do pool de conexões
        """
        # Garante que o diretório de logs existe
        ensure_directory_exists('logs')
        
        # Carrega credenciais do arquivo .env com os novos nomes de variáveis
        self.host = os.getenv('DB_HOST')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.db_name = os.getenv('DB_NAME')
        
        # Valida se as credenciais foram carregadas
        self._validate_credentials()
        
        # Parâmetros para processamento em lote
        self.pool_size = pool_size
        self.cnx_pool = None
        
        self.conn = None
        self.cursor = None
        
        logger.info(f"IbovespaDBManager inicializado para o banco {self.db_name}")
    
    def _validate_credentials(self) -> None:
        """Valida se as credenciais de banco de dados foram carregadas corretamente."""
        missing = []
        if not self.user:
            missing.append('DB_USER')
        if not self.password:
            missing.append('DB_PASSWORD')
        if not self.db_name:
            missing.append('DB_NAME')
        
        if missing:
            error_msg = f"Credenciais de banco de dados ausentes no arquivo .env: {', '.join(missing)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def _init_connection_pool(self):
        """Inicializa o pool de conexões para operações em lote."""
        if self.cnx_pool is None:
            try:
                self.cnx_pool = pooling.MySQLConnectionPool(
                    pool_name="ibovespa_pool",
                    pool_size=self.pool_size,
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.db_name
                )
                logger.info(f"Pool de conexões inicializado com {self.pool_size} conexões")
            except mysql.connector.Error as e:
                logger.error(f"Erro ao criar pool de conexões: {str(e)}")
                raise
    
    def connect(self, max_retries: int = 3, retry_delay: int = 2) -> None:
        """
        Estabelece conexão com o banco de dados, com suporte a retentativas.
        
        Args:
            max_retries: Número máximo de tentativas de conexão
            retry_delay: Tempo de espera entre tentativas (segundos)
        """
        attempt = 0
        last_error = None
        
        while attempt < max_retries:
            try:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.db_name
                )
                self.cursor = self.conn.cursor(buffered=True)
                logger.info("Conexão com o banco de dados MySQL estabelecida")
                return
            except mysql.connector.Error as e:
                attempt += 1
                last_error = e
                logger.warning(f"Tentativa {attempt} falhou: {str(e)}")
                
                if attempt < max_retries:
                    logger.info(f"Aguardando {retry_delay} segundos para nova tentativa")
                    time.sleep(retry_delay)
        
        logger.error(f"Falha ao conectar ao banco de dados após {max_retries} tentativas")
        raise last_error if last_error else ConnectionError("Não foi possível conectar ao banco de dados")
    
    def disconnect(self) -> None:
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.cursor.close()
            self.conn.close()
            self.conn = None
            self.cursor = None
            logger.info("Conexão com o banco de dados MySQL fechada")
    
    def __enter__(self):
        """Método para uso com context manager (with)."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Método para uso com context manager (with)."""
        self.disconnect()
    
    def execute_query(self, query: str, params: tuple = ()) -> Optional[List[tuple]]:
        """
        Executa uma consulta SQL.
        
        Args:
            query: String com a consulta SQL
            params: Tupla com parâmetros para a consulta
        
        Returns:
            Resultado da consulta ou None
        """
        try:
            # Converte valores NaN para None em parâmetros
            clean_params = tuple(nan_to_none(p) for p in params) if params else params
            
            self.cursor.execute(query, clean_params)
            
            # Se a consulta começa com SELECT, retorna os resultados
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.conn.commit()
                return None
        except mysql.connector.Error as e:
            self.conn.rollback()
            logger.error(f"Erro ao executar consulta: {str(e)}\nQuery: {query}")
            raise
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Executa uma consulta SQL com múltiplos conjuntos de parâmetros.
        
        Args:
            query: String com a consulta SQL
            params_list: Lista de tuplas com parâmetros
        
        Returns:
            Número de linhas afetadas
        """
        if not params_list:
            return 0
            
        try:
            # Converte valores NaN para None em cada tupla de parâmetros
            clean_params_list = [
                tuple(nan_to_none(p) for p in params)
                for params in params_list
            ]
            
            self.cursor.executemany(query, clean_params_list)
            self.conn.commit()
            return self.cursor.rowcount
        except mysql.connector.Error as e:
            self.conn.rollback()
            logger.error(f"Erro ao executar consulta em lote: {str(e)}\nQuery: {query}")
            raise
    
    def create_ft_ibovespa_table(self) -> None:
        """Cria a tabela Ft_Ibovespa se não existir."""
        try:
            # Tabela simplificada com apenas os dados essenciais do IBOVESPA
            self.execute_query('''
            CREATE TABLE IF NOT EXISTS Ft_Ibovespa (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                open DECIMAL(12,2),
                high DECIMAL(12,2),
                low DECIMAL(12,2),
                close DECIMAL(12,2),
                volume BIGINT,
                year INT,
                month INT,
                day INT,
                rentabilidade DECIMAL(10,4),
                media_movel_7d DECIMAL(12,2),
                media_movel_14d DECIMAL(12,2),
                media_movel_21d DECIMAL(12,2),
                media_movel_50d DECIMAL(12,2),
                media_movel_200d DECIMAL(12,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE INDEX idx_date (date)
            ) ENGINE=InnoDB
            ''')
            
            # Adiciona índices para consultas comuns
            self._add_indices()
            
            logger.info("Tabela Ft_Ibovespa criada com sucesso ou já existe")
        except mysql.connector.Error as e:
            logger.error(f"Erro ao criar tabela Ft_Ibovespa: {str(e)}")
            raise
    
    def _add_indices(self) -> None:
        """Adiciona índices úteis à tabela se não existirem."""
        try:
            # Verifica se os índices já existem
            indices = self.execute_query('''
                SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'Ft_Ibovespa'
            ''')
            existing_indices = {index[0] for index in indices} if indices else set()
            
            # Índice para consultas por ano e mês
            if 'idx_year_month' not in existing_indices:
                self.execute_query('''
                    CREATE INDEX idx_year_month ON Ft_Ibovespa (year, month)
                ''')
                logger.info("Índice idx_year_month adicionado")
                
            # Índice para consultas por preço de fechamento
            if 'idx_close' not in existing_indices:
                self.execute_query('''
                    CREATE INDEX idx_close ON Ft_Ibovespa (close)
                ''')
                logger.info("Índice idx_close adicionado")
                
        except mysql.connector.Error as e:
            logger.warning(f"Erro ao adicionar índices: {str(e)}")
            # Não interrompe a execução em caso de erro nos índices
    
    def get_last_update_date(self) -> Optional[datetime.date]:
        """
        Obtém a data do último registro na tabela Ft_Ibovespa.
        
        Returns:
            Data do último registro ou None se não houver registros
        """
        try:
            result = self.execute_query('''
            SELECT MAX(date) FROM Ft_Ibovespa
            ''')
            
            if result and result[0][0]:
                return result[0][0]  # MySQL já retorna um objeto date
            return None
        
        except mysql.connector.Error as e:
            logger.error(f"Erro ao obter última data: {str(e)}")
            return None
    
    def get_existing_dates(self) -> set:
        """
        Obtém o conjunto de datas já existentes na tabela.
        Útil para operações em lote para evitar tentativas de inserção duplicada.
        
        Returns:
            Conjunto de datas já existentes na tabela
        """
        try:
            result = self.execute_query("SELECT date FROM Ft_Ibovespa")
            if result:
                return {row[0] for row in result}
            return set()
        except mysql.connector.Error as e:
            logger.error(f"Erro ao obter datas existentes: {str(e)}")
            return set()
    
    def insert_ibovespa_data(self, data: pd.DataFrame, batch_size: int = 200) -> int:
        """
        Insere ou atualiza dados do IBOVESPA no banco de dados usando processamento em lote.
        
        Args:
            data: DataFrame com os dados do IBOVESPA
            batch_size: Tamanho do lote para inserções/atualizações
        
        Returns:
            Número de registros inseridos/atualizados
        """
        if data.empty:
            logger.warning("DataFrame vazio, nenhum dado para inserir")
            return 0
        
        inserted = 0
        updated = 0
        
        try:
            # Obtém datas já existentes para separar registros para insert e update
            existing_dates = self.get_existing_dates()
            
            # Prepara dados para inserção e atualização
            inserts = []
            updates = []
            
            for idx, row in data.iterrows():
                # Converte a data para o formato adequado
                if isinstance(row['Date'], str):
                    date_obj = datetime.strptime(row['Date'], '%Y-%m-%d').date()
                else:
                    date_obj = row['Date'].date() if hasattr(row['Date'], 'date') else row['Date']
                
                date_str = date_obj.strftime('%Y-%m-%d')
                
                # Prepara os valores para as médias móveis 
                media_movel_7d = row.get('media_movel_7d', None)
                media_movel_14d = row.get('media_movel_14d', None)
                media_movel_21d = row.get('media_movel_21d', None)
                media_movel_50d = row.get('media_movel_50d', None)
                media_movel_200d = row.get('media_movel_200d', None)
                
                # Valores para rentabilidade
                rentabilidade = row.get('rentabilidade', None)
                
                # Verifica se a data já existe no banco
                if date_obj in existing_dates:
                    # Para update - não precisamos do ID aqui, será buscado pela data
                    updates.append((
                        row['Open'], row['High'], row['Low'], row['Close'], row['Volume'],
                        row['year'], row['month'], row['day'], rentabilidade,
                        media_movel_7d, media_movel_14d, media_movel_21d, media_movel_50d, media_movel_200d,
                        date_str  # WHERE date = ?
                    ))
                else:
                    # Para insert
                    inserts.append((
                        date_str, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'],
                        row['year'], row['month'], row['day'], rentabilidade,
                        media_movel_7d, media_movel_14d, media_movel_21d, media_movel_50d, media_movel_200d
                    ))
            
            # Processamento em lote para inserções
            if inserts:
                for i in range(0, len(inserts), batch_size):
                    batch = inserts[i:i+batch_size]
                    if not batch:
                        continue
                        
                    query = '''
                    INSERT INTO Ft_Ibovespa 
                    (date, open, high, low, close, volume, year, month, day, rentabilidade,
                     media_movel_7d, media_movel_14d, media_movel_21d, media_movel_50d, media_movel_200d)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    
                    rows_affected = self.execute_many(query, batch)
                    inserted += rows_affected
                    
                    # Log de progresso
                    batch_num = i // batch_size + 1
                    total_batches = (len(inserts) - 1) // batch_size + 1
                    logger.info(f"Inseridos lote {batch_num}/{total_batches} ({rows_affected} registros)")
            
            # Processamento em lote para atualizações
            if updates:
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i+batch_size]
                    if not batch:
                        continue
                        
                    query = '''
                    UPDATE Ft_Ibovespa 
                    SET open = %s, high = %s, low = %s, close = %s, volume = %s, 
                        year = %s, month = %s, day = %s, rentabilidade = %s,
                        media_movel_7d = %s, media_movel_14d = %s, media_movel_21d = %s, 
                        media_movel_50d = %s, media_movel_200d = %s
                    WHERE date = %s
                    '''
                    
                    rows_affected = self.execute_many(query, batch)
                    updated += rows_affected
                    
                    # Log de progresso
                    batch_num = i // batch_size + 1
                    total_batches = (len(updates) - 1) // batch_size + 1
                    logger.info(f"Atualizados lote {batch_num}/{total_batches} ({rows_affected} registros)")
            
            total = inserted + updated
            logger.info(f"Processamento em lote concluído: {inserted} inseridos, {updated} atualizados, total: {total}")
            return total
        
        except mysql.connector.Error as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir dados do IBOVESPA em lote: {str(e)}")
            raise

    def get_table_row_count(self) -> int:
        """
        Retorna o número de registros na tabela Ft_Ibovespa.
        
        Returns:
            Número de registros na tabela
        """
        try:
            result = self.execute_query("SELECT COUNT(*) FROM Ft_Ibovespa")
            return result[0][0] if result and result[0] else 0
        except mysql.connector.Error as e:
            logger.error(f"Erro ao contar registros: {str(e)}")
            return 0
    
    def optimize_table(self) -> None:
        """Otimiza a tabela para melhor desempenho após grandes operações."""
        try:
            self.execute_query("OPTIMIZE TABLE Ft_Ibovespa")
            logger.info("Tabela Ft_Ibovespa otimizada")
        except mysql.connector.Error as e:
            logger.warning(f"Erro ao otimizar tabela: {str(e)}")


if __name__ == "__main__":
    # Código para testes quando o script é executado diretamente
    ensure_directory_exists('logs')
    
    with IbovespaDBManager() as db:
        # Cria a tabela se não existir
        db.create_ft_ibovespa_table()
        
        # Exemplo de consulta
        count = db.get_table_row_count()
        print(f"Tabela Ft_Ibovespa tem {count} registros")
        
        # Obtém a data da última atualização
        last_date = db.get_last_update_date()
        if last_date:
            print(f"Último registro em: {last_date}")
        else:
            print("Nenhum registro encontrado")