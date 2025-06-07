#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Módulo simplificado para obtenção de dados históricos do IBOVESPA.
Este script é responsável pela requisição e processamento inicial dos dados.
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import logging
import os
from typing import Optional
from pandas.tseries.offsets import BDay

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
        file_handler = logging.FileHandler(os.path.join('logs', 'fetch_data.log'), encoding='utf-8')
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


def fetch_ibovespa_data(
    start_date: str = '2018-01-01',
    end_date: Optional[str] = None,
    interval: str = '1d'
) -> pd.DataFrame:
    """
    Obtém dados históricos do IBOVESPA (^BVSP).
    
    Args:
        start_date: Data de início no formato 'YYYY-MM-DD'
        end_date: Data final no formato 'YYYY-MM-DD' (padrão: data atual)
        interval: Intervalo dos dados ('1d', '1wk', '1mo', etc.)
    
    Returns:
        DataFrame pandas com os dados históricos do IBOVESPA
    """
    try:
        # Se a data final não for especificada, usa a data atual
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Buscando dados do IBOVESPA de {start_date} até {end_date}")
        
        # Cria um objeto Ticker para o IBOVESPA
        ticker_obj = yf.Ticker('^BVSP')
        
        # Obtém os dados históricos
        df = ticker_obj.history(interval=interval, start=start_date, end=end_date)
        
        if df.empty:
            logger.warning("Nenhum dado encontrado para o IBOVESPA")
            return pd.DataFrame()
        
        # Resetando o índice para manter a data como coluna
        df.reset_index(inplace=True)
        
        # Adiciona features extras
        df = add_features(df)
        
        logger.info(f"Obtidos {len(df)} registros para o IBOVESPA")
        return df
    
    except Exception as e:
        logger.error(f"Erro ao buscar dados do IBOVESPA: {str(e)}")
        return pd.DataFrame()


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona features derivadas ao dataframe do IBOVESPA, 
    considerando apenas dias úteis para cálculos.
    
    Args:
        df: DataFrame com os dados originais
    
    Returns:
        DataFrame com as features adicionadas
    """
    try:
        # Criação de features temporais
        df['year'] = df['Date'].dt.year
        df['month'] = df['Date'].dt.month
        df['day'] = df['Date'].dt.day
        
        # Verifica se cada data é um dia útil
        df['is_business_day'] = df['Date'].apply(lambda x: BDay().is_on_offset(x))
        df_business = df[df['is_business_day']]
        
        # Cálculo de rentabilidade considerando apenas dias úteis
        df['rentabilidade'] = df['Close'].pct_change() * 100
        
        # Cálculo das médias móveis principais (considerando apenas dias úteis)
        for window in [7, 14, 21, 50, 200]:
            df[f'media_movel_{window}d'] = df['Close'].rolling(window=window).mean()
        
        return df
    
    except Exception as e:
        logger.error(f"Erro ao adicionar features: {str(e)}")
        return df

def save_to_csv(df: pd.DataFrame, filename: str = "IBOVESPA_historical.csv") -> str:
    """
    Salva o DataFrame em um arquivo CSV.
    
    Args:
        df: DataFrame com os dados do IBOVESPA
        filename: Nome do arquivo CSV
    
    Returns:
        Caminho completo para o arquivo salvo
    """
    if df.empty:
        logger.warning("Nenhum dado para salvar")
        return ""
    
    try:
        # Garante que o diretório de dados existe
        data_dir = "data"
        ensure_directory_exists(data_dir)
        
        # Caminho completo para o arquivo
        filepath = os.path.join(data_dir, filename)
        
        # Salva o DataFrame no arquivo CSV
        df.to_csv(filepath, index=False)
        logger.info(f"Dados salvos em {filepath}")
        
        return filepath
    
    except Exception as e:
        logger.error(f"Erro ao salvar dados em CSV: {str(e)}")
        return ""


if __name__ == "__main__":
    # Código para testes quando o script é executado diretamente
    ensure_directory_exists('logs')
    
    # Obtém dados do IBOVESPA desde 2018
    ibovespa_data = fetch_ibovespa_data()
    
    # Salva em CSV
    if not ibovespa_data.empty:
        save_to_csv(ibovespa_data)
        
        # Exibe algumas estatísticas básicas
        print(f"Registros obtidos: {len(ibovespa_data)}")
        print(f"Período: {ibovespa_data['Date'].min()} até {ibovespa_data['Date'].max()}")
        print(f"Preço de fechamento mais recente: {ibovespa_data['Close'].iloc[-1]:.2f}")