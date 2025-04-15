#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para limpeza e pré-processamento de dados.
Este script demonstra técnicas comuns de limpeza de dados usando pandas.
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/data_cleaning_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_data(file_path):
    """
    Carrega os dados de um arquivo CSV, Excel ou JSON.
    
    Args:
        file_path (str): Caminho para o arquivo de dados.
        
    Returns:
        pandas.DataFrame: DataFrame com os dados carregados.
    """
    logger.info(f"Carregando dados de {file_path}")
    
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith(('.xls', '.xlsx')):
        return pd.read_excel(file_path)
    elif file_path.endswith('.json'):
        return pd.read_json(file_path)
    else:
        raise ValueError(f"Formato de arquivo não suportado: {file_path}")


def remove_duplicates(df):
    """
    Remove linhas duplicadas do DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser processado.
        
    Returns:
        pandas.DataFrame: DataFrame sem duplicatas.
    """
    n_before = len(df)
    df = df.drop_duplicates(keep='first')
    n_after = len(df)
    
    logger.info(f"Removidas {n_before - n_after} linhas duplicadas.")
    return df


def handle_missing_values(df, numeric_strategy='mean', categorical_strategy='mode'):
    """
    Trata valores ausentes no DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser processado.
        numeric_strategy (str): Estratégia para colunas numéricas ('mean', 'median', 'zero').
        categorical_strategy (str): Estratégia para colunas categóricas ('mode', 'unknown').
        
    Returns:
        pandas.DataFrame: DataFrame com valores ausentes tratados.
    """
    logger.info("Tratando valores ausentes")
    df_clean = df.copy()
    
    # Identificar colunas numéricas e categóricas
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    
    # Tratar valores ausentes em colunas numéricas
    for col in numeric_cols:
        missing = df_clean[col].isnull().sum()
        if missing > 0:
            if numeric_strategy == 'mean':
                fill_value = df_clean[col].mean()
            elif numeric_strategy == 'median':
                fill_value = df_clean[col].median()
            elif numeric_strategy == 'zero':
                fill_value = 0
            else:
                raise ValueError(f"Estratégia não suportada: {numeric_strategy}")
                
            df_clean[col].fillna(fill_value, inplace=True)
            logger.info(f"Coluna {col}: {missing} valores ausentes preenchidos com {numeric_strategy} ({fill_value:.2f})")
    
    # Tratar valores ausentes em colunas categóricas
    for col in categorical_cols:
        missing = df_clean[col].isnull().sum()
        if missing > 0:
            if categorical_strategy == 'mode':
                fill_value = df_clean[col].mode()[0]
            elif categorical_strategy == 'unknown':
                fill_value = 'Unknown'
            else:
                raise ValueError(f"Estratégia não suportada: {categorical_strategy}")
                
            df_clean[col].fillna(fill_value, inplace=True)
            logger.info(f"Coluna {col}: {missing} valores ausentes preenchidos com {categorical_strategy} ({fill_value})")
    
    return df_clean


def convert_data_types(df, date_columns=None, categorical_columns=None):
    """
    Converte tipos de dados no DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser processado.
        date_columns (list): Lista de colunas para converter em datetime.
        categorical_columns (list): Lista de colunas para converter em categoria.
        
    Returns:
        pandas.DataFrame: DataFrame com tipos de dados convertidos.
    """
    logger.info("Convertendo tipos de dados")
    df_clean = df.copy()
    
    # Converter colunas de data
    if date_columns:
        for col in date_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                logger.info(f"Coluna {col} convertida para datetime")
    
    # Converter colunas categóricas
    if categorical_columns:
        for col in categorical_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype('category')
                logger.info(f"Coluna {col} convertida para categoria")
    
    return df_clean


def remove_outliers(df, columns, method='zscore', threshold=3):
    """
    Remove outliers do DataFrame.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser processado.
        columns (list): Lista de colunas para verificar outliers.
        method (str): Método para detecção ('zscore', 'iqr').
        threshold (float): Limite para consideração de outlier.
        
    Returns:
        pandas.DataFrame: DataFrame sem outliers.
    """
    logger.info(f"Removendo outliers usando método {method}")
    df_clean = df.copy()
    
    for col in columns:
        if col not in df_clean.columns or not pd.api.types.is_numeric_dtype(df_clean[col]):
            continue
            
        n_before = len(df_clean)
        
        if method == 'zscore':
            # Método Z-score
            z_scores = np.abs((df_clean[col] - df_clean[col].mean()) / df_clean[col].std())
            df_clean = df_clean[z_scores < threshold]
        elif method == 'iqr':
            # Método IQR (Intervalo Interquartil)
            Q1 = df_clean[col].quantile(0.25)
            Q3 = df_clean[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            df_clean = df_clean[(df_clean[col] >= lower_bound) & (df_clean[col] <= upper_bound)]
        
        n_after = len(df_clean)
        logger.info(f"Coluna {col}: {n_before - n_after} outliers removidos")
    
    return df_clean


def normalize_feature(df, column, method='minmax'):
    """
    Normaliza ou padroniza uma coluna numérica.
    
    Args:
        df (pandas.DataFrame): DataFrame a ser processado.
        column (str): Nome da coluna a ser normalizada.
        method (str): Método de normalização ('minmax', 'zscore').
        
    Returns:
        pandas.DataFrame: DataFrame com a coluna normalizada.
    """
    logger.info(f"Normalizando coluna {column} usando método {method}")
    df_clean = df.copy()
    
    if column not in df_clean.columns or not pd.api.types.is_numeric_dtype(df_clean[column]):
        logger.warning(f"Coluna {column} não encontrada ou não é numérica")
        return df_clean
    
    if method == 'minmax':
        # Normalização Min-Max para escala [0, 1]
        min_val = df_clean[column].min()
        max_val = df_clean[column].max()
        df_clean[f"{column}_normalized"] = (df_clean[column] - min_val) / (max_val - min_val)
    elif method == 'zscore':
        # Padronização Z-score
        mean_val = df_clean[column].mean()
        std_val = df_clean[column].std()
        df_clean[f"{column}_standardized"] = (df_clean[column] - mean_val) / std_val
    else:
        logger.warning(f"Método de normalização não suportado: {method}")
    
    return df_clean


def clean_data(input_path, output_path=None, **kwargs):
    """
    Função principal que executa todo o pipeline de limpeza de dados.
    
    Args:
        input_path (str): Caminho para o arquivo de entrada.
        output_path (str, optional): Caminho para salvar o arquivo processado.
        **kwargs: Argumentos adicionais para as funções de limpeza.
        
    Returns:
        pandas.DataFrame: DataFrame processado.
    """
    try:
        # Criar diretório de logs se não existir
        os.makedirs('logs', exist_ok=True)
        
        logger.info("Iniciando processo de limpeza de dados")
        
        # Carregar dados
        df = load_data(input_path)
        logger.info(f"Dados carregados com sucesso: {df.shape[0]} linhas e {df.shape[1]} colunas")
        
        # Remover duplicatas
        df = remove_duplicates(df)
        
        # Tratar valores ausentes
        numeric_strategy = kwargs.get('numeric_strategy', 'mean')
        categorical_strategy = kwargs.get('categorical_strategy', 'mode')
        df = handle_missing_values(df, numeric_strategy, categorical_strategy)
        
        # Converter tipos de dados
        date_columns = kwargs.get('date_columns', [])
        categorical_columns = kwargs.get('categorical_columns', [])
        df = convert_data_types(df, date_columns, categorical_columns)
        
        # Remover outliers (opcional)
        if 'outlier_columns' in kwargs:
            outlier_method = kwargs.get('outlier_method', 'zscore')
            outlier_threshold = kwargs.get('outlier_threshold', 3)
            df = remove_outliers(df, kwargs['outlier_columns'], outlier_method, outlier_threshold)
        
        # Normalização (opcional)
        if 'normalize_columns' in kwargs:
            norm_method = kwargs.get('normalize_method', 'minmax')
            for col in kwargs['normalize_columns']:
                df = normalize_feature(df, col, norm_method)
        
        # Salvar dados processados
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if output_path.endswith('.csv'):
                df.to_csv(output_path, index=False)
            elif output_path.endswith(('.xls', '.xlsx')):
                df.to_excel(output_path, index=False)
            elif output_path.endswith('.json'):
                df.to_json(output_path, orient='records')
                
            logger.info(f"Dados processados salvos em {output_path}")
        
        logger.info("Processo de limpeza de dados concluído com sucesso")
        return df
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    # Exemplo de uso
    input_file = "../datasets/exemplo/dados_brutos.csv"
    output_file = "../datasets/exemplo/dados_limpos.csv"
    
    # Parâmetros de limpeza
    params = {
        'numeric_strategy': 'mean',
        'categorical_strategy': 'mode',
        'date_columns': ['data_compra', 'data_entrega'],
        'categorical_columns': ['categoria', 'regiao'],
        'outlier_columns': ['valor', 'quantidade'],
        'outlier_method': 'iqr',
        'outlier_threshold': 1.5,
        'normalize_columns': ['valor'],
        'normalize_method': 'minmax'
    }
    
    # Executar limpeza
    try:
        df_clean = clean_data(input_file, output_file, **params)
        print(f"Dimensões do DataFrame limpo: {df_clean.shape}")
    except FileNotFoundError:
        print("Arquivo de exemplo não encontrado. Ajuste os caminhos para seus dados reais.")
    except Exception as e:
        print(f"Erro: {str(e)}") 