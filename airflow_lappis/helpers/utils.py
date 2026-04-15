import unicodedata
import re
import pandas as pd
import logging
from datetime import datetime

def normalize_column_name(col: str) -> str:
    """
    Normaliza o nome de uma coluna para um formato compatível com bancos de dados.

    O processo remove acentos, converte para minúsculas e substitui caracteres 
    especiais e espaços por underscores, garantindo que o nome seja snake_case.

    Etapas:
    1. Normaliza caracteres Unicode para remover acentos (NFKD).
    2. Converte para minúsculas.
    3. Substitui sequências de caracteres não alfanuméricos por um único underscore.
    4. Remove underscores no início ou fim da string.

    Args:
        col (str): O nome original da coluna (ex: "Razão Social - Matriz").

    Returns:
        str: O nome da coluna normalizado (ex: "razao_social_matriz").

    Example:
        >>> normalize_column_name("Data de Ingestão (%)")
        'data_de_ingestao'
    """

    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = col.lower()
    col = re.sub(r'[^a-z0-9]+', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')

    return col



def process_and_clean_data(raw_data):
    if not raw_data:
        logging.warning(f"Nenhum dado recebido ")
        return

    df = pd.DataFrame(raw_data)
    df.columns = [normalize_column_name(col) for col in df.columns]

    # 1. Datas para ISO string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)

    # 2. Limpeza de nulos e conversão de tipos
    df = df.astype(object).where(pd.notna(df), None)
    df = df.replace(['NaT', 'nan', 'NaN', 'None'], None)
    
    # 3. Forçar string e limpar novamente (garante consistência no Postgres)
    df = df.astype(str).replace({'NaT': None, 'nan': None, 'NaN': None, 'None': None})

    # 4. Ingestão de Metadados
    dt_ingest = datetime.now().isoformat()
    data_to_insert = df.to_dict(orient="records")
    for item in data_to_insert:
        item["dt_ingest"] = dt_ingest
    
    return data_to_insert