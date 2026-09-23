import logging
import re
import unicodedata
from datetime import datetime, timedelta

import pandas as pd
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable

from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

# Constantes
CONECTIVOS = frozenset(
    {"da", "das", "de", "do", "em", "e", "na", "no", "para", "ou", "com", "x", "que", "o"}
)

REGRAS_CORTE_TABELAS: dict[str, int] = {
    "tabela_3": 10,
    "tabela_7": 6,
    "tabela_9": 7,
}

MAX_COL_LEN = 63
VALORES_NULOS = ("nan", "none", "")


# Helpers: encurtar_nome_coluna
def _reordenar_prefixo_numerico(partes: list[str]) -> list[str]:
    """Move um prefixo numérico (ex: '12_a_14') para o final da lista."""
    if not partes or not partes[0] or not partes[0][0].isdigit():
        return partes

    idx_fim = 0
    for i, parte in enumerate(partes):
        if parte and (parte[0].isdigit() or parte in ("a", "x")):
            idx_fim = i + 1
        else:
            break

    if 0 < idx_fim < len(partes):
        return partes[idx_fim:] + partes[:idx_fim]
    return partes


def _remover_conectivos(partes: list[str]) -> list[str]:
    """Remove conectivos e partes vazias da lista."""
    filtradas = [p for p in partes if p and p.lower() not in CONECTIVOS]
    return filtradas or partes


def _aplicar_corte_tabela(partes: list[str], num_tabela: str) -> list[str]:
    """Remove prefixo fixo de partes para tabelas com regra especial."""
    tabela_key = num_tabela.lower()
    corte = REGRAS_CORTE_TABELAS.get(tabela_key)

    if corte is None or len(partes) <= 7:
        return partes

    cortadas = partes[corte:]
    logging.info(
        "[encurtar_nome_coluna] '%s' longo para %s — removendo %d partes iniciais",
        "_".join(partes),
        tabela_key,
        corte,
    )
    return cortadas if "_".join(cortadas) else partes


def _abreviar_partes_meio(partes: list[str]) -> str:
    """Abrevia partes do meio (exceto primeira e última) para caber em max_len."""
    if len(partes) <= 2:
        return "_".join(partes)

    meio_abreviado = [p[:5] if len(p) > 6 else p for p in partes[1:-1]]
    nome = "_".join([partes[0]] + meio_abreviado + [partes[-1]])

    logging.info("[encurtar_nome_coluna] Nome abreviado: %s", nome)
    return nome


def _truncar_preservando_ultima(nome: str, ultima: str, max_len: int) -> str:
    """Último recurso: trunca preservando a última palavra."""
    if ultima:
        espaco = max_len - len(ultima) - 1
        if espaco > 0:
            return f"{nome[:espaco]}_{ultima}"[:max_len]
    return nome[:max_len]


def encurtar_nome_coluna(
    nome: str,
    max_len: int = MAX_COL_LEN,
    num_tabela: str | None = None,
) -> str:
    """
    Limpa e encurta o nome da coluna:
    - Remove conectivos.
    - Se iniciar com número, move o prefixo numérico para o final.
    - Aplica regra de corte específica por tabela quando necessário.
    - Abrevia partes do meio mantendo primeira e última palavra.
    - Em último caso, trunca preservando a última palavra.
    """
    partes = _reordenar_prefixo_numerico(nome.split("_"))
    partes = _remover_conectivos(partes)

    nome_limpo = "_".join(partes)
    if len(nome_limpo) <= max_len:
        return nome_limpo

    if num_tabela:
        partes = _aplicar_corte_tabela(partes, num_tabela)
        nome_limpo = "_".join(partes)
        if len(nome_limpo) <= max_len:
            return nome_limpo

    nome_abreviado = _abreviar_partes_meio(partes)
    if len(nome_abreviado) <= max_len:
        return nome_abreviado

    return _truncar_preservando_ultima(
        nome_abreviado, partes[-1] if partes else "", max_len
    )


# Helpers: normalização de texto e nomes de tabela
def _remover_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )


def _normalizar_nome_coluna(
    col: str, idx: int, num_tabela: str | None, table_name: str
) -> str:
    """Limpa, normaliza e encurta o nome de uma coluna."""
    sem_acento = _remover_acentos(str(col))
    limpo = re.sub(
        r"[^\w%]",
        "",
        sem_acento.lower()
        .replace("%", "_porcentagem")
        .replace(" ", "_")
        .replace("-", "_"),
    )
    encurtado = encurtar_nome_coluna(limpo, num_tabela=num_tabela)
    return encurtado if encurtado != "none" else f"coluna_vazia_{idx}"


def _deduplicar_colunas(colunas: list[str], max_len: int = MAX_COL_LEN) -> list[str]:
    """Garante unicidade adicionando sufixo numérico às colunas duplicadas."""
    contagem: dict[str, int] = {}
    resultado: list[str] = []

    for col in colunas:
        if col not in contagem:
            contagem[col] = 0
            resultado.append(col)
            continue

        contagem[col] += 1
        sufixo = f"_{contagem[col]}"
        novo = (
            f"{col[:max_len - len(sufixo)]}{sufixo}"
            if len(col) + len(sufixo) > max_len
            else f"{col}{sufixo}"
        )
        resultado.append(novo)

    return resultado


def _construir_nome_tabela(
    arquivo: str, sheet_name: str, tema_ibge: str, sufixo: str
) -> str:
    """Deriva o nome da tabela de destino a partir dos metadados do arquivo."""
    clean_file = arquivo.split(".")[0].lower()
    match = re.search(r"(tabela_\d+)", clean_file)
    short_file = match.group(1) if match else clean_file[:15]

    clean_sheet = re.sub(
        r"[^\w]",
        "",
        _remover_acentos(sheet_name).lower().replace(" ", "_").replace("-", "_"),
    )
    prefixo = tema_ibge.lower().replace(" ", "_")
    return f"{prefixo}_{short_file}_{clean_sheet}{sufixo}"


def _obter_tema_ibge() -> str:
    config_str = Variable.get("ibge_censo_config", default_var='{"database": "Mulheres"}')
    return yaml.safe_load(config_str).get("database", "Mulheres")


# Helpers: extração do Excel
def _identificar_chunks_horizontais(df_aba: pd.DataFrame) -> list[pd.DataFrame]:
    """Divide o DataFrame pelas colunas totalmente vazias (separadores)."""
    cols_vazias = [
        i for i, col in enumerate(df_aba.columns) if df_aba[col].isnull().all()
    ]
    pontos = [-1] + cols_vazias + [len(df_aba.columns)]

    chunks = []
    for i in range(len(pontos) - 1):
        chunk = df_aba.iloc[:, pontos[i] + 1 : pontos[i + 1]].copy()
        chunk = chunk.dropna(axis=1, how="all").dropna(axis=0, how="all")
        if not chunk.empty and len(chunk.columns) > 1:
            chunks.append(chunk.reset_index(drop=True))
    return chunks


def _extrair_nome_coluna_cabecalho(linhas_cab: pd.DataFrame, col_idx: int) -> str:
    """Constrói o nome de uma coluna a partir de múltiplas linhas de cabeçalho."""
    pedacos = []
    for row_idx in range(len(linhas_cab)):
        val = str(linhas_cab.iloc[row_idx, col_idx]).strip()
        unicos = linhas_cab.iloc[row_idx].dropna().unique()
        if len(unicos) > 1 and val.lower() not in VALORES_NULOS:
            pedacos.append(val.split(" - ")[-1].strip())
    return "_".join(pedacos) if pedacos else f"coluna_vazia_{col_idx}"


def _construir_cabecalho(df_raw: pd.DataFrame, idx_dados: int) -> pd.DataFrame:
    """Retorna as linhas de cabeçalho, descartando a primeira se for muito longa."""
    cabecalho = df_raw.iloc[:idx_dados].copy().ffill(axis=1)
    primeira_linha = " ".join(
        str(v).strip()
        for v in cabecalho.iloc[0].tolist()
        if str(v).strip().lower() not in VALORES_NULOS
    )
    return cabecalho.iloc[1:] if len(primeira_linha) > 80 else cabecalho


def _processar_chunk_excel(
    df_raw: pd.DataFrame,
    idx: int,
    total: int,
    sheet_name: str,
    arquivo: str,
    tema_ibge: str,
) -> dict | None:
    """Processa um chunk horizontal do Excel e devolve o dict de metadados ou None."""
    mascara_num = df_raw.apply(
        lambda r: pd.to_numeric(r, errors="coerce").notna().sum() > 1, axis=1
    )
    if not mascara_num.any():
        return None

    idx_dados = mascara_num.idxmax()
    cabecalho = _construir_cabecalho(df_raw, idx_dados)
    nomes = [
        _extrair_nome_coluna_cabecalho(cabecalho, i) for i in range(len(df_raw.columns))
    ]

    df = df_raw.iloc[idx_dados:].copy()
    df.columns = nomes

    col_dim = df.columns[0]
    df = df.dropna(subset=[col_dim])
    df = df[~df[col_dim].astype(str).str.contains("Fonte:|Nota:", case=False, na=False)]

    return {
        "df": df,
        "sheet_name": sheet_name,
        "arquivo": arquivo,
        "sufixo": f"_parte_{idx + 1}" if total > 1 else "",
        "tema_ibge": tema_ibge,
    }


# Helpers: inserção no banco
def _processar_chunk_insercao(
    chunk_info: dict, db: ClientPostgresDB, schema: str
) -> str | None:
    """Limpa, deduplica e insere um chunk no banco. Retorna o nome da tabela ou None."""
    df: pd.DataFrame = chunk_info["df"]
    arquivo: str = chunk_info["arquivo"]
    sheet_name: str = chunk_info["sheet_name"]
    sufixo: str = chunk_info["sufixo"]
    tema_ibge: str = chunk_info["tema_ibge"]

    num_tabela_match = re.search(r"tabela[_\- ]?\d+", arquivo, re.IGNORECASE)
    num_tabela = num_tabela_match.group(0) if num_tabela_match else None

    table_name = _construir_nome_tabela(arquivo, sheet_name, tema_ibge, sufixo)

    colunas = [
        _normalizar_nome_coluna(c, idx, num_tabela, table_name)
        for idx, c in enumerate(df.columns)
    ]
    df.columns = _deduplicar_colunas(colunas)

    colunas_fantasma = [c for c in df.columns if c.startswith("coluna_vazia")]
    if colunas_fantasma:
        logging.info("Removendo colunas fantasmas: %s", colunas_fantasma)
        df = df.drop(columns=colunas_fantasma)

    if df.empty or len(df.columns) == 0:
        logging.warning("DataFrame vazio para %s. Pulando inserção.", table_name)
        return None

    col_pk = df.columns[0]
    df = df.drop_duplicates(subset=[col_pk])
    df["dt_ingest"] = datetime.now().isoformat()
    df["nome_fonte"] = arquivo

    db.insert_data(
        data=df.to_dict(orient="records"),
        table_name=table_name,
        schema=schema,
        primary_key=[col_pk],
        conflict_fields=[col_pk],
    )
    logging.info("Tabela criada/atualizada: %s.%s", schema, table_name)
    return table_name


# DAG
@dag(
    schedule_interval=get_dynamic_schedule("mulheres_censo_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "Rafael, Letícia",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mulheres", "censo_demografico", "ibge"],
)
def mulheres_censo_demografico_dag() -> None:
    """DAG para extrair, despivotar e armazenar dados do Censo 2022."""

    # Task 1: Listar arquivos no FTP
    @task
    def listar_arquivos_ftp() -> list:
        logging.info("[Task 1] Conectando ao FTP para listar arquivos...")
        tema_ibge = _obter_tema_ibge()
        arquivos = ClienteIBGE(database=tema_ibge).listar_arquivos_alvo()

        if not arquivos:
            logging.warning("Nenhum arquivo encontrado no FTP.")
        return arquivos

    # Task 2: Extrair dados do Excel
    @task
    def extrair_dados_excel(arquivo: str) -> list:
        logging.info("[Task 2] Extraindo dados do arquivo: %s", arquivo)
        tema_ibge = _obter_tema_ibge()

        buffer = ClienteIBGE(database=tema_ibge).obter_conteudo_arquivo(arquivo)
        if not buffer:
            raise ValueError(f"Falha ao baixar o arquivo {arquivo}")

        excel_file = pd.ExcelFile(buffer)
        abas_validas = [
            a
            for a in excel_file.sheet_names
            if "gráfico" not in a.lower() and "grafico" not in a.lower()
        ]
        sheet_name = abas_validas[-1] if abas_validas else excel_file.sheet_names[0]
        logging.info("Processando a aba: %s", sheet_name)

        df_aba = excel_file.parse(sheet_name, header=None)
        chunks = _identificar_chunks_horizontais(df_aba)

        return [
            resultado
            for idx, df_raw in enumerate(chunks)
            if (
                resultado := _processar_chunk_excel(
                    df_raw, idx, len(chunks), sheet_name, arquivo, tema_ibge
                )
            )
        ]

    # Task 3: Limpar e inserir dados no banco
    @task
    def limpar_e_inserir_dados(chunks_data: list) -> str:
        logging.info("[Task 3] Limpando nomes de colunas e inserindo dados...")
        db = ClientPostgresDB(get_postgres_conn())
        schema = "censo_demografico"

        tabelas = [
            nome
            for chunk in chunks_data
            if (nome := _processar_chunk_insercao(chunk, db, schema))
        ]
        return f"Processadas {len(tabelas)} tabelas com sucesso"

    lista_de_arquivos = listar_arquivos_ftp()
    dados_extraidos = extrair_dados_excel.expand(arquivo=lista_de_arquivos)
    limpar_e_inserir_dados.expand(chunks_data=dados_extraidos)


mulheres_censo_demografico_dag()
