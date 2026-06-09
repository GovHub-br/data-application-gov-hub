import logging
import os
import re
import unicodedata
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task

from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

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
TEMA_IBGE = "Unidades_de_Conservacao"
SCHEMA_DESTINO = "censo_demografico"


class ClienteIBGE_UC(ClienteIBGE):
    def __init__(self) -> None:
        super().__init__(database=TEMA_IBGE)


def _obter_tema_ibge() -> str:
    return TEMA_IBGE


def _remover_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )


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
    """Limpa e encurta o nome da coluna."""
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


def _construir_nome_tabela(arquivo: str, sufixo: str) -> str:
    clean_file = os.path.basename(arquivo).split(".")[0].lower()
    clean_file = re.sub(r"[^\w]", "_", _remover_acentos(clean_file))
    clean_file = re.sub(r"_+", "_", clean_file).strip("_")
    # Truncate to 40 chars max to keep total table name under 63 chars
    clean_file = clean_file[:40]
    return f"un_cons_{clean_file}{sufixo}"


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
) -> dict | None:
    """Processa um chunk horizontal do Excel e devolve o dict de metadados ou None."""
    mascara_num = df_raw.apply(
        lambda r: pd.to_numeric(r, errors="coerce").notna().sum() >= 1, axis=1
    )
    if not mascara_num.any():
        # Fallback: use first non-empty row as data start
        mascara_nao_vazia = df_raw.apply(
            lambda r: r.astype(str).str.strip().ne("").any(), axis=1
        )
        if not mascara_nao_vazia.any():
            return None
        mascara_num = mascara_nao_vazia

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
    df = df.where(pd.notnull(df), other=None)

    return {
        "records": df.to_dict(orient="records"),
        "sheet_name": sheet_name,
        "arquivo": arquivo,
        "sufixo": f"_parte_{idx + 1}" if total > 1 else "",
    }


def _processar_chunk_insercao(
    chunk_info: dict, db: ClientPostgresDB, schema: str
) -> str | None:
    """Limpa, deduplica e insere um chunk no banco. Retorna o nome da tabela ou None."""
    df: pd.DataFrame = pd.DataFrame(chunk_info["records"])
    arquivo: str = chunk_info["arquivo"]
    sheet_name: str = chunk_info["sheet_name"]
    sufixo: str = chunk_info["sufixo"]

    num_tabela_match = re.search(r"tabela[_\- ]?\d+", arquivo, re.IGNORECASE)
    num_tabela = num_tabela_match.group(0) if num_tabela_match else None

    table_name = _construir_nome_tabela(arquivo, sufixo)

    # Capture original headers before normalization
    original_colunas = [str(c) for c in df.columns]

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

    try:
        db.insert_data(
            data=df.to_dict(orient="records"),
            table_name=table_name,
            schema=schema,
            primary_key=[col_pk],
            conflict_fields=[col_pk],
        )
    except Exception as exc:
        logging.error(
            "[ingestao_ibge_unidades_conservacao] Falha ao inserir na tabela %s.%s: %s",
            schema,
            table_name,
            exc,
        )
        return None

    logging.info("Tabela criada/atualizada: %s.%s", schema, table_name)

    # COMMENT ON TABLE
    fonte_esc = arquivo.replace("'", "''")
    db.execute_non_query(
        f"COMMENT ON TABLE {schema}.{table_name} IS "
        f"'Censo Demografico 2022 - Unidades de Conservacao - {fonte_esc}';"
    )

    # COMMENT ON COLUMN
    for col_norm, col_orig in zip(df.columns, original_colunas):
        orig_esc = col_orig.replace("'", "''")
        db.execute_non_query(
            f"COMMENT ON COLUMN {schema}.{table_name}.{col_norm} IS '{orig_esc}';"
        )

    return table_name


def _listar_arquivos_recursivo(cliente: ClienteIBGE_UC) -> list[str]:
    """Lista recursivamente arquivos Excel dentro do diretório raiz do tema."""

    def _walk_ftp(ftp, diretorio_atual: str = "") -> list[str]:
        arquivos: list[str] = []

        try:
            itens = ftp.nlst()
        except Exception as exc:
            logging.warning(
                "[ingestao_ibge_unidades_conservacao] Falha ao listar '%s': %s",
                diretorio_atual or "/",
                exc,
            )
            return arquivos

        pasta_origem = ftp.pwd()
        for item in itens:
            if item in (".", ".."):
                continue

            caminho_relativo = f"{diretorio_atual}/{item}" if diretorio_atual else item
            entrou_em_dir = False
            try:
                ftp.cwd(item)
                entrou_em_dir = True
                arquivos.extend(_walk_ftp(ftp, caminho_relativo))
            except Exception:
                if item.lower().endswith((".xlsx", ".xls")):
                    arquivos.append(caminho_relativo)
            finally:
                if entrou_em_dir:
                    ftp.cwd(pasta_origem)

        return arquivos

    with cliente._conectar() as ftp:
        return sorted(_walk_ftp(ftp))


@dag(
    schedule_interval=get_dynamic_schedule("ingestao_ibge_unidades_conservacao"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "Airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ibge", "unidades_conservacao", "ftp", "ingestao"],
)
def ingestao_ibge_unidades_conservacao_dag() -> None:
    """DAG para extrair e armazenar dados do FTP do IBGE para Unidades de Conservação."""

    @task
    def listar_arquivos_ftp() -> list[str]:
        logging.info(
            "[ingestao_ibge_unidades_conservacao] Conectando ao FTP para listar arquivos..."
        )

        cliente = ClienteIBGE_UC()
        arquivos = _listar_arquivos_recursivo(cliente)

        if not arquivos:
            logging.warning(
                "[ingestao_ibge_unidades_conservacao] Nenhum arquivo Excel encontrado no FTP."
            )
            return []

        contagem_por_pasta: dict[str, int] = {}
        for arquivo in arquivos:
            pasta_principal = arquivo.split("/", 1)[0]
            contagem_por_pasta[pasta_principal] = (
                contagem_por_pasta.get(pasta_principal, 0) + 1
            )

        for pasta, quantidade in sorted(contagem_por_pasta.items()):
            logging.info(
                "[ingestao_ibge_unidades_conservacao] %d arquivo(s) encontrado(s) em %s",
                quantidade,
                pasta,
            )

        return arquivos

    @task
    def extrair_dados_excel(arquivo: str) -> list:
        logging.info(
            "[ingestao_ibge_unidades_conservacao] Extraindo dados do arquivo: %s",
            arquivo,
        )

        cliente = ClienteIBGE_UC()
        buffer = cliente.obter_conteudo_arquivo(arquivo)
        if not buffer:
            raise ValueError(f"Falha ao baixar o arquivo {arquivo}")

        if arquivo.lower().endswith(".csv"):
            df_aba = pd.read_csv(buffer, header=None, encoding="utf-8", sep=";")
            sheet_name = "csv"
            chunks = _identificar_chunks_horizontais(df_aba)
        else:
            excel_file = pd.ExcelFile(buffer)
            palavras_excluir = {"notas", "legenda", "fonte", "índice", "indice"}
            abas_validas = [
                aba
                for aba in excel_file.sheet_names
                if "gráfico" not in aba.lower()
                and "grafico" not in aba.lower()
                and not any(palavra in aba.lower() for palavra in palavras_excluir)
            ]
            sheet_name = abas_validas[-1] if abas_validas else excel_file.sheet_names[0]
            logging.info(
                "[ingestao_ibge_unidades_conservacao] Processando a aba: %s",
                sheet_name,
            )

            df_aba = excel_file.parse(sheet_name, header=None)
            chunks = _identificar_chunks_horizontais(df_aba)

        return [
            resultado
            for idx, df_raw in enumerate(chunks)
            if (
                resultado := _processar_chunk_excel(
                    df_raw, idx, len(chunks), sheet_name, arquivo
                )
            )
        ]

    @task
    def limpar_e_inserir_dados(chunks_data: list) -> str:
        logging.info(
            "[ingestao_ibge_unidades_conservacao] Limpando nomes de colunas e inserindo dados..."
        )

        db = ClientPostgresDB(get_postgres_conn())
        tabelas = []
        for chunk in chunks_data:
            logging.info("Tipo do chunk recebido: %s", type(chunk))
            nome = _processar_chunk_insercao(chunk, db, SCHEMA_DESTINO)
            if nome:
                tabelas.append(nome)

        return f"Processadas {len(tabelas)} tabelas com sucesso"

    lista_de_arquivos = listar_arquivos_ftp()
    dados_extraidos = extrair_dados_excel.expand(arquivo=lista_de_arquivos)
    limpar_e_inserir_dados.expand(chunks_data=dados_extraidos)


ingestao_ibge_unidades_conservacao_dag()