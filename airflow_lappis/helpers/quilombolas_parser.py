"""
Parsing e limpeza dos arquivos do Censo 2022 — Quilombolas:
alfabetização e características dos domicílios (universo).
"""

from __future__ import annotations

import io
import logging
import re
import unicodedata
from dataclasses import dataclass, field

import pandas as pd

PREFIXO_TABELA = "Q_A_C_D_"
MAX_COL_LEN = 63
VALORES_NULOS = frozenset({"nan", "none", ""})

SUBPASTAS_DADOS = ("Apendices", "Tabelas_de_resultados", "Tabelas_selecionadas")
INDICES_FTP: list[tuple[str, str]] = [
    ("Tabelas_de_resultados", "indice_de_tabelas_de_resultados.txt"),
    ("Tabelas_selecionadas", "indice_de_tabelas_seleciondas.txt"),
]


@dataclass
class ChunkProcessado:
    """Resultado do processamento de um bloco tabular."""

    df: pd.DataFrame
    table_name: str
    table_comment: str
    primary_key: list[str] = field(default_factory=list)
    col_comments: dict[str, str] = field(default_factory=dict)
    arquivo: str = ""
    subcaminho: str = ""
    sheet_name: str = ""


def _remover_acentos(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn"
    )


def normalizar_nome_coluna(nome: str, idx: int) -> str:
    """Converte descrição original em identificador PostgreSQL."""
    sem_acento = _remover_acentos(str(nome))
    limpo = re.sub(
        r"[^\w%]",
        "",
        sem_acento.lower()
        .replace("%", "_porcentagem")
        .replace(" ", "_")
        .replace("-", "_"),
    )
    if not limpo or limpo == "none":
        return f"coluna_{idx}"
    if len(limpo) > MAX_COL_LEN:
        limpo = limpo[:MAX_COL_LEN]
    return limpo


def deduplicar_colunas(colunas: list[str]) -> list[str]:
    contagem: dict[str, int] = {}
    resultado: list[str] = []
    for col in colunas:
        if col not in contagem:
            contagem[col] = 0
            resultado.append(col)
            continue
        contagem[col] += 1
        sufixo = f"_{contagem[col]}"
        if len(col) + len(sufixo) > MAX_COL_LEN:
            base = col[: MAX_COL_LEN - len(sufixo)]
        else:
            base = col
        resultado.append(f"{base}{sufixo}")
    return resultado


def construir_nome_tabela(arquivo: str, sufixo: str = "") -> str:
    """Gera nome no padrão Q_A_C_D_[nome_da_tabela]."""
    stem = arquivo.rsplit(".", maxsplit=1)[0]
    nome = re.sub(r"[^\w]", "_", _remover_acentos(stem).lower())
    nome = re.sub(r"_+", "_", nome).strip("_")
    return f"{PREFIXO_TABELA}{nome}{sufixo}"


def construir_nome_tabela_indice(subpasta: str) -> str:
    slug = re.sub(r"[^\w]", "_", _remover_acentos(subpasta).lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return f"{PREFIXO_TABELA}indice_{slug}"


def _identificar_chunks_horizontais(df_aba: pd.DataFrame) -> list[pd.DataFrame]:
    cols_vazias = [
        i for i, col in enumerate(df_aba.columns) if df_aba[col].isnull().all()
    ]
    pontos = [-1] + cols_vazias + [len(df_aba.columns)]
    chunks: list[pd.DataFrame] = []
    for i in range(len(pontos) - 1):
        chunk = df_aba.iloc[:, pontos[i] + 1 : pontos[i + 1]].copy()
        chunk = chunk.dropna(axis=1, how="all").dropna(axis=0, how="all")
        if not chunk.empty and len(chunk.columns) > 1:
            chunks.append(chunk.reset_index(drop=True))
    return chunks


def _texto_eh_linha_titulo(texto: str) -> bool:
    """Detecta linha de título IBGE sem regex sujeito a backtracking (ReDoS)."""
    lower = texto.lower().lstrip()
    prefixos = (
        "tabela complementar",
        "tabela de resultado",
        "tabela de resultados",
        "tabela",
        "apêndice",
        "apendice",
    )
    for prefixo in prefixos:
        if not lower.startswith(prefixo):
            continue
        sufixo = lower[len(prefixo) :].lstrip()
        return bool(sufixo) and sufixo[0].isdigit()
    return False


def _localizar_linha_titulo(df_raw: pd.DataFrame) -> int | None:
    for idx in range(len(df_raw)):
        texto = " ".join(
            str(v).strip()
            for v in df_raw.iloc[idx].tolist()
            if str(v).strip().lower() not in VALORES_NULOS
        )
        if _texto_eh_linha_titulo(texto):
            return idx
    return None


_PALAVRAS_CABECALHO_DIM = frozenset(
    {
        "estado",
        "codigo",
        "código",
        "sigla",
        "nome",
        "territorio",
        "território",
        "unidade",
        "status",
        "simbolo",
        "símbolo",
        "significado",
        "legenda",
    }
)


def _eh_linha_cabecalho_dimensao(linha: pd.Series) -> bool:
    textos = [
        str(v).strip().lower()
        for v in linha.tolist()
        if str(v).strip().lower() not in VALORES_NULOS
    ]
    if len(textos) < 2:
        return False
    return any(
        any(palavra in texto for palavra in _PALAVRAS_CABECALHO_DIM) for texto in textos
    )


def _detectar_inicio_dados(df_raw: pd.DataFrame, idx_titulo: int | None) -> int | None:
    mascara_num = df_raw.apply(
        lambda r: pd.to_numeric(r, errors="coerce").notna().sum() > 1, axis=1
    )
    if mascara_num.any():
        return int(mascara_num.idxmax())

    inicio_busca = (idx_titulo + 1) if idx_titulo is not None else 0
    for idx in range(inicio_busca, len(df_raw)):
        linha = df_raw.iloc[idx]
        textos = [
            str(v).strip()
            for v in linha.tolist()
            if str(v).strip().lower() not in VALORES_NULOS
        ]
        if len(textos) < 2:
            continue
        joined = " ".join(textos)
        if re.search(r"fonte:|nota:|legenda", joined, re.IGNORECASE):
            continue
        if _eh_linha_cabecalho_dimensao(linha) and idx + 1 < len(df_raw):
            return idx + 1
        return idx
    return None


def _extrair_comentario_tabela(df_raw: pd.DataFrame, idx_titulo: int | None) -> str:
    if idx_titulo is None:
        return ""
    texto = " ".join(
        str(v).strip()
        for v in df_raw.iloc[idx_titulo].tolist()
        if str(v).strip().lower() not in VALORES_NULOS
    )
    if " - " in texto:
        return texto.split(" - ", maxsplit=1)[-1].strip()
    return texto.strip()


def _construir_cabecalho_flat(
    df_raw: pd.DataFrame, idx_titulo: int | None, idx_dados: int
) -> pd.DataFrame:
    inicio = (idx_titulo + 1) if idx_titulo is not None else 0
    fim_cab = idx_dados
    if idx_dados > 0 and _eh_linha_cabecalho_dimensao(df_raw.iloc[idx_dados - 1]):
        fim_cab = idx_dados
    cab = df_raw.iloc[inicio:fim_cab].copy().ffill(axis=1)
    linhas_validas = []
    for row_idx in range(len(cab)):
        valores = [
            str(v).strip()
            for v in cab.iloc[row_idx].tolist()
            if str(v).strip().lower() not in VALORES_NULOS
        ]
        if valores:
            linhas_validas.append(row_idx)
    return cab.iloc[linhas_validas] if linhas_validas else cab.iloc[0:0]


def _extrair_nome_coluna_flat(cabecalho: pd.DataFrame, col_idx: int) -> str:
    pedacos: list[str] = []
    for row_idx in range(len(cabecalho)):
        val = str(cabecalho.iloc[row_idx, col_idx]).strip()
        if val.lower() in VALORES_NULOS:
            continue
        row_vals = cabecalho.iloc[row_idx].dropna().astype(str).str.strip()
        unicos = [v for v in row_vals.unique() if v.lower() not in VALORES_NULOS]
        if len(unicos) > 1:
            pedacos.append(val.split(" - ")[-1].strip())
        elif len(unicos) == 1 and len(row_vals) == 1:
            pedacos.append(val.split(" - ")[-1].strip())
    return "_".join(pedacos) if pedacos else f"coluna_{col_idx}"


_COLUNAS_AUDITORIA = frozenset({"dt_ingest", "nome_fonte", "subcaminho_fonte"})


def _colunas_dados(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in _COLUNAS_AUDITORIA]


def resolver_chave_primaria(df: pd.DataFrame) -> list[str]:
    """
    Define a menor chave composta (prefixo à esquerda) que identifica cada linha.

    Nas tabelas IBGE as dimensões ficam à esquerda e as medidas à direita; usar
    só as 3 primeiras colunas falha quando há vários territórios por UF.
    """
    cols = _colunas_dados(df)
    if not cols:
        return [df.columns[0]]
    if len(cols) == 1:
        return cols

    for n in range(1, len(cols) + 1):
        subset = cols[:n]
        if df.drop_duplicates(subset=subset).shape[0] == len(df):
            return subset

    return cols


def _limpar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    col_dim = df.columns[0]
    df = df.dropna(subset=[col_dim])
    df = df[
        ~df[col_dim]
        .astype(str)
        .str.contains(r"Fonte:|Nota:|Legenda", case=False, na=False)
    ]
    return df.drop_duplicates()


def processar_chunk_excel(
    df_raw: pd.DataFrame,
    arquivo: str,
    subcaminho: str,
    sheet_name: str,
    sufixo: str = "",
) -> ChunkProcessado | None:
    """Aplica flattening de cabeçalhos IBGE e prepara metadados de comentários."""
    idx_titulo = _localizar_linha_titulo(df_raw)
    idx_dados = _detectar_inicio_dados(df_raw, idx_titulo)
    if idx_dados is None:
        logging.warning(
            "[quilombolas_parser] Sem dados tabulares em %s / %s", arquivo, sheet_name
        )
        return None

    cabecalho = _construir_cabecalho_flat(df_raw, idx_titulo, idx_dados)
    nomes_originais = [
        _extrair_nome_coluna_flat(cabecalho, i) for i in range(len(df_raw.columns))
    ]
    nomes_norm = deduplicar_colunas(
        [normalizar_nome_coluna(n, i) for i, n in enumerate(nomes_originais)]
    )

    df = df_raw.iloc[idx_dados:].copy()
    df.columns = nomes_norm
    col_comments = {
        norm: orig
        for norm, orig in zip(nomes_norm, nomes_originais, strict=True)
        if norm != orig or orig
    }
    df = _limpar_dataframe(df)

    colunas_fantasma = [c for c in df.columns if c.startswith("coluna_")]
    if colunas_fantasma:
        df = df.drop(columns=colunas_fantasma)
        col_comments = {
            k: v for k, v in col_comments.items() if k not in colunas_fantasma
        }

    if df.empty or len(df.columns) == 0:
        return None

    return ChunkProcessado(
        df=df,
        table_name=construir_nome_tabela(arquivo, sufixo=sufixo),
        table_comment=_extrair_comentario_tabela(df_raw, idx_titulo),
        primary_key=resolver_chave_primaria(df),
        col_comments=col_comments,
        arquivo=arquivo,
        subcaminho=subcaminho,
        sheet_name=sheet_name,
    )


def extrair_chunks_de_excel(
    buffer: io.BytesIO,
    arquivo: str,
    subcaminho: str,
) -> list[ChunkProcessado]:
    """Processa todas as abas e blocos horizontais de um arquivo Excel."""
    excel_file = pd.ExcelFile(buffer)
    abas_validas = [
        a
        for a in excel_file.sheet_names
        if "gráfico" not in a.lower()
        and "grafico" not in a.lower()
        and "nota" not in a.lower()
    ]
    if not abas_validas:
        abas_validas = [excel_file.sheet_names[0]]

    chunks: list[ChunkProcessado] = []
    for sheet_name in abas_validas:
        df_aba = excel_file.parse(sheet_name, header=None)
        partes = _identificar_chunks_horizontais(df_aba)
        for idx, df_raw in enumerate(partes):
            sufixo = f"_parte_{idx + 1}" if len(partes) > 1 else ""
            resultado = processar_chunk_excel(
                df_raw, arquivo, subcaminho, sheet_name, sufixo=sufixo
            )
            if resultado:
                chunks.append(resultado)
    return chunks


_TIPOS_INDICE: tuple[tuple[str, str], ...] = (
    ("cartograma", "Cartograma"),
    ("tabela", "Tabela"),
    ("apêndice", "Apêndice"),
    ("apendice", "Apêndice"),
)


def _parsear_linha_indice(linha: str) -> tuple[str, str, str] | None:
    """Extrai tipo, número e descrição de uma linha de índice (sem ReDoS)."""
    lower = linha.lower()
    for chave, rotulo in _TIPOS_INDICE:
        if not lower.startswith(chave):
            continue
        resto = linha[len(chave) :].lstrip()
        numero_match = re.match(r"[0-9]+", resto)
        if not numero_match:
            return None
        numero = numero_match.group(0)
        descricao = resto[numero_match.end() :].lstrip()
        if descricao and descricao[0] in "-–:":
            descricao = descricao[1:].lstrip()
        return rotulo, numero, descricao or linha
    return None


def parsear_arquivo_indice(conteudo: str, subpasta: str) -> ChunkProcessado:
    """Converte arquivo de índice TXT em tabela estruturada."""
    linhas = [ln.strip() for ln in conteudo.splitlines() if ln.strip()]
    registros: list[dict[str, str]] = []
    for idx, linha in enumerate(linhas, start=1):
        parsed = _parsear_linha_indice(linha)
        if parsed:
            tipo, numero, descricao = parsed
            registros.append(
                {
                    "ordem": str(idx),
                    "tipo": tipo,
                    "numero": numero,
                    "descricao": descricao,
                    "linha_original": linha,
                }
            )
        else:
            registros.append(
                {
                    "ordem": str(idx),
                    "tipo": "",
                    "numero": "",
                    "descricao": linha,
                    "linha_original": linha,
                }
            )

    df = pd.DataFrame(registros)
    return ChunkProcessado(
        df=df,
        table_name=construir_nome_tabela_indice(subpasta),
        table_comment=f"Índice de tabelas — {subpasta.replace('_', ' ')}",
        primary_key=["ordem"],
        col_comments={
            "ordem": "Ordem da linha no arquivo de índice",
            "tipo": "Tipo do item (Tabela, Cartograma, Apêndice)",
            "numero": "Número do item no índice",
            "descricao": "Descrição original do item",
            "linha_original": "Texto original da linha no arquivo de índice",
        },
        arquivo=f"indice_{subpasta.lower()}.txt",
        subcaminho=subpasta,
        sheet_name="indice",
    )


def preparar_registros_insercao(chunk: ChunkProcessado) -> list[dict[str, str]]:
    """Adiciona colunas de auditoria, deduplica pela PK e serializa para inserção."""
    from datetime import datetime

    df = chunk.df.copy()
    pk = chunk.primary_key or resolver_chave_primaria(df)
    df = df.drop_duplicates(subset=pk, keep="last")
    df["dt_ingest"] = datetime.now().isoformat()
    df["nome_fonte"] = chunk.arquivo
    df["subcaminho_fonte"] = chunk.subcaminho
    return df.astype(str).to_dict(orient="records")

