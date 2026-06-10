import gc
import io
import logging
import re
import shutil
import signal
import tempfile
import unicodedata
import zipfile
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd

class TimeoutLeituraError(Exception):
    """Exceção customizada para planilhas que demoram muito para serem lidas."""
    pass

log = logging.getLogger(__name__)

_TIMEOUT_SEGUNDOS = 120  # 2 minutos por arquivo


class TimeoutLeituraError(TimeoutError):
    """Leitura da planilha excedeu o tempo limite — arquivo ignorado."""


def _com_timeout(fn, *args, segundos: int = _TIMEOUT_SEGUNDOS, **kwargs):
    """Executa *fn* com alarme SIGALRM.  Se demorar mais que *segundos*,
    levanta :class:`TimeoutLeituraError` em vez de travar o Worker.

    Usa ``signal.alarm`` (POSIX) — compatível com o Linux do Airflow.
    """
    def _handler(signum, frame):
        raise TimeoutLeituraError(
            f"Leitura abortada após {segundos}s — arquivo ignorado por lentidão"
        )

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(segundos)
    try:
        return fn(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

# ────────────────────────────────────────────────────────────────
# Mapeamento de abas PNAB → tabela destino no banco
# ────────────────────────────────────────────────────────────────
ABA_PARA_TABELA: dict[str, str] = {
    "informacoes": "raw_pnab_informacoes",
    "acoes gerais": "raw_pnab_acoes_gerais",
    "acoes cultura viva": "raw_pnab_acoes_cultura_viva",
    "operacionalizacao": "raw_pnab_operacionalizacao",
    "lista de contemplados geral": "raw_pnab_lista_contemplados_geral",
    "lista contemplados pncv": "raw_pnab_lista_contemplados_pncv",
}


def _norm_texto(s: str) -> str:
    """Normaliza texto para comparação tolerante de nomes de abas:
    lowercase, sem acentos, espaços colapsados."""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _resolver_tabela_aba(nome_aba: str) -> str | None:
    """Retorna o nome da tabela destino para uma aba, ou None se não mapeada.

    Usa ``_norm_texto`` para tolerância a acentos/caixa/extra whitespace.
    """
    aba_norm = _norm_texto(nome_aba)
    for chave, tabela in ABA_PARA_TABELA.items():
        if aba_norm == chave:
            return tabela
    # Fallback: substring para abas com sufixos extras (ex: "1. Informações")
    for chave, tabela in ABA_PARA_TABELA.items():
        if chave in aba_norm or aba_norm in chave:
            return tabela
    return None


def normalizar_nome(nome: Any) -> str:
    """Normaliza string para comparação: lowercase, sem acentos, sem números,
    sem pontuação, espaços colapsados."""
    if nome is None:
        return ""
    s = str(nome).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\d+", " ", s)
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_ENGINE_MAP = {
    ".xlsx": "calamine",
    ".xlsm": "calamine",
    ".xls": "calamine",
    ".xlsb": "calamine",
    # .ods intencionalmente REMOVIDO — a engine odf (fallback) carrega
    # o DOM XML inteiro em memória e causa OOM Kills em planilhas grandes.
    # Calamine pode ler .ods, mas se falhar o fallback é destrutivo.
}

# Extensões proibidas: causam OOM no Worker do Airflow
_EXTENSOES_PROIBIDAS = {".ods"}


def detectar_engine(file_path: str | Path) -> Optional[str]:
    """Retorna a engine pandas adequada para a extensão do arquivo.

    Retorna ``None`` para extensões proibidas (.ods), emitindo warning.
    """
    ext = Path(file_path).suffix.lower()
    if ext in _EXTENSOES_PROIBIDAS:
        log.warning(
            "[extracao_planilhas.py] Extensão '%s' proibida (OOM risk) — "
            "arquivo será pulado",
            ext,
        )
        return None
    return _ENGINE_MAP.get(ext)


def _eh_zip_excel(source: str | Path | io.BytesIO) -> bool:
    """Verifica se o conteúdo é um ZIP (XLSX/XLSM/XLSB usam esse formato).

    Funciona tanto com caminho de arquivo quanto com buffer em memória
    (``io.BytesIO``). No caso de buffer, usa ``zipfile.is_zipfile()``
    que aceita objetos file-like nativamente.
    """
    try:
        if isinstance(source, io.BytesIO):
            pos = source.tell()
            source.seek(0)
            result = zipfile.is_zipfile(source)
            source.seek(pos)
            return result
        # Caminho em disco — abre normalmente
        with open(source, "rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except (OSError, zipfile.BadZipFile):
        return False


def abrir_excel(
    source: str | Path | io.BytesIO,
    file_name: str | None = None,
) -> pd.ExcelFile:
    """Abre planilha a partir de caminho ou buffer em memória.

    Prioriza a leitura 100% em memória (BytesIO) com o motor ``calamine``,
    que é memory-safe e transforma corrupções em exceções normais em vez
    de causar OOM. Motores legados (openpyxl, xlrd, pyxlsb) são
    tentados como fallback — e estes podem exigir arquivo em disco, que
    é criado sob demanda e limpo automaticamente via ``finally``.

    Parameters
    ----------
    source : str | Path | io.BytesIO
        Caminho do arquivo físico OU buffer em memória com os bytes.
    file_name : str | None
        Nome do arquivo (para log e detecção de extensão). Obrigatório
        quando ``source`` é ``io.BytesIO``.

    Levanta RuntimeError se nenhuma tentativa funcionar.
    Levanta RuntimeError imediatamente para extensões proibidas (.ods).
    """
    # --- Resolve nome, extensão e buffer ---
    if isinstance(source, io.BytesIO):
        if not file_name:
            raise ValueError("file_name é obrigatório quando source é BytesIO")
        nome = file_name
        suffix = Path(nome).suffix.lower()
        buffer = source
        buffer.seek(0)
    else:
        source = Path(source)
        nome = source.name
        suffix = source.suffix.lower()
        buffer = None

    # --- Proteção contra OOM: pula .ods imediatamente ---
    if suffix in _EXTENSOES_PROIBIDAS:
        log.warning(
            "[extracao_planilhas.py] Arquivo '%s' (.ods) pulado — "
            "engine odf causa OOM em planilhas grandes",
            nome,
        )
        raise RuntimeError(
            f"Extensão .ods proibida (OOM risk): '{nome}'. "
            "Use .xlsx ou .xls como alternativa."
        )

    # --- Monta lista de tentativas (engine, precisa_disco) ---
    tentativas: list[tuple[str, str, bool]] = []
    # 1a tentativa: calamine em memória
    tentativas.append(("calamine", nome, False))

    # Fallbacks por extensão (podem exigir disco)
    # NOTA: .ods NÃO tem fallback — a engine odf carrega o DOM XML
    # inteiro e causa OOM Kills. O early-return acima já rejeita .ods.
    fallback_engine: str | None = None
    if suffix == ".xls":
        fallback_engine = "xlrd"
    elif suffix in {".xlsx", ".xlsm"}:
        fallback_engine = "openpyxl"
    elif suffix == ".xlsb":
        fallback_engine = "pyxlsb"

    if fallback_engine:
        tentativas.append((fallback_engine, nome, True))

    # Fallbacks extras para XLS que na verdade é ZIP disfarçado
    if suffix == ".xls":
        if _eh_zip_excel(source):
            tentativas.append(("openpyxl", nome, True))
            tentativas.append(("pyxlsb", nome, True))

    # Desduplica preservando ordem
    vistos: set[str] = set()
    tentativas_unicas: list[tuple[str, str, bool]] = []
    for engine, n, disco in tentativas:
        if engine not in vistos:
            tentativas_unicas.append((engine, n, disco))
            vistos.add(engine)

    # --- Tenta cada engine ---
    tmp_path: str | None = None
    ultimo_erro: Exception | None = None

    try:
        for engine, n, precisa_disco in tentativas_unicas:
            try:
                if precisa_disco and buffer is not None:
                    # Motor legado que exige arquivo físico — grava sob demanda
                    if tmp_path is None:
                        tmp_dir = tempfile.mkdtemp(prefix="extracao_fallback_")
                        tmp_path = str(Path(tmp_dir) / n)
                        with open(tmp_path, "wb") as f:
                            buffer.seek(0)
                            f.write(buffer.read())
                        log.debug(
                            "[extracao_planilhas.py] Fallback gravou buffer em '%s'",
                            tmp_path,
                        )
                    read_source = tmp_path
                elif not precisa_disco and buffer is not None:
                    # calamine com BytesIO — 100% em memória
                    buffer.seek(0)
                    read_source = buffer
                else:
                    # source é caminho de arquivo (str ou Path)
                    read_source = str(source)

                xls = _com_timeout(pd.ExcelFile, read_source, engine=engine)
                if xls.sheet_names:
                    log.info(
                        "[extracao_planilhas.py] Aberto '%s' com engine '%s'",
                        n,
                        engine,
                    )
                    # Limpa tempfile ANTES de retornar — sucesso, não precisa mais
                    if tmp_path is not None:
                        tmp_dir = str(Path(tmp_path).parent)
                        shutil.rmtree(tmp_dir, ignore_errors=True)
                        log.debug(
                            "[extracao_planilhas.py] Tempfile '%s' removido (sucesso)",
                            tmp_dir,
                        )
                    return xls
            except TimeoutLeituraError:
                raise  # propaga imediatamente — não tenta fallback
            except Exception as exc:
                ultimo_erro = exc
                log.debug(
                    "[extracao_planilhas.py] Falha '%s' com engine '%s': %s",
                    n,
                    engine,
                    exc,
                )
    finally:
        # --- Limpeza garantida do tempfile de fallback (caminho de falha) ---
        if tmp_path is not None:
            tmp_dir = str(Path(tmp_path).parent)
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                log.debug(
                    "[extracao_planilhas.py] Tempfile '%s' removido (finally)",
                    tmp_dir,
                )
            except Exception:
                pass

    raise RuntimeError(
        f"Não foi possível abrir o arquivo '{nome}'. "
        f"Último erro: {type(ultimo_erro).__name__}: {ultimo_erro}"
    ) from ultimo_erro


def extrair_tabela_raw(
    file_path: str | Path | io.BytesIO,
    regex_header: re.Pattern,
    col_header_idx: int = 1,
    col_categoria_idx: int = 0,
    min_len_categoria: int = 6,
    file_name: str | None = None,
) -> list[dict]:
    """Extrai tabelas de um arquivo de planilha usando o padrão ELT.

    **Comportamento**:

    1. Lê **todas** as abas do arquivo com ``dtype=str``.
    2. Para cada aba, procura a **linha âncora** onde:
       - ``col_categoria_idx`` (default: col 0) está vazia
       - ``col_header_idx`` (default: col 1) normalizada bate em ``regex_header``
    3. Fatia o DataFrame a partir dessa linha âncora.
    4. Detecta "subtabelas" (blocos de categoria) usando heurística de
       separadores: linhas onde a coluna ``col_categoria_idx`` tem texto
       com comprimento > ``min_len_categoria`` e as demais colunas são NaN.
    5. Para cada subtabela, atribui a coluna ``tipo_edital`` com o nome
       da categoria detectada.
    6. **NÃO** renomeia colunas para ``c001``, **NÃO** exclui colunas
       residuais, **NÃO** empilha header como dado.

    Parameters
    ----------
    file_path : str | Path | io.BytesIO
        Caminho do arquivo físico OU buffer em memória com os bytes da planilha.
        Quando ``io.BytesIO``, o parâmetro ``file_name`` deve ser informado.
    regex_header : re.Pattern
        Pattern compilado usado para identificar a linha âncora de cabeçalho.
        Para LPG: ``re.compile(r"edita(?:is|l)", re.IGNORECASE)``
        Para PNAB: ``re.compile(r"contemplad|acompanhament", re.IGNORECASE)``
    col_header_idx : int
        Índice da coluna usada na busca do regex (default: 1, segunda coluna).
    col_categoria_idx : int
        Índice da coluna onde se busca o nome da categoria (default: 0).
    min_len_categoria : int
        Comprimento mínimo de texto na col 0 para considerar uma "linha de
        categoria" (default: 6).
    file_name : str | None
        Nome do arquivo (para log e metadados). Obrigatório quando
        ``file_path`` é ``io.BytesIO``.

    Returns
    -------
    list[dict]
        Lista de dicts, um por subtabela extraída. Cada dict contém:
        - ``nome_arquivo`` (str): nome do arquivo de origem
        - ``aba`` (str): nome da aba de origem
        - ``tipo_edital`` (str | None): categoria detectada
        - ``dados`` (pd.DataFrame): DataFrame com colunas originais do Excel
          + coluna ``tipo_edital``. Todas as colunas são ``str``.
    """
    # Resolve nome do arquivo para logs e metadados
    if isinstance(file_path, io.BytesIO):
        nome_arquivo = file_name or "unknown"
        source = file_path
    else:
        source = Path(file_path)
        nome_arquivo = source.name

    resultados: list[dict] = []

    try:
        xls = abrir_excel(source, file_name=nome_arquivo)
    except TimeoutLeituraError:
        raise
    except Exception as exc:
        log.error(
            "[extracao_planilhas.py] Falha ao abrir '%s': %s",
            nome_arquivo,
            exc,
        )
        return resultados

    with xls:
        abas = xls.sheet_names
        for aba in abas:
            try:
                df = _com_timeout(pd.read_excel, xls, sheet_name=aba, dtype=str)
            except TimeoutLeituraError:
                raise
            except Exception as exc:
                log.warning(
                    "[extracao_planilhas.py] Erro ao ler aba '%s' de '%s': %s",
                    aba,
                    nome_arquivo,
                    exc,
                )
                continue

            if df.empty or df.shape[1] < (max(col_header_idx, col_categoria_idx) + 1):
                continue

            # --- Busca da linha âncora ---
            start_idx: int | None = None
            categoria_edital: str | None = None

            for i in range(len(df)):
                try:
                    val_cat = df.iloc[i, col_categoria_idx]
                except IndexError:
                    continue

                col_cat_vazia = (
                    val_cat is None
                    or pd.isna(val_cat)
                    or str(val_cat).strip() == ""
                )

                try:
                    val_header = df.iloc[i, col_header_idx]
                except IndexError:
                    continue

                col_hdr_norm = normalizar_nome(val_header)

                if col_cat_vazia and regex_header.search(col_hdr_norm):
                    start_idx = i
                    # Categoria = último valor não-nulo na col_categoria_idx
                    # antes da linha âncora
                    col_cat_slice = df.iloc[:i, col_categoria_idx]
                    nao_nulos = col_cat_slice.dropna()
                    if nao_nulos.any():
                        categoria_edital = str(nao_nulos.iloc[-1]).strip()
                    break

            if start_idx is None:
                continue

            # --- Fatia a partir do header ---
            header = df.iloc[start_idx].tolist()
            body = df.iloc[start_idx + 1:].copy()
            body.columns = header
            body = body.reset_index(drop=True)

            # --- Delimitação de subtabelas ---
            list_start_idx = [0]
            lista_categorias = [categoria_edital]

            for idx, row in body.iterrows():
                valores = row.tolist()
                try:
                    texto = str(valores[col_categoria_idx]).strip()
                except IndexError:
                    continue

                demais = [
                    v for j, v in enumerate(valores) if j != col_categoria_idx
                ]
                if len(texto) > min_len_categoria and all(
                    pd.isna(v) for v in demais
                ):
                    lista_categorias.append(texto)
                    list_start_idx.append(idx)

            bounds = list_start_idx + [len(body)]
            tuplas = list(zip(bounds[:-1], bounds[1:]))

            # --- Extração de cada subtabela ---
            for j, (a, b) in enumerate(tuplas):
                if j < len(tuplas) - 1:
                    df_y = body.iloc[a:b].copy()
                else:
                    df_y = body.iloc[a:].copy()

                tipo = lista_categorias[j]

                # Para subtabelas após a primeira (j > 0), a primeira linha
                # é o "separador de categoria" e a segunda é o header real
                if j > 0 and len(df_y) >= 2:
                    header2 = df_y.iloc[1].tolist()
                    df_y = df_y.iloc[2:].copy()
                    df_y.columns = header2
                    df_y = df_y.reset_index(drop=True)

                df_y["tipo_edital"] = tipo

                resultados.append({
                    "nome_arquivo": nome_arquivo,
                    "aba": aba,
                    "tipo_edital": tipo,
                    "dados": df_y,
                })

    # Libera referências cíclicas do ExcelFile e DataFrames intermediários
    del xls
    gc.collect()

    return resultados


# ────────────────────────────────────────────────────────────────
# Extração PNAB — roteamento por aba + sub-tabelas
# ────────────────────────────────────────────────────────────────

_HEADER_MARKERS = [
    "nome do edital",
    "nome do(a) contemplado",
    "cpf",
    "cnpj",
    "nome do projeto",
    "valor pago",
    "link",
    "publicacao",
    "resultado do edital",
]


def _parece_header(row_values) -> bool:
    """Heurística da PoC: uma linha parece um cabeçalho de sub-tabela
    se contém ≥2 dos marcadores conhecidos."""
    joined = " | ".join(_norm_texto(v) for v in row_values if v is not None)
    hits = sum(1 for m in _HEADER_MARKERS if m in joined)
    return hits >= 2


def _eh_valor_nulo(v) -> bool:
    """Verifica se um valor de célula é nulo/vazio para fins de delimitação
    de sub-tabelas. Consolida a checagem espalhada pela PoC em um helper."""
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    if str(v).strip() == "":
        return True
    return False


def extrair_pnab(
    file_buffer: io.BytesIO,
    file_name: str,
    id_anexo: str,
) -> list[dict]:
    """Extrai tabelas de um arquivo PNAB usando roteamento por aba.

    Lê as abas do arquivo, mapeia cada uma para uma tabela destino via
    ``ABA_PARA_TABELA`` e, dentro de cada aba, detecta sub-tabelas
    separadas por linhas de categoria (nome do edital). Cada DataFrame
    gerado recebe ``id_anexo`` na primeira posição e ``tipo_edital``.

    Parameters
    ----------
    file_buffer : io.BytesIO
        Buffer em memória com os bytes da planilha.
    file_name : str
        Nome do arquivo (para log e detecção de extensão).
    id_anexo : str
        Identificador do anexo para rastreabilidade. Será a primeira
        coluna de todo DataFrame gerado.

    Returns
    -------
    list[dict]
        Lista de dicts com ``nome_tabela_destino`` e ``dataframe``.
        Abas não mapeadas são ignoradas silenciosamente.
    """
    resultados: list[dict] = []

    try:
        xls = abrir_excel(file_buffer, file_name=file_name)
    except TimeoutLeituraError:
        raise
    except Exception as exc:
        log.error(
            "[extracao_planilhas.py] Falha ao abrir PNAB '%s': %s",
            file_name,
            exc,
        )
        return resultados

    with xls:
        for aba in xls.sheet_names:
            tabela_destino = _resolver_tabela_aba(aba)
            if tabela_destino is None:
                log.debug(
                    "[extracao_planilhas.py] Aba '%s' não mapeada — pulando",
                    aba,
                )
                continue

            log.info(
                "[extracao_planilhas.py] Aba '%s' → %s",
                aba,
                tabela_destino,
            )

            try:
                df = _com_timeout(pd.read_excel, xls, sheet_name=aba, dtype=str)
            except TimeoutLeituraError:
                raise
            except Exception as exc:
                log.warning(
                    "[extracao_planilhas.py] Erro ao ler aba '%s' de '%s': %s",
                    aba,
                    file_name,
                    exc,
                )
                continue

            if df.empty:
                continue

            # --- Busca do cabeçalho âncora ---
            # Heurística 1 (PoC original): col 0 vazia + "nome do edital" na col 1.
            # Heurística 2 (flexível): qualquer linha com ≥2 HEADER_MARKERS.
            start_idx: int | None = None
            categoria_edital: str | None = None

            for i in range(len(df)):
                row_vals = df.iloc[i].tolist()

                # Heurística 1: col 0 vazia + "nome do edital" na col 1 (padrão PoC)
                # Usa == para igualdade (fidelidade à PoC), não substring
                col0_vazia = (
                    pd.isna(df.iloc[i, 0])
                    or str(df.iloc[i, 0]).strip() == ""
                )
                col1_norm = _norm_texto(df.iloc[i, 1]) if df.shape[1] > 1 else ""

                if col0_vazia and col1_norm == "nome do edital":
                    col0_slice = df.iloc[:i, 0].dropna()
                    if col0_slice.any():
                        categoria_edital = str(col0_slice.iloc[-1]).strip()
                    start_idx = i
                    break

                # Heurística 2: qualquer linha que pareça header
                # (≥2 marcadores) — apenas se Heurística 1 não bateu
                if _parece_header(row_vals):
                    col0_slice = df.iloc[:i, 0].dropna()
                    if col0_slice.any():
                        categoria_edital = str(col0_slice.iloc[-1]).strip()
                    start_idx = i
                    break

            if start_idx is None:
                # Aba sem âncora: usa a aba inteira como uma tabela
                # (caso de abas simples como "Informações")
                df_out = df.copy()
                df_out.insert(0, "id_anexo", id_anexo)
                resultados.append({
                    "nome_tabela_destino": tabela_destino,
                    "dataframe": df_out,
                })
                continue

            # --- Fatia a partir do header ---
            header = df.iloc[start_idx].tolist()
            body = df.iloc[start_idx + 1:].copy()
            body.columns = header
            body = body.reset_index(drop=True)

            # --- Delimitação de sub-tabelas (lógica PoC) ---
            # Quebras: linhas onde col 0 tem texto > 6 chars e as
            # demais colunas são NaN/vazias (separadores de categoria).
            list_start_idx = [0]
            lista_categorias = [categoria_edital]

            for idx, row in body.iterrows():
                valores = row.tolist()
                try:
                    texto = str(valores[0]).strip()
                except (IndexError, TypeError):
                    continue
                demais = [v for j, v in enumerate(valores) if j != 0]
                if len(texto) > 6 and all(_eh_valor_nulo(v) for v in demais):
                    lista_categorias.append(texto)
                    list_start_idx.append(idx)

            bounds = list_start_idx + [len(body)]
            tuplas = list(zip(bounds[:-1], bounds[1:]))

            # --- Extração de cada sub-tabela ---
            for j, (a, b) in enumerate(tuplas):
                if j < len(tuplas) - 1:
                    df_y = body.iloc[a:b].copy()
                else:
                    df_y = body.iloc[a:].copy()

                tipo = lista_categorias[j]

                # Para sub-tabelas após a primeira (j > 0), a primeira
                # linha é o separador de categoria e a segunda é o header
                if j > 0 and len(df_y) >= 2:
                    header2 = df_y.iloc[1].tolist()
                    df_y = df_y.iloc[2:].copy()
                    df_y.columns = header2
                    df_y = df_y.reset_index(drop=True)

                df_y["tipo_edital"] = tipo
                df_y.insert(0, "id_anexo", id_anexo)

                resultados.append({
                    "nome_tabela_destino": tabela_destino,
                    "dataframe": df_y,
                })

    # Libera referências cíclicas do ExcelFile e DataFrames intermediários
    del xls
    gc.collect()

    return resultados


def extrair_lpg(
    file_buffer: io.BytesIO,
    file_name: str,
    id_anexo: str,
) -> list[dict]:
    """Extrai tabelas de um arquivo LPG usando roteamento por template.

    Inspeciona as abas do arquivo e roteia a extração conforme 3 padrões:

    1. TEMPLATE EDITAIS (Anexo II) — aba ``Lista dos Editais``
    2. TEMPLATE CONTEMPLADOS (Anexo III) — aba ``Lista dos Contemplados``
    3. TEMPLATE DADOS BÁSICOS — abas com prefixo numérico
       (ex: ``1.Instrumentos``, ``2.1.Pessoa Física``)

    Cada DataFrame gerado recebe ``id_anexo`` como primeira coluna e
    é tipado como ``str`` (padrão Raw ELT).

    Parameters
    ----------
    file_buffer : io.BytesIO
        Buffer em memória com os bytes da planilha.
    file_name : str
        Nome do arquivo (para log e detecção de extensão).
    id_anexo : str
        Identificador do anexo para rastreabilidade.

    Returns
    -------
    list[dict]
        Lista de dicts com ``nome_tabela_destino`` e ``dataframe``.
    """
    resultados: list[dict] = []

    try:
        xls = abrir_excel(file_buffer, file_name=file_name)
    except TimeoutLeituraError:
        raise
    except Exception as exc:
        log.error(
            "[extracao_planilhas.py] Falha ao abrir LPG '%s': %s",
            file_name,
            exc,
        )
        return resultados

    sheet_names = xls.sheet_names
    log.info(
        "[extracao_planilhas.py] LPG '%s' — abas detectadas: %s",
        file_name,
        sheet_names,
    )

    # ── Normaliza nomes de abas para matching tolerante ──
    abas_norm = {_norm_texto(s): s for s in sheet_names}

    # ── TEMPLATE EDITAIS (Anexo II) ──
    aba_editais_orig = None
    for norm, orig in abas_norm.items():
        if "lista dos editais" in norm:
            aba_editais_orig = orig
            break

    if aba_editais_orig is not None:
        log.info(
            "[extracao_planilhas.py] LPG '%s' — Template Editais "
            "detectado (aba '%s')",
            file_name,
            aba_editais_orig,
        )
        try:
            df = _com_timeout(
                pd.read_excel, xls, sheet_name=aba_editais_orig, dtype=str
            )
        except TimeoutLeituraError:
            raise
        except Exception as exc:
            log.warning(
                "[extracao_planilhas.py] Erro ao ler aba '%s' de '%s': %s",
                aba_editais_orig,
                file_name,
                exc,
            )
            df = pd.DataFrame()

        if not df.empty:
            df_editais = _extrair_lpg_editais(df, id_anexo)
            if df_editais is not None and not df_editais.empty:
                resultados.append({
                    "nome_tabela_destino": "lpg_editais",
                    "dataframe": df_editais,
                })

        gc.collect()

    # ── TEMPLATE CONTEMPLADOS (Anexo III) ──
    aba_contemplados_orig = None
    for norm, orig in abas_norm.items():
        if "lista dos contemplados" in norm:
            aba_contemplados_orig = orig
            break

    if aba_contemplados_orig is not None:
        log.info(
            "[extracao_planilhas.py] LPG '%s' — Template Contemplados "
            "detectado (aba '%s')",
            file_name,
            aba_contemplados_orig,
        )
        try:
            df = _com_timeout(
                pd.read_excel, xls, sheet_name=aba_contemplados_orig, dtype=str
            )
        except TimeoutLeituraError:
            raise
        except Exception as exc:
            log.warning(
                "[extracao_planilhas.py] Erro ao ler aba '%s' de '%s': %s",
                aba_contemplados_orig,
                file_name,
                exc,
            )
            df = pd.DataFrame()

        if not df.empty:
            df_cont = _extrair_lpg_contemplados(df, id_anexo)
            if df_cont is not None and not df_cont.empty:
                resultados.append({
                    "nome_tabela_destino": "lpg_contemplados",
                    "dataframe": df_cont,
                })

        gc.collect()

    # ── TEMPLATE DADOS BÁSICOS — abas com prefixo numérico ──
    abas_dados = []
    padrao_num = re.compile(r"^\d+[\d.]*\s*\.?")
    for s in sheet_names:
        if padrao_num.match(s.strip()):
            # Exclui abas já tratadas acima
            s_norm = _norm_texto(s)
            if "lista dos editais" in s_norm or "lista dos contemplados" in s_norm:
                continue
            abas_dados.append(s)

    if abas_dados:
        log.info(
            "[extracao_planilhas.py] LPG '%s' — Template Dados Básicos "
            "detectado (%d abas: %s)",
            file_name,
            len(abas_dados),
            abas_dados,
        )
        for aba in abas_dados:
            try:
                df = _com_timeout(
                    pd.read_excel, xls, sheet_name=aba, header=1, dtype=str
                )
            except TimeoutLeituraError:
                raise
            except Exception as exc:
                log.warning(
                    "[extracao_planilhas.py] Erro ao ler aba de dados "
                    "'%s' de '%s': %s",
                    aba,
                    file_name,
                    exc,
                )
                gc.collect()
                continue

            if df.empty:
                gc.collect()
                continue

            # Drop da linha de instruções (primeira linha após header)
            df = df.iloc[1:].copy()
            df = df.reset_index(drop=True)

            # Sanitiza nome da aba → nome de tabela
            nome_tabela = _sanitizar_nome_tabela_lpg(aba)
            df.insert(0, "id_anexo", id_anexo)
            df = df.astype(str)

            resultados.append({
                "nome_tabela_destino": nome_tabela,
                "dataframe": df,
            })

            del df
            gc.collect()

    # Libera referências do ExcelFile
    del xls
    gc.collect()

    return resultados


# ── Helpers para extração LPG ──

_ANCORA_EDITAIS = ["nome do edital", "breve descricao do edital"]
_ANCORA_CONTEMPLADOS = ["nome do edital", "nome do(a) contemplado(a)"]


def _encontrar_linha_cabecalho(
    df: pd.DataFrame,
    ancoras: list[str],
) -> int | None:
    """Encontra o índice da linha de cabeçalho ancorada por colunas.

    Percorre as linhas do DataFrame e retorna o índice da primeira linha
    cujos valores normalizados contêm todas as âncoras fornecidas.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame com dados brutos (sem header definido).
    ancoras : list[str]
        Lista de strings de âncora normalizadas (lowercase, sem acentos).

    Returns
    -------
    int | None
        Índice da linha de cabeçalho, ou ``None`` se não encontrada.
    """
    for i in range(len(df)):
        row_vals = [_norm_texto(v) for v in df.iloc[i].tolist()]
        if all(any(a in v for v in row_vals) for a in ancoras):
            return i
    return None


def _extrair_subtabelas_ffill(
    df_body: pd.DataFrame,
    nome_categoria_col: str,
    categoria_inicial: str | None,
) -> pd.DataFrame:
    """Extrai sub-tabelas com ffill de categoria (lógica comum Editais/Contemplados).

    Identifica linhas separadoras de categoria (col 0 com texto > 6 chars
    e demais colunas NaN/vazias), atribui a categoria via ffill e remove
    as linhas separadoras.

    Parameters
    ----------
    df_body : pd.DataFrame
        DataFrame com header já definido (sem a linha de cabeçalho).
    nome_categoria_col : str
        Nome da coluna de categoria (ex: ``categoria_edital``).
    categoria_inicial : str | None
        Categoria da primeira sub-tabela (extraída antes do header).

    Returns
    -------
    pd.DataFrame
        DataFrame com a coluna de categoria adicionada.
    """
    # Identifica categorias: col 0 com texto > 6 chars, demais NaN
    is_categoria = df_body.apply(
        lambda row: (
            len(str(row.iloc[0]).strip()) > 6
            and all(_eh_valor_nulo(v) for v in row.iloc[1:])
        ),
        axis=1,
    )

    df_body = df_body.copy()
    df_body[nome_categoria_col] = None

    if categoria_inicial:
        df_body.loc[0, nome_categoria_col] = categoria_inicial

    for idx in df_body.index[is_categoria]:
        df_body.loc[idx, nome_categoria_col] = str(df_body.loc[idx, df_body.columns[0]]).strip()

    # ffill para preencher as linhas abaixo de cada categoria
    df_body[nome_categoria_col] = df_body[nome_categoria_col].ffill()

    # Remove as linhas separadoras de categoria
    df_body = df_body[~is_categoria].copy()
    df_body = df_body.reset_index(drop=True)

    return df_body


def _extrair_lpg_editais(
    df: pd.DataFrame,
    id_anexo: str,
) -> pd.DataFrame | None:
    """Extrai tabela de editais LPG com ffill de categoria.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame bruto lido da aba ``Lista dos Editais``.
    id_anexo : str
        Identificador do anexo.

    Returns
    -------
    pd.DataFrame | None
        DataFrame com ``id_anexo`` e ``categoria_edital``, ou ``None``.
    """
    ancoras_norm = [_norm_texto(a) for a in _ANCORA_EDITAIS]
    start_idx = _encontrar_linha_cabecalho(df, ancoras_norm)

    if start_idx is None:
        log.warning(
            "[extracao_planilhas.py] LPG Editais: âncora %s não encontrada — "
            "aba descartada",
            _ANCORA_EDITAIS,
        )
        return None

    # Extrai categoria antes do header (col 0)
    categoria_inicial = None
    col0_slice = df.iloc[:start_idx, 0].dropna()
    if col0_slice.any():
        categoria_inicial = str(col0_slice.iloc[-1]).strip()

    # Define header e body
    header = df.iloc[start_idx].tolist()
    body = df.iloc[start_idx + 1:].copy()
    body.columns = header
    body = body.reset_index(drop=True)

    # ffill de sub-tabelas
    body = _extrair_subtabelas_ffill(body, "categoria_edital", categoria_inicial)

    body.insert(0, "id_anexo", id_anexo)
    body = body.astype(str)

    return body


def _extrair_lpg_contemplados(
    df: pd.DataFrame,
    id_anexo: str,
) -> pd.DataFrame | None:
    """Extrai tabela de contemplados LPG com ffill de categoria.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame bruto lido da aba ``Lista dos Contemplados``.
    id_anexo : str
        Identificador do anexo.

    Returns
    -------
    pd.DataFrame | None
        DataFrame com ``id_anexo`` e ``categoria_contemplado``, ou ``None``.
    """
    ancoras_norm = [_norm_texto(a) for a in _ANCORA_CONTEMPLADOS]
    start_idx = _encontrar_linha_cabecalho(df, ancoras_norm)

    if start_idx is None:
        log.warning(
            "[extracao_planilhas.py] LPG Contemplados: âncora %s não "
            "encontrada — aba descartada",
            _ANCORA_CONTEMPLADOS,
        )
        return None

    # Extrai categoria antes do header (col 0)
    categoria_inicial = None
    col0_slice = df.iloc[:start_idx, 0].dropna()
    if col0_slice.any():
        categoria_inicial = str(col0_slice.iloc[-1]).strip()

    # Define header e body
    header = df.iloc[start_idx].tolist()
    body = df.iloc[start_idx + 1:].copy()
    body.columns = header
    body = body.reset_index(drop=True)

    # ffill de sub-tabelas
    body = _extrair_subtabelas_ffill(
        body, "categoria_contemplado", categoria_inicial
    )

    body.insert(0, "id_anexo", id_anexo)
    body = body.astype(str)

    return body


def _sanitizar_nome_tabela_lpg(nome_aba: str) -> str:
    """Sanitiza nome de aba LPG para nome de tabela PostgreSQL.

    Transforma ``1.Instrumentos`` → ``lpg_dados_instrumentos``,
    ``2.1.Pessoa Física`` → ``lpg_dados_pessoa_fisica``, etc.

    Regras:
    1. Remove prefixo numérico (ex: ``1.``, ``2.1.``).
    2. Substitui caracteres não alfanuméricos por ``_``.
    3. Colapsa ``_`` consecutivos e remove leading/trailing ``_``.
    4. Prefixa com ``lpg_dados_``.

    Parameters
    ----------
    nome_aba : str
        Nome original da aba.

    Returns
    -------
    str
        Nome sanitizado de tabela.
    """
    # Remove prefixo numérico (ex: "1.", "2.1.", "3.")
    nome = re.sub(r"^[\d.]+\s*\.?\s*", "", nome_aba)
    # Normaliza: lowercase, sem acentos
    nome = _norm_texto(nome)
    # Substitui não-alfanuméricos por _
    nome = re.sub(r"[^a-z0-9]+", "_", nome)
    # Colapsa _ consecutivos e remove bordas
    nome = re.sub(r"_+", "_", nome).strip("_")
    # Prefixa
    return f"lpg_dados_{nome}"


def listar_arquivos_locais(
    base_dir: str | Path,
    extensoes: set[str] | None = None,
) -> list[str]:
    """Lista arquivos de planilha recursivamente em um diretório local.

    Parameters
    ----------
    base_dir : str | Path
        Diretório raiz da busca.
    extensoes : set[str] | None
        Extensões válidas (default: {".xlsx", ".xls", ".xlsm", ".xlsb"}).

    Returns
    -------
    list[str]
        Caminhos absolutos dos arquivos encontrados.
    """
    if extensoes is None:
        extensoes = {".xlsx", ".xls", ".xlsm", ".xlsb"}

    base_dir = Path(base_dir)
    if not base_dir.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {base_dir}")

    arquivos: list[str] = []
    for f in base_dir.rglob("*"):
        if f.is_file() and f.suffix.lower() in extensoes and not f.name.startswith("~$"):
            arquivos.append(str(f))

    return sorted(arquivos)


def listar_arquivos_s3(
    bucket: str,
    prefix: str,
    extensoes: set[str] | None = None,
) -> list[dict[str, str]]:
    """Lista objetos de planilha em um bucket S3.

    Parameters
    ----------
    bucket : str
        Nome do bucket S3.
    prefix : str
        Prefixo (pasta) dentro do bucket.
    extensoes : set[str] | None
        Extensões válidas (default: {".xlsx", ".xls", ".xlsm", ".xlsb"}).

    Returns
    -------
    list[dict[str, str]]
        Lista de dicts com ``{"bucket": ..., "key": ...}`` para cada arquivo.
    """
    if extensoes is None:
        extensoes = {".xlsx", ".xls", ".xlsm", ".xlsb"}

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="minio_default")
    client = hook.get_client_type("s3")

    paginator = client.get_paginator("list_objects_v2")
    arquivos: list[dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
            if ext in extensoes and not Path(key).name.startswith("~$"):
                arquivos.append({"bucket": bucket, "key": key})

    return arquivos
