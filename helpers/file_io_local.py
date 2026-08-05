"""Utilitarios de I/O local para o pipeline BSC/PNAB.

Padrao de idempotencia real (nao o bug do legado): o nome do arquivo
identifica univocamente a chave de negocio (ente+periodo, CPF, CNPJ) e
**nunca** carrega timestamp -- assim ``checkpoint_exists`` antes de cada
requisicao garante que um re-run pula o que ja foi extraido em vez de gerar
um arquivo novo do lado do antigo (ver bug 4 do ``bsc-api-extractor``, onde
``salvar_json_raw`` adicionava timestamp ao nome e quebrava o checkpoint por
periodo do extrato BB Agil).
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def checkpoint_exists(path: Path) -> bool:
    """Retorna True se o arquivo de saida ja existe (extracao ja feita)."""
    return path.exists()


def save_json_checkpoint(data: Any, path: Path) -> Path:
    """Salva ``data`` como JSON em ``path``, criando os diretorios pai se
    necessario. Nome de arquivo fixo por chave de negocio -- sem timestamp."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("[file_io_local] JSON salvo | path=%s", path)
    return path


def load_json_checkpoint(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_dataframe(df: pd.DataFrame, path: Path) -> Path:
    """Salva ``df`` como Parquet em ``path`` (sobrescreve -- nome de
    arquivo fixo por estagio do pipeline, sem acumular versoes com
    timestamp)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info("[file_io_local] Parquet salvo | path=%s | linhas=%d", path, len(df))
    return path


def flatten_records(
    documentos: list[dict[str, Any]], record_key: Optional[str] = None
) -> list[dict[str, Any]]:
    """Achata uma lista de respostas brutas (em memoria) em registros tabulares.

    Se ``record_key`` for informado (ex.: "transactions", "subtransactions"),
    cada documento deve conter uma lista nessa chave; os campos do nivel raiz
    do documento (ex.: ente, agencia) sao propagados para cada registro da
    lista. Caso contrario, cada documento vira um unico registro.

    Extraida de ``flatten_json_dir_to_dataframe`` para poder achatar respostas
    da API acumuladas em memoria (Postgres como destino) sem precisar
    materializar cada uma como arquivo em disco primeiro.
    """
    registros: list[dict[str, Any]] = []

    for data in documentos:
        if record_key is None:
            registros.append(data)
            continue

        itens = data.get(record_key, [])
        campos_raiz = {k: v for k, v in data.items() if k != record_key}
        for item in itens:
            registros.append({**campos_raiz, **item})

    return registros


def flatten_json_dir_to_dataframe(
    dir_path: Path,
    record_key: Optional[str] = None,
    pattern: str = "**/*.json",
) -> pd.DataFrame:
    """Percorre os JSONs brutos em ``dir_path`` e monta um DataFrame tabular.

    Se ``record_key`` for informado (ex.: "transactions", "subtransactions"),
    cada JSON deve conter uma lista nessa chave; os campos do nivel raiz do
    JSON (ex.: agencia, conta) sao propagados para cada registro da lista.
    Caso contrario, cada arquivo JSON vira um unico registro.
    """
    if not dir_path.exists():
        logger.warning("[file_io_local] Diretorio nao existe: %s", dir_path)
        return pd.DataFrame()

    arquivos = sorted(dir_path.glob(pattern))
    documentos = [load_json_checkpoint(arquivo) for arquivo in arquivos]
    registros = flatten_records(documentos, record_key=record_key)

    logger.info(
        "[file_io_local] %d arquivos lidos de %s -> %d registros",
        len(arquivos),
        dir_path,
        len(registros),
    )
    return pd.DataFrame(registros)
