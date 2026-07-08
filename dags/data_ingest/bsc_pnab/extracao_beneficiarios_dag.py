import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.sdk import dag, task

import config_bsc_pnab as settings
from cliente_bsc import AsyncBscClient
from execucao_assincrona_bsc import executar_lote
from schedule_loader import get_dynamic_schedule

default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

LOTE_CPF_LIST = 45


def _normalizar_documentos(serie: pd.Series) -> pd.Series:
    return serie.dropna().astype(str).str.strip().apply(lambda v: re.sub(r"\D", "", v))


def _cpfs_unicos_do_fato_bbagil() -> list[str]:
    df_fato = pd.read_parquet(settings.FATO_BBAGIL_PATH)
    documentos = _normalizar_documentos(df_fato["documento_beneficiario_bbagil"])
    # Filtro de 11 digitos sempre ativo -- corrige o bug do legado
    # (cpf.py:65) onde esse filtro ficava comentado e CPFs invalidos
    # chegavam a API.
    cpfs = sorted(documentos[documentos.str.len() == 11].unique().tolist())
    logging.info("[extracao_beneficiarios_dag] %d CPFs unicos no fato_bbagil", len(cpfs))
    return cpfs


def _cnpjs_unicos_do_fato_bbagil() -> list[str]:
    df_fato = pd.read_parquet(settings.FATO_BBAGIL_PATH)
    documentos = _normalizar_documentos(df_fato["documento_beneficiario_bbagil"])
    cnpjs = sorted(documentos[documentos.str.len() == 14].unique().tolist())
    logging.info(
        "[extracao_beneficiarios_dag] %d CNPJs unicos no fato_bbagil", len(cnpjs)
    )
    return cnpjs


def _extrair_por_documento(
    documentos: list[str],
    caminho_saida: Any,
    chamar_api: Any,
) -> dict[str, int]:
    """Padrao comum as 4 consultas 1-CPF-por-chamada (BPC, CadUnico,
    Relacao Trabalhista, CNPJ): filtra o que ja tem checkpoint em disco e
    delega o restante ao ``AsyncBscClient``."""
    pendentes = [doc for doc in documentos if not caminho_saida(doc).exists()]
    return executar_lote(
        itens_pendentes=pendentes, chamar_api=chamar_api, caminho_saida=caminho_saida
    )


def _extrair_cpf_list() -> dict[str, int]:
    cpfs = _cpfs_unicos_do_fato_bbagil()
    lotes = list(
        enumerate(cpfs[i : i + LOTE_CPF_LIST] for i in range(0, len(cpfs), LOTE_CPF_LIST))
    )

    def _caminho(item: tuple[int, list[str]]) -> Path:
        indice, _ = item
        return settings.CPF_LIST_DIR / f"cpf_batch_{indice:05d}.json"

    async def _chamar(client: AsyncBscClient, item: tuple[int, list[str]]) -> Any:
        _, lote = item
        return await client.cpf_list(lote)

    pendentes = [item for item in lotes if not _caminho(item).exists()]
    return executar_lote(
        itens_pendentes=pendentes, chamar_api=_chamar, caminho_saida=_caminho
    )


def _extrair_cnpj_detalhe() -> dict[str, int]:
    async def _chamar(client: AsyncBscClient, cnpj: str) -> Any:
        return await client.cnpj_detalhe(cnpj)

    return _extrair_por_documento(
        _cnpjs_unicos_do_fato_bbagil(),
        lambda cnpj: settings.CNPJ_DIR / f"cnpj_{cnpj}.json",
        _chamar,
    )


def _extrair_bpc() -> dict[str, int]:
    async def _chamar(client: AsyncBscClient, cpf: str) -> Any:
        return await client.beneficio_prestacao_continuada(cpf)

    return _extrair_por_documento(
        _cpfs_unicos_do_fato_bbagil(),
        lambda cpf: settings.BPC_DIR / f"bpc_cpf_{cpf}.json",
        _chamar,
    )


def _extrair_cadunico() -> dict[str, int]:
    async def _chamar(client: AsyncBscClient, cpf: str) -> Any:
        return await client.cadunico_cpf(cpf)

    return _extrair_por_documento(
        _cpfs_unicos_do_fato_bbagil(),
        lambda cpf: settings.CADUNICO_DIR / f"cadunico_cpf_{cpf}.json",
        _chamar,
    )


def _extrair_relacao_trabalhista() -> dict[str, int]:
    async def _chamar(client: AsyncBscClient, cpf: str) -> Any:
        return await client.relacao_trabalhista(cpf)

    return _extrair_por_documento(
        _cpfs_unicos_do_fato_bbagil(),
        lambda cpf: settings.RELACAO_TRABALHISTA_DIR
        / f"relacao_trabalhista_cpf_{cpf}.json",
        _chamar,
    )


@dag(
    dag_id="extracao_beneficiarios_bbagil_dag",
    schedule=get_dynamic_schedule("extracao_beneficiarios_bbagil_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "pnab", "bsc", "beneficiarios", "raw"],
)
def extracao_beneficiarios_bbagil_dag() -> None:
    """DAG de consulta dos beneficiarios do BB Gestao Agil no BSC/SERPRO
    (CPF em lote, CNPJ, BPC, CadUnico e Relacao Trabalhista).

    Depende do ``fato_bbagil.parquet`` produzido por ``extracao_bbagil_dag``
    -- roda depois dela (se o arquivo nao existir, a task falha de forma
    explicita, sem silenciar o erro). Todas as tasks tem checkpoint por
    CPF/CNPJ/lote (idempotencia real: nunca reconsulta o que ja tem arquivo
    em disco) e delegam toda a concorrencia/retry ao ``AsyncBscClient`` via
    ``execucao_assincrona_bsc.executar_lote``. A logica de cada consulta
    fica nas funcoes ``_extrair_*`` no topo do modulo; a DAG so orquestra.
    """

    @task
    def extrair_cpf_list() -> dict[str, int]:
        return _extrair_cpf_list()

    @task
    def extrair_cnpj_detalhe() -> dict[str, int]:
        return _extrair_cnpj_detalhe()

    @task
    def extrair_bpc() -> dict[str, int]:
        return _extrair_bpc()

    @task
    def extrair_cadunico() -> dict[str, int]:
        return _extrair_cadunico()

    @task
    def extrair_relacao_trabalhista() -> dict[str, int]:
        return _extrair_relacao_trabalhista()

    # As 5 consultas sao independentes entre si (todas dependem so do
    # fato_bbagil ja existir em disco) -- o Airflow paraleliza livremente.
    extrair_cpf_list()
    extrair_cnpj_detalhe()
    extrair_bpc()
    extrair_cadunico()
    extrair_relacao_trabalhista()


extracao_beneficiarios_bbagil_dag()
