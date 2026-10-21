"""
DAG de ingestão — Censo 2022: Quilombolas, alfabetização e características dos domicílios.

Fonte FTP:
/Censos/Censo_Demografico_2022/Quilombolas_alfabetizacao_e_caracteristicas_dos_domicílios_Resultados_do_universo/
"""

import logging
from datetime import datetime, timedelta
from typing import Any

import yaml
from airflow.decorators import dag, task
from airflow.models import Variable

from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from quilombolas_parser import (
    INDICES_FTP,
    SUBPASTAS_DADOS,
    extrair_chunks_de_excel,
    parsear_arquivo_indice,
    preparar_registros_insercao,
)
from schedule_loader import get_dynamic_schedule

SCHEMA_DESTINO = "censo_demografico"
DATABASE_FTP = (
    "Quilombolas_alfabetizacao_e_caracteristicas_dos_domicílios_Resultados_do_universo"
)


def _obter_database_ftp() -> str:
    config_str = Variable.get(
        "ibge_quilombolas_config",
        default_var=f'{{"database": "{DATABASE_FTP}"}}',
    )
    return yaml.safe_load(config_str).get("database", DATABASE_FTP)


def _chunk_para_payload(chunk: Any) -> dict[str, Any]:
    return {
        "table_name": chunk.table_name,
        "table_comment": chunk.table_comment,
        "col_comments": chunk.col_comments,
        "records": preparar_registros_insercao(chunk),
        "primary_key": chunk.primary_key,
    }


def _inserir_payloads(db: ClientPostgresDB, payloads: list[dict[str, Any]]) -> list[str]:
    tabelas: list[str] = []
    for payload in payloads:
        pk = payload["primary_key"]
        db.insert_data(
            data=payload["records"],
            table_name=payload["table_name"],
            schema=SCHEMA_DESTINO,
            primary_key=pk,
            conflict_fields=pk,
        )
        db.apply_comments(
            schema=SCHEMA_DESTINO,
            table_name=payload["table_name"],
            table_comment=payload.get("table_comment"),
            column_comments=payload.get("col_comments"),
        )
        tabelas.append(payload["table_name"])
        logging.info(
            "Tabela criada/atualizada com comentários: %s.%s",
            SCHEMA_DESTINO,
            payload["table_name"],
        )
    return tabelas


@dag(
    schedule_interval=get_dynamic_schedule("quilombolas_censo_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "Lucas Guimaraes",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["quilombolas", "censo_demografico", "ibge", "alfabetizacao"],
)
def quilombolas_alfabetizacao_censo_dag() -> None:
    """Extrai dados quilombolas do Censo 2022 via FTP e carrega em censo_demografico."""

    @task
    def listar_arquivos_dados() -> list[dict[str, str]]:
        logging.info("[Task 1] Listando arquivos de dados no FTP...")
        cliente = ClienteIBGE(database=_obter_database_ftp())
        arquivos = cliente.listar_arquivos_em_subpastas(
            list(SUBPASTAS_DADOS),
            formato_preferido="xlsx",
        )
        logging.info("%d arquivo(s) de dados encontrado(s).", len(arquivos))
        return arquivos

    @task
    def listar_arquivos_indices() -> list[dict[str, str]]:
        logging.info("[Task 1b] Listando arquivos de índice no FTP...")
        indices = ClienteIBGE(database=_obter_database_ftp()).listar_arquivos_texto(
            INDICES_FTP
        )
        logging.info("%d arquivo(s) de índice encontrado(s).", len(indices))
        return indices

    @task
    def extrair_dados_excel(entrada: dict[str, str]) -> list[dict[str, Any]]:
        subcaminho = entrada["subcaminho"]
        arquivo = entrada["arquivo"]
        logging.info("[Task 2] Extraindo %s/%s", subcaminho, arquivo)

        buffer = ClienteIBGE(database=_obter_database_ftp()).obter_conteudo_arquivo(
            arquivo, subcaminho=subcaminho
        )
        if not buffer:
            raise ValueError(f"Falha ao baixar {subcaminho}/{arquivo}")

        chunks = extrair_chunks_de_excel(buffer, arquivo, subcaminho)
        return [_chunk_para_payload(c) for c in chunks]

    @task
    def extrair_indice(entrada: dict[str, str]) -> list[dict[str, Any]]:
        subcaminho = entrada["subcaminho"]
        arquivo = entrada["arquivo"]
        logging.info("[Task 2b] Extraindo índice %s/%s", subcaminho, arquivo)

        conteudo = ClienteIBGE(database=_obter_database_ftp()).obter_conteudo_texto(
            arquivo, subcaminho=subcaminho
        )
        if not conteudo:
            raise ValueError(f"Falha ao baixar índice {subcaminho}/{arquivo}")

        chunk = parsear_arquivo_indice(conteudo, subcaminho)
        return [_chunk_para_payload(chunk)]

    @task
    def inserir_chunks(payloads: list[dict[str, Any]]) -> list[str]:
        if not payloads:
            return []
        db = ClientPostgresDB(get_postgres_conn())
        return _inserir_payloads(db, payloads)

    @task
    def consolidar_resultado(
        tabelas_dados: list[list[str]], tabelas_indices: list[list[str]]
    ) -> str:
        total = sum(len(t) for t in tabelas_dados) + sum(len(t) for t in tabelas_indices)
        return f"Processadas {total} tabelas no schema {SCHEMA_DESTINO}"

    lista_dados = listar_arquivos_dados()
    lista_indices = listar_arquivos_indices()

    payloads_excel = extrair_dados_excel.expand(entrada=lista_dados)
    payloads_indices = extrair_indice.expand(entrada=lista_indices)

    tabelas_dados = inserir_chunks.expand(payloads=payloads_excel)
    tabelas_indices = inserir_chunks.expand(payloads=payloads_indices)

    consolidar_resultado(tabelas_dados, tabelas_indices)


quilombolas_alfabetizacao_censo_dag()
