import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from extracao_por_plano_acao import carregar_planos_acao, extrair_por_plano_acao
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_plano_acao_dado_bancario_dag",
    schedule=get_dynamic_schedule("api_plano_acao_dado_bancario_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "dado_bancario", "raw"],
)
def api_plano_acao_dado_bancario_dag() -> None:
    """Passo 3B da secao 6: contas bancarias de cada plano de acao.

    Grava **todas** as contas de cada plano, com todas as colunas da origem,
    em ``transferegov.plano_acao_dado_bancario_minc``. Antes essa informacao
    era efeito colateral da DAG do BB Agil, que guardava uma unica conta por
    plano e apenas quatro campos -- perdendo tanto a granularidade ("uma
    linha por registro de conta bancaria", secao 7.1) quanto colunas da
    origem.

    Escolher qual conta consultar no BB Agil continua existindo, mas como
    regra de consumo, dentro de ``extracao_bbagil_dag`` -- nao mais como
    filtro na ingestao.
    """

    @task
    def fetch_dados_bancarios() -> list[dict[str, Any]]:
        db = ClientPostgresDB(get_postgres_conn())
        planos = carregar_planos_acao(db)

        if not planos:
            raise ValueError(
                "[api_plano_acao_dado_bancario_dag.py] Nenhum plano de ação "
                f"encontrado em {schemas.SCHEMA_TRANSFEREGOV}."
                f"{schemas.TABELA_PLANO_ACAO}"
            )

        logging.info(
            "[api_plano_acao_dado_bancario_dag.py] Buscando contas de %d planos "
            "de ação",
            len(planos),
        )

        api = ClienteTransfereGov()
        contas = extrair_por_plano_acao(
            planos,
            buscar=api.get_dados_bancarios_by_plano_acao,
            rotulo="dados bancários",
        )

        if not contas:
            raise ValueError(
                "[api_plano_acao_dado_bancario_dag.py] Nenhuma conta bancária "
                "foi extraída"
            )

        # Validacao 12.5: conta sem agencia/numero nao serve para consultar o
        # extrato. Continua sendo gravada (e dado da origem), mas contada.
        inutilizaveis = sum(
            1
            for conta in contas
            if not conta.get("numero_agencia_plano_acao_dado_bancario")
            or not conta.get("numero_conta_plano_acao_dado_bancario")
        )
        if inutilizaveis:
            logging.warning(
                "[api_plano_acao_dado_bancario_dag.py] %d contas sem agência ou "
                "número utilizáveis para consulta no BB Ágil",
                inutilizaveis,
            )

        return contas

    @task
    def load_dados_bancarios_to_postgres(contas: list[dict[str, Any]]) -> None:
        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            contas,
            # Identificador do dado bancario na origem (secao 9.1).
            table_name=schemas.TABELA_PLANO_ACAO_DADO_BANCARIO,
            primary_key=["id_plano_acao_dado_bancario"],
            conflict_fields=["id_plano_acao_dado_bancario"],
            schema=schemas.SCHEMA_TRANSFEREGOV,
        )

        logging.info(
            "[api_plano_acao_dado_bancario_dag.py] Carga concluída com %s registros",
            len(contas),
        )

    load_dados_bancarios_to_postgres(fetch_dados_bancarios())


api_plano_acao_dado_bancario_dag()
