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
    dag_id="api_plano_acao_meta_dag",
    schedule=get_dynamic_schedule("api_plano_acao_meta_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "metas", "raw"],
)
def api_plano_acao_meta_dag() -> None:
    """Passo 3A da secao 6 da especificacao: metas de cada plano de acao.

    Disparada por ``api_planos_acao_dag`` (as metas so podem ser buscadas
    depois que os planos existem, porque o endpoint so filtra por
    ``id_plano_acao``). Grava em
    ``transferegov.plano_acao_meta_minc`` com ``id_programa`` e ``cod_ibge``
    propagados da tabela-pai.
    """

    @task
    def fetch_metas() -> list[dict[str, Any]]:
        db = ClientPostgresDB(get_postgres_conn())
        planos = carregar_planos_acao(db)

        if not planos:
            raise ValueError(
                "[api_plano_acao_meta_dag.py] Nenhum plano de ação encontrado em "
                f"{schemas.SCHEMA_TRANSFEREGOV}.{schemas.TABELA_PLANO_ACAO}"
            )

        logging.info(
            "[api_plano_acao_meta_dag.py] Buscando metas de %d planos de ação",
            len(planos),
        )

        api = ClienteTransfereGov()
        metas = extrair_por_plano_acao(
            planos,
            buscar=api.get_metas_by_plano_acao,
            rotulo="metas",
        )

        if not metas:
            raise ValueError("[api_plano_acao_meta_dag.py] Nenhuma meta foi extraída")

        return metas

    @task
    def load_metas_to_postgres(metas: list[dict[str, Any]]) -> None:
        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            metas,
            table_name=schemas.TABELA_PLANO_ACAO_META,
            # Identificador da meta na origem (secao 9.1). O nome do campo e
            # id_meta_plano_acao, nao id_meta como sugere o diagrama ER.
            primary_key=["id_meta_plano_acao"],
            conflict_fields=["id_meta_plano_acao"],
            schema=schemas.SCHEMA_TRANSFEREGOV,
        )

        logging.info(
            "[api_plano_acao_meta_dag.py] Carga concluída com %s registros",
            len(metas),
        )

    load_metas_to_postgres(fetch_metas())


api_plano_acao_meta_dag()
