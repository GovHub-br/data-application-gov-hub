import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Param
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from typing import Dict


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "admin",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["siape", "admin", "drop"],
    params={
        "tabela": Param("", description="Nome da tabela a ser dropada"),
        "schema": Param("", description="Schema onde está a tabela"),
    },
)
def drop_tabela_parametrizada_dag() -> None:
    """
    DAG para remover uma tabela específica de um schema no Postgres.
    Tabela e schema devem ser informados via parâmetros 'tabela' e 'schema'.
    """

    @task
    def drop_tabela(task_params: Dict[str, str]) -> None:
        tabela = task_params.get("tabela")
        schema = task_params.get("schema")

        if not tabela:
            raise ValueError("Parâmetro 'tabela' não informado.")
        if not schema:
            raise ValueError("Parâmetro 'schema' não informado.")

        logging.info(f"Iniciando remoção da tabela {schema}.{tabela}")
        conn_str = get_postgres_conn()
        db = ClientPostgresDB(conn_str)

        try:
            db.drop_table_if_exists(tabela, schema=schema)
            logging.info(f"Tabela {schema}.{tabela} removida com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao remover tabela {schema}.{tabela}: {e}")
            raise

    drop_tabela({"tabela": "{{ params.tabela }}", "schema": "{{ params.schema }}"})


dag_instance = drop_tabela_parametrizada_dag()
