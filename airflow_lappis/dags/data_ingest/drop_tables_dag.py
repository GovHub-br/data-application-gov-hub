import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB


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
)
def drop_siape_tabelas_dag() -> None:
    """
    DAG para remover tabelas antigas do schema siape.
    Útil para limpar estrutura antes de reingestão.
    """

    @task
    def drop_tabelas() -> None:
        logging.info("Iniciando remoção de tabelas antigas do schema siape")
        conn_str = get_postgres_conn()
        db = ClientPostgresDB(conn_str)

        tabelas = ["dados_funcionais", "dados_funcionais_siape"]

        for tabela in tabelas:
            try:
                db.drop_table_if_exists(tabela, schema="siape")
                logging.info(f"Tabela {tabela} removida com sucesso.")
            except Exception as e:
                logging.error(f"Erro ao remover tabela {tabela}: {e}")

    drop_tabelas()


dag_instance = drop_siape_tabelas_dag()
