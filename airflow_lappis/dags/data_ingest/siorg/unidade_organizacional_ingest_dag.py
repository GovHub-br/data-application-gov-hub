import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_siorg import ClienteSiorg
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["estrutura_organizacional", "siorg"],
)
def api_unidade_organizacional_dag() -> None:
    """DAG para buscar e armazenar dados da Estrutura Organizacional
    de uma API no PostgreSQL."""

    @task
    def fetch_estrutura_organizacional_resumida() -> None:
        logging.info(
            "[unidade_organizacional_ingest_dag.py] "
            "Starting fetch_estrutura_organizacional_resumida task"
        )
        api = ClienteSiorg()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        codigo_poder = "1"
        codigo_esfera = "1"
        codigo_unidade = "7"

        try:
            logging.info(
                "[unidade_organizacional_ingest_dag.py] "
                "Fetching estrutura organizacional resumida for "
                f"codigoUnidade: {codigo_unidade}"
            )
            estrutura_resumida = api.get_estrutura_organizacional_resumida(
                codigo_poder=codigo_poder,
                codigo_esfera=codigo_esfera,
                codigo_unidade=codigo_unidade,
            )
            if estrutura_resumida:
                # Adicionar dt_ingest a cada item
                for item in estrutura_resumida:
                    item["dt_ingest"] = datetime.now().isoformat()

                logging.info(
                    "[unidade_organizacional_ingest_dag.py] "
                    "Inserting estrutura organizacional resumida for "
                    f"codigoUnidade: {codigo_unidade} into PostgreSQL"
                )
                db.insert_data(
                    estrutura_resumida,
                    "unidade_organizacional",
                    conflict_fields=["codigoUnidade"],
                    primary_key=["codigoUnidade"],
                    schema="siorg",
                )
            else:
                logging.warning(
                    "[unidade_organizacional_ingest_dag.py] "
                    "No estrutura organizacional resumida found for "
                    f"codigoUnidade: {codigo_unidade}"
                )
        except Exception as e:
            logging.error(
                "[unidade_organizacional_ingest_dag.py] "
                "Error fetching estrutura organizacional resumida for "
                f"codigoUnidade {codigo_unidade}: {e}"
            )

    fetch_estrutura_organizacional_resumida()


dag_instance = api_unidade_organizacional_dag()
