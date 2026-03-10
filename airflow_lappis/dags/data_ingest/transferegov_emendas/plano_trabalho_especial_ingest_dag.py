import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("plano_trabalho_especial_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "plano_trabalho", "MIR"],
)
def api_plano_trabalho_especial_dag() -> None:
    """DAG para buscar e armazenar planos de trabalho especiais do Transfere Gov."""

    @task
    def fetch_and_store_plano_trabalho_especial() -> None:
        logging.info(
            "[plano_trabalho_especial_ingest_dag.py] Iniciando extração de "
            "planos de trabalho especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        plano_data = api.get_all_plano_trabalho_especial(page_size=1000)

        if plano_data and len(plano_data) > 0:
            for item in plano_data:
                item["dt_ingest"] = datetime.now().isoformat()

            logging.info(
                f"[plano_trabalho_especial_ingest_dag.py] Inserindo "
                f"{len(plano_data)} planos de trabalho no schema transferegov_emendas"
            )

            db.insert_data(
                plano_data,
                "plano_trabalho_especial",
                conflict_fields=["id_plano_trabalho"],
                primary_key=["id_plano_trabalho"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[plano_trabalho_especial_ingest_dag.py] Concluído. "
                f"Total de {len(plano_data)} registros processados."
            )
        else:
            logging.warning(
                "[plano_trabalho_especial_ingest_dag.py] Nenhum plano de trabalho "
                "encontrado"
            )

    fetch_and_store_plano_trabalho_especial()


api_plano_trabalho_especial_dag()
