import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("metas_especiais_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "metas_especiais"],
)
def api_metas_especiais_dag() -> None:
    """DAG para buscar e armazenar metas especiais do Transfere Gov."""

    @task
    def fetch_and_store_metas_especiais() -> None:
        logging.info(
            "[metas_especiais_ingest_dag.py] Iniciando extração de metas especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        metas_data = api.get_all_metas_especiais()

        if not metas_data:
            logging.warning(
                "[metas_especiais_ingest_dag.py] Nenhuma meta especial encontrada"
            )
            return

        for meta in metas_data:
            meta["dt_ingest"] = datetime.now().isoformat()

        db.insert_data(
            metas_data,
            "metas_especiais",
            conflict_fields=["id_meta"],
            primary_key=["id_meta"],
            schema="transferegov_emendas",
        )

        logging.info(
            f"[metas_especiais_ingest_dag.py] Concluído. "
            f"Total: {len(metas_data)} metas especiais inseridas/atualizadas"
        )

    fetch_and_store_metas_especiais()


api_metas_especiais_dag()
