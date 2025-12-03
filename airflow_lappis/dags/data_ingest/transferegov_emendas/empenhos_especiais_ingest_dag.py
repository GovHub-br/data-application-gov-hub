import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("empenhos_especiais_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Leonardo",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "empenhos_especiais"],
)
def api_empenhos_especiais_dag() -> None:
    """DAG para buscar e armazenar empenhos especiais do Transfere Gov."""

    @task
    def fetch_and_store_empenhos_especiais() -> None:
        logging.info(
            "[empenhos_especiais_ingest_dag.py] Iniciando extração de "
            "empenhos especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Buscar IDs dos planos de ação especiais
        query = (
            "SELECT DISTINCT id_plano_acao "
            "FROM transferegov_emendas.planos_acao_especiais"
        )
        planos_acao_ids = db.execute_query(query)

        if not planos_acao_ids:
            logging.warning(
                "[empenhos_especiais_ingest_dag.py] Nenhum plano de ação encontrado"
            )
            return

        total_empenhos = 0
        for (id_plano_acao,) in planos_acao_ids:
            logging.info(
                f"[empenhos_especiais_ingest_dag.py] Buscando empenhos especiais "
                f"para plano de ação {id_plano_acao}"
            )

            empenhos_data = api.get_all_empenhos_especiais_by_plano_acao(id_plano_acao)

            if empenhos_data:
                for empenho in empenhos_data:
                    empenho["dt_ingest"] = datetime.now().isoformat()

                db.insert_data(
                    empenhos_data,
                    "empenhos_especiais",
                    conflict_fields=["id_empenho"],
                    primary_key=["id_empenho"],
                    schema="transferegov_emendas",
                )
                total_empenhos += len(empenhos_data)

        logging.info(
            f"[empenhos_especiais_ingest_dag.py] Concluído. "
            f"Total: {total_empenhos} empenhos especiais inseridos/atualizados"
        )

    fetch_and_store_empenhos_especiais()


dag_instance = api_empenhos_especiais_dag()
