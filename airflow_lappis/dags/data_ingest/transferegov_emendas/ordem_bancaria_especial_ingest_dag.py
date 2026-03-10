import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("ordem_bancaria_especial_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "ordem_bancaria_especial", "MIR"],
)
def api_ordem_bancaria_especial_dag() -> None:
    """DAG para buscar e armazenar ordens bancárias especiais do Transfere Gov."""

    @task
    def fetch_and_store_ordem_bancaria_especial() -> None:
        logging.info(
            "[ordem_bancaria_especial_ingest_dag.py] Iniciando extração ordens bancárias especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todas as ordens bancárias especiais com paginação automática
        ordem_data = api.get_all_ordens_bancarias_especiais(page_size=1000)

        if ordem_data and len(ordem_data) > 0:
            # Adicionar dt_ingest a cada documento
            for documento in ordem_data:
                documento["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[ordem_bancaria_especial_ingest_dag.py] Inserindo "
                f"{len(ordem_data)} ordens bancárias especiais no "
                f"schema transfere_gov"
            )
            db.insert_data(
                ordem_data,
                "ordens_bancarias_especiais",
                conflict_fields=["id_op_ob"],
                primary_key=["id_op_ob"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[ordem_bancaria_especial_ingest_dag.py] Concluído. "
                f"Total de {len(ordem_data)} ordens bancárias especiais "
                f"inseridas/atualizadas"
            )
        else:
            logging.warning(
                "[ordem_bancaria_especial_ingest_dag.py] Nenhuma ordem bancária especial "
                "encontrada"
            )

    fetch_and_store_ordem_bancaria_especial()


api_ordem_bancaria_especial_dag()
