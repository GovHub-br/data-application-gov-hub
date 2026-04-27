import logging
from airflow.decorators import dag, task
from datetime import datetime
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
    },
    tags=["transfere_gov_api", "relatorio_gestao_especial", "MIR"],
)
def api_relatorio_gestao_especial_dag() -> None:

    @task
    def fetch_and_store_relatorios() -> None:
        logging.info("[relatorio_gestao] Iniciando extração massiva global...")

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        relatorios_data = api.get_all_relatorio_gestao_especial(page_size=1000)

        if not relatorios_data:
            logging.warning("[relatorio_gestao] Nenhum relatório retornado da API.")
            return

        timestamp_atual = datetime.now().isoformat()

        for row in relatorios_data:
            row["dt_ingest"] = timestamp_atual

        logging.info(
            f"[relatorio_gestao] Inserindo {len(relatorios_data)} registros no Postgres."
        )

        db.insert_data(
            relatorios_data,
            table_name="relatorio_gestao_especial",
            conflict_fields=["id_relatorio_gestao"],
            primary_key=["id_relatorio_gestao"],
            schema="transfere_gov_emendas",
        )

        logging.info("[relatorio_gestao] Ingestão concluída com sucesso.")

    fetch_and_store_relatorios()


api_relatorio_gestao_especial_dag()
