import logging
from schedule_loader import get_dynamic_schedule
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("relatorio_gestao_novo_especial_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "relatorio_gestao_novo_especial", "MIR"],
)
def api_relatorio_gestao_novo_especial_dag() -> None:
    """DAG para buscar e armazenar relatórios de gestão novo especial do Transfere Gov"""

    @task
    def fetch_and_store_relatorios_gestao_novo_especial() -> None:
        logging.info(
            "[relatorio_gestao_novo_especial_dag.py] Iniciando extração relatórios de"
            " gestão novo especial"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todos os relatórios de gestão novo especial com paginação automática
        relatorios_data = api.get_all_relatorios_gestao_novo_especial(page_size=1000)

        if relatorios_data and len(relatorios_data) > 0:
            # Adicionar dt_ingest a cada relatório
            for relatorio in relatorios_data:
                relatorio["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[relatorio_gestao_novo_especial_dag.py] Inserindo "
                f"{len(relatorios_data)} relatórios de gestão no "
                f"schema transfere_gov"
            )
            db.insert_data(
                relatorios_data,
                "relatorios_gestao_novo_especial",
                conflict_fields=["id_relatorio_gestao_novo"],
                primary_key=["id_relatorio_gestao_novo"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[relatorio_gestao_novo_especial_dag.py] Concluído. "
                f"Total de {len(relatorios_data)} relatórios de gestão novo especial "
                f"inseridos/atualizados"
            )
        else:
            logging.info(
                "[relatorio_gestao_novo_especial_dag.py] Nenhum relatório de gestão novo "
                "especial encontrado"
            )

    fetch_and_store_relatorios_gestao_novo_especial()


api_relatorio_gestao_novo_especial_dag()
