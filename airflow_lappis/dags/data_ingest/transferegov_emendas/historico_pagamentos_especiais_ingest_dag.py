import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("historico_pagamentos_especiais_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "historico_pagamentos_especiais", "MIR"],
)
def api_historico_pagamentos_especiais_dag() -> None:
    """DAG para buscar e armazenar histórico de pagamentos especiais do Transfere Gov."""

    @task
    def fetch_and_store_historico_pagamentos_especiais() -> None:
        logging.info(
            "[historico_pagamentos_especiais_ingest_dag.py] Iniciando extração histórico "
            "de pagamentos especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn('postgres_mir')
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todos os documentos hábeis especiais com paginação automática
        historico_data = api.get_all_historico_pagamentos_especiais(page_size=1000)

        if historico_data and len(historico_data) > 0:
            # Adicionar dt_ingest a cada documento
            for documento in historico_data:
                documento["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[historico_pagamentos_especiais_ingest_dag.py] Inserindo "
                f"{len(historico_data)} registros de histórico de pagamentos especiais no "
                f"schema transfere_gov"
            )
            db.insert_data(
                historico_data,
                "historico_pagamentos_especiais",
                conflict_fields=["id_historico_op_ob"],
                primary_key=["id_historico_op_ob"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[historico_pagamentos_especiais_ingest_dag.py] Concluído. "
                f"Total de {len(historico_data)} registros de histórico de pagamentos"
                " especiais "
                f"inseridos/atualizados"
            )
        else:
            logging.warning(
                "[historico_pagamentos_especiais_ingest_dag.py] Nenhum registro de "
                "histórico de pagamento especial encontrado"
            )

    fetch_and_store_historico_pagamentos_especiais()


api_historico_pagamentos_especiais_dag()
