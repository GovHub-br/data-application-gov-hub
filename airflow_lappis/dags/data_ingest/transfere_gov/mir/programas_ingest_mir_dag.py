import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_ted import ClienteTed
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("api_programas_ted_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ted_api", "programas", "MIR"],
)
def api_programas_ted_dag() -> None:

    @task
    def fetch_and_ingest_programas() -> None:
        logging.info("Iniciando extração de programas")

        api = ClienteTed()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        sigla_alvo = Variable.get("airflow_orgao_ted", default_var="MIR")

        logging.info(f"Filtrando programas pela sigla: {sigla_alvo}")
        programas_data = api.get_programas_by_sigla_unidade_descentralizadora(sigla_alvo)

        if not programas_data:
            logging.warning(f"Nenhum dado retornado para a sigla {sigla_alvo}")
            return

        for programa in programas_data:
            programa["dt_ingest"] = datetime.now().isoformat()

        db.insert_data(
            programas_data,
            "programas",
            primary_key=["id_programa"],
            conflict_fields=["id_programa"],
            schema="transfere_gov",
        )

        logging.info(
            f"Sucesso: {len(programas_data)} programas inseridos/atualizados "
            f"para {sigla_alvo}"
        )

    fetch_and_ingest_programas()


api_programas_ted_dag()
