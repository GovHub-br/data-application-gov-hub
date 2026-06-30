import logging
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_contratos import ClienteContratos
from cliente_postgres import ClientPostgresDB


@dag(
    dag_id="contratos_inativos_mir_ingest_dag",
    schedule_interval=get_dynamic_schedule("contratos_inativos_mir_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["contratos_inativos_api", "compras_gov", "MIR"],
)
def api_contratos_inativos_dag() -> None:
    """DAG para buscar e armazenar contratos inativos de uma API no PostgreSQL."""

    @task
    def fetch_and_store_contratos_inativos() -> None:
        logging.info("Iniciando fetch_and_store_contratos_mir_inativos")

        orgao_alvo = Variable.get("airflow_orgao_1", default_var=None)
        if not orgao_alvo:
            logging.error("Variável airflow_orgao_1 não definida!")
            raise ValueError("airflow_orgao_1 não definida")

        orgaos_config_str = Variable.get("airflow_variables", default_var="{}")
        orgaos_config = yaml.safe_load(orgaos_config_str)

        ug_codes = orgaos_config.get(orgao_alvo, {}).get("codigos_ug", [])

        if not ug_codes:
            logging.warning(f"Nenhum código UG encontrado para o órgão '{orgao_alvo}'")
            return

        api = ClienteContratos()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        for ug_code in ug_codes:
            logging.info(f"Buscando contratos inativos para UG: {ug_code}")
            contratos = api.get_contratos_inativos_by_ug(ug_code)
            if contratos:
                # Adicionar dt_ingest a cada contrato
                for contrato in contratos:
                    contrato["dt_ingest"] = datetime.now().isoformat()

                logging.info(
                    f"Inserindo contratos inativos da UG {ug_code} no schema compras_gov"
                )
                db.insert_data(
                    contratos,
                    "contratos",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="compras_gov",
                )
            else:
                logging.warning(f"Nenhum contrato inativo encontrado para UG {ug_code}")

    fetch_and_store_contratos_inativos()


dag_instance = api_contratos_inativos_dag()