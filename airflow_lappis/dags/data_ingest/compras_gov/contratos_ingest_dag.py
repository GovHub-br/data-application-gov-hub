import logging
import yaml
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_contratos import ClienteContratos
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["contratos_api", "compras_gov"],
)
def api_contratos_dag() -> None:
    """DAG para buscar e armazenar contratos por órgão definido."""

    @task
    def fetch_and_store_contratos() -> None:
        logging.info("[contratos_ingest_dag.py] Iniciando extração")

        orgao_alvo = Variable.get("airflow_orgao", default_var=None)
        if not orgao_alvo:
            logging.error("Variável airflow_orgao não definida!")
            raise ValueError("airflow_orgao não definida")

        orgaos_config_str = Variable.get("airflow_variables", default_var="{}")
        orgaos_config = yaml.safe_load(orgaos_config_str)

        codigos_ug = orgaos_config.get(orgao_alvo, {}).get("codigos_ug", [])

        if not codigos_ug:
            logging.warning(f"Nenhum código UG encontrado para o órgão '{orgao_alvo}'")
            return

        api = ClienteContratos()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        for ug_code in codigos_ug:
            logging.info(f"Buscando contratos para UG: {ug_code}")
            contratos = api.get_contratos_by_ug(ug_code)

            if contratos:
                # Adicionar dt_ingest a cada contrato
                for contrato in contratos:
                    contrato["dt_ingest"] = datetime.now().isoformat()

                logging.info(f"Inserindo contratos da UG {ug_code} no schema compras_gov")
                db.insert_data(
                    contratos,
                    "contratos",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="compras_gov",
                )
            else:
                logging.warning(f"Nenhum contrato encontrado para UG {ug_code}")

    trigger_contratos_inativos = TriggerDagRunOperator(
        task_id="trigger_contratos_inativos",
        trigger_dag_id="api_contratos_inativos_dag",
        wait_for_completion=False,
    )

    fetch_and_store_contratos() >> trigger_contratos_inativos


dag_instance = api_contratos_dag()
