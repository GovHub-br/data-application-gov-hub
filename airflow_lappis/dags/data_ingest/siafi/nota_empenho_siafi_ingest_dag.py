import logging
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from datetime import datetime, timedelta
from typing import Dict, Any
from cliente_siafi import ClienteSiafi
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 3, 17),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "ano_inicio": Param(
            default=None,
            type=["integer", "null"],
            title="Ano de Início",
            description="Backfill: Ano inicio para busca de notas de empenho. (type=int)",
        ),
        "ano_fim": Param(
            default=None,
            type=["integer", "null"],
            title="Ano de Fim",
            description="Backfill: Ano final para busca de notas de empenho. (type=int)",
        ),
    },
    tags=["nota_empenho", "siafi_api"],
)
def nota_empenho_siafi_ingest_dag() -> None:
    @task
    def fetch_and_store_notas_empenho(**context: Dict[str, Any]) -> None:
        logging.info("Iniciando fetch_and_store_notas_empenho")

        orgao_alvo = Variable.get("airflow_orgao", default_var=None)
        if not orgao_alvo:
            logging.error("Variável airflow_orgao não definida!")
            raise ValueError("airflow_orgao não definida")

        orgaos_config_str = Variable.get("airflow_variables", default_var="{}")
        orgaos_config = yaml.safe_load(orgaos_config_str)

        ugs_emitentes = orgaos_config.get(orgao_alvo, {}).get("codigos_ug", [])

        if not ugs_emitentes:
            logging.warning(f"Nenhum código UG encontrado para o órgão '{orgao_alvo}'")
            return

        cliente = ClienteSiafi()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        params = context["params"]
        ano_inicio = params.get("ano_inicio")
        ano_fim = params.get("ano_fim")

        ano_atual = datetime.now().year
        ano_inicio = ano_inicio or ano_atual
        ano_fim = ano_fim or ano_atual

        anos_consulta = list(range(ano_inicio, ano_fim + 1))

        for ug in ugs_emitentes:
            for ano in anos_consulta:
                num_empenho = 1
                while True:
                    num_empenho_str = str(num_empenho).zfill(6)
                    resultado = cliente.consultar_nota_empenho(ug, ano, num_empenho_str)
                    if not resultado:
                        break
                    resultado["dt_ingest"] = datetime.now().isoformat()
                    db.insert_data(
                        [resultado],
                        "notas_empenho",
                        conflict_fields=["numEmpenho", "anoEmpenho"],
                        primary_key=["numEmpenho", "anoEmpenho"],
                        schema="siafi",
                    )
                    num_empenho += 1

    fetch_and_store_notas_empenho()


dag_instance = nota_empenho_siafi_ingest_dag()
