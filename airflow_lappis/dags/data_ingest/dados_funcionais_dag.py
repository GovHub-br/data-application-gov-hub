import os
import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_siape import ClienteSiape
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joyce",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["siape", "dados_funcionais"],
)
def siape_dados_funcionais_dag() -> None:
    """
    DAG que extrai dados funcionais do SIAPE via API SOAP
    e insere no schema 'siape', tabela 'dados_funcionais'.
    """

    @task
    def fetch_and_store_dados_funcionais() -> None:
        logging.info(
            "[siape_dados_funcionais_dag] Iniciando extração dos dados funcionais"
        )

        cliente_siape = ClienteSiape()

        context = {
            "siglaSistema": "PETRVS-IPEA",
            "nomeSistema": "PDG-PETRVS-IPEA",
            "senha": os.getenv("SIAPE_PASSWORD_USER"),
            "cpf": os.getenv("SIAPE_CPF_USER"),
            "codOrgao": "45206",
            "parmExistPag": "b",
            "parmTipoVinculo": "c",
        }

        resposta_xml = cliente_siape.call("consultaDadosFuncionais.xml.j2", context)
        dados_dict = ClienteSiape.parse_xml_to_dict(resposta_xml)

        if not dados_dict:
            logging.warning(
                "[siape_dados_funcionais_dag] Nenhum dado retornado da API SIAPE"
            )
            return

        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        logging.info("[siape_dados_funcionais_dag] Inserindo dados no banco de dados")

        db.insert_data(
            [dados_dict],
            table_name="dados_funcionais",
            conflict_fields=None,
            primary_key=None,
            schema="siape",
        )

        logging.info("[siape_dados_funcionais_dag] Dados inseridos com sucesso")

    fetch_and_store_dados_funcionais()


dag_instance = siape_dados_funcionais_dag()
