import os
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
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
    tags=["siape", "aposentadoria"],
)
def siape_lista_info_aposentadoria_dag() -> None:
    """
    DAG que consome o endpoint listaInformacoesAposentadoria da API SIAPE
    e armazena os dados no schema 'siape'.
    """

    @task
    def fetch_and_store_aposentadoria_info() -> None:
        logging.info("Iniciando extração de informações de aposentadoria por CPF")
        cliente_siape = ClienteSiape()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        query = "SELECT DISTINCT cpf FROM siape.lista_servidores WHERE cpf IS NOT NULL"
        cpfs = [row[0] for row in db.execute_query(query)]
        logging.info(f"Total de CPFs encontrados: {len(cpfs)}")

        for cpf in cpfs:
            try:
                context = {
                    "siglaSistema": "PETRVS-IPEA",
                    "nomeSistema": "PDG-PETRVS-IPEA",
                    "senha": os.getenv("SIAPE_PASSWORD_USER"),
                    "cpf": cpf,
                    "codOrgao": "45206",
                    "parmExistPag": "b",
                    "parmTipoVinculo": "c",
                }

                resposta_xml = cliente_siape.call(
                    "listaInformacoesAposentadoria.xml.j2", context
                )
                dados = ClienteSiape.parse_xml_to_dict(resposta_xml)

                if not dados:
                    logging.warning(f"Nenhum dado encontrado para CPF {cpf}")
                    continue

                db.alter_table(
                    data=dados,
                    table_name="info_aposentadoria",
                    schema="siape",
                )
                db.insert_data(
                    [dados],
                    table_name="info_aposentadoria",
                    conflict_fields=["cpf"],
                    primary_key=["cpf"],
                    schema="siape",
                )

                logging.info(f"Dado de aposentadoria inserido para CPF {cpf}")

            except Exception as e:
                logging.error(f"Erro ao processar CPF {cpf}: {e}")
                continue

    fetch_and_store_aposentadoria_info()


dag_instance = siape_lista_info_aposentadoria_dag()
