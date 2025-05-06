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
    tags=["siape", "lista_servidores"],
)
def siape_lista_servidores_dag() -> None:

    @task
    def fetch_and_store_lista_servidores() -> None:
        logging.info("Iniciando extração de servidores por UORG")
        cliente_siape = ClienteSiape()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        query = "SELECT codigo FROM siape.lista_uorgs"
        codigos_uorg = [row[0] for row in db.execute_query(query)]
        logging.info(f"Total de UORGs encontradas: {len(codigos_uorg)}")

        ns = {
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns1": "http://servico.wssiapenet",
            "ns2": "http://entidade.wssiapenet",
        }

        for cod in codigos_uorg:
            try:
                context = {
                    "siglaSistema": "PETRVS-IPEA",
                    "nomeSistema": "PDG-PETRVS-IPEA",
                    "senha": os.getenv("SIAPE_PASSWORD_USER"),
                    "cpf": os.getenv("SIAPE_CPF_USER"),
                    "codOrgao": "45206",
                    "codUorg": cod,
                }

                resposta_xml = cliente_siape.call("listaServidores.xml.j2", context)
                dados = ClienteSiape.parse_xml_to_list(
                    xml_string=resposta_xml, element_tag="ns2:Servidor", namespaces=ns
                )

                if not dados:
                    logging.info(f"Nenhum servidor encontrado para UORG {cod}")
                    continue

                for row in dados:
                    row["codUorg"] = str(cod)

                db.insert_data(
                    dados,
                    table_name="lista_servidores",
                    conflict_fields=["cpf", "codUorg"],
                    primary_key=["cpf", "codUorg"],
                    schema="siape",
                )

                logging.info(f"{len(dados)} servidores inseridos para UORG {cod}")

            except Exception as e:
                logging.error(f"Erro ao processar UORG {cod}: {e}")
                continue

    fetch_and_store_lista_servidores()


dag_instance = siape_lista_servidores_dag()
