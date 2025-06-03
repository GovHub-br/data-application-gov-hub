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
    tags=["siape", "lista_uorgs"],
)
def siape_lista_uorgs_dag() -> None:
    """
    DAG que extrai a lista de UORGs do SIAPE via API SOAP
    e insere no schema 'siape', tabela 'lista_uorgs'.
    """

    @task
    def fetch_and_store_lista_uorgs() -> None:
        logging.info("[siape_lista_uorgs_dag] Iniciando extração da lista de UORGs")

        cliente_siape = ClienteSiape()

        context = {
            "siglaSistema": "PETRVS-IPEA",
            "nomeSistema": "PDG-PETRVS-IPEA",
            "senha": os.getenv("SIAPE_PASSWORD_USER"),
            "cpf": os.getenv("SIAPE_CPF_USER"),
            "codOrgao": "45206",
        }

        resposta_xml = cliente_siape.call("listaUorgs.xml.j2", context)

        # Define os namespaces e o elemento que queremos extrair
        ns = {
            "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns1": "http://servico.wssiapenet",
            "ns2": "http://entidade.wssiapenet",
        }

        dados_lista = ClienteSiape.parse_xml_to_list(
            xml_string=resposta_xml, element_tag="ns2:Uorg", namespaces=ns
        )

        if not dados_lista:
            logging.warning("Nenhum dado retornado da API listaUorgs")
            return

        for item in dados_lista:
            if "dataultimatransacao" in item:
                item["dt_ultima_transacao"] = item.pop("dataultimatransacao")

        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        logging.info("Inserindo dados no banco de dados")

        db.insert_data(
            dados_lista,
            table_name="lista_uorgs",
            conflict_fields=["codigo"],
            primary_key=["codigo"],
            schema="siape",
        )

        logging.info("Dados inseridos com sucesso")

    fetch_and_store_lista_uorgs()


dag_instance = siape_lista_uorgs_dag()
