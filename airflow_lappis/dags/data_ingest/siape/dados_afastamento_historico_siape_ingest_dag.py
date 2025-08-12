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
    tags=["siape", "afastamento_historico"],
)
def siape_afastamento_historico_dag() -> None:
    """
    DAG que consome o endpoint consultaDadosAfastamentoHistorico da API SIAPE
    e armazena os dados de afastamentos antigos dos servidores no schema 'siape'.
    """

    @task
    def fetch_and_store_afastamento_historico() -> None:
        logging.info("Iniciando extração de dados de afastamento histórico por CPF")
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
                    "anoInicial": "2024",
                    "mesInicial": "01",
                    "anoFinal": "2025",
                    "mesFinal": "12",
                }

                resposta_xml = cliente_siape.call(
                    "consultaDadosAfastamentoHistorico.xml.j2", context
                )

                dados = ClienteSiape.parse_afastamento_historico(resposta_xml)

                if not dados:
                    logging.info(f"Nenhum dado de afastamento histórico para CPF {cpf}")
                    continue

                for row in dados:
                    row["cpf"] = cpf
                    row["dt_ingest"] = datetime.now().isoformat()

                if dados:
                    db.alter_table(
                        data=dados[0],
                        table_name="afastamento_historico",
                        schema="siape",
                    )

                    db.insert_data(
                        dados,
                        table_name="afastamento_historico",
                        conflict_fields=None,
                        primary_key=None,
                        schema="siape",
                    )

                    logging.info(f"{len(dados)} registros inseridos para CPF {cpf}")

            except Exception as e:
                logging.error(f"Erro ao processar CPF {cpf}: {e}")
                continue

    fetch_and_store_afastamento_historico()


dag_instance = siape_afastamento_historico_dag()
