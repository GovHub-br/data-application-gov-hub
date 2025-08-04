import os
import logging
import requests
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
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["siape", "pensoes_instituidas"],
)
def siape_pensoes_instituidas_dag() -> None:
    """
    DAG que consome o endpoint consultaPensoesInstituidas da API SIAPE
    e armazena dados de pensões instituídas no schema 'siape'.
    """

    @task
    def fetch_and_store_pensoes_instituidas() -> None:
        logging.info("Iniciando extração de dados de pensões instituídas por CPF")
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
                    "consultaPensoesInstituidas.xml.j2", context
                )
                dados = ClienteSiape.parse_pensoes_instituidas(resposta_xml)

                if not dados:
                    logging.warning(f"Nenhum dado de pensão instituída para CPF {cpf}")
                    continue

                # Adiciona CPF a cada registro e gera ID único
                for i, registro in enumerate(dados):
                    registro["cpf"] = cpf
                    # Cria ID único baseado em CPF + índice + campos únicos
                    base_id = f"{cpf}_{i}"
                    cpf_pensionista = registro.get("cpfPensionista", "")
                    matricula_pensionista = registro.get("matriculaPensionista", "")
                    identificador = f"{base_id}_{cpf_pensionista}_{matricula_pensionista}"
                    registro["id_registro"] = identificador

                if dados:
                    # Usa o primeiro registro para criar/ajustar a estrutura da tabela
                    db.alter_table(
                        data=dados[0],
                        table_name="pensoes_instituidas",
                        schema="siape",
                    )

                    db.insert_data(
                        dados,
                        table_name="pensoes_instituidas",
                        conflict_fields=["id_registro"],
                        primary_key=["id_registro"],
                        schema="siape",
                    )

                    logging.info(
                        f"Inseridos {len(dados)} registros de pensões para CPF {cpf}"
                    )

            except requests.exceptions.HTTPError as e:
                if "500" in str(e):
                    logging.warning(f"Servidor retornou erro 500 para CPF {cpf}")
                else:
                    logging.error(f"Erro HTTP ao processar CPF {cpf}: {e}")
                continue
            except Exception as e:
                logging.error(f"Erro ao processar CPF {cpf}: {e}")
                continue

    fetch_and_store_pensoes_instituidas()


dag_instance = siape_pensoes_instituidas_dag()
