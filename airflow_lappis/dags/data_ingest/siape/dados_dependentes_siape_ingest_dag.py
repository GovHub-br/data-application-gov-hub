import os
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_siape import ClienteSiape
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("dados_dependentes_siape_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joyce",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["siape", "dados_dependentes"],
)
def siape_dados_dependentes_dag() -> None:
    """
    DAG que consome o endpoint consultaDadosDependentes da API SIAPE
    e armazena os dados de dependentes dos servidores no schema 'siape'.
    """

    @task
    def fetch_and_store_dados_dependentes() -> None:
        logging.info("Iniciando extração de dados de dependentes por CPF")
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
                    "consultaDadosDependentes.xml.j2", context
                )
                dados = ClienteSiape.parse_dependentes(resposta_xml)

                if not dados:
                    logging.warning(f"Nenhum dependente encontrado para CPF {cpf}")
                    continue

                for row in dados:
                    # A tag cpf do XML é o CPF do dependente. Ela é guardada em
                    # cpf_dependente antes de a coluna cpf receber o CPF do servidor,
                    # que faz parte da chave da tabela.
                    row["cpf_dependente"] = row.get("cpf")
                    row["cpf"] = cpf
                    # Um dependente pode ter vários benefícios (IR, plano de saúde,
                    # pré-escolar), e o mesmo benefício em períodos diferentes. Cada
                    # par benefício + início é um registro. Vazio em vez de nulo para
                    # o ON CONFLICT casar também os dependentes sem benefício.
                    row["codBeneficio"] = row.get("codBeneficio") or ""
                    row["dataInicio"] = row.get("dataInicio") or ""
                    row["dt_ingest"] = datetime.now().isoformat()

                db.alter_table(
                    data=dados[0],
                    table_name="dados_dependentes",
                    schema="siape",
                )

                db.insert_data(
                    dados,
                    table_name="dados_dependentes",
                    conflict_fields=["cpf", "nome", "codBeneficio", "dataInicio"],
                    primary_key=["cpf", "nome", "codBeneficio", "dataInicio"],
                    schema="siape",
                )

                logging.info(f"{len(dados)} dependente(s) inserido(s) para CPF {cpf}")

            except Exception as e:
                logging.error(f"Erro ao processar CPF {cpf}: {e}")
                continue

    fetch_and_store_dados_dependentes()


dag_instance = siape_dados_dependentes_dag()
