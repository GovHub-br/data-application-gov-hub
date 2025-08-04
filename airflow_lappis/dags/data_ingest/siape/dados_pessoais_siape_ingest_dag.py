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
    tags=["siape", "dados_pessoais"],
)
def siape_dados_pessoais_dag() -> None:
    """
    DAG que consome o endpoint consultaDadosPessoais da API SIAPE
    e armazena os dados pessoais de servidores públicos no schema 'siape'.
    """

    @task
    def fetch_and_store_dados_pessoais() -> None:
        logging.info("Iniciando extração de dados pessoais por CPF")
        cliente_siape = ClienteSiape()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # --- Garantia de schema, tabela e PRIMARY KEY ---
        logging.info("Verificando existência da tabela e constraint PRIMARY KEY")
        ddl = """
        DO $$
        BEGIN
            -- Cria schema se não existir
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.schemata WHERE schema_name = 'siape'
            ) THEN
                EXECUTE 'CREATE SCHEMA siape';
            END IF;

            -- Cria tabela se não existir
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'siape' AND table_name = 'dados_pessoais'
            ) THEN
                EXECUTE '
                    CREATE TABLE siape.dados_pessoais (
                        cpf TEXT PRIMARY KEY -- Define cpf como PK já na criação
                    )
                ';
            END IF;

            -- Adiciona PK se a tabela existe mas ainda não tem
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_schema = 'siape'
                AND table_name = 'dados_pessoais'
                AND constraint_type = 'PRIMARY KEY'
            ) THEN
                EXECUTE (
                    'ALTER TABLE siape.dados_pessoais ADD CONSTRAINT dados_pessoais_pkey '
                    'PRIMARY KEY (cpf)'
                );
            END IF;
        END
        $$;
        """
        db.execute_non_query(ddl)
        logging.info("Estrutura da tabela verificada/criada com sucesso.")

        # --- Continua fluxo normal ---
        query = "SELECT DISTINCT cpf FROM siape.lista_servidores WHERE cpf IS NOT NULL"
        cpfs = [row[0] for row in db.execute_query(query)]
        logging.info(f"Total de CPFs encontrados: {len(cpfs)}")

        for cpf in cpfs:
            try:
                logging.info(f"Processando CPF: {cpf}")

                context = {
                    "siglaSistema": "PETRVS-IPEA",
                    "nomeSistema": "PDG-PETRVS-IPEA",
                    "senha": os.getenv("SIAPE_PASSWORD_USER"),
                    "cpf": cpf,
                    "codOrgao": "45206",
                    "parmExistPag": "b",
                    "parmTipoVinculo": "c",
                }

                resposta_xml = cliente_siape.call("consultaDadosPessoais.xml.j2", context)
                logging.debug(f"XML bruto para CPF {cpf}:\n{resposta_xml}")

                dados = ClienteSiape.parse_xml_to_dict(resposta_xml)
                logging.debug(f"Dados parseados para CPF {cpf}: {dados}")

                if not dados:
                    logging.warning(f"Nenhum dado pessoal encontrado para CPF {cpf}")
                    continue

                dados["cpf"] = cpf
                logging.info(f"Dados finais prontos para inserção: {dados}")

                db.alter_table(
                    data=dados,
                    table_name="dados_pessoais",
                    schema="siape",
                )

                db.insert_data(
                    [dados],
                    table_name="dados_pessoais",
                    conflict_fields=["cpf"],
                    primary_key=["cpf"],
                    schema="siape",
                )

                logging.info(f"Dado pessoal inserido com sucesso para CPF {cpf}")

            except Exception as e:
                logging.error(f"Erro ao processar CPF {cpf}: {e}", exc_info=True)
                continue

    fetch_and_store_dados_pessoais()


dag_instance = siape_dados_pessoais_dag()
