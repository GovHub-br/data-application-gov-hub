import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from postgres_helpers import get_postgres_conn
from cliente_siape import ClienteSiape
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joyce",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["debug", "siape", "dados_pessoais"],
)
def siape_dados_pessoais_debug() -> None:

    @task
    def testar_insercao_siape() -> None:
        cliente_siape = ClienteSiape()
        db = ClientPostgresDB(get_postgres_conn())

        cpf = "13086455705"
        context = {
            "siglaSistema": "PETRVS-IPEA",
            "nomeSistema": "PDG-PETRVS-IPEA",
            "senha": os.getenv("SIAPE_PASSWORD_USER"),
            "cpf": cpf,
            "codOrgao": "45206",
            "parmExistPag": "b",
            "parmTipoVinculo": "c",
        }

        xml = cliente_siape.call("consultaDadosPessoais.xml.j2", context)
        dados = ClienteSiape.parse_xml_to_dict(xml)

        if not dados:
            raise ValueError(f"Nenhum dado retornado para CPF {cpf}")

        dados["cpf"] = cpf

        print(f"🔍 Dados a serem inseridos: {dados}")

        # db.alter_table(data=dados, table_name="dados_pessoais", schema="siape")

        db.insert_data(
            [dados],
            table_name="dados_pessoais",
            conflict_fields=["cpf"],
            primary_key=["cpf"],
            schema="siape",
        )

    testar_insercao_siape()


dag_instance = siape_dados_pessoais_debug()
