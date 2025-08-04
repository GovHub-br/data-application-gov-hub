from airflow.decorators import dag, task
from datetime import datetime, timedelta
from cliente_siafi import ClienteSiafi
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 12),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nota_credito", "siafi_api"],
)
def nota_credito_siafi_dag() -> None:
    @task
    def fetch_and_store_nota_credito() -> None:
        cliente_siafi = ClienteSiafi()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        notas_credito = db.get_nota_credito()

        for nota_credito in notas_credito:
            cd_ug_emitente_nota = nota_credito[0]
            cd_gestao_emitente_nota = nota_credito[1]
            tx_numero_nota = nota_credito[2]

            if not all([cd_ug_emitente_nota, cd_gestao_emitente_nota, tx_numero_nota]):
                continue

            ano = tx_numero_nota[:4]
            numero = tx_numero_nota[-6:]

            response = cliente_siafi.consultar_nota_credito(
                ug=cd_ug_emitente_nota,
                gestao=cd_gestao_emitente_nota,
                ano=ano,
                numero=numero,
            )
            if response:
                response["ano"] = ano
                db.insert_data(
                    [response],
                    "nota_credito",
                    conflict_fields=["numero", "ano"],
                    primary_key=["numero", "ano"],
                    schema="siafi",
                )

    fetch_and_store_nota_credito()


dag_instance = nota_credito_siafi_dag()
