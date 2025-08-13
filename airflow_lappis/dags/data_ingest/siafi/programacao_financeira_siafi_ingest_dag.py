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
    tags=["programacao_financeira", "siafi_api"],
)
def programacao_financeira_siafi_dag() -> None:
    @task
    def fetch_and_store_programacao_financeira() -> None:
        cliente_siafi = ClienteSiafi()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        programacoes = db.get_programacao_financeira()

        for programacao in programacoes:
            tx_numero_programacao, ug_emitente_programacao = programacao
            ano = int(str(tx_numero_programacao)[:4])
            num_lista = int(str(tx_numero_programacao)[-6:])
            ug_emitente = int(ug_emitente_programacao)

            response = cliente_siafi.consultar_programacao_financeira(
                ug_emitente, ano, num_lista
            )
            if response:
                response["dt_ingest"] = datetime.now().isoformat()
                db.insert_data(
                    [response],
                    "programacao_financeira_siafi",
                    conflict_fields=["TRF__numeroDocumento"],
                    primary_key=["TRF__numeroDocumento"],
                    schema="siafi",
                )

    fetch_and_store_programacao_financeira()


dag_instance = programacao_financeira_siafi_dag()
