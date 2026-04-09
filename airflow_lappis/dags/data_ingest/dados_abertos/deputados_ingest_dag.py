import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_deputados import ClienteDeputados
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("deputados_ingest_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Leonardo e Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["camara_deputados", "deputados", "dados_abertos", "MIR"],
)
def deputados_ingest_dag() -> None:
    """DAG para buscar e armazenar dados de deputados da Câmara dos Deputados."""

    @task
    def fetch_and_store_deputados() -> None:
        logging.info("[deputados_ingest_dag.py] Iniciando extração de deputados")

        api = ClienteDeputados()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        deputados_data = api.get_all_deputados()

        unique_key = ["id", "siglapartido", "idlegislatura"]

        if deputados_data:
            vistos = set()
            lista_limpa = []

            for item in deputados_data:

                if item.get("siglaPartido") is None:
                    item["siglaPartido"] = "Sem Partido"

                chave_unica = tuple(item.get(key) for key in unique_key)

                if chave_unica not in vistos:
                    item["dt_ingest"] = datetime.now().isoformat()
                    lista_limpa.append(item)
                    vistos.add(chave_unica)

            deputados_data = lista_limpa

            logging.info(
                f"[deputados_ingest_dag.py] Inserindo "
                f"{len(deputados_data)} deputados no schema camara_deputados"
            )

            db.insert_data(
                deputados_data,
                "deputados",
                conflict_fields=["id", "siglapartido", "idlegislatura"],
                primary_key=["id", "siglapartido", "idlegislatura"],
                schema="camara_deputados",
            )

            logging.info(
                f"[deputados_ingest_dag.py] Concluído. "
                f"Total de {len(deputados_data)} registros processados."
            )
        else:
            logging.warning("[deputados_ingest_dag.py] Nenhum deputado encontrado")

    fetch_and_store_deputados()


deputados_ingest_dag()
