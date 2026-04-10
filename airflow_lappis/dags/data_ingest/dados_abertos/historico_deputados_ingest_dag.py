import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_deputados import ClienteDeputados
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("historico_eputados_ingest_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Luana",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["camara_deputados", "deputados", "dados_abertos", "MIR"],
)
def historico_deputados_ingest_dag() -> None:
    """DAG para buscar e armazenar as mudanças no exercício parlamentar de um deputado"""

    @task
    def fetch_and_store_historico() -> None:
        logging.info("[historico_deputados_ingest_dag.py] Iniciando extração de deputados")

        api = ClienteDeputados()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        historico_data = api.get_historico_all_deputados()

        unique_key = ["id", "siglapartido", "idlegislatura", "datahora"]

        if historico_data:
            vistos = set()
            lista_limpa = []
            dt_ingest = datetime.now().isoformat()

            for item in historico_data:

                if item.get("siglaPartido") is None:
                    item["siglaPartido"] = "Sem Partido"

                chave_unica = tuple(item.get(key) for key in unique_key)

                if chave_unica not in vistos:
                    item["dt_ingest"] = dt_ingest
                    lista_limpa.append(item)
                    vistos.add(chave_unica)

            historico_data = lista_limpa

            logging.info(
                f"[historico_deputados_ingest_dag.py] Inserindo "
                f"{len(historico_data)} deputados no schema camara_deputados"
            )

            db.insert_data(
                historico_data,
                "historico_deputados",
                conflict_fields=unique_key,
                primary_key=unique_key,
                schema="camara_deputados",
            )

            logging.info(
                f"[historico_deputados_ingest_dag.py] Concluído. "
                f"Total de {len(historico_data)} registros processados."
            )
        else:
            logging.warning("[historico_deputados_ingest_dag.py] Nenhum deputado encontrado")

    fetch_and_store_historico()


historico_deputados_ingest_dag()