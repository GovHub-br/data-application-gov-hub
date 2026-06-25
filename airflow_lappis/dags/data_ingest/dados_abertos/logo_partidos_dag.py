import logging
import time
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_partidos import ClientePartidos
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("logo_partidos_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Ingrid",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["logo_partidos", "partidos", "dados_abertos", "MIR"],
)
def logo_partidos_dag() -> None:
    """DAG para buscar e armazenar dados de partidos e seus logos da Câmara dos Deputados."""

    @task
    def fetch_and_store_partidos() -> None:
        logging.info("[logo_partidos_dag.py] Iniciando extração de partidos")

        api = ClientePartidos()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        partidos_basicos = api.get_all_partidos()

        partidos_completos = []

        if partidos_basicos:
            for p in partidos_basicos:
                partido_id = p.get("id")
                if partido_id:
                    detalhe = api.get_partido_by_id(partido_id)
                    if detalhe:
                        registro = {
                            "id": detalhe.get("id"),
                            "sigla": detalhe.get("sigla"),
                            "nome": detalhe.get("nome"),
                            "uri": detalhe.get("uri"),
                            "urllogo": detalhe.get("urlLogo"),
                            "dt_ingest": datetime.now().isoformat(),
                        }
                        partidos_completos.append(registro)

                    time.sleep(0.5)

            logging.info(
                f"[logo_partidos_dag.py] Inserindo "
                f"{len(partidos_completos)} partidos no schema camara_deputados"
            )

            if partidos_completos:
                db.insert_data(
                    partidos_completos,
                    "logo_partidos",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="dados_abertos",
                )

                logging.info(
                    f"[logo_partidos_dag.py] Concluído. "
                    f"Total de {len(partidos_completos)} registros processados."
                )
            else:
                logging.warning(
                    "[logo_partidos_dag.py] Nenhum dado de detalhe retornado para os partidos."
                )
        else:
            logging.warning(
                "[logo_partidos_dag.py] Nenhum partido encontrado na lista principal."
            )

    fetch_and_store_partidos()


logo_partidos_dag()
