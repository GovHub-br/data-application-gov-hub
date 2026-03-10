import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_senadores import ClienteSenadores
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("senadores_ingest_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Cibelly",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["senado_federal", "senadores", "dados_abertos", "MIR"],
)
def senadores_ingest_dag() -> None:
    """DAG para buscar e armazenar dados de senadores do Senado Federal."""

    @task
    def fetch_and_store_senadores() -> None:
        logging.info("[senadores_ingest_dag.py] Iniciando extração de senadores")

        api = ClienteSenadores()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        senadores_data = api.get_senadores_atuais()

        if senadores_data and len(senadores_data) > 0:
            registros_limpos = []

            for item in senadores_data:
                info = item.get("IdentificacaoParlamentar", {})
                mandato = item.get("Mandato", {})

                senador_simplificado = {
                    "id": info.get("CodigoParlamentar"),
                    "nome_parlamentar": info.get("NomeParlamentar"),
                    "nome_completo": info.get("NomeCompletoParlamentar"),
                    "sexo": info.get("SexoParlamentar"),
                    "forma_tratamento": info.get("FormaTratamento"),
                    "url_foto": info.get("UrlFotoParlamentar"),
                    "url_pagina": info.get("UrlPaginaParlamentar"),
                    "email": info.get("EmailParlamentar"),
                    "sigla_partido": info.get("SiglaPartidoParlamentar"),
                    "uf": info.get("UfParlamentar"),
                    "id_legislatura": mandato.get("CodigoLegislatura"),
                    "dt_ingest": datetime.now().isoformat(),
                }
                registros_limpos.append(senador_simplificado)

            logging.info(
                f"[senadores_ingest_dag.py] Inserindo {len(registros_limpos)} "
                f"senadores simplificados no schema senado_federal"
            )

            db.insert_data(
                registros_limpos,
                "senadores",
                conflict_fields=["id"],
                primary_key=["id"],
                schema="senado_federal",
            )

    fetch_and_store_senadores()


senadores_ingest_dag()
