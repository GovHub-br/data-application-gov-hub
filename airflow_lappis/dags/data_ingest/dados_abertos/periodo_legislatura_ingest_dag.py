import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_senadores import ClienteSenadores
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("periodo_legislatura_ingest_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Luana",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["senado_federal", "dados_abertos", "MIR"],
)
def periodo_legislacao_ingest_dag() -> None:
    """DAG para buscar e armazenar período de legislação parlamentares"""

    @task
    def fetch_and_store_periodo_legislacao() -> None:
        logging.info("Iniciando extração de legislaturas")

        api = ClienteSenadores()
        db = ClientPostgresDB(get_postgres_conn("postgres_mir"))

        lista_legislaturas = api.get_periodo_legislacao()

        if not lista_legislaturas or not isinstance(lista_legislaturas, list):
            logging.error(f"Esperava uma lista, mas recebi: {type(lista_legislaturas)}")
            return

        registros_limpos = []

        for leg in lista_legislaturas:
            item_limpo = {
                "id": int(leg.get("NumeroLegislatura")),
                "data_inicio": leg.get("DataInicio"),
                "data_fim": leg.get("DataFim"),
                "data_eleicao": leg.get("DataEleicao"),
                "dt_ingest": datetime.now().isoformat(),
            }
            registros_limpos.append(item_limpo)

        logging.info(f"Preparados {len(registros_limpos)} registros para o banco.")

        if registros_limpos:
            db.insert_data(
                registros_limpos,
                "legislaturas",
                conflict_fields=["id"],
                primary_key=["id"],
                schema="senado_federal",
            )

    fetch_and_store_periodo_legislacao()


periodo_legislacao_ingest_dag()
