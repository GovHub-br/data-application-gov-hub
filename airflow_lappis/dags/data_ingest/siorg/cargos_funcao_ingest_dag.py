import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_siorg import ClienteSiorg
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "Davi",
    },
    tags=["estrutura_organizacional", "siorg"],
)
def api_cargos_funcao_dag() -> None:
    @task
    def fetch_and_store_cargos_funcao() -> None:
        api = ClienteSiorg()
        db = ClientPostgresDB(get_postgres_conn())
        try:
            cargos_funcao_data = api.get_cargos_funcao()
            if not cargos_funcao_data:
                logging.warning("Nenhum dado retornado pela API de cargos/função.")
                return

            registros = []
            for tipo in cargos_funcao_data:
                tipo_base = {k: v for k, v in tipo.items() if k != "cargosFuncoes"}
                if "cargosFuncoes" in tipo and "cargoFuncao" in tipo["cargosFuncoes"]:
                    for cargo in tipo["cargosFuncoes"]["cargoFuncao"]:
                        registro = {**tipo_base, **cargo}
                        registro["dt_ingest"] = datetime.now().isoformat()
                        registros.append(registro)
            if registros:
                db.insert_data(
                    registros,
                    "cargos_funcao",
                    conflict_fields=["codigoCargoFuncao"],
                    primary_key=["codigoCargoFuncao"],
                    schema="siorg",
                )
            else:
                logging.warning("Nenhum cargo/função encontrado para inserir.")
        except Exception as e:
            logging.error(f"Erro ao buscar/inserir cargos função: {e}")

    fetch_and_store_cargos_funcao()


dag_instance = api_cargos_funcao_dag()
