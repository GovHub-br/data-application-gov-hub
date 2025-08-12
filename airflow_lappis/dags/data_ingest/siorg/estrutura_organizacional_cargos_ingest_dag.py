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
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "owner": "Davi"},
    tags=["estrutura_organizacional", "siorg"],
)
def api_estrutura_organizacional_cargos_dag() -> None:
    """Busca dados da estrutura organizacional via API e armazena no PostgreSQL."""

    @task
    def fetch_estrutura_organizacional_cargos() -> None:
        try:
            api = ClienteSiorg()
            db = ClientPostgresDB(get_postgres_conn())
            codigo_unidades = db.get_codigo_unidade()

            if not codigo_unidades:
                logging.warning("Nenhum código de unidade encontrado.")
                return

            for unidade in codigo_unidades:
                codigo_unidade = unidade["codigounidade"]
                ordem_grandeza = unidade["ordem_grandeza"]

                try:
                    estrutura_cargos = api.get_estrutura_organizacional_cargos(
                        codigo_unidade
                    )

                    if estrutura_cargos:
                        estrutura_cargos["ordem_grandeza"] = ordem_grandeza
                        estrutura_cargos["dt_ingest"] = datetime.now().isoformat()

                        db.insert_data(
                            [estrutura_cargos],
                            "estrutura_organizacional_cargos",
                            conflict_fields=["codigoUnidade"],
                            primary_key=["codigoUnidade"],
                            schema="siorg",
                        )
                    else:
                        logging.debug(f"Sem dados para codigoUnidade {codigo_unidade}.")

                except Exception as e:
                    logging.error(
                        f"Erro ao processar codigoUnidade {codigo_unidade}: {e}",
                        exc_info=True,
                    )

        except Exception as e:
            logging.error(f"Erro geral na tarefa: {e}", exc_info=True)
            raise

    fetch_estrutura_organizacional_cargos()


dag_instance = api_estrutura_organizacional_cargos_dag()
