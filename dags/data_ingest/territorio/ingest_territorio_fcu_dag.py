"""DAG de ingestão do crosswalk de território IBGE (Censo 2022).

Carrega o CSV `fcu_setores_2022.csv` (setor censitário -> FCU/concentração
urbana -> município -> UF) numa tabela raw no Postgres, para o dbt consumir
como source. Base para a 4ª cota (território, 20%) da Meta 3.

O CSV vem do IBGE CD2022 (Composição das Concentrações Urbanas), quebrado em
setores por script externo. Fica em data/external/territorio/ (volume mapeado
em /opt/airflow/data). Schedule=None: ingestão manual/pontual (dado estável,
não muda entre censos).
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

# Tabela auxiliar (crosswalk IBGE), fora do modelo do documento. Fica em
# transferegov para nao criar um schema so para ela.
_SCHEMA = schemas.SCHEMA_TRANSFEREGOV
_TABLE = "territorio_fcu_setores"
_CSV_PATH = Path("/opt/airflow/data/external/territorio/fcu_setores_2022.csv")

default_args = {
    "owner": "Meta 3 - cotas",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="ingest_territorio_fcu_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "territorio", "ibge", "cotas", "raw"],
)
def ingest_territorio_fcu_dag() -> None:
    """Carrega o crosswalk setor->FCU->município->UF no Postgres."""

    @task
    def carregar_csv_territorio() -> int:
        if not _CSV_PATH.exists():
            raise FileNotFoundError(
                f"[ingest_territorio_fcu_dag] CSV não encontrado em {_CSV_PATH}. "
                "Coloque fcu_setores_2022.csv em data/external/territorio/."
            )

        csv_data = _CSV_PATH.read_text(encoding="utf-8")
        n_linhas = csv_data.count("\n")

        db = ClientPostgresDB(get_postgres_conn())
        # insert_csv_data faz drop+recreate da tabela (dado é snapshot estático
        # do censo; recarregar por inteiro é correto e simples).
        db.insert_csv_data(csv_data, table_name=_TABLE, schema=_SCHEMA)

        logging.info(
            "[ingest_territorio_fcu_dag] %d linhas carregadas em %s.%s",
            n_linhas,
            _SCHEMA,
            _TABLE,
        )
        return n_linhas

    carregar_csv_territorio()


ingest_territorio_fcu_dag()
