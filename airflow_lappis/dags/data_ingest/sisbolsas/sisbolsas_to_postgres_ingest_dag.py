import logging
from datetime import datetime, timedelta
from typing import Dict, List, TypedDict
from airflow.decorators import dag, task
from airflow.models import Variable
from cliente_postgres import ClientPostgresDB
from cliente_sqlserver import ClientSQLServerDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


class SQLServerTableConfig(TypedDict):
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    primary_key: List[str]


TABLES_TO_SYNC_VARIABLE = "sisbolsas_tables_to_sync"


def _load_tables_from_variable() -> List[SQLServerTableConfig]:
    return Variable.get(
        TABLES_TO_SYNC_VARIABLE,
        default_var=[],
        deserialize_json=True,
    )


@dag(
    schedule_interval=get_dynamic_schedule("sisbolsas_to_postgres_ingest_dag"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sql_server", "postgres", "sisbolsas"],
)
def sql_server_to_postgres_ingest_dag() -> None:
    """Replica tabelas do SisBolsas para o Postgres Analytics."""

    @task
    def replicate_table(table_cfg: SQLServerTableConfig) -> Dict[str, int]:
        sql_server = ClientSQLServerDB("mssql_conn_id_sisbolsas")
        postgres = ClientPostgresDB(get_postgres_conn())
        source_schema = table_cfg["source_schema"]
        source_table = table_cfg["source_table"]
        target_schema = table_cfg["target_schema"]
        target_table = table_cfg["target_table"]
        primary_key = table_cfg["primary_key"]

        source_name = f"{source_schema}.{source_table}"

        logging.info(
            "[sql_server_to_postgres_ingest_dag.py] Iniciando replicacao de %s",
            source_name,
        )

        rows = sql_server.fetch_table_all(
            schema=source_schema,
            table_name=source_table,
        )

        if rows:
            postgres.insert_data(
                data=rows,
                table_name=target_table,
                conflict_fields=primary_key,
                primary_key=primary_key,
                schema=target_schema,
            )

        logging.info(
            "[sql_server_to_postgres_ingest_dag.py] Replicacao concluida "
            "para %s. Registros processados=%s",
            source_name,
            len(rows),
        )

        return {source_name: len(rows)}

    tables_to_sync = _load_tables_from_variable()
    replicate_table.expand(table_cfg=tables_to_sync)


sql_server_to_postgres_ingest_dag()
