import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from cliente_sqlserver import ClientSQLServerDB
from sql_server_helper import get_sql_server_conn


@dag(
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sql_server", "example"],
)
def sql_server_query_example_dag() -> None:
    """DAG simples para testar consulta em SQL Server via MsSqlHook."""

    @task
    def run_sql_server_query() -> list[dict[str, str]]:
        # Validates the Airflow connection and logs useful metadata.
        get_sql_server_conn("mssql_default")

        db = ClientSQLServerDB(mssql_conn_id="mssql_default")
        query = """
            SELECT TOP 1
                TABLE_SCHEMA,
                TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            ORDER BY TABLE_NAME
        """

        rows = db.execute_query(query)
        result = [
            {
                "table_schema": str(row[0]),
                "table_name": str(row[1]),
            }
            for row in rows
        ]

        logging.info(
            "[sql_server_query_example_dag.py] Resultado da consulta: %s",
            result,
        )
        return result

    run_sql_server_query()


dag_instance = sql_server_query_example_dag()
