import logging
from typing import Any, List, Tuple

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


class ClientSQLServerDB:
    """Client for interacting with SQL Server using Airflow's MsSqlHook."""

    def __init__(self, mssql_conn_id: str = "mssql_default") -> None:
        self.mssql_conn_id = mssql_conn_id
        self.hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        logging.info(
            "[cliente_sqlserver.py] Initialized ClientSQLServerDB with "
            f"mssql_conn_id={mssql_conn_id}"
        )

    def execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        """Execute a SELECT query and return rows."""
        logging.info(f"[cliente_sqlserver.py] Executing query: {query}")
        rows = self.hook.get_records(sql=query)
        logging.info(
            "[cliente_sqlserver.py] Query executed successfully, fetched "
            f"{len(rows)} rows"
        )
        return rows

    def execute_non_query(self, query: str) -> None:
        """Execute a SQL command that does not return rows."""
        logging.info(f"[cliente_sqlserver.py] Executing non-query: {query}")
        self.hook.run(sql=query)
        logging.info("[cliente_sqlserver.py] Non-query executed successfully")
