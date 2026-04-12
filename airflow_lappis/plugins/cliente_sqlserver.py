import logging
import re
from typing import Any, Dict, List, Tuple
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

    def fetch_table_all(
        self,
        schema: str,
        table_name: str,
    ) -> List[Dict[str, Any]]:
        """Fetch all rows from a SQL Server table using SELECT *."""

        full_table_name = f"{schema}.{table_name}"

        query = f"SELECT * FROM {full_table_name}"
        logging.info(f"[cliente_sqlserver.py] Executing query: {query}")

        conn = self.hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                logging.info(
                    "[cliente_sqlserver.py] No rows found in %s",
                    full_table_name,
                )
                return []

            columns = [description[0] for description in cursor.description]
            records = [dict(zip(columns, row)) for row in rows]

            logging.info(
                "[cliente_sqlserver.py] Query executed successfully, fetched %s rows "
                "from %s",
                len(records),
                full_table_name,
            )
            return records
        finally:
            cursor.close()
            conn.close()
