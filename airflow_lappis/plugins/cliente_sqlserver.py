import logging
import re
import math
from datetime import date, datetime, time
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

    @staticmethod
    def _sanitize_value(value: Any) -> Any:
        """Normaliza valores vindos do SQL Server para tipos seguros."""
        if value is None:
            return None

        if isinstance(value, str):
            cleaned_value = value.replace("\x00", "")
            if cleaned_value.strip().lower() in {"nat", "nan"}:
                return None
            return cleaned_value

        if isinstance(value, float) and math.isnan(value):
            return None

        if hasattr(value, "is_nan") and value.is_nan():
            return None

        if str(value) == "NaT":
            return None

        if isinstance(value, (datetime, date, time)):
            return value.isoformat()

        return value

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
            records = [
                {
                    column: self._sanitize_value(value)
                    for column, value in zip(columns, row)
                }
                for row in rows
            ]

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
