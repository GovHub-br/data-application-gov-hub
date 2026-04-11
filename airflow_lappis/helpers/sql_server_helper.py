import logging

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def get_sql_server_conn(data_base_name: str = "mssql_default") -> str:
    try:
        hook = MsSqlHook(mssql_conn_id=data_base_name)
        connection = hook.get_connection(data_base_name)

        # Open the DB-API connection once to fail fast if credentials are invalid.
        hook.get_conn()

        schema = connection.schema or ""
        port = connection.port or 1433

        logging.info(
            "[sql_server_helper] Obtained SQL Server connection: "
            f"database={schema}, user={connection.login}, "
            f"host={connection.host}, port={port}"
        )

        return (
            f"server={connection.host};database={schema};"
            f"user={connection.login};password={connection.password};port={port}"
        )
    except Exception as e:
        logging.error(f"Failed to obtain SQL Server connection: {e}")
        raise
