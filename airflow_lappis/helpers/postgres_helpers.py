import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_postgres_conn(conn_id: str = "postgres_default") -> str:
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)

        conn = hook.get_conn()

        airflow_conn = hook.get_connection(conn_id)

        schema = conn.info.dbname
        user = conn.info.user
        host = conn.info.host
        port = conn.info.port
        password = airflow_conn.password

        logging.info(
            f"[postgres_helpers] Obtained PostgreSQL connection: "
            f"dbname={schema}, user={user}, host={host}, port={port}"
        )

        return f"dbname={schema} user={user} password={password} host={host} port={port}"

    except Exception as e:
        logging.error(f"Failed to obtain PostgreSQL connection: {e}")
        raise
