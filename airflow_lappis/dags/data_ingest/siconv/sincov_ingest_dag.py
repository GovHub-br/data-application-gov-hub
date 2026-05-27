import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import psycopg2
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_siconv import ClienteSiconv
from tabelas_siconv import TABELAS_SICONV


@dag(
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "Luana",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["siconv", "MIR"],
)
def siconv_ingestao_dag() -> None:  # noqa: C901

    @task
    def baixar_siconv() -> str:
        cliente = ClienteSiconv()
        cliente.baixar_zip()
        return cliente.ZIP_PATH

    @task
    def ingerir_tabela(
        zip_path: str,
        nome_tabela: str,
        nome_csv: str,
        conflict_fields: list,
        primary_key: list,
        skip_rows: int,
        colunas: list,
        truncate_before_insert: bool = False,
    ) -> None:

        postgres_conn_str = get_postgres_conn("postgres_mir")

        logging.info(f"Iniciando ingestão da tabela {nome_tabela}")

        db = ClientPostgresDB(postgres_conn_str)
        cliente = ClienteSiconv()

        gerador_registros = cliente.ler_csv(
            nome_csv, skip_rows, colunas_esperadas=colunas
        )

        lote = []
        tamanho_lote = 5000
        total_inserido = 0

        conn = psycopg2.connect(postgres_conn_str)
        try:
            if truncate_before_insert:
                logging.info(f"Truncando tabela siconv.{nome_tabela}...")
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        DO $$ BEGIN
                        IF EXISTS (
                            SELECT FROM pg_tables
                            WHERE schemaname = 'siconv'
                            AND tablename = '{nome_tabela}'
                        ) THEN
                            TRUNCATE TABLE siconv.{nome_tabela};
                        END IF;
                        END $$;
                    """
                    )

            for registro in gerador_registros:
                lote.append(registro)

                if len(lote) >= tamanho_lote:
                    lote = [dict(t) for t in {tuple(d.items()) for d in lote}]

                    db.insert_data(
                        lote,
                        nome_tabela,
                        conflict_fields=conflict_fields,
                        primary_key=primary_key,
                        schema="siconv",
                        conn=conn,
                    )

                    total_inserido += len(lote)
                    logging.info(f"{total_inserido} registros processados...")
                    lote = []

            if lote:
                lote = [dict(t) for t in {tuple(d.items()) for d in lote}]

                db.insert_data(
                    lote,
                    nome_tabela,
                    conflict_fields=conflict_fields,
                    primary_key=primary_key,
                    schema="siconv",
                    conn=conn,
                )

                total_inserido += len(lote)

            conn.commit()
        finally:
            conn.close()

        if total_inserido == 0:
            logging.warning(f"Nenhum registro processado para {nome_tabela}")
        else:
            logging.info(
                f"Ingestão finalizada: {total_inserido} registros em {nome_tabela}"
            )

    @task
    def deletar_zip(zip_path: str) -> None:
        import os

        if os.path.exists(zip_path):
            os.remove(zip_path)
            logging.info(f"Arquivo {zip_path} deletado com sucesso")
        else:
            logging.warning(f"Arquivo {zip_path} não encontrado")

    path_zip = baixar_siconv()

    ultima_task = path_zip

    for tabela in TABELAS_SICONV:
        task_atual = ingerir_tabela.override(task_id=f"ingerir_{tabela['nome_tabela']}")(
            zip_path=path_zip,
            nome_tabela=tabela["nome_tabela"],
            nome_csv=tabela["nome_csv"],
            conflict_fields=tabela["conflict_fields"],
            primary_key=tabela["primary_key"],
            skip_rows=tabela["skip_rows"],
            colunas=tabela["colunas"],
            truncate_before_insert=tabela.get("truncate_before_insert", False),
        )

        ultima_task >> task_atual
        ultima_task = task_atual

    ultima_task >> deletar_zip(path_zip)


siconv_ingestao_dag()
