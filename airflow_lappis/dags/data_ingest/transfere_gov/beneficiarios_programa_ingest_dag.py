import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed, numeros_como_texto


@dag(
    schedule_interval=get_dynamic_schedule("beneficiarios_programa_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joao",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ted_api", "programas"],
)
def beneficiarios_programa_dag() -> None:
    """Beneficiários dos programas (endpoint programa_beneficiario), por id_programa.
    Agendar depois da DAG de programas.

    A API devolve o retrato completo, entao a carga substitui a tabela inteira
    (linhas removidas na fonte saem do banco). Se a API falhar para qualquer
    programa, a DAG falha e a tabela fica como estava, sem gravar um retrato
    incompleto.
    """

    @task
    def fetch_and_store_programas_beneficiario() -> None:
        api = ClienteTed()
        db = ClientPostgresDB(get_postgres_conn())

        linhas: list = []
        for id_programa in db.get_id_programas():
            dados = api.get_beneficiarios_by_id_programa(id_programa)
            if dados is None:
                raise RuntimeError(
                    f"Falha na API de TED ao buscar beneficiários dos programas "
                    f"(id_programa {id_programa})"
                )
            linhas.extend(numeros_como_texto(dados))

        if not linhas:
            logging.warning("API de TED sem beneficiários dos programas; tabela mantida")
            return

        dt_ingest = datetime.now().isoformat()
        for linha in linhas:
            linha["dt_ingest"] = dt_ingest

        db.replace_rows(
            linhas,
            "programas_beneficiario",
            delete_where="true",
            delete_params=(),
            schema="transfere_gov",
            preserva_nulos=True,
        )
        logging.info(
            f"{len(linhas)} linhas gravadas em transfere_gov.programas_beneficiario"
        )

    fetch_and_store_programas_beneficiario()


beneficiarios_programa_dag()
