import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed, numeros_como_texto


@dag(
    schedule_interval=get_dynamic_schedule("eventos_nota_credito_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joao",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["notas de credito", "ted_api"],
)
def eventos_nota_credito_dag() -> None:
    """Eventos das notas de crédito (endpoint evento), por id_nota. Os ids saem de
    transfere_gov.notas_de_credito: agendar depois da DAG de notas de crédito.

    A API devolve o retrato completo, entao a carga substitui a tabela inteira
    (linhas removidas na fonte saem do banco). Se a API falhar para qualquer
    nota de crédito, a DAG falha e a tabela fica como estava, sem gravar um retrato
    incompleto.
    """

    @task
    def fetch_and_store_eventos_nota_credito() -> None:
        api = ClienteTed()
        db = ClientPostgresDB(get_postgres_conn())

        linhas: list = []
        for id_nota in db.get_id_notas_credito():
            dados = api.get_eventos_by_id_nota(id_nota)
            if dados is None:
                raise RuntimeError(
                    f"Falha na API de TED ao buscar eventos das notas de crédito "
                    f"(id_nota {id_nota})"
                )
            linhas.extend(numeros_como_texto(dados))

        if not linhas:
            logging.warning("API de TED sem eventos das notas de crédito; tabela mantida")
            return

        dt_ingest = datetime.now().isoformat()
        for linha in linhas:
            linha["dt_ingest"] = dt_ingest

        db.replace_rows(
            linhas,
            "eventos_nota_credito",
            delete_where="true",
            delete_params=(),
            schema="transfere_gov",
            preserva_nulos=True,
        )
        logging.info(
            f"{len(linhas)} linhas gravadas em transfere_gov.eventos_nota_credito"
        )

    fetch_and_store_eventos_nota_credito()


dag_instance = eventos_nota_credito_dag()
