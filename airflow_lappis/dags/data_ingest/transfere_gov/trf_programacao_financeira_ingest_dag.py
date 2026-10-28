import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed, numeros_como_texto


@dag(
    schedule_interval=get_dynamic_schedule("trf_programacao_financeira_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joao",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["programacao_financeira", "ted_api"],
)
def trf_programacao_financeira_dag() -> None:
    """TRF da programação financeira (endpoint trf), por id_programacao. Os ids saem
    de transfere_gov.programacao_financeira: agendar depois da DAG de programação
    financeira.

    A API devolve o retrato completo, entao a carga substitui a tabela inteira
    (linhas removidas na fonte saem do banco). Se a API falhar para qualquer
    programação financeira, a DAG falha e a tabela fica como estava, sem gravar um retrato
    incompleto.
    """

    @task
    def fetch_and_store_trf_programacao_financeira() -> None:
        api = ClienteTed()
        db = ClientPostgresDB(get_postgres_conn())

        linhas: list = []
        for id_programacao in db.get_id_programacoes_financeiras():
            dados = api.get_trf_by_id_programacao(id_programacao)
            if dados is None:
                raise RuntimeError(
                    f"Falha na API de TED ao buscar TRF da programação financeira "
                    f"(id_programacao {id_programacao})"
                )
            linhas.extend(numeros_como_texto(dados))

        if not linhas:
            logging.warning(
                "API de TED sem TRF da programação financeira; tabela mantida"
            )
            return

        dt_ingest = datetime.now().isoformat()
        for linha in linhas:
            linha["dt_ingest"] = dt_ingest

        db.replace_rows(
            linhas,
            "trf_programacao_financeira",
            delete_where="true",
            delete_params=(),
            schema="transfere_gov",
            preserva_nulos=True,
        )
        logging.info(
            f"{len(linhas)} linhas gravadas em transfere_gov.trf_programacao_financeira"
        )

    fetch_and_store_trf_programacao_financeira()


dag_instance = trf_programacao_financeira_dag()
