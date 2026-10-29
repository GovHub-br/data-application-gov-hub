import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed, numeros_como_texto


@dag(
    schedule_interval=get_dynamic_schedule("plano_acao_meta_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Joao",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ted_api", "planos_acao"],
)
def plano_acao_meta_dag() -> None:
    """Metas dos planos de ação (endpoint plano_acao_meta), por id_plano_acao.
    Agendar depois da DAG de planos de ação.

    A API devolve o retrato completo, entao a carga substitui a tabela inteira
    (linhas removidas na fonte saem do banco). Se a API falhar para qualquer
    plano de ação, a DAG falha e a tabela fica como estava, sem gravar um retrato
    incompleto.
    """

    @task
    def fetch_and_store_planos_acao_meta() -> None:
        api = ClienteTed()
        db = ClientPostgresDB(get_postgres_conn())

        linhas: list = []
        for id_plano_acao in db.get_id_planos_acao():
            dados = api.get_metas_by_id_plano_acao(id_plano_acao)
            if dados is None:
                raise RuntimeError(
                    f"Falha na API de TED ao buscar metas "
                    f"(id_plano_acao {id_plano_acao})"
                )
            linhas.extend(numeros_como_texto(dados))

        if not linhas:
            logging.warning("API de TED sem metas; tabela mantida")
            return

        dt_ingest = datetime.now().isoformat()
        for linha in linhas:
            linha["dt_ingest"] = dt_ingest

        db.replace_rows(
            linhas,
            "planos_acao_meta",
            delete_where="true",
            delete_params=(),
            schema="transfere_gov",
            preserva_nulos=True,
        )
        logging.info(f"{len(linhas)} linhas gravadas em transfere_gov.planos_acao_meta")

    fetch_and_store_planos_acao_meta()


plano_acao_meta_dag()
