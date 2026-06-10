import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
)
from airflow.models import Variable

from cliente_ibge import ClienteIBGE
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

logger = logging.getLogger(__name__)

# Defaults da Pesquisa Industrial Mensal - Produção Física - Brasil (PIM-PF Brasil).
# Agregado 8888 da SIDRA: "Indicadores conjunturais da indústria, no Brasil"
# (séries reformuladas em 2023). Documentação: https://sidra.ibge.gov.br/tabela/8888
#
# - variaveis="all": traz todas as variáveis disponíveis (variação mensal, variação
#   acumulada no ano, variação acumulada em 12 meses, número-índice, etc.), deixando
#   para o dbt silver/gold decidir quais consumir.
# - periodos="-13": últimos 13 períodos. Captura revisões retroativas comuns na PIM-PF
#   sem trazer toda a série histórica em cada execução diária.
# - classificacao_id / categoria omitidos: a API devolve todas as combinações
#   (indústria geral, seções, atividades, categorias de uso, etc.) para o bronze.
#
# Para reduzir volume ou refinar o recorte em produção, defina a Airflow Variable
# `IBGE_PIM_PF_BRASIL_CONFIG` (JSON) sobrescrevendo qualquer um destes campos.
DEFAULT_CONFIG = {
    "agregado": 8888,
    "variaveis": "all",
    "periodos": "-13",
    "nivel": "N1",
    "localidade": "1",
    "classificacao_id": None,
    "categoria": None,
}

SCHEMA = "ibge"
TABELA = "ibge_pim_pf_brasil"
PRIMARY_KEY = [
    "variavel_id",
    "localidade_id",
    "periodo",
    "classificacao_id",
    "categoria_id",
]

default_args = {
    "owner": "Lucas Bottino",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="ibge_pim_pf_brasil_ingest_dag",
    schedule_interval=get_dynamic_schedule(
        "ibge_pim_pf_brasil_ingest_dag", default="@daily"
    ),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ibge", "sidra", "pim_pf", "ingestao"],
)
def ibge_pim_pf_brasil_ingest_dag() -> None:
    """
    DAG de ingestão diária da PIM-PF Brasil (IBGE / SIDRA agregado 8888).

    A pesquisa tem periodicidade mensal, mas a DAG roda diariamente para capturar
    novas divulgações e eventuais revisões retroativas do IBGE. Como o `insert_data`
    do `ClientPostgresDB` usa `ON CONFLICT DO UPDATE`, execuções repetidas no mesmo
    período são idempotentes.

    Config pode ser sobrescrita via Airflow Variable `IBGE_PIM_PF_BRASIL_CONFIG`
    (JSON com chaves: agregado, variaveis, periodos, nivel, localidade,
    classificacao_id, categoria).

    Tabela de destino: `ibge.ibge_pim_pf_brasil`.
    """

    @task
    def fetch_and_store() -> None:
        logger.info("[ibge_pim_pf_brasil_ingest_dag] Iniciando ingestão da PIM-PF Brasil")

        # Lê config dentro da task para evitar parse no top-level do scheduler.
        config = Variable.get(
            "IBGE_PIM_PF_BRASIL_CONFIG",
            deserialize_json=True,
            default_var=DEFAULT_CONFIG,
        )
        # Preenche chaves ausentes com o default — permite overrides parciais.
        for key, value in DEFAULT_CONFIG.items():
            config.setdefault(key, value)

        logger.info(
            "[ibge_pim_pf_brasil_ingest_dag] Config efetiva: "
            "agregado=%s variaveis=%s periodos=%s nivel=%s localidade=%s "
            "classificacao_id=%s categoria=%s",
            config["agregado"],
            config["variaveis"],
            config["periodos"],
            config["nivel"],
            config["localidade"],
            config["classificacao_id"],
            config["categoria"],
        )

        try:
            api = ClienteIBGE()
            dados_api = api.get_dados_agregados(
                agregado=config["agregado"],
                variaveis=config["variaveis"],
                periodos=config["periodos"],
                nivel=config["nivel"],
                localidade=config["localidade"],
                classificacao_id=config["classificacao_id"],
                categoria=config["categoria"],
            )

            if dados_api is None:
                raise AirflowFailException(
                    "[ibge_pim_pf_brasil_ingest_dag] ClienteIBGE retornou None "
                    "para o agregado %s." % config["agregado"]
                )

            registros = ClienteIBGE.transformar_resposta(dados_api)

            if not registros:
                raise AirflowSkipException(
                    "[ibge_pim_pf_brasil_ingest_dag] Nenhum registro extraído "
                    "da resposta da API IBGE — possivelmente sem novas "
                    "publicações no período consultado."
                )

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            logger.info(
                "[ibge_pim_pf_brasil_ingest_dag] Inserindo %d registros em %s.%s",
                len(registros),
                SCHEMA,
                TABELA,
            )

            db.insert_data(
                data=registros,
                table_name=TABELA,
                conflict_fields=PRIMARY_KEY,
                primary_key=PRIMARY_KEY,
                schema=SCHEMA,
            )

            logger.info(
                "[ibge_pim_pf_brasil_ingest_dag] Ingestão de %s.%s concluída "
                "com sucesso (%d registros).",
                SCHEMA,
                TABELA,
                len(registros),
            )

        except (AirflowFailException, AirflowSkipException):
            raise
        except Exception as e:
            logger.error("[ibge_pim_pf_brasil_ingest_dag] Erro inesperado: %s", e)
            raise AirflowException(
                f"[ibge_pim_pf_brasil_ingest_dag] Erro inesperado: {e}"
            ) from e

    fetch_and_store()


dag_instance = ibge_pim_pf_brasil_ingest_dag()
