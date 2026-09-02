import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param

from cliente_dou import ClienteDou
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

logger = logging.getLogger(__name__)

# Constantes

SCHEMA = "dou"
TABLE = "secao1"
CONFLICT_FIELDS = ["urlTitle", "pubDate"]
# Nome da Variable Airflow que contém os órgãos monitorados.
VARIABLE_ORGAOS = "dou_orgaos_monitorados"


# DAG

@dag(
    dag_id="dag_dou_secao_1",
    schedule_interval=get_dynamic_schedule("dag_dou_secao_1", default="0 6 * * 1-5"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "Rafael, Letícia",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "data_inicio": Param(
            default=None,
            type=["string", "null"],
            title="Data de Início",
            description=(
                "Backfill: data inicial da busca no formato DD/MM/AAAA. "
                "Se não preenchido, usa a data de hoje."
            ),
        ),
        "data_fim": Param(
            default=None,
            type=["string", "null"],
            title="Data de Fim",
            description=(
                "Backfill: data final da busca no formato DD/MM/AAAA. "
                "Se não preenchido, usa a data de hoje."
            ),
        ),
    },
    tags=["dou", "secao1", "diario_oficial"],
)
def dag_dou_secao_1() -> None:
    """DAG de ingestão das publicações da Seção 1 do DOU.

    Executa de segunda a sexta-feira às 06h (horário de Brasília).
    Suporta backfill via formulário na UI do Airflow usando os parâmetros
    ``data_inicio`` e ``data_fim`` (formato DD/MM/AAAA).

    Os dados são gravados em ``dou.secao1`` no PostgreSQL com deduplicação
    pela chave composta (urlTitle, pubDate).
    """

    @task
    def buscar_e_inserir_secao1(**context: Dict[str, Any]) -> None:
        params = context["params"]
        data_inicio_str: str | None = params.get("data_inicio")
        data_fim_str: str | None = params.get("data_fim")

        # Se não informado via Param, usa a data de hoje
        hoje = date.today()
        formato_dou = "%d/%m/%Y"

        if data_inicio_str:
            data_inicio = datetime.strptime(data_inicio_str, formato_dou).date()
        else:
            data_inicio = hoje

        if data_fim_str:
            data_fim = datetime.strptime(data_fim_str, formato_dou).date()
        else:
            data_fim = hoje

        if data_fim < data_inicio:
            raise ValueError(
                f"data_fim ({data_fim}) não pode ser anterior a data_inicio ({data_inicio})."
            )

        # Termos/órgãos monitorados vindos da Variable Airflow.
        # Formato esperado: texto simples, um órgão por linha.
        orgaos_raw: str = Variable.get(VARIABLE_ORGAOS, default_var="")
        orgaos: list[str] = [
            linha.strip().strip('"')
            for linha in orgaos_raw.splitlines()
            if linha.strip()
        ]

        if not orgaos:
            logger.warning(
                "[dag_dou_secao_1] Nenhum órgão configurado na variável '%s'. "
                "Abortando sem inserções.",
                VARIABLE_ORGAOS,
            )
            return

        cliente = ClienteDou()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Itera por cada dia do intervalo
        delta_dias = (data_fim - data_inicio).days
        datas = [
            (data_inicio + timedelta(days=d)).strftime(formato_dou)
            for d in range(delta_dias + 1)
        ]

        total_inserido = 0

        for data_str in datas:
            logger.info(
                "[dag_dou_secao_1] Buscando Seção 1 do DOU para data: %s", data_str
            )

            publicacoes = cliente.buscar_todas_publicacoes(
                termos=orgaos,
                data=data_str,
                secao=1,
            )

            if not publicacoes:
                logger.info(
                    "[dag_dou_secao_1] Nenhuma publicação encontrada para %s.", data_str
                )
                continue

            # Adiciona metadado de ingestão
            for item in publicacoes:
                item["dt_ingest"] = datetime.now().isoformat()

            db.insert_data(
                publicacoes,
                table_name=TABLE,
                conflict_fields=CONFLICT_FIELDS,
                primary_key=CONFLICT_FIELDS,
                schema=SCHEMA,
            )
            total_inserido += len(publicacoes)
            logger.info(
                "[dag_dou_secao_1] %d publicações inseridas em %s.%s para %s.",
                len(publicacoes),
                SCHEMA,
                TABLE,
                data_str,
            )

        logger.info(
            "[dag_dou_secao_1] Ingestão concluída. Total inserido: %d registros.",
            total_inserido,
        )

    buscar_e_inserir_secao1()


dag_instance = dag_dou_secao_1()
