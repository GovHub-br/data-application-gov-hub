import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import schemas_minc as schemas
from cliente_postgres import ClientPostgresDB
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

_URL_CONSULTA_PROGRAMA = (
    "https://api.transferegov.gestao.gov.br/fundoafundo/programa?codigo_programa=eq.{}"
)


def _politica_por_id_programa() -> dict[int, dict[str, str]]:
    """Mapeia ``id_programa`` -> ``sigla``/``politica_publica`` a partir da
    Variable ``transferegov_politicas_publicas``.

    A secao 7.2 exige ``sigla`` e ``politica_publica`` em ``programa_minc``,
    mas o endpoint ``/programa`` nao devolve nenhum dos dois -- essa relacao
    e decisao do MinC, nao dado da origem. Como o repositorio nao versiona
    catalogo, a Variable e a unica fonte possivel. Formato esperado::

        [{"sigla": "LPG",
          "politica_publica": "LEI PAULO GUSTAVO (2022)",
          "id_programas": [46, 47]}, ...]
    """
    politicas = Variable.get(
        "transferegov_politicas_publicas",
        default=[],
        deserialize_json=True,
    )

    if not politicas:
        logging.warning(
            "[api_programas_dag.py] Variable 'transferegov_politicas_publicas' "
            "nao configurada — 'sigla' e 'politica_publica' ficarao nulas em %s.%s",
            schemas.SCHEMA_TRANSFEREGOV,
            schemas.TABELA_PROGRAMA,
        )

    return {
        int(id_programa): {
            "sigla": politica["sigla"],
            "politica_publica": politica["politica_publica"],
        }
        for politica in politicas
        for id_programa in politica.get("id_programas", [])
    }


@dag(
    dag_id="api_programas_dag",
    schedule=get_dynamic_schedule("api_programas_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "programas", "raw"],
)
def api_programas_dag() -> None:
    @task
    def fetch_programas() -> list[dict[str, Any]]:
        logging.info("[api_programas_dag.py] Iniciando extração de programas")
        ids_alvo = Variable.get(
            "transferegov_programas_ids",
            default=schemas.PROGRAMAS_IDS_PADRAO,
            deserialize_json=True,
        )
        politicas = _politica_por_id_programa()

        api = ClienteTransfereGov()
        programas_data: list[dict[str, Any]] = []

        for id_programa in ids_alvo:
            logging.info("[api_programas_dag.py] Buscando programa ID: %s", id_programa)
            programa = api.get_programa_by_id(int(id_programa))

            if programa:
                # Campos obrigatorios da secao 7.2 que nao vem da API. O
                # codigo_programa vem no payload e e mantido como texto (e
                # identificador de negocio, nao numero).
                politica = politicas.get(int(id_programa), {})
                programa["sigla"] = politica.get("sigla")
                programa["politica_publica"] = politica.get("politica_publica")
                programa["url_consulta"] = _URL_CONSULTA_PROGRAMA.format(
                    programa.get("codigo_programa")
                )
                programa["dt_ingest"] = datetime.now().isoformat()
                programas_data.append(programa)

                if not politica:
                    logging.warning(
                        "[api_programas_dag.py] Programa %s sem política pública "
                        "mapeada na Variable 'transferegov_politicas_publicas'",
                        id_programa,
                    )
            else:
                logging.warning(
                    "[api_programas_dag.py] Programa não encontrado para ID: %s",
                    id_programa,
                )

        if not programas_data:
            raise ValueError("[api_programas_dag.py] Nenhum programa foi extraído")

        # Validacao 12.1: programa no escopo que a API nao devolveu.
        if len(programas_data) < len(ids_alvo):
            logging.warning(
                "[api_programas_dag.py] %d de %d programas do escopo não foram "
                "encontrados na API",
                len(ids_alvo) - len(programas_data),
                len(ids_alvo),
            )

        logging.info(
            "[api_programas_dag.py] Extração concluída com %s registros",
            len(programas_data),
        )
        return programas_data

    @task
    def load_programas_to_postgres(programas_data: list[dict[str, Any]]) -> None:
        logging.info("[api_programas_dag.py] Iniciando carga no PostgreSQL")

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            programas_data,
            table_name=schemas.TABELA_PROGRAMA,
            primary_key=["id_programa"],
            conflict_fields=["id_programa"],
            schema=schemas.SCHEMA_TRANSFEREGOV,
        )

        logging.info(
            "[api_programas_dag.py] Carga concluída com %s registros",
            len(programas_data),
        )

    carga_finalizada = load_programas_to_postgres(fetch_programas())

    trigger_planos_acao = TriggerDagRunOperator(
        task_id="trigger_planos_acao",
        trigger_dag_id="api_planos_acao_dag",
        wait_for_completion=False,
    )

    carga_finalizada >>  trigger_planos_acao


api_programas_dag()
