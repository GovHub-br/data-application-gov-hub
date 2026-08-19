import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk import Variable

from cliente_postgres import ClientPostgresDB
from extracao_ibge import ClienteIBGE
from postgres_helpers import get_postgres_conn

logger = logging.getLogger(__name__)

SCHEMA = "ibge_sidra"

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


_IBGE_CONFIG_DEFAULTS: dict[str, Any] = {
    "batch_size": 50,
    "localidades_nivel": "N3",
    "resultados_agregados_ids": [],
    "resultados_periodos": "ultimo",
    "resultados_nivel": "N3",
}


def _get_config() -> dict[str, Any]:
    """Lê a variável ``ibge_config`` e mescla com os defaults."""
    config = Variable.get("ibge_config", default={}, deserialize_json=True)
    return {**_IBGE_CONFIG_DEFAULTS, **config}


def _batch(lista: list, tamanho: int) -> list[list]:
    """Divide uma lista em sublistas de tamanho fixo."""
    return [lista[i : i + tamanho] for i in range(0, len(lista), tamanho)]


@dag(
    dag_id="ibge_agregados_dag",
    schedule="0 3 * * 0",  # toda semana às 3h de domingo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["extração", "ibge", "agregados", "raw"],
    doc_md=__doc__,
)
def ibge_agregados_dag() -> None:

    # ------------------------------------------------------------------
    # Task 1 – Catálogo: pesquisas e agregados
    # ------------------------------------------------------------------

    @task
    def extrair_catalogo() -> list[dict[str, Any]]:
        """Extrai GET /agregados e carrega pesquisas + catálogo de agregados."""
        api = ClienteIBGE()

        pesquisas = api.get_pesquisas()
        if not pesquisas:
            raise ValueError("[ibge_dag] Nenhuma pesquisa retornada da API")

        db = ClientPostgresDB(get_postgres_conn())

        # Tabela de pesquisas (estrutura de grupos)
        import json

        pesquisas_planas = [
            {
                "id": p.get("id"),
                "nome": p.get("nome"),
                "agregados": json.dumps(p.get("agregados", []), ensure_ascii=False),
            }
            for p in pesquisas
        ]
        db.drop_table_if_exists("pesquisas", schema=SCHEMA)
        db.insert_data(pesquisas_planas, table_name="pesquisas", schema=SCHEMA)
        logger.info("[ibge_dag] pesquisas carregadas: %s", len(pesquisas_planas))

        # Tabela plana de todos os agregados (reusa pesquisas já buscadas)
        agregados = api.listar_agregados(pesquisas=pesquisas)
        db.drop_table_if_exists("agregados_catalogo", schema=SCHEMA)
        db.insert_data(agregados, table_name="agregados_catalogo", schema=SCHEMA)
        logger.info("[ibge_dag] agregados_catalogo carregados: %s", len(agregados))

        return agregados

    # ------------------------------------------------------------------
    # Task 2 – Prepara batches para extração paralela
    # ------------------------------------------------------------------

    @task
    def preparar_batches(catalogo: list[dict[str, Any]]) -> list[list[str]]:
        """Agrupa IDs de agregados em batches para o dynamic task mapping."""
        batch_size = int(_get_config()["batch_size"])
        ids = [str(agr["id"]) for agr in catalogo]
        batches = _batch(ids, batch_size)
        logger.info(
            "[ibge_dag] %s IDs → %s batches de %s",
            len(ids),
            len(batches),
            batch_size,
        )
        return batches

    # ------------------------------------------------------------------
    # Tasks dinâmicas – metadados, períodos, variáveis, localidades
    # ------------------------------------------------------------------

    @task(max_active_tis_per_dagrun=4)
    def extrair_metadados(batch: list[str]) -> None:
        """GET /agregados/{id}/metadados para cada ID do batch."""
        api = ClienteIBGE()
        dados = api.get_metadados_batch(batch)
        if not dados:
            logger.warning("[ibge_dag] metadados: batch sem resultados %s", batch[:3])
            return

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            dados,
            table_name="metadados",
            schema=SCHEMA,
            primary_key=["agregado_id"],
            conflict_fields=["agregado_id"],
        )
        logger.info("[ibge_dag] metadados inseridos: %s registros", len(dados))

    @task(max_active_tis_per_dagrun=4)
    def extrair_periodos(batch: list[str]) -> None:
        """GET /agregados/{id}/periodos para cada ID do batch."""
        api = ClienteIBGE()
        dados = api.get_periodos_batch(batch)
        if not dados:
            logger.warning("[ibge_dag] periodos: batch sem resultados %s", batch[:3])
            return

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(dados, table_name="periodos", schema=SCHEMA)
        logger.info("[ibge_dag] periodos inseridos: %s registros", len(dados))

    @task(max_active_tis_per_dagrun=4)
    def extrair_variaveis(batch: list[str]) -> None:
        """GET /agregados/{id}/variaveis para cada ID do batch."""
        api = ClienteIBGE()
        dados = api.get_variaveis_batch(batch)
        if not dados:
            logger.warning("[ibge_dag] variaveis: batch sem resultados %s", batch[:3])
            return

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(dados, table_name="variaveis", schema=SCHEMA)
        logger.info("[ibge_dag] variaveis inseridas: %s registros", len(dados))

    @task(max_active_tis_per_dagrun=4)
    def extrair_localidades(batch: list[str]) -> None:
        """GET /agregados/{id}/localidades/{nivel} para cada ID do batch."""
        nivel = _get_config()["localidades_nivel"]
        api = ClienteIBGE()
        dados = api.get_localidades_batch(batch, nivel=nivel)
        if not dados:
            logger.warning("[ibge_dag] localidades: batch sem resultados %s", batch[:3])
            return

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(dados, table_name="localidades", schema=SCHEMA)
        logger.info("[ibge_dag] localidades inseridas: %s registros", len(dados))

    # ------------------------------------------------------------------
    # Task de resultados (séries) – apenas para agregados configurados
    # ------------------------------------------------------------------

    @task
    def extrair_resultados() -> None:
        """GET /agregados/{id}/periodos/{per}/variaveis/{var} para IDs configurados.

        Ativada somente quando ``ibge_resultados_agregados_ids`` estiver
        preenchida com ao menos um ID. Caso contrário, ignora silenciosamente.
        """
        cfg = _get_config()
        ids_alvo: list[str] = cfg["resultados_agregados_ids"]
        if not ids_alvo:
            logger.info(
                "[ibge_dag] resultados: 'resultados_agregados_ids' vazia em "
                "ibge_config — task ignorada"
            )
            return

        periodos = cfg["resultados_periodos"]
        nivel = cfg["resultados_nivel"]

        api = ClienteIBGE()
        db = ClientPostgresDB(get_postgres_conn())

        total = 0
        for agregado_id in ids_alvo:
            dados = api.get_resultados_achatar(
                agregado_id=agregado_id,
                periodos=periodos,
                nivel=nivel,
            )
            if dados:
                db.insert_data(dados, table_name="resultados", schema=SCHEMA)
                total += len(dados)
                logger.info(
                    "[ibge_dag] resultados: agregado=%s → %s linhas", agregado_id, len(dados)
                )
            else:
                logger.warning(
                    "[ibge_dag] resultados: agregado=%s sem dados", agregado_id
                )

        logger.info("[ibge_dag] resultados: total inserido = %s linhas", total)

    # ------------------------------------------------------------------
    # Grafo da DAG
    # ------------------------------------------------------------------

    catalogo = extrair_catalogo()
    batches = preparar_batches(catalogo)

    metadados_done = extrair_metadados.expand(batch=batches)
    periodos_done = extrair_periodos.expand(batch=batches)
    variaveis_done = extrair_variaveis.expand(batch=batches)
    localidades_done = extrair_localidades.expand(batch=batches)

    resultados_done = extrair_resultados()

    # Resultados só corre após o catálogo estar carregado
    catalogo >> resultados_done


ibge_agregados_dag()
