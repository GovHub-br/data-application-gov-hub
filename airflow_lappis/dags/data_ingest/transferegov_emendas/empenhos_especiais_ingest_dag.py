import logging
from airflow.decorators import dag, task
from datetime import datetime
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


TAMANHO_LOTE = 200


def criar_lotes(dados: list, tamanho: int) -> list[list]:
    """Agrupa os dados em lotes de tamanho fixo para processamento paralelo."""
    return [dados[i : i + tamanho] for i in range(0, len(dados), tamanho)]


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Leonardo",
        "retries": 0,
    },
    tags=["transfere_gov_api", "empenhos_especiais"],
)
def api_empenhos_especiais_dag() -> None:
    """DAG para ingestão de empenhos especiais do TransfereGov."""

    @task
    def obter_planos_acao() -> list:
        """Consulta os IDs dos planos de ação cadastrados no banco."""
        logging.info("[empenhos_especiais] Consultando planos de ação...")

        db = ClientPostgresDB(get_postgres_conn())

        query = """
            SELECT DISTINCT id_plano_acao
            FROM transferegov_emendas.planos_acao_especiais
        """

        resultado = db.execute_query(query)

        # Extrai apenas o ID de cada tupla retornada
        return [linha[0] for linha in resultado] if resultado else []

    @task
    def particionar_planos(planos_ids: list) -> list:
        """Particiona os IDs em lotes menores para execução paralela."""
        return criar_lotes(planos_ids, TAMANHO_LOTE)

    @task
    def processar_empenhos(lote_planos: list) -> None:
        """
        Processa um lote de planos de ação:
        1. Busca empenhos via API para cada plano
        2. Adiciona timestamp de ingestão
        3. Deduplica registros por id_empenho
        4. Persiste no Postgres via UPSERT
        """
        api = ClienteTransfereGov()
        db = ClientPostgresDB(get_postgres_conn())

        timestamp = datetime.now().isoformat()
        empenhos_coletados = []

        for plano_id in lote_planos:
            logging.info(f"[empenhos_especiais] Processando plano {plano_id}")

            empenhos = api.get_all_empenhos_especiais_by_plano_acao(plano_id)

            if not empenhos:
                continue

            for emp in empenhos:
                emp["dt_ingest"] = timestamp

            empenhos_coletados.extend(empenhos)

        if not empenhos_coletados:
            logging.info("[empenhos_especiais] Nenhum empenho encontrado no lote.")
            return

        # Deduplica mantendo apenas a última ocorrência de cada id_empenho
        empenhos_unicos = {emp["id_empenho"]: emp for emp in empenhos_coletados}
        empenhos_finais = list(empenhos_unicos.values())

        logging.info(f"[empenhos_especiais] Persistindo {len(empenhos_finais)} empenhos.")

        db.insert_data(
            empenhos_finais,
            table_name="empenhos_especiais",
            conflict_fields=["id_empenho"],
            primary_key=["id_empenho"],
            schema="transferegov_emendas",
        )

    planos = obter_planos_acao()
    lotes = particionar_planos(planos)
    processar_empenhos.expand(lote_planos=lotes)


dag_instance = api_empenhos_especiais_dag()
