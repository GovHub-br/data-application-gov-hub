import logging
from airflow.decorators import dag, task
from datetime import datetime
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


from typing import Iterator


CHUNK_SIZE = 200


def chunk_list(lst: list, size: int) -> Iterator[list]:
    """Divide uma lista em partes menores (chunks) de tamanho fixo."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus e Gabriel",
        "retries": 0,
        # "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "planos_acao_especiais"],
)
def api_executor_especial_dag() -> None:

    @task
    def fetch_planos_acao() -> list:
        """
        Busca todos os IDs dos planos de ação na base Postgres.
        Cada ID será posteriormente usado para buscar executores na API.
        """
        logging.info("[executor_especial] Buscando planos de ação...")

        db = ClientPostgresDB(get_postgres_conn())

        query = """
            SELECT DISTINCT id_plano_acao
            FROM transferegov_emendas.planos_acao_especiais
        """

        result = db.execute_query(query)

        # Converte lista de tuplas [(123,), (456,)] para [123, 456]
        return [row[0] for row in result]

    @task
    def split_chunks(planos_ids: list) -> list:
        """
        Divide a lista de planos em múltiplos chunks para paralelizar via Airflow.
        Ex.: 48k IDs → 240 tasks (se CHUNK_SIZE = 200)
        """
        return list(chunk_list(planos_ids, CHUNK_SIZE))

    @task
    def process_chunk(chunk: list) -> None:
        """
        Para cada chunk:
        - Busca executores de cada plano de ação na API do TransfereGov
        - Enrica dados com timestamp
        - Remove duplicados
        - Insere no Postgres com UPSERT
        """
        api = ClienteTransfereGov()
        db = ClientPostgresDB(get_postgres_conn())

        timestamp = datetime.now().isoformat()
        all_executores = []

        # Processa cada plano de ação pertencente a este chunk
        for plano_id in chunk:
            logging.info(f"[executor_especial] Buscando executores do plano {plano_id}")

            executores = api.get_all_executores_especiais_by_plano_acao(plano_id)

            if not executores:
                # Não há executores para este plano -> pula
                continue

            # Adiciona metadados aos registros
            for executor in executores:
                executor["id_plano_acao"] = plano_id
                executor["dt_ingest"] = timestamp

            all_executores.extend(executores)

        if not all_executores:
            logging.info("[executor_especial] Chunk sem resultados.")
            return

        logging.info(
            f"[executor_especial] Inserindo {len(all_executores)} executores no Postgres."
        )

        # Remove duplicatas usando a chave (id_plano_acao, id_executor)
        unique = {}
        for row in all_executores:
            key = (row["id_plano_acao"], row["id_executor"])
            unique[key] = row  # se já existir, substitui e mantém apenas 1

        all_executores = list(unique.values())

        # Insere com UPSERT no Postgres
        db.insert_data(
            all_executores,
            table_name="executor_especial",
            conflict_fields=["id_plano_acao", "id_executor"],
            primary_key=["id_plano_acao", "id_executor"],
            schema="transferegov_emendas",
        )

    ids = fetch_planos_acao()  # Busca todos os planos de ação
    chunks = split_chunks(ids)  # Divide em várias listas menores
    process_chunk.expand(chunk=chunks)  # Executa cada chunk em tasks paralelas


# Instancia a DAG para o Airflow carregar
dag_instance = api_executor_especial_dag()
