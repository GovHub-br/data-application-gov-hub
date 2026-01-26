import logging
from airflow.decorators import dag, task
from datetime import datetime
from typing import Iterator

# Imports dos seus módulos personalizados
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


CHUNK_SIZE = 200


def chunk_list(lst: list, size: int) -> Iterator[list]:
    """Divide uma lista em partes menores (chunks) de tamanho fixo."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def _fetch_relatorios_for_plano(
    api: ClienteTransfereGov, plano_id: int, timestamp: str
) -> list:
    """Busca relatórios de um plano e adiciona metadados."""
    logging.info(f"[relatorio_gestao] Buscando relatórios do plano {plano_id}")

    relatorios = api.get_all_relatorio_gestao_especial_by_plano_acao(plano_id)

    if not relatorios:
        return []

    # Adiciona metadados aos registros
    for item in relatorios:
        # Garante que o ID do plano está no registro
        # (caso a API não retorne no corpo)
        item["id_plano_acao"] = plano_id
        item["dt_ingest"] = timestamp

    return relatorios


def _remove_duplicates(all_relatorios: list) -> list:
    """Remove duplicatas usando id_relatorio_gestao como chave."""
    unique = {}
    for row in all_relatorios:
        # Baseado na imagem do Swagger, a PK parece ser
        # id_relatorio_gestao. Se quiser garantir unicidade por plano,
        # pode usar tupla (id_plano_acao, id_relatorio_gestao)
        key = row.get("id_relatorio_gestao")
        if key:
            unique[key] = row
        else:
            # Fallback caso venha sem ID (improvável, mas seguro)
            logging.warning(f"Registro sem id_relatorio_gestao encontrado: {row}")

    return list(unique.values())


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 0,
        # "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "relatorio_gestao_especial"],
)
def api_relatorio_gestao_especial_dag() -> None:

    @task
    def setup_table() -> None:
        """
        Cria a tabela antes de qualquer processamento paralelo.
        Evita race conditions nas tasks paralelas.
        """
        logging.info("[relatorio_gestao] Configurando tabela...")

        db = ClientPostgresDB(get_postgres_conn())

        # Remove a tabela antiga se existir (para garantir estrutura correta)
        db.drop_table_if_exists(
            table_name="relatorio_gestao_especial", schema="transferegov_emendas"
        )

        # Cria tabela com TODOS os campos que virão da API
        # Baseado na estrutura real retornada pela API TransfereGov
        sample_data = {
            "id_relatorio_gestao": "0",
            "situacao_relatorio_gestao": "",
            "parecer_relatorio_gestao": "",
            "id_plano_acao": "0",
            "dt_ingest": datetime.now().isoformat(),
        }

        db.create_table_if_not_exists(
            sample_data,
            table_name="relatorio_gestao_especial",
            primary_key=["id_relatorio_gestao"],
            schema="transferegov_emendas",
        )

        logging.info("[relatorio_gestao] Tabela configurada com sucesso")

    @task
    def fetch_planos_acao() -> list:
        """
        Busca todos os IDs dos planos de ação na base Postgres.
        Usa a mesma tabela base da DAG anterior para obter os IDs.
        """
        logging.info("[relatorio_gestao] Buscando planos de ação...")

        db = ClientPostgresDB(get_postgres_conn())

        # Mantive a mesma query da DAG anterior, pois o filtro é por plano de ação
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
        """
        return list(chunk_list(planos_ids, CHUNK_SIZE))

    @task
    def process_chunk(chunk: list) -> None:
        """
        Para cada chunk:
        - Busca relatórios de gestão de cada plano de ação na API
        - Enrica dados com timestamp e id_plano_acao
        - Remove duplicados
        - Insere no Postgres com UPSERT
        """
        api = ClienteTransfereGov()
        db = ClientPostgresDB(get_postgres_conn())

        timestamp = datetime.now().isoformat()
        all_relatorios = []

        # Processa cada plano de ação pertencente a este chunk
        for plano_id in chunk:
            relatorios = _fetch_relatorios_for_plano(api, plano_id, timestamp)
            all_relatorios.extend(relatorios)

        if not all_relatorios:
            logging.info("[relatorio_gestao] Chunk sem resultados.")
            return

        logging.info(
            f"[relatorio_gestao] Inserindo {len(all_relatorios)} registros no Postgres."
        )

        all_relatorios_unique = _remove_duplicates(all_relatorios)

        if not all_relatorios_unique:
            return

        # Insere com UPSERT no Postgres
        db.insert_data(
            all_relatorios_unique,
            table_name="relatorio_gestao_especial",
            # Define o conflito na PK da tabela
            conflict_fields=["id_relatorio_gestao"],
            primary_key=["id_relatorio_gestao"],
            schema="transferegov_emendas",
        )

    setup = setup_table()  # Cria a tabela primeiro
    ids = fetch_planos_acao()  # Busca todos os planos de ação
    chunks = split_chunks(ids)  # Divide em várias listas menores

    # Garante que a tabela é criada antes do processamento paralelo
    setup >> ids
    process_chunk.expand(chunk=chunks)  # Executa cada chunk em tasks paralelas


# Instancia a DAG
dag_instance = api_relatorio_gestao_especial_dag()
