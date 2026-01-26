import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_pncp import ClientePNCP


def padronizar_colunas_json(lista_de_dicts: list[dict]) -> list[dict]:
    """
    Padroniza uma lista de dicionários para garantir que todos tenham as mesmas chaves.
    Caso alguma coluna esteja ausente, será preenchida com None (null no banco).

    Args:
        lista_de_dicts: Lista de dicionários JSON já flattenizados.

    Returns:
        Lista de dicionários padronizados com todas as chaves presentes.
    """
    todas_as_chaves: set[str] = set()
    for item in lista_de_dicts:
        todas_as_chaves.update(item.keys())

    for item in lista_de_dicts:
        for chave in todas_as_chaves:
            item.setdefault(chave, None)

    return lista_de_dicts


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 5),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pncp", "compras_publicas", "itens_resultados"],
)
def pncp_contratacoes_itens_resultados_dag() -> None:
    """
    DAG responsável por buscar **ITENS** e **RESULTADOS** das contratações no PNCP.

    Fluxo:
      1. Ler da tabela `pncp.contratacoes_publicacao` todos os valores de
         `numeroControlePNCP`.
      2. Para cada controle encontrado, chamar a API PNCP para obter:
         - Lista de itens de contratação
         - Lista de resultados de cada item
      3. Persistir os dados em duas tabelas distintas no schema `pncp`:
         - `pncp.contratacoes_itens`
         - `pncp.contratacoes_resultados`

    Observações:
    - Cada registro recebe o campo `dt_ingest` com a data/hora da ingestão.
    - Conflitos são resolvidos via upsert (`ON CONFLICT`).
    """

    @task
    def fetch_and_store_itens_resultados() -> None:
        # --- Configuração de clientes ---
        api = ClientePNCP()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        dt_ingest_iso = datetime.now().isoformat()

        # --- Obter lista de numeroControlePNCP ---
        try:
            lista_tuplas = db.execute_query(
                "SELECT numerocontrolepncp FROM pncp.contratacoes_publicacao"
            )
        except Exception as e:
            logging.error(
                "[pncp_itens_resultados_dag] Erro ao buscar numeroControlePNCP: %s", e
            )
            raise

        lista_controles = [t[0] for t in lista_tuplas if t and t[0]]

        logging.info(
            "[pncp_itens_resultados_dag] Iniciando coleta | total_controles=%d",
            len(lista_controles),
        )

        # --- Coleta na API ---
        try:
            itens, resultados_brutos = api.get_itens_e_resultados(lista_controles)
            #            itens_flat = db._flatten_data(itens)
            resultados_flat = db._flatten_data(resultados_brutos)
            #            itens = padronizar_colunas_json(itens_flat)
            resultados = padronizar_colunas_json(resultados_flat)
        except Exception as e:
            logging.error(
                "[pncp_itens_resultados_dag] Erro ao coletar dados da API PNCP: %s", e
            )
            raise

        # --- Enriquecer com dt_ingest ---
        for r in itens:
            if isinstance(r, dict):
                r["dt_ingest"] = dt_ingest_iso
        for r in resultados:
            if isinstance(r, dict):
                r["dt_ingest"] = dt_ingest_iso

        # --- Persistência ---
        if itens:
            db.insert_data(
                itens,
                table_name="contratacoes_itens",
                schema="pncp",
                conflict_fields=["numeroItem", "numeroControlePNCP"],
                primary_key=["numeroItem", "numeroControlePNCP"],
            )
            logging.info("[pncp_itens_resultados_dag] Inseridos %d itens.", len(itens))
        else:
            logging.warning(
                "[pncp_itens_resultados_dag] Nenhum item retornado para inserção."
            )

        if resultados:
            db.insert_data(
                resultados,
                table_name="contratacoes_resultados",
                schema="pncp",
                conflict_fields=[
                    "numeroItem",
                    "numeroControlePNCPCompra",
                    "sequencialResultado",
                    "situacaoCompraItemResultadoId",
                ],
                primary_key=[
                    "numeroItem",
                    "numeroControlePNCPCompra",
                    "sequencialResultado",
                    "situacaoCompraItemResultadoId",
                ],
            )
            logging.info(
                "[pncp_itens_resultados_dag] Inseridos %d resultados.", len(resultados)
            )
        else:
            logging.warning(
                "[pncp_itens_resultados_dag] Nenhum resultado retornado para inserção."
            )

        logging.info("[pncp_itens_resultados_dag] Processo concluído com sucesso.")

    fetch_and_store_itens_resultados()


dag_instance = pncp_contratacoes_itens_resultados_dag()
