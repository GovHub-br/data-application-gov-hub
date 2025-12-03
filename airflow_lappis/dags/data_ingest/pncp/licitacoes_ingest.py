import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
import yaml

from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_pncp import ClientePNCP


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 4),
    catchup=False,
    default_args={
        "owner": "Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pncp", "compras_publicas"],
)
def pncp_publicacoes_dag() -> None:
    """
    DAG para buscar publicações de contratações no PNCP e armazenar no PostgreSQL.
    Pega `pncp_codigo_modalidade_contratacao` e `pncp_cnpj` das Airflow Variables.
    """

    @task
    def fetch_and_store_pncp_publicacoes() -> None:

        orgao_alvo = Variable.get("airflow_orgao", default_var=None)
        if not orgao_alvo:
            logging.error("Variável airflow_orgao não definida!")
            raise ValueError("airflow_orgao não definida")

        orgaos_config_str = Variable.get("airflow_variables", default_var="{}")
        orgaos_config = yaml.safe_load(orgaos_config_str)

        orgao_cfg = orgaos_config.get(orgao_alvo, {})
        cnpj_orgao = orgao_cfg.get("orgao_pncp", [])
        modalidades_list = orgao_cfg.get("modalidade_pncp", [])

        try:
            cnpj_orgao_int = int(cnpj_orgao)
        except ValueError:
            logging.error(
                "[pncp_publicacoes_dag] Variável pncp_codigo_modalidade_contratacao "
                "inválida: %r",
                cnpj_orgao,
            )
            raise

        end_date = datetime.today()
        start_date = end_date - timedelta(weeks=260)

        data_inicial = start_date.strftime("%Y%m%d")
        data_final = end_date.strftime("%Y%m%d")

        logging.info(
            "[pncp_publicacoes_dag] Iniciando coleta | modalidade=%s | cnpj=%s | "
            "janela=[%s, %s]",
            modalidades_list,
            cnpj_orgao_int,
            data_inicial,
            data_final,
        )

        # --- Clientes/API e DB ---
        api = ClientePNCP()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        for cod_modalidade in modalidades_list:

            try:
                cod_modalidade_int = int(cod_modalidade)
            except ValueError:
                logging.error(
                    "[pncp_publicacoes_dag] Variável pncp_codigo_modalidade_contratacao "
                    "inválida: %r",
                    cod_modalidade,
                )
                raise
            # --- Coleta semestral com paginação interna do cliente ---
            registros = api.get_contratacoes_publicacao_semestral(
                data_inicial=data_inicial,
                data_final=data_final,
                codigo_modalidade_contratacao=cod_modalidade_int,
                cnpj=cnpj_orgao_int,
            )

            if not registros:
                logging.warning(
                    "[pncp_publicacoes_dag] Nenhum registro retornado do PNCP."
                )
                pass

            # Enriquecer com dt_ingest
            dt_ingest_iso = datetime.now().isoformat()
            for r in registros:
                # garante que é dict antes de setar a chave
                if isinstance(r, dict):
                    r["dt_ingest"] = dt_ingest_iso

            # --- Persistência ---
            logging.info(
                "[pncp_publicacoes_dag] Inserindo %s registros no schema "
                "pncp.tabela=contratacoes_publicacao",
                len(registros),
            )

            # Se você conhecer a(s) PK(s) de fato, preencha em
            # primary_key/conflict_fields.
            db.insert_data(
                registros,
                table_name="contratacoes_publicacao",
                conflict_fields=["numeroControlePNCP"],
                primary_key=["numeroControlePNCP"],
                schema="pncp",
            )

        logging.info("[pncp_publicacoes_dag] Inserção concluída com sucesso.")

    fetch_and_store_pncp_publicacoes()


dag_instance = pncp_publicacoes_dag()