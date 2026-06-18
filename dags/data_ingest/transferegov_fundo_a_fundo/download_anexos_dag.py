import base64
import logging

import requests
from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import TriggerRule
from datetime import datetime, timedelta


URL_BASE_RG = "https://fundos.transferegov.sistema.gov.br/maisbrasil-transferencia-backend/api/public/anexos/rg/"

default_args = {
    "owner": "Wallyson Souza",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="download_anexos_transferegov_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["minc", "transferegov", "extracao", "anexos"],
)
def download_anexos_dag():

    @task
    def buscar_ids_pendentes() -> list:
        """
        Garante a existência da coluna caminho_minio no banco e busca apenas os IDs
        que ainda não possuem esse caminho registrado, fazendo JOIN para descobrir
        o programa (PNAB ou LPG) e definindo o bucket correspondente.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # 1. Garante que a coluna caminho_minio existe na tabela do banco
        alter_table_query = """
        ALTER TABLE transferegov_fundo_a_fundo.anexos_relatorios
        ADD COLUMN IF NOT EXISTS caminho_minio VARCHAR(500);
        """
        pg_hook.run(alter_table_query)
        logging.info("Garantida a existência da coluna 'caminho_minio' na tabela.")

        # 2. Busca os arquivos Excel/ODS não baixados e descobre o bucket via id_programa
        query = """
        SELECT
            ar.id,
            CASE
                WHEN pa.id_programa IN ('46', '47') THEN 'anexos-lpg'
                WHEN pa.id_programa IN ('60', '61', '62') THEN 'anexos-pnab'
                ELSE 'anexos-outros'
            END AS bucket_name
        FROM transferegov_fundo_a_fundo.anexos_relatorios ar
        JOIN transferegov_fundo_a_fundo.relatorios_gestao rg
            ON ar.id_relatorio_gestao = rg.id_relatorio_gestao
        JOIN transferegov_fundo_a_fundo.raw_planos_acao pa
            ON rg.id_plano_acao = pa.id_plano_acao
        WHERE (ar.nome ILIKE '%.xls' OR ar.nome ILIKE '%.xlsx' OR ar.nome ILIKE '%.ods')
          AND ar.caminho_minio IS NULL
        LIMIT 3000;
        """
        records = pg_hook.get_records(query)

        # Cria uma lista de dicionários com as infos do anexo
        ids_para_baixar = [{"id": str(r[0]), "bucket": str(r[1])} for r in records]

        if not ids_para_baixar:
            logging.info("Nenhum arquivo pendente de download encontrado no banco de dados.")
        else:
            logging.info(f"Encontrados {len(ids_para_baixar)} anexos pendentes para download nesta rodada.")

        return ids_para_baixar

    @task(max_active_tis_per_dag=3)
    def baixar_e_salvar_anexos(pendente: dict):
        """
        Processa UM único anexo por invocação (Dynamic Task Mapping).
        Faz o request na API, decodifica o base64, salva no MinIO no bucket
        correspondente e atualiza o banco com o caminho registrado.
        A concorrência é controlada por max_active_tis_per_dag=3 para evitar
        HTTP 429 (rate limit) na API do governo.
        """
        anexo_id = pendente["id"]
        bucket_name = pendente["bucket"]

        # Hooks instanciados dentro da task — cada mapped task tem sua própria conexão
        minio_conn_id = Variable.get("minio_conn_id", default="minio_default")
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        # Verifica e cria o bucket caso ainda não exista
        try:
            if not s3_hook.check_for_bucket(bucket_name):
                s3_hook.create_bucket(bucket_name=bucket_name)
        except Exception as e:
            logging.warning(f"Não foi possível verificar ou criar o bucket '{bucket_name}': {e}")

        url = f"{URL_BASE_RG}{anexo_id}"

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        dados_json = response.json()
        arquivo_base64 = dados_json.get("arquivo")
        nome_arquivo = dados_json.get("nome", "arquivo_sem_nome.bin")

        if not arquivo_base64:
            logging.warning(f"Anexo {anexo_id} não possui a chave 'arquivo' com conteúdo em base64.")
            return

        # Decodifica o base64 para bytes
        arquivo_bytes = base64.b64decode(arquivo_base64)

        # Definindo a chave do arquivo no MinIO
        chave_s3 = f"anexo_{anexo_id}_{nome_arquivo}"

        # Salva os bytes decodificados diretamente no MinIO
        s3_hook.load_bytes(
            bytes_data=arquivo_bytes,
            key=chave_s3,
            bucket_name=bucket_name,
            replace=True,
        )

        # Formata o path final do minio (Ex: anexos-pnab/anexo_ID_Nome.xls)
        path_minio = f"{bucket_name}/{chave_s3}"

        # Atualiza o banco de dados com o caminho do arquivo
        update_query = """
        UPDATE transferegov_fundo_a_fundo.anexos_relatorios
        SET caminho_minio = %s
        WHERE id = %s
        """
        pg_hook.run(update_query, parameters=(path_minio, anexo_id))

        logging.info(f"Anexo {anexo_id} baixado e path ({path_minio}) registrado no BD com sucesso.")

    # ── Task final que dispara a DAG de extração ──
    # Usa ALL_DONE para que o trigger execute mesmo se alguns downloads falharem
    trigger_extracao = TriggerDagRunOperator(
        task_id="trigger_extracao_anexos",
        trigger_dag_id="extracao_anexos_dag",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Fluxo da DAG — Dynamic Task Mapping: cada pendente vira uma task individual
    lista_pendentes = buscar_ids_pendentes()
    baixar_e_salvar_anexos.expand(pendente=lista_pendentes) >> trigger_extracao


# Instancia a DAG
dag_instance = download_anexos_dag()
