import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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

_S3_BUCKET = "gestao-financeira-fundo-a-fundo"
# Endpoints de gestao financeira do Transferegov. O documento nao preve
# essas duas tabelas, mas elas sao do mesmo dominio -- ficam no schema
# transferegov para o banco nao ganhar um quarto schema.
_SCHEMA = schemas.SCHEMA_TRANSFEREGOV


def _get_s3_hook() -> S3Hook:
    """Instancia o S3Hook padrao do repositorio e garante o bucket de destino.

    Mesmo padrao de conexao/criacao de bucket usado em
    ``download_anexos_dag.py``: conn_id vem da Variable ``minio_conn_id``
    (default ``minio_default``), e o bucket e criado sob demanda se ainda
    nao existir.
    """
    minio_conn_id = Variable.get("minio_conn_id", default="minio_default")
    hook = S3Hook(aws_conn_id=minio_conn_id)

    try:
        if not hook.check_for_bucket(_S3_BUCKET):
            hook.create_bucket(bucket_name=_S3_BUCKET)
    except Exception as exc:
        logging.warning(
            "[api_movimentacoes_financeiras_dag.py] Nao foi possivel "
            "verificar ou criar o bucket '%s': %s",
            _S3_BUCKET,
            exc,
        )

    return hook


@dag(
    dag_id="api_movimentacoes_financeiras_dag",
    schedule=get_dynamic_schedule("api_movimentacoes_financeiras_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "gestao_financeira", "raw"],
)
def api_movimentacoes_financeiras_dag() -> None:
    """DAG de ingestao dos endpoints PostgREST ``/gestao_financeira_lancamentos``
    e ``/gestao_financeira_subtransacoes`` do Transferegov Fundo a Fundo.

    Fluxo API -> MinIO -> Postgres, em dois pares extracao/carga sequenciais:

    1. ``extrair_lancamentos_para_minio`` -> ``carregar_lancamentos_no_postgres``
       Busca TODOS os lancamentos financeiros em bloco (paginacao simples
       limit/offset, sem filtro por FK — o endpoint nao possui
       ``id_plano_acao`` no payload, confirmado via Swagger oficial).
       Nao depende de ``raw_planos_acao`` estar carregada. Salva o JSON
       bruto agregado no MinIO e so depois insere em
       ``raw_gestao_financeira_lancamentos`` via ``ClientPostgresDB``. O
       cruzamento com plano de acao (via
       ``cnpj_ente_solicitante_gestao_financeira`` ou pelo endpoint-ponte
       ``/plano_acao_dado_bancario``) fica para uma etapa de transformacao
       posterior, fora do escopo desta DAG de raw.
    2. ``extrair_subtransacoes_para_minio`` -> ``carregar_subtransacoes_no_postgres``
       Encadeada apos a carga dos lancamentos: le de volta os
       ``id_lancamento_gestao_financeira`` recem-persistidos (FK
       confirmada no payload de subtransacoes) e busca as subtransacoes de
       cada um. Mesmo padrao raw -> Postgres.

    Toda a logica de request/paginacao PostgREST vive em
    ``cliente_transferegov_fundo_a_fundo.ClienteTransfereGov`` — esta DAG
    contem apenas orquestracao e movimentacao de dados.
    """

    @task
    def extrair_lancamentos_para_minio() -> dict[str, str]:
        """Busca todos os lancamentos financeiros e salva o JSON bruto
        agregado no MinIO.

        O endpoint /gestao_financeira_lancamentos NAO possui id_plano_acao
        no payload (confirmado via Swagger oficial) — por isso a extracao e
        feita em bloco (paginacao simples), sem depender de
        raw_planos_acao estar carregada e sem filtro por FK. O cruzamento
        com plano de acao (via cnpj_ente_solicitante_gestao_financeira ou
        pelo endpoint-ponte /plano_acao_dado_bancario) fica para uma etapa
        de transformacao posterior, fora do escopo desta DAG de raw.
        """
        api = ClienteTransfereGov()
        lancamentos_data = api.get_lancamentos_financeiros()

        if not lancamentos_data:
            raise ValueError(
                "[api_movimentacoes_financeiras_dag.py] Nenhum lancamento "
                "financeiro foi extraido"
            )

        for lancamento in lancamentos_data:
            lancamento["dt_ingest"] = datetime.now().isoformat()

        logging.info(
            "[api_movimentacoes_financeiras_dag.py] %d lancamentos "
            "financeiros extraidos",
            len(lancamentos_data),
        )

        hook = _get_s3_hook()
        chave_s3 = (
            "lancamentos/gestao_financeira_lancamentos_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        hook.load_string(
            string_data=json.dumps(lancamentos_data, ensure_ascii=False),
            key=chave_s3,
            bucket_name=_S3_BUCKET,
            replace=True,
        )

        logging.info(
            "[api_movimentacoes_financeiras_dag.py] %d lancamentos salvos em "
            "s3://%s/%s",
            len(lancamentos_data),
            _S3_BUCKET,
            chave_s3,
        )

        return {"bucket": _S3_BUCKET, "key": chave_s3}

    @task
    def carregar_lancamentos_no_postgres(info_minio: dict[str, str]) -> None:
        """Le o JSON bruto de lancamentos do MinIO e insere no Postgres."""
        hook = _get_s3_hook()
        conteudo = hook.read_key(key=info_minio["key"], bucket_name=info_minio["bucket"])
        lancamentos_data = json.loads(conteudo)

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            lancamentos_data,
            table_name="raw_gestao_financeira_lancamentos",
            primary_key=["id_lancamento_gestao_financeira"],
            conflict_fields=["id_lancamento_gestao_financeira"],
            schema=_SCHEMA,
        )

        logging.info(
            "[api_movimentacoes_financeiras_dag.py] Carga de lancamentos "
            "concluida com %s registros",
            len(lancamentos_data),
        )

    @task
    def extrair_subtransacoes_para_minio() -> dict[str, str]:
        """Busca subtransacoes de todos os lancamentos ja carregados no
        Postgres e salva o JSON bruto agregado no MinIO."""
        db = ClientPostgresDB(get_postgres_conn())
        ids_lancamentos = db.get_id_lancamentos_financeiros(
            schema=_SCHEMA, table_name="raw_gestao_financeira_lancamentos"
        )

        if not ids_lancamentos:
            raise ValueError(
                "[api_movimentacoes_financeiras_dag.py] Nenhum lancamento "
                "encontrado em raw_gestao_financeira_lancamentos"
            )

        api = ClienteTransfereGov()
        subtransacoes_data: list[dict[str, Any]] = []

        for id_lancamento in ids_lancamentos:
            logging.info(
                "[api_movimentacoes_financeiras_dag.py] Buscando subtransacoes "
                "para lancamento ID: %s",
                id_lancamento,
            )
            subtransacoes = api.get_subtransacoes_by_lancamento(int(id_lancamento))

            if subtransacoes:
                for subtransacao in subtransacoes:
                    subtransacao["dt_ingest"] = datetime.now().isoformat()

                subtransacoes_data.extend(subtransacoes)
                logging.info(
                    "[api_movimentacoes_financeiras_dag.py] Lancamento %s: %d "
                    "subtransacoes encontradas",
                    id_lancamento,
                    len(subtransacoes),
                )
            else:
                logging.warning(
                    "[api_movimentacoes_financeiras_dag.py] Nenhuma subtransacao "
                    "encontrada para lancamento ID: %s",
                    id_lancamento,
                )

        if not subtransacoes_data:
            raise ValueError(
                "[api_movimentacoes_financeiras_dag.py] Nenhuma subtransacao "
                "foi extraida"
            )

        hook = _get_s3_hook()
        chave_s3 = (
            "subtransacoes/gestao_financeira_subtransacoes_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        hook.load_string(
            string_data=json.dumps(subtransacoes_data, ensure_ascii=False),
            key=chave_s3,
            bucket_name=_S3_BUCKET,
            replace=True,
        )

        logging.info(
            "[api_movimentacoes_financeiras_dag.py] %d subtransacoes salvas em "
            "s3://%s/%s",
            len(subtransacoes_data),
            _S3_BUCKET,
            chave_s3,
        )

        return {"bucket": _S3_BUCKET, "key": chave_s3}

    @task
    def carregar_subtransacoes_no_postgres(info_minio: dict[str, str]) -> None:
        """Le o JSON bruto de subtransacoes do MinIO e insere no Postgres."""
        hook = _get_s3_hook()
        conteudo = hook.read_key(key=info_minio["key"], bucket_name=info_minio["bucket"])
        subtransacoes_data = json.loads(conteudo)

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            subtransacoes_data,
            table_name="raw_gestao_financeira_subtransacoes",
            primary_key=["id_subtransacao_gestao_financeira"],
            conflict_fields=["id_subtransacao_gestao_financeira"],
            schema=_SCHEMA,
        )

        logging.info(
            "[api_movimentacoes_financeiras_dag.py] Carga de subtransacoes "
            "concluida com %s registros",
            len(subtransacoes_data),
        )

    info_lancamentos = extrair_lancamentos_para_minio()
    carga_lancamentos = carregar_lancamentos_no_postgres(info_lancamentos)

    info_subtransacoes = extrair_subtransacoes_para_minio()
    carregar_subtransacoes_no_postgres(info_subtransacoes)

    # Subtransacoes dependem dos lancamentos ja estarem persistidos no
    # Postgres (get_id_lancamentos_financeiros le dessa tabela) — por isso a
    # dependencia de ordem explicita, mesmo sem troca de dado via XCom.
    carga_lancamentos >> info_subtransacoes


api_movimentacoes_financeiras_dag()
