import logging
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from airflow.decorators import dag, task

from cliente_deputados import ClienteDeputados
from cliente_postgres import ClientPostgresDB
from cliente_senadores import ClienteSenadores
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

CONTROLE_TABLE = "parlamentares_controle"
CONTROLE_SCHEMA = "dados_abertos"

# Helpers


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
              FROM information_schema.tables
             WHERE table_schema = %s
               AND table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(cursor.fetchone()[0])


def _table_has_column(cursor, schema: str, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
              FROM information_schema.columns
             WHERE table_schema = %s
               AND table_name = %s
               AND column_name = %s
        )
        """,
        (schema, table, column),
    )
    return bool(cursor.fetchone()[0])


def _fetch_historico_ids(cursor, schema: str, table: str) -> set[int]:
    if not _table_exists(cursor, schema, table):
        return set()

    cursor.execute(
        f"""
        SELECT DISTINCT CAST(id::text AS BIGINT)
          FROM {schema}.{table}
         WHERE id IS NOT NULL
           AND id::text ~ '^[0-9]+$'
        """
    )
    return {int(row[0]) for row in cursor.fetchall()}


def _clean_existing_historico(
    conn_str: str, schema: str, table: str, records: list[dict]
) -> None:
    """Remove o histórico antigo para evitar duplicação antes do insert em lote."""
    if not records:
        return

    ids = tuple(
        set(item["parlamentar_id"] for item in records if "parlamentar_id" in item)
    )
    if not ids:
        return

    conn = psycopg2.connect(conn_str)
    try:
        with conn.cursor() as cursor:
            if _table_exists(cursor, schema, table):
                cursor.execute(
                    f"DELETE FROM {schema}.{table} WHERE parlamentar_id IN %s", (ids,)
                )
        conn.commit()
    finally:
        conn.close()


# ---


@dag(
    schedule_interval=get_dynamic_schedule("parlamentares_controle_historico_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["MIR", "dados_abertos", "parlamentares", "deputados", "senadores", "historico"],
)
def parlamentares_controle_historico_dag() -> None:  # noqa: C901
    """
    Sincroniza parlamentares atuais e controla extracao de historico por ciclo temporal.
    """

    @task
    def sync_atuais() -> dict[str, list[int]]:
        """Task 1: Busca parlamentares atuais da Câmara e do Senado."""
        logging.info(
            "[parlamentares_controle_historico_dag.py] Iniciando sync de "
            "parlamentares atuais"
        )

        cliente_deputados = ClienteDeputados()
        cliente_senadores = ClienteSenadores()

        deputados = cliente_deputados.get_deputados_atuais()
        senadores = cliente_senadores.get_senadores_atuais()

        if deputados is None:
            raise RuntimeError(
                "Falha ao obter snapshot atual da Camara. "
                "Execucao interrompida para evitar fechamento indevido."
            )

        if not senadores:
            raise RuntimeError(
                "Falha ao obter snapshot atual do Senado (vazio inesperado). "
                "Execucao interrompida para evitar fechamento indevido."
            )

        deputados_ids = {
            int(item["id"])
            for item in deputados
            if isinstance(item, dict) and item.get("id") is not None
        }
        senadores_ids = {
            int(item.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar"))
            for item in senadores
            if isinstance(item, dict)
            and item.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar")
            is not None
        }

        payload = {
            "camara": sorted(list(deputados_ids)),
            "senado": sorted(list(senadores_ids)),
        }

        logging.info(
            f"[parlamentares_controle_historico_dag.py] Sync concluido: "
            f"camara={len(payload['camara'])}, senado={len(payload['senado'])}"
        )
        return payload

    @task
    def state_logic(parlamentares_atuais: dict[str, list[int]]) -> list[dict[str, str]]:
        """Task 2: Mantém a tabela de controle com estados de atividade."""
        conn_str = get_postgres_conn("postgres_mir")

        create_table_sql = f"""
            CREATE SCHEMA IF NOT EXISTS {CONTROLE_SCHEMA};
            CREATE TABLE IF NOT EXISTS {CONTROLE_SCHEMA}.{CONTROLE_TABLE} (
                fonte TEXT NOT NULL,
                parlamentar_id BIGINT NOT NULL,
                status TEXT NOT NULL,
                first_seen_at TIMESTAMP NOT NULL,
                last_seen_at TIMESTAMP NOT NULL,
                last_historico_at TIMESTAMP NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (fonte, parlamentar_id)
            );
        """

        now = datetime.now()

        conn = psycopg2.connect(conn_str)
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)

                cursor.execute(f"SELECT COUNT(*) FROM {CONTROLE_SCHEMA}.{CONTROLE_TABLE}")
                controle_vazio = cursor.fetchone()[0] == 0

                if controle_vazio:
                    historico_camara_ids = _fetch_historico_ids(
                        cursor, "camara_deputados", "deputados"
                    )
                    historico_senado_ids = _fetch_historico_ids(
                        cursor, "senado_federal", "senadores"
                    )

                    for fonte, ids_historicos in (
                        ("camara", historico_camara_ids),
                        ("senado", historico_senado_ids),
                    ):
                        ids_atuais = set(parlamentares_atuais.get(fonte, []))
                        universo = ids_historicos.union(ids_atuais)

                        if not universo:
                            continue

                        values = [
                            (
                                fonte,
                                parlamentar_id,
                                "ATIVO" if parlamentar_id in ids_atuais else "INATIVO",
                                now,
                                now,
                                now,
                            )
                            for parlamentar_id in universo
                        ]

                        psycopg2.extras.execute_values(
                            cursor,
                            f"""
                            INSERT INTO {CONTROLE_SCHEMA}.{CONTROLE_TABLE}
                                (
                                    fonte,
                                    parlamentar_id,
                                    status,
                                    first_seen_at,
                                    last_seen_at,
                                    updated_at
                                )
                            VALUES %s
                            ON CONFLICT (fonte, parlamentar_id)
                            DO UPDATE SET
                                status = EXCLUDED.status,
                                updated_at = EXCLUDED.updated_at
                            """,
                            values,
                        )

                        logging.info(
                            f"[parlamentares_controle_historico_dag.py] "
                            f"Bootstrap realizado para fonte={fonte}: "
                            f"universo={len(universo)}, atuais={len(ids_atuais)}"
                        )

                for fonte in ("camara", "senado"):
                    ids_atuais = [int(v) for v in parlamentares_atuais.get(fonte, [])]

                    if ids_atuais:
                        values = [
                            (fonte, parlamentar_id, "ATIVO", now, now, now)
                            for parlamentar_id in ids_atuais
                        ]
                        psycopg2.extras.execute_values(
                            cursor,
                            f"""
                            INSERT INTO {CONTROLE_SCHEMA}.{CONTROLE_TABLE}
                                (
                                    fonte,
                                    parlamentar_id,
                                    status,
                                    first_seen_at,
                                    last_seen_at,
                                    updated_at
                                )
                            VALUES %s
                            ON CONFLICT (fonte, parlamentar_id)
                            DO UPDATE SET
                                status = EXCLUDED.status,
                                last_seen_at = EXCLUDED.last_seen_at,
                                updated_at = EXCLUDED.updated_at
                            """,
                            values,
                        )

                        cursor.execute(
                            f"""
                            UPDATE {CONTROLE_SCHEMA}.{CONTROLE_TABLE}
                               SET status = 'PENDENTE_FECHAMENTO',
                                   updated_at = %s
                             WHERE fonte = %s
                               AND status = 'ATIVO'
                               AND last_seen_at < %s
                            """,
                            (now, fonte, now),
                        )
                    else:
                        logging.warning(
                            f"[parlamentares_controle_historico_dag.py] Snapshot "
                            f"de atuais vazio para fonte={fonte}. Fechamento "
                            "ignorado nesta execucao."
                        )

                # Evita recarga inicial desnecessaria em parlamentares
                # ja contidos na bronze nativa.
                if _table_exists(
                    cursor, "camara_deputados", "deputados_historico"
                ) and _table_has_column(
                    cursor, "camara_deputados", "deputados_historico", "parlamentar_id"
                ):
                    cursor.execute(
                        f"""
                        UPDATE {CONTROLE_SCHEMA}.{CONTROLE_TABLE} c
                           SET last_historico_at = %s,
                               updated_at = %s
                         WHERE c.fonte = 'camara'
                           AND c.last_historico_at IS NULL
                           AND EXISTS (
                               SELECT 1
                                 FROM camara_deputados.deputados_historico h
                                WHERE h.parlamentar_id::text ~ '^[0-9]+$'
                                  AND CAST(h.parlamentar_id::text AS BIGINT)
                                    = c.parlamentar_id
                           )
                        """,
                        (now, now),
                    )

                if _table_exists(
                    cursor, "senado_federal", "senadores_historico"
                ) and _table_has_column(
                    cursor, "senado_federal", "senadores_historico", "parlamentar_id"
                ):
                    cursor.execute(
                        f"""
                            UPDATE {CONTROLE_SCHEMA}.{CONTROLE_TABLE} c
                            SET last_historico_at = %s,
                                updated_at = %s
                            WHERE c.fonte = 'senado'
                            AND c.last_historico_at IS NULL
                            AND EXISTS (
                                SELECT 1
                                    FROM senado_federal.senadores_historico h
                                    WHERE h.parlamentar_id::text ~ '^[0-9]+$'
                                    AND CAST(h.parlamentar_id::text AS BIGINT)
                                        = c.parlamentar_id
                            )
                        """,
                        (now, now),
                    )

                cursor.execute(
                    f"""
                    SELECT fonte, parlamentar_id, status, last_historico_at
                      FROM {CONTROLE_SCHEMA}.{CONTROLE_TABLE}
                     WHERE (status = 'ATIVO' AND (
                               last_historico_at IS NULL
                               OR last_historico_at <= NOW() - INTERVAL '7 days'
                           ))
                        OR status = 'PENDENTE_FECHAMENTO'
                     ORDER BY
                         CASE WHEN status = 'PENDENTE_FECHAMENTO' THEN 0 ELSE 1 END,
                         COALESCE(last_historico_at, TIMESTAMP '1900-01-01') ASC,
                         parlamentar_id ASC
                    """
                )
                rows = cursor.fetchall()
            conn.commit()
        finally:
            conn.close()

        candidatos = [
            {
                "fonte": row[0],
                "parlamentar_id": str(row[1]),
                "status": row[2],
                "last_historico_at": row[3].isoformat() if row[3] else "",
            }
            for row in rows
        ]

        logging.info(
            f"[parlamentares_controle_historico_dag.py] State logic concluido. "
            f"Parlamentares elegiveis para historico: {len(candidatos)}"
        )
        return candidatos

    @task
    def extrair_historico(candidatos: list[dict[str, str]]) -> None:  # noqa: C901
        """Task 3: Extrai histórico da fonte oficial e injeta na base."""
        if not candidatos:
            logging.info(
                "[parlamentares_controle_historico_dag.py] Nenhum parlamentar "
                "elegivel para historico"
            )
            return

        conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(conn_str)
        cliente_deputados = ClienteDeputados()
        cliente_senadores = ClienteSenadores()

        now = datetime.now()
        historico_camara: list[dict] = []
        historico_senado: list[dict] = []
        status_updates: list[tuple[str, str, int]] = []

        for candidato in candidatos:
            fonte = candidato["fonte"]
            parlamentar_id = int(candidato["parlamentar_id"])
            status = candidato["status"]

            try:
                extracao_ok = False

                if fonte == "camara":
                    dados = cliente_deputados.get_historico_deputado(parlamentar_id)
                    if dados is not None:
                        extracao_ok = True
                        for item in dados:
                            if isinstance(item, dict):
                                item["parlamentar_id"] = parlamentar_id
                                item["fonte"] = fonte
                                item["dt_ingest"] = now.isoformat()
                                historico_camara.append(item)
                else:
                    dados = cliente_senadores.get_filiacoes_senador(parlamentar_id)
                    if dados is not None:
                        extracao_ok = True
                        for item in dados:
                            if isinstance(item, dict):
                                item["parlamentar_id"] = parlamentar_id
                                item["fonte"] = fonte
                                item["dt_ingest"] = now.isoformat()
                                historico_senado.append(item)

                if not extracao_ok:
                    logging.warning(
                        f"[parlamentares_controle_historico_dag.py] Sem confirmação de "
                        f"extracao para fonte={fonte}, parlamentar_id={parlamentar_id}. "
                        "Status mantido."
                    )
                    continue

                novo_status = "INATIVO" if status == "PENDENTE_FECHAMENTO" else "ATIVO"
                status_updates.append((novo_status, fonte, parlamentar_id))
            except Exception as e:
                logging.error(
                    f"[parlamentares_controle_historico_dag.py] Erro ao extrair "
                    f"historico fonte={fonte}, parlamentar_id={parlamentar_id}: {e}"
                )

        if historico_camara:
            _clean_existing_historico(
                conn_str, "camara_deputados", "deputados_historico", historico_camara
            )
            db.insert_data(
                historico_camara,
                table_name="deputados_historico",
                schema="camara_deputados",
            )

        if historico_senado:
            _clean_existing_historico(
                conn_str, "senado_federal", "senadores_historico", historico_senado
            )
            db.insert_data(
                historico_senado,
                table_name="senadores_historico",
                schema="senado_federal",
            )

        if status_updates:
            conn = psycopg2.connect(conn_str)
            try:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_batch(
                        cursor,
                        f"""
                        UPDATE {CONTROLE_SCHEMA}.{CONTROLE_TABLE}
                           SET status = %s,
                               last_historico_at = %s,
                               updated_at = %s
                         WHERE fonte = %s
                           AND parlamentar_id = %s
                        """,
                        [
                            (status, now, now, fonte, parlamentar_id)
                            for status, fonte, parlamentar_id in status_updates
                        ],
                    )
                conn.commit()
            finally:
                conn.close()

        logging.info(
            f"[parlamentares_controle_historico_dag.py] Extração concluida. "
            f"Historico camara={len(historico_camara)}, "
            f"historico senado={len(historico_senado)}, "
            f"status atualizados={len(status_updates)}"
        )

    parlamentares_atuais = sync_atuais()
    candidatos = state_logic(parlamentares_atuais)
    extrair_historico(candidatos)


parlamentares_controle_historico_dag()
