import logging
import re
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras
from pandas import json_normalize
import pandas as pd
import io


class ClientPostgresDB:
    """Client for interacting with PostgreSQL database."""

    SEPARATOR = "__"
    TYPE_MAP = {int: "BIGINT", float: "NUMERIC", bool: "BOOLEAN"}

    @staticmethod
    def _get_column_type(value: Any) -> str:
        return ClientPostgresDB.TYPE_MAP.get(type(value), "TEXT")

    @staticmethod
    def _unique_index_name(table_name: str, columns: List[str]) -> str:
        raw = f"uq_{table_name}_{'_'.join(columns)}"
        return re.sub(r"[^\w]", "_", raw)[:63]

    def _flatten_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(
            map(
                lambda d: {
                    str(k): v if type(v) is not list else str(v) for k, v in d.items()
                },
                json_normalize(data, sep=ClientPostgresDB.SEPARATOR).to_dict(
                    orient="records"
                ),
            )
        )

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str
        logging.info(
            f"[cliente_postgres.py] Initialized ClientPostgresDB with conn_str: "
            f"{conn_str}"
        )

    @contextmanager
    def _connect(self):
        """Context manager that guarantees the connection is closed after use.

        psycopg2's native context manager only handles transactions
        (commit/rollback) but does not close the connection.
        """
        conn = psycopg2.connect(self.conn_str)
        try:
            yield conn
        finally:
            conn.close()

    def create_table_if_not_exists(
        self,
        sample_data: Dict[str, Any],
        table_name: str,
        primary_key: Optional[List[str]] = None,
        schema: str = "raw",
        conn=None,
    ) -> None:
        def _execute(connection):
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                logging.info(f"[cliente_postgres.py] Schema {schema} ensured to exist")

                flattened_sample = self._flatten_data([sample_data])[0]
                column_definitions: List[str] = []

                for column in flattened_sample.keys():
                    column_definitions.append(f"{column} TEXT")

                if primary_key:
                    pk_str = ", ".join(primary_key)
                    column_definitions.append(f"PRIMARY KEY ({pk_str})")

                create_table_query = (
                    f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ("
                    f"{', '.join(column_definitions)});"
                )

                try:
                    cursor.execute(create_table_query)
                    logging.info(
                        f"[cliente_postgres.py] Table {schema}.{table_name} created "
                        f"or already exists"
                    )
                except psycopg2.Error as err:
                    logging.error(
                        f"[cliente_postgres.py] Failed to create table {schema}."
                        f"{table_name}. Error: {str(err)}"
                    )
                    raise RuntimeError(
                        f"Failed to create table {schema}.{table_name}"
                    ) from err

        if conn is not None:
            _execute(conn)
        else:
            with self._connect() as new_conn:
                _execute(new_conn)
                new_conn.commit()

    def insert_data(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        conflict_fields: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        schema: str = "raw",
        conn=None,
    ) -> None:
        if not data:
            logging.warning(
                f"[cliente_postgres.py] No data to insert into {schema}.{table_name}"
            )
            return

        flattened_data = self._flatten_data(data)
        columns = list(flattened_data[0].keys())
        column_probe = {col: None for col in columns}

        self.create_table_if_not_exists(
            column_probe, table_name, primary_key=primary_key, schema=schema, conn=conn
        )
        self.alter_table(column_probe, table_name, schema=schema, conn=conn)
        if conflict_fields:
            self.ensure_unique_constraint(
                schema, table_name, conflict_fields, conn=conn
            )

        values = [tuple(item.get(col) for col in columns) for item in flattened_data]

        sql = f"INSERT INTO {schema}.{table_name} ({', '.join(columns)}) VALUES %s"

        if conflict_fields:
            conflict_str = ", ".join(conflict_fields)
            update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
            sql += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"

        def _execute(connection):
            with connection.cursor() as cursor:
                try:
                    psycopg2.extras.execute_values(cursor, sql, values)
                    logging.info(
                        f"[cliente_postgres.py] Inserted data into {schema}.{table_name}"
                    )
                except psycopg2.errors.UndefinedColumn as err:
                    logging.warning(
                        f"[cliente_postgres.py] Missing column detected in {schema}.{table_name}: "
                        f"{err}. Tentando alterar tabela e reinserir."
                    )
                    connection.rollback()
                    column_probe = {col: None for col in columns}
                    self.alter_table(
                        column_probe, table_name, schema=schema, conn=connection
                    )
                    psycopg2.extras.execute_values(cursor, sql, values)
                    logging.info(
                        f"[cliente_postgres.py] Inserted data into {schema}.{table_name} after alter"
                    )
                except psycopg2.Error as err:
                    logging.error(
                        f"[cliente_postgres.py] Failed to insert data into {schema}."
                        f"{table_name}. Error: {str(err)}"
                    )
                    raise RuntimeError(
                        f"Failed to insert data into {schema}.{table_name}"
                    ) from err

        if conn is not None:
            _execute(conn)
        else:
            with self._connect() as new_conn:
                _execute(new_conn)
                new_conn.commit()

    def execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        logging.info(f"[cliente_postgres.py] Executing query: {query}")
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info(
                    f"[cliente_postgres.py] Query executed successfully, fetched "
                    f"{len(results)} rows"
                )
                return results

    def get_contratos_ids(self, schema: str = "compras_gov") -> List[int]:
        query = f"SELECT id FROM {schema}.contratos"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [row[0] for row in cursor.fetchall()]

    def get_id_programas(self) -> List[int]:
        query = "SELECT id_programa FROM transfere_gov.programas"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [row[0] for row in cursor.fetchall()]

    def get_id_planos_acao(self) -> List[int]:
        query = "SELECT id_plano_acao FROM transfere_gov.planos_acao"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [row[0] for row in cursor.fetchall()]

    def drop_table_if_exists(self, table_name: str, schema: str = "raw") -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name};")
                    conn.commit()
                    print(f"Tabela {schema}.{table_name} removida com sucesso.")
                except Exception as e:
                    print(f"Erro ao remover a tabela {schema}.{table_name}: {e}")

    def insert_csv_data(
        self, csv_data: str, table_name: str, schema: str = "raw"
    ) -> None:
        df = pd.read_csv(io.StringIO(csv_data))
        data = df.to_dict(orient="records")
        self.drop_table_if_exists(table_name, schema)
        self.insert_data(data, table_name, primary_key=None, schema=schema)

    def get_programacao_financeira(self) -> List[Tuple[Any, ...]]:
        query = (
            "SELECT tx_numero_programacao, ug_emitente_programacao "
            "FROM transfere_gov.programacao_financeira"
        )
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    def alter_table(
        self,
        data: Dict[str, Any],
        table_name: str,
        schema: str = "raw",
        conn=None,
    ) -> None:
        flattened_data = self._flatten_data([data])[0]
        columns = list(flattened_data.keys())

        def _execute(connection):
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                """
                )
                existing_columns = [row[0] for row in cursor.fetchall()]

                for column in columns:
                    if column not in existing_columns:
                        alter_query = (
                            f"ALTER TABLE {schema}.{table_name} "
                            f"ADD COLUMN IF NOT EXISTS {column} TEXT;"
                        )
                        try:
                            cursor.execute(alter_query)
                            logging.info(
                                f"[cliente_postgres.py] Added column {column} "
                                f"to {schema}.{table_name}"
                            )
                        except psycopg2.Error as e:
                            logging.error(
                                f"[cliente_postgres.py] Failed to add {column} "
                                f"to {schema}.{table_name}. Error: {str(e)}"
                            )

        if conn is not None:
            _execute(conn)
        else:
            with self._connect() as new_conn:
                _execute(new_conn)
                new_conn.commit()

        logging.info(
            f"[cliente_postgres.py] Table {schema}.{table_name} altered successfully"
        )

    def get_nota_credito(self) -> List[Tuple[Any, ...]]:
        query = (
            "SELECT cd_ug_emitente_nota, cd_gestao_emitente_nota, tx_numero_nota "
            "FROM transfere_gov.notas_de_credito"
        )
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()

    def remove_duplicates(
        self, table_name: str, column_mapping: Dict[int, str], schema: str = "siafi"
    ) -> None:
        columns = ", ".join(column_mapping.values())
        delete_query = f"""
        DELETE FROM {schema}.{table_name}
        WHERE ctid NOT IN (
            SELECT MIN(ctid)
            FROM {schema}.{table_name}
            GROUP BY {columns}
        );
        """
        vacuum_query = f"VACUUM {schema}.{table_name};"

        try:
            logging.info(
                f"Executando query para remover duplicados em {schema}.{table_name}"
            )

            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query)
                    conn.commit()
                    logging.info(
                        f"Duplicados removidos com sucesso de {schema}.{table_name}"
                    )
        except Exception as e:
            logging.error(
                f"Erro ao remover duplicados de {schema}.{table_name}: {str(e)}"
            )
            raise

        conn = None
        try:
            conn = psycopg2.connect(self.conn_str)
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(vacuum_query)
                logging.info(f"VACUUM executado com sucesso em {schema}.{table_name}")
        except Exception as e:
            logging.warning(
                "Falha ao executar VACUUM em %s.%s (deduplicacao concluida): %s",
                schema,
                table_name,
                str(e),
            )
        finally:
            if conn:
                conn.close()

    def get_codigo_unidade(self) -> list[dict]:
        query = """
            SELECT codigounidade, ordem_grandeza
            FROM pessoas.unidade_organizacional
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"codigounidade": int(row[0]), "ordem_grandeza": int(row[1])}
                    for row in rows
                ]

    def ensure_unique_constraint(
        self,
        schema: str,
        table_name: str,
        columns: List[str],
        conn=None,
    ) -> None:
        """
        Garante índice UNIQUE para ON CONFLICT quando a tabela já existia
        sem a PK composta correta (CREATE TABLE IF NOT EXISTS não altera constraints).
        """
        if not columns:
            return

        index_name = self._unique_index_name(table_name, columns)
        cols_sql = ", ".join(columns)
        query = (
            f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
            f"ON {schema}.{table_name} ({cols_sql});"
        )

        def _execute(connection):
            with connection.cursor() as cursor:
                try:
                    cursor.execute(query)
                    logging.info(
                        "[cliente_postgres.py] Unique index %s on %s.%s (%s)",
                        index_name,
                        schema,
                        table_name,
                        cols_sql,
                    )
                except psycopg2.Error as err:
                    logging.error(
                        "[cliente_postgres.py] Failed to create unique index on "
                        "%s.%s: %s",
                        schema,
                        table_name,
                        err,
                    )
                    raise RuntimeError(
                        f"Failed to ensure unique constraint on {schema}.{table_name}"
                    ) from err

        if conn is not None:
            _execute(conn)
        else:
            with self._connect() as new_conn:
                _execute(new_conn)
                new_conn.commit()

    def execute_non_query(self, query: str) -> None:
        logging.info(f"[cliente_postgres.py] Executando non-query: {query}")
        with self._connect() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query)
                    conn.commit()
                    logging.info("[cliente_postgres.py] Non-query executado com sucesso")
                except psycopg2.Error as e:
                    logging.error(
                        f"[cliente_postgres.py] Erro ao executar non-query. Erro: {e}"
                    )
                    raise RuntimeError("Erro ao executar comando SQL sem retorno") from e

    def apply_comments(
        self,
        schema: str,
        table_name: str,
        table_comment: str | None = None,
        column_comments: dict[str, str] | None = None,
    ) -> None:
        """Aplica COMMENT ON TABLE e COMMENT ON COLUMN no PostgreSQL."""
        queries: list[str] = []
        tabela = f"{schema}.{table_name}"

        if table_comment:
            desc = table_comment.replace("'", "''")
            queries.append(f"COMMENT ON TABLE {tabela} IS '{desc}';")

        for coluna, descricao in (column_comments or {}).items():
            if not descricao:
                continue
            desc = descricao.replace("'", "''")
            queries.append(f"COMMENT ON COLUMN {tabela}.{coluna} IS '{desc}';")

        for query in queries:
            self.execute_non_query(query)

    def get_dashboard_kpis(self) -> Dict[str, int]:
        query = "SELECT kpi, valor FROM pessoas.kpis_servidores"
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return {row[0]: row[1] for row in cursor.fetchall()}

    def get_dashboard_genero(self) -> Dict[str, float]:
        query = """
            SELECT
                genero,
                ROUND(percentual_distribuicao * 100, 1) as percentual
            FROM pessoas.distribuicao_genero
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                genero_data = {}
                for row in cursor.fetchall():
                    genero = row[0].lower() if row[0] else "n/a"
                    genero_data[f"{genero}_percent"] = float(row[1])
                return genero_data

    def get_dashboard_raca_cor(self) -> List[Dict[str, Any]]:
        query = """
            SELECT
                COALESCE(cor_raca, 'NÃO DECLARADA') as nome_cor,
                quantidade_servidores as valor
            FROM pessoas.distribuicao_raca_cor
            ORDER BY quantidade_servidores DESC
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [
                    {"nome_cor": row[0], "valor": row[1]} for row in cursor.fetchall()
                ]

    def get_dashboard_situacao_funcional(self) -> List[Dict[str, Any]]:
        query = """
            SELECT
                situacao_funcional_original as label,
                quantidade_servidores as valor
            FROM pessoas.distribuicao_situacao_funcional
            ORDER BY quantidade_servidores DESC
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [{"label": row[0], "valor": row[1]} for row in cursor.fetchall()]

    def get_dashboard_mapa_uf(self) -> Dict[str, Dict[str, Any]]:
        query = """
            SELECT
                sigla_uf,
                nome_uf,
                valor,
                percentual
            FROM pessoas.distribuicao_mapa_uf
            ORDER BY sigla_uf
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return {
                    row[0]: {"nome": row[1], "valor": row[2], "percentual": row[3]}
                    for row in cursor.fetchall()
                }

    def get_dashboard_tabela_servidores(self, limit: int = 100) -> List[Dict[str, Any]]:
        query = """
            SELECT
                cargo,
                genero,
                situacao,
                cidade,
                estado,
                total
            FROM pessoas.tabela_servidores_agregada
            ORDER BY total DESC
            LIMIT %s
        """
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (limit,))
                return [
                    {
                        "cargo": row[0],
                        "genero": row[1],
                        "situacao": row[2],
                        "cidade": row[3],
                        "estado": row[4],
                        "total": row[5],
                    }
                    for row in cursor.fetchall()
                ]
