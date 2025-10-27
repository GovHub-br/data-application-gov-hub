import logging
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras  # Added import for execute_values
from pandas import json_normalize
import pandas as pd
import io


class ClientPostgresDB:
    """Client for interacting with PostgreSQL database."""

    SEPARATOR = "__"
    TYPE_MAP = {int: "BIGINT", float: "NUMERIC", bool: "BOOLEAN"}

    @staticmethod
    def _get_column_type(value: Any) -> str:
        """
        Determine PostgreSQL column type from Python value.

        Args:
            value: Python value to analyze

        Returns:
            PostgreSQL column type as string
        """
        return ClientPostgresDB.TYPE_MAP.get(type(value), "TEXT")

    def _flatten_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Flatten nested JSON data.

        Args:
            data (List[Dict[str, Any]]): List of nested dictionaries.

        Returns:
            List[Dict[str, Any]]: List of flat dictionaries.
        """
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

    def create_table_if_not_exists(
        self,
        sample_data: Dict[str, Any],
        table_name: str,
        primary_key: Optional[List[str]] = None,
        schema: str = "raw",
    ) -> None:
        """Create table dynamically based on data structure.

        Args:
            sample_data: Sample data to determine schema
            table_name: Name of table to create
            primary_key: List of primary key column names
            schema: Database schema name
        """
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
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

    def insert_data(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        conflict_fields: Optional[List[str]] = None,
        primary_key: Optional[List[str]] = None,
        schema: str = "raw",
    ) -> None:
        """Insert data into database table.

        Args:
            data: List of dictionaries to insert
            table_name: Target table name
            conflict_fields: List of column names for conflict resolution
            primary_key: List of primary key column names
            schema: Database schema name
        """
        if not data:
            logging.warning(
                f"[cliente_postgres.py] No data to insert into {schema}.{table_name}"
            )
            return

        self.create_table_if_not_exists(
            data[0], table_name, primary_key=primary_key, schema=schema
        )

        flattened_data = self._flatten_data(data)
        columns = list(flattened_data[0].keys())
        values = [tuple(item.values()) for item in flattened_data]

        sql = f"INSERT INTO {schema}.{table_name} ({', '.join(columns)}) VALUES %s"

        if conflict_fields:
            conflict_str = ", ".join(conflict_fields)
            update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
            sql += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                try:
                    psycopg2.extras.execute_values(cursor, sql, values)
                    conn.commit()
                    logging.info(
                        f"[cliente_postgres.py] Inserted data into {schema}."
                        f"{table_name}"
                    )
                except psycopg2.Error as err:
                    logging.error(
                        f"[cliente_postgres.py] Failed to insert data into {schema}."
                        f"{table_name}. Error: {str(err)}"
                    )
                    raise RuntimeError(
                        f"Failed to insert data into {schema}.{table_name}"
                    ) from err

    def execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        """Execute a query and return the results.

        Args:
            query: SQL query to execute

        Returns:
            List of tuples containing the query results
        """
        logging.info(f"[cliente_postgres.py] Executing query: {query}")
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info(
                    f"[cliente_postgres.py] Query executed successfully, fetched "
                    f"{len(results)} rows"
                )
                return results

    def get_contratos_ids(self, schema: str = "compras_gov") -> List[int]:
        """Extrai todos os IDs de contratos da tabela contratos."""
        query = f"SELECT id FROM {schema}.contratos"

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                contratos_ids = [row[0] for row in cursor.fetchall()]
                return contratos_ids

    def get_id_programas(self) -> List[int]:
        """Extrai todos os IDs de programas da tabela beneficiário."""
        query = "SELECT id_programa FROM transfere_gov.programas"

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                id_programas = [row[0] for row in cursor.fetchall()]
                return id_programas
            
    def get_id_planos_acao(self) -> List[int]:
        """Extrai todos os IDs de planos de ação da tabela de planos de ação."""
        query = "SELECT id_plano_acao FROM transfere_gov.planos_acao"

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                id_planos_acao = [row[0] for row in cursor.fetchall()]
                return id_planos_acao

    def drop_table_if_exists(self, table_name: str, schema: str = "raw") -> None:
        """Remove a tabela se ela existir."""
        conn = psycopg2.connect(self.conn_str)
        cursor = conn.cursor()
        drop_table_query = f"DROP TABLE IF EXISTS {schema}.{table_name};"
        try:
            cursor.execute(drop_table_query)
            conn.commit()
            print(f"Tabela {schema}.{table_name} removida com sucesso.")
        except Exception as e:
            print(f"Erro ao remover a tabela {schema}.{table_name}: {e}")
        finally:
            cursor.close()
            conn.close()

    def insert_csv_data(
        self, csv_data: str, table_name: str, schema: str = "raw"
    ) -> None:
        """
        Insere dados de um CSV no banco de dados, garantindo que a tabela seja criada.
        Se a tabela existir, ela será removida antes da inserção dos novos dados.

        Args:
            csv_data (str): Dados do CSV como string.
            table_name (str): Nome da tabela de destino.
            schema (str): Nome do schema do banco (padrão: "raw").
        """
        # Converte o CSV para DataFrame
        df = pd.read_csv(io.StringIO(csv_data))

        # Converte o DataFrame para lista de dicionários
        data = df.to_dict(orient="records")

        # Remove a tabela existente
        self.drop_table_if_exists(table_name, schema)

        # Insere os novos dados
        self.insert_data(data, table_name, primary_key=None, schema=schema)

    def get_programacao_financeira(self) -> List[Tuple[Any, ...]]:
        """Extrai o numero_programacao e ug_emitente da tabela programacao_financeira.

        Returns:
            List[Tuple[Any, ...]]: Lista de tuplas com numero_programacao e ug_emitente
        """
        query = (
            "SELECT tx_numero_programacao, ug_emitente_programacao "
            "FROM transfere_gov.programacao_financeira"
        )
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                programacao_financeira = cursor.fetchall()
                return programacao_financeira

    def alter_table(
        self, data: Dict[str, Any], table_name: str, schema: str = "raw"
    ) -> None:
        """
        Alter table to add columns that exist in the data but not in the table.
        All new columns will be created as TEXT type.

        Args:
            data: Sample data containing new columns
            table_name: Name of table to alter
            schema: Database schema name
        """
        flattened_data = self._flatten_data([data])[0]
        columns = list(flattened_data.keys())

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                # Get existing columns
                cursor.execute(
                    f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                """
                )
                existing_columns = [row[0] for row in cursor.fetchall()]

                # Add columns that don't exist
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

                conn.commit()
                logging.info(
                    f"[cliente_postgres.py] Table {schema}.{table_name} "
                    f"altered successfully"
                )

    def get_nota_credito(self) -> List[Tuple[Any, ...]]:
        """Extrai o número da nota de crédito e o valor da tabela nota_credito.

        Returns:
            List[Tuple[Any, ...]]: Lista de tuplas com número da nota de crédito e valor
        """
        query = (
            "SELECT cd_ug_emitente_nota, cd_gestao_emitente_nota, tx_numero_nota "
            "FROM transfere_gov.notas_de_credito"
        )

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                nota_credito = cursor.fetchall()
                return nota_credito

    def remove_duplicates(
        self, table_name: str, column_mapping: Dict[int, str], schema: str = "siafi"
    ) -> None:
        """
        Remove duplicados de uma tabela e otimiza a tabela.

        Args:
            table_name (str): Nome da tabela no banco de dados.
            column_mapping (Dict[int, str]): Mapeamento das colunas.
            schema (str): Schema do banco de dados (padrão: "siafi").
        """
        try:
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

            logging.info(
                f"Executando query para remover duplicados em {schema}.{table_name}"
            )

            with psycopg2.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query)
                    conn.commit()
                    logging.info(
                        f"Duplicados removidos com sucesso de {schema}.{table_name}"
                    )

            conn = psycopg2.connect(self.conn_str)
            conn.autocommit = True
            cursor = conn.cursor()
            try:
                cursor.execute(vacuum_query)
                logging.info(
                    f"VACUUM FULL executado com sucesso em {schema}.{table_name}"
                )
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            logging.error(
                f"Erro ao remover duplicados ou otimizar {schema}.{table_name}: {str(e)}"
            )
            raise

    def get_codigo_unidade(self) -> list[dict]:
        """Retorna código da unidade e ordem de grandeza da tabela."""
        query = """
            SELECT codigounidade, ordem_grandeza
            FROM pessoas.unidade_organizacional
        """

        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"codigounidade": int(row[0]), "ordem_grandeza": int(row[1])}
                    for row in rows
                ]

    def execute_non_query(self, query: str) -> None:
        """
        Executa uma query que não retorna resultados (como DDL ou blocos DO $$).

        Args:
            query (str): Comando SQL que não retorna resultados.
        """
        logging.info(f"[cliente_postgres.py] Executando non-query: {query}")
        with psycopg2.connect(self.conn_str) as conn:
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
