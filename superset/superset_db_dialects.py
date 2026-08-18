"""SQLAlchemy dialect that makes Postgres schema/table reflection grant-aware.

Superset's "+ Dataset" table picker and the SQL Lab schema/table dropdown use
SQLAlchemy's ``get_schema_names``/``get_table_names``/``get_view_names``. The
stock Postgres dialect implements these by querying ``pg_catalog`` directly
(``pg_namespace``/``pg_class``), which Postgres exposes to every authenticated
role regardless of GRANT/REVOKE. In practice this means a read-only role with
zero privilege on a schema (e.g. a "bronze" layer revoked via dbt's
``+grants``) still shows up as a browsable option in Superset, even though any
actual SELECT against it fails.

``information_schema.tables``/``information_schema.schemata`` are already
privilege-aware in Postgres (they only list objects the connected role has
some privilege on), so swapping the reflection queries to use them makes the
picker match reality automatically -- no manual Superset permission upkeep
per table, it just tracks whatever dbt's ``+grants`` currently allow.

Usage: register once (done at import time below) and point a Superset
database connection's SQLAlchemy URI at ``postgresql+infoschema://...``
instead of ``postgresql+psycopg2://...``. Only apply this to connections
where you want browsing restricted to granted objects (e.g. the read-only
"user" connection) -- the admin/owner connection can keep the default driver.
"""

from sqlalchemy import sql
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import reflection


class PGDialectInfoSchema(PGDialect_psycopg2):
    """Postgres dialect where schema/table/view listing honors GRANT/REVOKE."""

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        result = connection.execute(
            sql.text(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY schema_name"
            )
        )
        return [name for name, in result]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute(
            sql.text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
                "ORDER BY table_name"
            ),
            {"schema": schema or self.default_schema_name},
        )
        return [name for name, in result]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        result = connection.execute(
            sql.text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_type = 'VIEW' "
                "ORDER BY table_name"
            ),
            {"schema": schema or self.default_schema_name},
        )
        return [name for name, in result]


registry.register(
    "postgresql.infoschema", "superset_db_dialects", "PGDialectInfoSchema"
)
