#!/bin/bash
set -e

# Role somente-leitura usado pelo +grants do dbt para
# restringir bronze/silver/gold no Postgres local de desenvolvimento.

# Nome e senha vêm do .env (DB_DW_READONLY_ROLE/DB_DW_READONLY_PASSWORD),

DB_DW_READONLY_ROLE="${DB_DW_READONLY_ROLE:-de_postgres}"
DB_DW_READONLY_PASSWORD="${DB_DW_READONLY_PASSWORD:-de_postgres}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	DO \$\$
	BEGIN
	    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${DB_DW_READONLY_ROLE}') THEN
	        CREATE ROLE ${DB_DW_READONLY_ROLE} LOGIN PASSWORD '${DB_DW_READONLY_PASSWORD}';
	    END IF;
	END
	\$\$;

	GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${DB_DW_READONLY_ROLE};
EOSQL
