export PYTHONPATH := $(CURDIR)/airflow_lappis
export MYPYPATH := $(CURDIR):$(CURDIR)/airflow_lappis/dags:$(CURDIR)/airflow_lappis/helpers:$(CURDIR)/airflow_lappis/plugins

AIRFLOW_SERVICE ?= airflow
AIRFLOW_LOCAL_ORGAO ?= ipea
AIRFLOW_LOCAL_DB_HOST ?= postgres
AIRFLOW_LOCAL_DB_NAME ?= postgres
AIRFLOW_LOCAL_DB_USER ?= postgres
AIRFLOW_LOCAL_DB_PASSWORD ?= postgres
AIRFLOW_LOCAL_DB_PORT ?= 5432

setup:
	@if ! command -v poetry >/dev/null 2>&1; then \
		echo "Poetry não encontrado. Instale antes com pipx install poetry==1.8.5"; \
		exit 1; \
	fi
	@if [ ! -f .env ]; then \
		if [ -f local.env ]; then \
			cp local.env .env; \
			echo ".env criado a partir de local.env"; \
		else \
			echo "local.env não encontrado. Crie o .env manualmente."; \
			exit 1; \
		fi; \
	fi
	poetry self add poetry-plugin-export || true
	poetry config virtualenvs.in-project false
	poetry lock
	poetry install --no-root --with dev
	poetry export --without-hashes --format=requirements.txt > requirements.generated.txt
	bash setup-git-hooks.sh
	docker compose up -d --build
	$(MAKE) dev
	$(MAKE) dev-check

format:
	poetry run black .
	poetry run ruff check --fix .
	poetry run sqlfmt ./airflow_lappis/dags/dbt

lint:
	poetry run black . --check
	poetry run ruff check .
	poetry run mypy . --explicit-package-bases --install-types --non-interactive
	poetry run sqlfmt ./airflow_lappis/dags/dbt --check
	[ "${GITLAB_CI}" ] || poetry run sqlfluff lint ./airflow_lappis/dags/dbt

lint-ci:
	poetry run sqlfmt ./airflow_lappis/dags/dbt --check
	poetry run sqlfluff lint ./airflow_lappis/dags/dbt --config .sqlfluff.ci --ignore templating

test:
	poetry run pytest tests
	@echo ""
	@echo "Coverage HTML: $(CURDIR)/htmlcov/index.html"

dev:
	@docker compose ps --status running $(AIRFLOW_SERVICE) >/dev/null 2>&1 || (echo "Serviço '$(AIRFLOW_SERVICE)' não está em execução. Rode: docker compose up -d" && exit 1)
	@echo "Aguardando Airflow/DB ficarem prontos..."
	@docker compose exec -T $(AIRFLOW_SERVICE) sh -c 'for i in $$(seq 1 30); do airflow db init >/dev/null 2>&1 && exit 0; sleep 2; done; echo "Airflow DB não ficou pronto a tempo para inicializar."; exit 1'
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow variables set airflow_orgao '$(AIRFLOW_LOCAL_ORGAO)'
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow variables set airflow_variables '{"ipea":{"codigos_ug":[113601,113602]},"unb":{"codigos_ug":[154040]},"ibama":{"codigos_ug":[440001,440048,440050]},"mgi":{"codigos_ug":[201082]}}'
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow variables set dynamic_schedules '{"empenhos_tesouro_ingest_dag":{"type":"cron","value":"0 13 * * 1-6"},"nc_tesouro_ingest_dag":{"type":"cron","value":"0 13 * * 1-6"},"pf_tesouro_ingest_dag":{"type":"cron","value":"0 13 * * 1-6"},"visao_orcamentaria_ingest":{"type":"cron","value":"0 13 * * 1-6"}}'
	@docker compose exec -T $(AIRFLOW_SERVICE) sh -c "printf '%s\n' '{\"postgres_default\":{\"conn_type\":\"postgres\",\"host\":\"$(AIRFLOW_LOCAL_DB_HOST)\",\"schema\":\"$(AIRFLOW_LOCAL_DB_NAME)\",\"login\":\"$(AIRFLOW_LOCAL_DB_USER)\",\"password\":\"$(AIRFLOW_LOCAL_DB_PASSWORD)\",\"port\":$(AIRFLOW_LOCAL_DB_PORT)}}' > /tmp/airflow-connections.json && airflow connections import --overwrite /tmp/airflow-connections.json && rm -f /tmp/airflow-connections.json"
	@echo "Ambiente local do Airflow configurado com sucesso."

dev-check:
	@docker compose ps --status running $(AIRFLOW_SERVICE) >/dev/null 2>&1 || (echo "Serviço '$(AIRFLOW_SERVICE)' não está em execução. Rode: docker compose up -d" && exit 1)
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow variables get airflow_orgao >/dev/null
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow variables get airflow_variables >/dev/null
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow variables get dynamic_schedules >/dev/null
	@docker compose exec -T $(AIRFLOW_SERVICE) airflow connections get postgres_default >/dev/null
	@echo "Validação concluída: variables e connection do Airflow estão configuradas."
