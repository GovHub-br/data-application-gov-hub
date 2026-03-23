export PYTHONPATH := $(CURDIR)/airflow_lappis
export MYPYPATH := $(CURDIR):$(CURDIR)/airflow_lappis/dags:$(CURDIR)/airflow_lappis/helpers:$(CURDIR)/airflow_lappis/plugins

setup:
	pip install poetry==1.8.5
	poetry config virtualenvs.in-project false
	poetry config warnings.export false
	poetry lock
	poetry install --no-root --with dev
	poetry export --without-hashes --format=requirements.txt > requirements.generated.txt
	bash setup-git-hooks.sh

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

superset-export:
	@echo "Exporting all Superset dashboards to superset/exports/dashboards.json ..."
	@mkdir -p superset/exports
	docker compose exec superset superset export-dashboards -f /app/superset_home/exports/dashboards.json
	@echo "Done. Commit superset/exports/dashboards.json to preserve your dashboards."

superset-import:
	@echo "Importing Superset dashboards from superset/exports/ ..."
	docker compose exec superset bash -c "\
		for f in /app/superset_home/exports/*.json /app/superset_home/exports/*.zip; do \
			[ -f \"\$$f\" ] || continue; \
			echo \"Importing \$$f ...\"; \
			superset import-dashboards -p \"\$$f\" || echo \"Warning: could not import \$$f\"; \
		done"
	@echo "Import complete."
