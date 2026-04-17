export PYTHONPATH := $(CURDIR)/airflow_lappis
export MYPYPATH := $(CURDIR):$(CURDIR)/airflow_lappis/dags:$(CURDIR)/airflow_lappis/helpers:$(CURDIR)/airflow_lappis/plugins

setup:
	@if ! command -v poetry >/dev/null 2>&1; then \
		echo "Poetry não encontrado. Instale antes com pipx install poetry==1.8.5"; \
		exit 1; \
	fi
	poetry self add poetry-plugin-export || true
	poetry config virtualenvs.in-project false
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
