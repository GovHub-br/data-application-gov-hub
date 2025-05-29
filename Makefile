export PYTHONPATH := $(CURDIR)/airflow_lappis
export MYPYPATH := $(CURDIR):$(CURDIR)/airflow_lappis/dags:$(CURDIR)/airflow_lappis/helpers:$(CURDIR)/airflow_lappis/plugins

setup:
	pip install poetry==1.8.5
	poetry config virtualenvs.in-project false
	poetry config warnings.export false
	poetry lock
	poetry install --no-root --with dev
	poetry export --without-hashes --format=requirements.txt > requirements.txt
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

test:
	poetry run pytest tests
