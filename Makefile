COMPOSE_FILE := infra/docker-compose.yml

export PYTHONPATH := $(CURDIR)/dags:$(CURDIR)/plugins:$(CURDIR)/helpers
export MYPYPATH := $(CURDIR):$(CURDIR)/dags:$(CURDIR)/helpers:$(CURDIR)/plugins

.PHONY: setup format lint lint-ci test compose-config up down logs-airflow \
        docs-setup docs-collect docs-build docs-serve docs-clean

DOCS_DIR := docs-pages
DOCS_VENV := $(DOCS_DIR)/.venv
DOCS_PY := $(DOCS_VENV)/bin/python

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
	poetry run sqlfmt ./dbt

lint:
	poetry run black . --check
	poetry run ruff check .
	poetry run mypy . --explicit-package-bases --install-types --non-interactive
	poetry run sqlfmt ./dbt --check
	[ "${GITLAB_CI}" ] || poetry run sqlfluff lint ./dbt

lint-ci:
	poetry run sqlfmt ./dbt --check
	poetry run sqlfluff lint ./dbt --config .sqlfluff.ci --ignore templating

test:
	poetry run pytest tests

compose-config:
	docker compose -f $(COMPOSE_FILE) config

up:
	docker compose -f $(COMPOSE_FILE) up postgres airflow airflow-mcp

down:
	docker compose -f $(COMPOSE_FILE) down

logs-airflow:
	docker compose -f $(COMPOSE_FILE) logs airflow --tail=200

# ─── Site de documentação ──────────────────────────────────────────────────
#
# Virtualenv próprio, com jinja2 e pyyaml apenas. Não precisa de Airflow, dbt
# nem Poetry — é o que permite a documentação ser construída por quem não tem o
# ambiente do pipeline montado, e pelo CI sem VPN.
#
# `docs-collect` usa rede (git, gh); `docs-build` não usa nada. A separação é de
# propósito: o build no CI nunca depende de rede nem de banco, e o diff de uma
# coleta mostra exatamente o que mudou no período.

$(DOCS_PY):
	python3 -m venv $(DOCS_VENV)
	$(DOCS_VENV)/bin/pip install --quiet --upgrade pip
	$(DOCS_VENV)/bin/pip install --quiet -r $(DOCS_DIR)/requirements.txt

docs-setup: $(DOCS_PY)

docs-collect: $(DOCS_PY)
	cd $(DOCS_DIR) && PYTHONPATH=. .venv/bin/python -m tooling.collect

docs-build: $(DOCS_PY)
	cd $(DOCS_DIR) && PYTHONPATH=. .venv/bin/python -m tooling.build

docs-serve: docs-build
	@echo "→ http://localhost:8000"
	cd $(DOCS_DIR)/site && python3 -m http.server 8000

docs-clean:
	rm -rf $(DOCS_DIR)/site $(DOCS_VENV)
