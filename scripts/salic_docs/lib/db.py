"""Conexão somente-leitura ao data warehouse (schema bronze) para o
pipeline de documentação do SALIC.

Lê as credenciais do .env na raiz do repo (DB_DW_HOST/PORT/USER/PASS), mas
conecta no banco `minc` (não no `DB_DW_DATABASE` do .env, que é usado pelo
dbt e aponta para outro banco/schema).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

REPO_ROOT = Path(__file__).resolve().parents[3]
STATEMENT_TIMEOUT_MS = 5 * 60 * 1000  # 5 min por query, defensivo


def _load_env() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip()


def get_connection(dbname: str = "minc") -> Any:
    """Abre uma conexão somente-leitura ao data warehouse."""
    _load_env()
    conn = psycopg2.connect(
        host=os.environ["DB_DW_HOST"],
        port=os.environ["DB_DW_PORT"],
        user=os.environ["DB_DW_USER"],
        password=os.environ["DB_DW_PASS"],
        dbname=dbname,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def dict_cursor(conn: Any) -> Any:
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
