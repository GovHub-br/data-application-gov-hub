"""
common.py: caminhos e utilidades compartilhadas pelo pipeline de documentação.

Nada aqui toca banco, rede ou dbt. Os caminhos abaixo são o único ponto do
projeto que conhece a estrutura de pastas do repositório — se ela mudar, muda
aqui e o resto continua funcionando.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]

DOCS_DIR = ROOT_DIR / "docs-pages"
DBT_DIR = ROOT_DIR / "dbt" / "minc"
MODELS_DIR = DBT_DIR / "models"
DAGS_DIR = ROOT_DIR / "dags"
PLUGINS_DIR = ROOT_DIR / "plugins"

SRC_DIR = DOCS_DIR / "src"
DATA_DIR = SRC_DIR / "_data"
DIAGRAMAS_DIR = SRC_DIR / "_diagramas"
TEMPLATES_DIR = SRC_DIR / "templates"
ASSETS_DIR = SRC_DIR / "assets"
SITE_DIR = DOCS_DIR / "site"

CURADORIA = SRC_DIR / "dominios.yml"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-7s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("docs")


def write_json(nome: str, payload: dict[str, Any]) -> Path:
    """Grava um JSON de acervo em src/_data/<nome>.json.

    O acervo é versionado de propósito: sem ele, o build no CI não reproduz o
    site. O `indent=2` e o `ensure_ascii=False` existem para o diff de uma
    coleta ser legível — é por ele que se enxerga o que mudou no período.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    destino = DATA_DIR / f"{nome}.json"
    destino.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    log.info("acervo %-10s → %s", nome, destino.relative_to(ROOT_DIR))
    return destino


def read_json(nome: str) -> dict[str, Any]:
    """Lê um JSON de acervo. Devolve dict vazio se ainda não existir."""
    origem = DATA_DIR / f"{nome}.json"
    if not origem.exists():
        return {}
    dados: dict[str, Any] = json.loads(origem.read_text(encoding="utf-8"))
    return dados


def run(cmd: list[str], cwd: Path | None = None) -> str:
    """Executa um comando e devolve o stdout, ou string vazia se falhar.

    Não levanta: um coletor que falha não pode derrubar a coleta inteira. Quem
    chama decide o que fazer com a resposta vazia — normalmente, manter o dado
    da coleta anterior.
    """
    try:
        resultado = subprocess.run(
            cmd,
            cwd=cwd or ROOT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        return resultado.stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as erro:
        log.warning("comando falhou (%s): %s", " ".join(cmd[:3]), erro)
        return ""
