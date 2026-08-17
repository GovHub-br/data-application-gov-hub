"""
dbt_models.py: inventário e linhagem dos modelos dbt.

Lê a árvore de arquivos, não o `manifest.json`. A convenção de pastas do
repositório — `models/<dominio>_dbt/<camada>/<modelo>.sql` — já carrega domínio
e camada, e os `ref()`/`source()` no SQL dão a linhagem. Assim a coleta roda
offline: sem dbt instalado, sem VPN e sem conexão com o Postgres.

O preço dessa escolha é acoplamento à convenção de pastas. Se alguém reorganizar
`models/`, este coletor para de classificar direito — e é por isso que a
convenção está escrita no CLAUDE.md.

Saída: docs-pages/src/_data/dbt.json
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from tooling.common import MODELS_DIR, log, write_json

RE_REF = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
RE_SOURCE = re.compile(r"""source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""")
RE_MATERIALIZED = re.compile(r"""materialized\s*=\s*['"](\w+)['"]""")

CAMADAS = ("bronze", "silver", "gold", "views")


def _yaml_da_pasta(pasta: Path) -> dict[str, Any]:
    """Lê o schema.yml da pasta. Devolve dict vazio quando não há ou é inválido."""
    schema = pasta / "schema.yml"
    if not schema.exists():
        return {}
    try:
        return yaml.safe_load(schema.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as erro:
        log.warning("schema.yml inválido em %s: %s", pasta, erro)
        return {}


def _metadados(pasta: Path) -> dict[str, dict[str, Any]]:
    """Extrai descrição, colunas descritas e contagem de testes por modelo."""
    dados = _yaml_da_pasta(pasta)
    saida: dict[str, dict[str, Any]] = {}
    for modelo in dados.get("models") or []:
        if not isinstance(modelo, dict) or not modelo.get("name"):
            continue
        colunas = modelo.get("columns") or []
        testes = len(modelo.get("tests") or modelo.get("data_tests") or [])
        for coluna in colunas:
            testes += len(coluna.get("tests") or coluna.get("data_tests") or [])
        saida[modelo["name"]] = {
            "descricao": (modelo.get("description") or "").strip(),
            "colunas": [
                {
                    "nome": c.get("name", ""),
                    "descricao": (c.get("description") or "").strip(),
                }
                for c in colunas
                if isinstance(c, dict)
            ],
            "testes": testes,
        }
    return saida


def _classificar(relativo: Path) -> tuple[str, str]:
    """Devolve (domínio, camada) a partir do caminho relativo a models/."""
    partes = relativo.parts[:-1]
    if not partes:
        return "geral", "outros"
    dominio = partes[0].removesuffix("_dbt")
    camada = next((p for p in partes if p in CAMADAS), "outros")
    return dominio, camada


def coletar() -> dict[str, Any]:
    if not MODELS_DIR.exists():
        log.warning("pasta de modelos não encontrada: %s", MODELS_DIR)
        vazio: dict[str, Any] = {
            "modelos": [],
            "dominios": {},
            "sources": [],
            "totais": {},
        }
        write_json("dbt", vazio)
        return vazio

    modelos: list[dict[str, Any]] = []
    cache_meta: dict[Path, dict[str, dict[str, Any]]] = {}

    for arquivo in sorted(MODELS_DIR.rglob("*.sql")):
        relativo = arquivo.relative_to(MODELS_DIR)
        dominio, camada = _classificar(relativo)
        sql = arquivo.read_text(encoding="utf-8", errors="replace")

        if arquivo.parent not in cache_meta:
            cache_meta[arquivo.parent] = _metadados(arquivo.parent)
        meta = cache_meta[arquivo.parent].get(arquivo.stem, {})

        materializacao = RE_MATERIALIZED.search(sql)
        modelos.append(
            {
                "nome": arquivo.stem,
                "dominio": dominio,
                "camada": camada,
                "caminho": str(arquivo.relative_to(MODELS_DIR.parents[2])),
                "descricao": meta.get("descricao", ""),
                "colunas": meta.get("colunas", []),
                "colunas_descritas": sum(
                    1 for c in meta.get("colunas", []) if c["descricao"]
                ),
                "testes": meta.get("testes", 0),
                "materializacao": materializacao.group(1) if materializacao else "",
                "depende_de": sorted(set(RE_REF.findall(sql))),
                "sources": sorted({f"{a}.{b}" for a, b in RE_SOURCE.findall(sql)}),
                "linhas_sql": sql.count("\n") + 1,
            }
        )

    # sources declaradas — a lista de origem, que o site mostra como "de onde vem"
    sources: list[dict[str, Any]] = []
    arquivo_sources = MODELS_DIR / "sources.yml"
    if arquivo_sources.exists():
        try:
            dados = yaml.safe_load(arquivo_sources.read_text(encoding="utf-8")) or {}
            for grupo in dados.get("sources") or []:
                sources.append(
                    {
                        "nome": grupo.get("name", ""),
                        "schema": grupo.get("schema", grupo.get("name", "")),
                        "descricao": (grupo.get("description") or "").strip(),
                        "tabelas": [
                            {
                                "nome": t.get("name", ""),
                                "descricao": (t.get("description") or "").strip(),
                            }
                            for t in grupo.get("tables") or []
                        ],
                    }
                )
        except yaml.YAMLError as erro:
            log.warning("sources.yml inválido: %s", erro)

    dominios: dict[str, Any] = {}
    for m in modelos:
        d = dominios.setdefault(m["dominio"], {"modelos": 0, "testes": 0, "camadas": {}})
        d["modelos"] += 1
        d["testes"] += m["testes"]
        d["camadas"][m["camada"]] = d["camadas"].get(m["camada"], 0) + 1

    payload = {
        "modelos": modelos,
        "dominios": dominios,
        "sources": sources,
        "totais": {
            "modelos": len(modelos),
            "testes": sum(m["testes"] for m in modelos),
            "com_descricao": sum(1 for m in modelos if m["descricao"]),
            "sources": sum(len(s["tabelas"]) for s in sources),
        },
    }
    write_json("dbt", payload)
    return payload
