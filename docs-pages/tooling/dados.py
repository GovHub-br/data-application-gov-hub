"""
dados.py: junta o acervo coletado com a curadoria escrita à mão.

O acervo (`src/_data/*.json`) diz o que existe. A curadoria (`src/dominios.yml`)
diz o que aquilo significa. Este módulo cruza os dois e entrega ao template um
contexto pronto — para que nenhum template precise calcular nada.

Regra que sustenta o site inteiro: **nenhum número é digitado num template**.
Se o número que você quer não existe aqui, ele se calcula aqui, e o template
consome. Número escrito à mão mente na primeira mudança do repositório.
"""

from __future__ import annotations

from typing import Any

import yaml

from tooling.common import CURADORIA, log, read_json


def _curadoria() -> dict[str, Any]:
    if not CURADORIA.exists():
        raise SystemExit(f"falta a curadoria: {CURADORIA}")
    dados = yaml.safe_load(CURADORIA.read_text(encoding="utf-8")) or {}
    if not dados.get("dominios"):
        raise SystemExit("dominios.yml não declara nenhum domínio")
    return dados


def montar() -> dict[str, Any]:
    dbt = read_json("dbt")
    airflow = read_json("airflow")
    entregas = read_json("entregas")
    curada = _curadoria()

    if not dbt:
        raise SystemExit(
            "acervo vazio — rode `make docs-collect` antes de `make docs-build`"
        )

    modelos = dbt.get("modelos", [])
    por_dominio: dict[str, list[dict[str, Any]]] = {}
    for m in modelos:
        por_dominio.setdefault(m["dominio"], []).append(m)

    dominios: list[dict[str, Any]] = []
    for item in curada["dominios"]:
        slug = item["slug"]
        seus = por_dominio.get(slug, [])
        if not seus and not item.get("sem_modelos"):
            # Falhar aqui é de propósito: slug que não casa é quase sempre erro
            # de digitação, e um domínio vazio passaria despercebido no site.
            raise SystemExit(
                f"domínio '{slug}' não casou nenhum modelo. Confira se a pasta "
                f"dbt/minc/models/{slug}_dbt/ existe, ou declare `sem_modelos: true`."
            )

        camadas: dict[str, list[dict[str, Any]]] = {}
        for m in seus:
            camadas.setdefault(m["camada"], []).append(m)

        golds = sorted(camadas.get("gold", []), key=lambda m: m["nome"])
        dominios.append(
            {
                **item,
                "modelos": sorted(seus, key=lambda m: (m["camada"], m["nome"])),
                "camadas": camadas,
                "golds": golds,
                "metricas": {
                    "modelos": len(seus),
                    "testes": sum(m["testes"] for m in seus),
                    "com_descricao": sum(1 for m in seus if m["descricao"]),
                    "gold": len(golds),
                },
            }
        )

    fora = set(por_dominio) - {d["slug"] for d in curada["dominios"]}
    if fora:
        log.warning(
            "domínios sem curadoria, fora do site: %s — acrescente em dominios.yml",
            ", ".join(sorted(fora)),
        )

    dags = airflow.get("dags", [])
    descritos = sum(1 for m in modelos if m["descricao"])

    metricas = {
        "modelos": len(modelos),
        "dominios": len(dominios),
        "dags": len(dags),
        "fontes": airflow.get("totais", {}).get("fontes", 0),
        "clientes": airflow.get("totais", {}).get("clientes", 0),
        "testes": sum(m["testes"] for m in modelos),
        "tabelas_origem": dbt.get("totais", {}).get("sources", 0),
        "com_descricao": descritos,
        "cobertura_descricao": round(100 * descritos / len(modelos)) if modelos else 0,
        "entregas": entregas.get("totais", {}).get("entregas", 0),
        "prs": entregas.get("totais", {}).get("pull_requests", 0),
        "desde": entregas.get("totais", {}).get("primeiro_commit", ""),
        "ate": entregas.get("totais", {}).get("ultimo_commit", ""),
    }

    return {
        "programa": curada.get("programa", {}),
        "metas": curada.get("metas", []),
        "dominios": dominios,
        "metricas": metricas,
        "dags": dags,
        "por_fonte": airflow.get("por_fonte", {}),
        "clientes": airflow.get("clientes", []),
        "sources": dbt.get("sources", []),
        "entregas": entregas.get("entregas", []),
        "pull_requests": entregas.get("pull_requests", []),
        "por_mes": entregas.get("por_mes", {}),
        "modelos": modelos,
    }
